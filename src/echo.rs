// SPDX-License-Identifier: MIT
// Copyright 2025. Thomas Bertschinger

use clap::Parser;
use io_uring::{IoUring, cqueue, opcode, types};
use log::*;

use std::fmt;
use std::io;
use std::net::TcpListener;
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicU16, Ordering};

const GROUP_ID: u16 = 17;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 8)]
    num_bufs: u16,

    #[arg(long, default_value_t = 1024)]
    buf_size: u32,

    #[arg(long, default_value_t = 8)]
    entries_in_ring: u32,

    #[arg(short, long, default_value_t = 0)]
    port: u16,
}

fn main() -> io::Result<()> {
    env_logger::init();

    let args = Args::parse();

    let mut ring = IoUring::new(args.entries_in_ring)?;

    let mut buffer_map = BufferMap::new(&args, &mut ring);

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port))?;
    println!("Listening on {}", listener.local_addr().unwrap());
    let listen_fd = types::Fd(listener.as_raw_fd());

    let user_data = Box::new(Operation::Accept);
    let accept_submission = opcode::AcceptMulti::new(listen_fd)
        .build()
        .user_data(operation_to_u64(user_data));

    unsafe {
        ring.submission()
            .push(&accept_submission)
            .expect("queue is full");
    }

    ring.submit_and_wait(1)?;

    loop {
        let cqe = ring.completion().next().expect("completion queue is empty");
        let op: Box<Operation> = unsafe {
            Box::from_raw(
                std::ptr::with_exposed_provenance::<Operation>(cqe.user_data() as usize) as *mut _,
            )
        };
        debug!("{op}: {cqe:?}");

        match *op {
            Operation::Accept => {
                handle_accept(&cqe, &mut ring);
                if !cqueue::more(cqe.flags()) {
                    resubmit_accept(listen_fd, &mut ring, op)?;
                } else {
                    // Need to leak the user_data again since this is a multi-shot accept.
                    let _ = Box::into_raw(op);
                }
            }
            Operation::Recv(ref r) => {
                let fd = r.fd;
                handle_receive(op, fd, cqe, &mut ring, &mut buffer_map)?;
            }
            Operation::Send(s) => {
                s.handle(cqe, &mut buffer_map);
            }
        }

        ring.submit_and_wait(1)?;
    }
}

#[derive(Debug)]
enum Operation {
    Accept,
    Recv(Receive),
    Send(Send),
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Accept => write!(f, "Accept"),
            Self::Recv(r) => write!(f, "Receive on FD {}", r.fd),
            Self::Send(s) => write!(f, "Send: data was {} bytes", s.len),
        }
    }
}

/// Temporarily "leak" the Operation so that the kernel side can take ownership of it until the
/// completion is processed.
///
/// Exposes provenance so that a pointer to the Operation can be acquired with the proper
/// provenance when processing the completion that holds this data.
fn operation_to_u64(op: Box<Operation>) -> u64 {
    Box::into_raw(op).expose_provenance() as u64
}

/// Leak an operation without the need to expose its provenance, because it was already exposed.
/// Useful when re-submitting multishot requests with the same user_data from the original
/// submission.
fn operation_to_u64_noexpose(op: Box<Operation>) -> u64 {
    Box::into_raw(op) as u64
}

#[derive(Debug)]
struct Receive {
    fd: i32,
}

impl Receive {
    fn new(fd: i32) -> Self {
        Self { fd }
    }
}

#[derive(Debug)]
struct Send {
    group_id: u16,
    buf: Vec<u8>,
    len: usize,
}

impl Send {
    fn new(group_id: u16, buf: Vec<u8>, len: usize) -> Self {
        Self { group_id, buf, len }
    }

    fn buf_ptr(&self) -> *const u8 {
        self.buf.as_ptr()
    }

    fn handle(self, cqe: cqueue::Entry, buffer_map: &mut BufferMap) {
        match cqe.result() {
            res if res < 0 => {
                error!("Error in send: {res}");
            }
            res if (res as usize) < self.len => {
                // TODO: handle short send by resubmitting...
                warn!("Short send: only sent {} out of {} bytes", res, self.len);
            }
            _ => {}
        }

        // SAFETY: the buf and group_id came from a completed Recv.
        unsafe {
            buffer_map.resubmit_buf(self.buf, self.group_id);
        }
    }
}

fn handle_accept(cqe: &cqueue::Entry, ring: &mut IoUring) {
    // need to check fd for error as well as flags
    let fd = cqe.result();

    if fd < 0 {
        todo!("handle error in accept");
    }

    let user_data = Receive::new(fd);
    let user_data = Box::new(Operation::Recv(user_data));

    let submission = opcode::RecvMulti::new(types::Fd(fd), GROUP_ID)
        .build()
        .user_data(operation_to_u64(user_data));

    unsafe {
        ring.submission().push(&submission).expect("queue is full");
    }
}

fn resubmit_accept(
    listen_fd: types::Fd,
    ring: &mut IoUring,
    user_data: Box<Operation>,
) -> io::Result<()> {
    warn!("Multishot accept did not set MORE flag; resubmitting");
    let accept_submission = opcode::AcceptMulti::new(listen_fd)
        .build()
        .user_data(operation_to_u64_noexpose(user_data));

    unsafe {
        ring.submission()
            .push(&accept_submission)
            .expect("queue is full");
    }

    ring.submit_and_wait(1)?;

    Ok(())
}

fn handle_receive(
    op: Box<Operation>,
    fd: i32,
    cqe: cqueue::Entry,
    ring: &mut IoUring,
    buffer_map: &mut BufferMap,
) -> io::Result<()> {
    match cqe.result() {
        // Error:
        res if res < 0 => {
            warn!("error: {res}");
        }
        // Connection is done:
        0 => {
            trace!("Closing connection on fd {fd}");
            let _ = unsafe { libc::close(fd) };
            return Ok(());
        }
        // Got data:
        amount => {
            let fd = types::Fd(fd);
            let group_id: u16 = cqueue::buffer_select(cqe.flags()).unwrap();

            // SAFETY: the group_id was just gotten from the completion
            let buf = unsafe { buffer_map.take_buf(group_id) };

            let user_data = Send::new(group_id, buf, amount as usize);
            let send = opcode::Send::new(fd, user_data.buf_ptr(), amount as u32);
            let user_data = Box::new(Operation::Send(user_data));
            let send = send.build().user_data(operation_to_u64(user_data));

            unsafe {
                ring.submission().push(&send).expect("queue is full");
            }
        }
    }

    if !cqueue::more(cqe.flags()) {
        // resubmit receive
        resubmit_receive(fd, ring, op)?;
    } else {
        // Need to leak the user_data again since this is a multi-shot receive.
        let _ = Box::into_raw(op);
    }

    Ok(())
}

fn resubmit_receive(fd: i32, ring: &mut IoUring, user_data: Box<Operation>) -> io::Result<()> {
    warn!("Multishot receive did not set MORE flag; resubmitting");
    let submission = opcode::RecvMulti::new(types::Fd(fd), GROUP_ID)
        .build()
        .user_data(operation_to_u64_noexpose(user_data));

    unsafe {
        ring.submission().push(&submission).expect("queue is full");
    }

    Ok(())
}

/// A memory map of a ring of buffer descriptors shared with the kernel.
struct BufferMap {
    /// Pointer to the memory shared with the kernel which holds the `struct io_uring_buf`s. Its
    /// size is `sizeof(struct io_uring_buf) * num_entries`.
    addr: *mut libc::c_void,

    /// The number of entries in the shared buffer ring.
    num_entries: u16,

    /// The size of each buffer.
    buf_size: u32,

    /// The tail of the ring, including unpublished buffers. This is the index of the next unused
    /// slot.
    private_tail: u16,

    group_id: u16,

    buffers: Vec<Vec<u8>>,
}

impl BufferMap {
    pub fn new(args: &Args, ring: &mut IoUring) -> Self {
        let num_entries = args.num_bufs;
        assert!(num_entries < u16::MAX);
        assert!(num_entries & (num_entries - 1) == 0); // must be a power of 2

        let len = (num_entries as usize) * std::mem::size_of::<types::BufRingEntry>();
        let addr = unsafe {
            match libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED | libc::MAP_POPULATE,
                -1,
                0,
            ) {
                libc::MAP_FAILED => panic!("mmap: {:?}", io::Error::last_os_error()),
                addr => addr,
            }
        };

        let mut buffer_map = Self {
            addr,
            num_entries,
            buf_size: args.buf_size,
            private_tail: 0,
            group_id: GROUP_ID,
            buffers: Vec::new(),
        };

        unsafe {
            ring.submitter()
                .register_buf_ring(buffer_map.addr as u64, num_entries, buffer_map.group_id)
                .unwrap();
        };

        for i in 0..num_entries {
            buffer_map.buffers.push(vec![0; args.buf_size as usize]);
            let addr: *mut u8 = buffer_map.buffers[i as usize].as_ptr() as *mut u8;
            buffer_map.push_buf(addr, args.buf_size, i);
        }

        buffer_map.publish_bufs();

        buffer_map
    }

    /// Add a buffer described by `addr`, `len`, and `bid` to the buffer map.
    fn push_buf(&mut self, addr: *mut u8, len: u32, bid: u16) {
        let entries = self.addr as *mut types::BufRingEntry;
        let index: u16 = self.private_tail & self.mask();

        // SAFETY: ...
        let entry = unsafe { entries.add(index as usize) };
        // SAFETY: ...
        let entry = unsafe { &mut *entry };

        entry.set_addr(addr as u64);
        entry.set_len(len);
        entry.set_bid(bid);

        self.private_tail = self.private_tail.wrapping_add(1);
    }

    /// Advance the shared tail to publish new buffers to the kernel.
    fn publish_bufs(&mut self) {
        let base_entry = self.addr as *const types::BufRingEntry;

        // SAFETY: ...
        let shared_tail = unsafe { types::BufRingEntry::tail(base_entry) };
        let shared_tail = shared_tail as *const AtomicU16;

        // SAFETY: ...
        unsafe { (*shared_tail).store(self.private_tail, Ordering::Release) };
    }

    fn mask(&self) -> u16 {
        self.num_entries - 1
    }

    /// SAFETY:
    ///
    /// The caller must ensure that the buffer ID is one returned by the kernel in a completion
    /// event, and which has not been re-submitted to the kernel. Otherwise, reading the buffer can
    /// result in a data race with the kernel writing to that buffer.
    pub unsafe fn take_buf(&mut self, id: u16) -> Vec<u8> {
        std::mem::take(&mut self.buffers[id as usize])
    }

    /// SAFETY:
    ///
    /// Has the same requirements as take_buf()
    pub unsafe fn resubmit_buf(&mut self, mut buf: Vec<u8>, id: u16) {
        self.push_buf(buf.as_mut_ptr(), self.buf_size, id);
        self.buffers[id as usize] = buf;
        self.publish_bufs();
    }
}
