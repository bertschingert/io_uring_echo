// SPDX-License-Identifier: MIT
// Copyright 2025. Thomas Bertschinger

use io_uring::{IoUring, cqueue, opcode, types};

use std::io;
use std::net::TcpListener;
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicU16, Ordering};

const BUF_SIZE: u32 = 128;
const GROUP_ID: u16 = 17;

fn main() -> io::Result<()> {
    let mut ring = IoUring::new(8)?;

    let mut buffer_map = BufferMap::new(4, &mut ring);

    let listener = TcpListener::bind("127.0.0.1:0")?;
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
        eprintln!("{cqe:?}");

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
                s.handle(&mut buffer_map);
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
}

impl Send {
    fn new(group_id: u16, buf: Vec<u8>) -> Self {
        Self { group_id, buf }
    }

    fn buf_ptr(&self) -> *const u8 {
        self.buf.as_ptr()
    }

    fn handle(self, buffer_map: &mut BufferMap) {
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
            eprintln!("error: {res}");
        }
        // Connection is done:
        0 => {
            let _ = unsafe { libc::close(fd) };
            return Ok(());
        }
        // Got data:
        amount => {
            let fd = types::Fd(fd);
            let group_id: u16 = cqueue::buffer_select(cqe.flags()).unwrap();

            // SAFETY: the group_id was just gotten from the completion
            let buf = unsafe { buffer_map.take_buf(group_id) };

            let user_data = Send::new(group_id, buf);
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

    /// The tail of the ring, including unpublished buffers. This is the index of the next unused
    /// slot.
    private_tail: u16,

    group_id: u16,

    buffers: Vec<Vec<u8>>,
}

impl BufferMap {
    pub fn new(num_entries: u16, ring: &mut IoUring) -> Self {
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
            buffer_map.buffers.push(vec![0; BUF_SIZE as usize]);
            let addr: *mut u8 = buffer_map.buffers[i as usize].as_ptr() as *mut u8;
            buffer_map.push_buf(addr, BUF_SIZE, i);
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
        self.push_buf(buf.as_mut_ptr(), BUF_SIZE, id);
        self.buffers[id as usize] = buf;
        self.publish_bufs();
    }
}
