// SPDX-License-Identifier: MIT
// Copyright 2025. Thomas Bertschinger

use clap::Parser;

use std::io::{self, Read, Write};
use std::net::TcpStream;

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value_t = 8)]
    num_threads: usize,

    #[arg(short, long)]
    port: u16,
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    let join_handles =
        (0..args.num_threads).map(|_| std::thread::spawn(move || test_client(args.port)));

    for handle in join_handles {
        handle.join().unwrap();
    }

    Ok(())
}

fn test_client(port: u16) {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    let mut buf: Vec<u8> = vec![0; 4096];
    for _ in 0..8 {
        stream.write_all(b"hey there").unwrap();
        let n_read = stream.read(&mut buf).unwrap();
        assert_eq!(&buf[..n_read], b"hey there");
    }
}
