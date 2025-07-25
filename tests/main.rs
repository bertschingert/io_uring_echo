use std::fs::File;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;

const PORT: &str = "8989";

struct Server {
    process: std::process::Child,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.process.kill();
    }
}

#[test]
fn the_test() {
    let _server = Server {
        process: Command::new(env!("CARGO_BIN_EXE_echo"))
            .args(["-p", PORT])
            .spawn()
            .unwrap(),
    };

    let mut stream = client_connect();

    let mut data_source = File::open("/dev/urandom").unwrap();

    for i in 0..15 {
        let mut send_buf = vec![0; usize::pow(2, i)];
        let mut recv_buf = vec![1; usize::pow(2, i)];

        data_source.read_exact(&mut send_buf).unwrap();
        stream.write_all(&send_buf).unwrap();

        stream.read_exact(&mut recv_buf).unwrap();

        assert_eq!(send_buf, recv_buf);
    }
}

fn client_connect() -> TcpStream {
    for _ in 0..20 {
        match TcpStream::connect(format!("127.0.0.1:{PORT}")) {
            Ok(stream) => return stream,
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(10)),
        }
    }

    panic!("Should have been able to connect to server by now.");
}
