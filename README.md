# io_uring Echo server

This is an example server that simply echoes back any data it receives.

It uses `io_uring` with multishot accept and receive, and ring-mapped provided buffers.

## Usage

```bash
io_uring_echo $ cargo run --bin echo
# ...
Listening on 127.0.0.1:35931


# in another terminal:
$ nc localhost 35931
# Send:
This is a test
# Receive:
This is a test
```
