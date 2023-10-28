use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::net::ToSocketAddrs;

use crate::parse_response;
use crate::serialize_request;
use crate::Request;

#[cfg(not(test))]
const INITIAL_BUFFER_SIZE: usize = 4096;
#[cfg(test)]
const INITIAL_BUFFER_SIZE: usize = 32;

// The buffer can be resized as long as it is < MAX_BUFFER_SIZE.
// If the client requests too much data, we reject the request.
#[cfg(not(test))]
const MAX_BUFFER_SIZE: usize = 1024 * 1024; // 1MB
#[cfg(test)]
const MAX_BUFFER_SIZE: usize = 93;

pub struct Client {
    stream: TcpStream,
}

impl Client {
    pub fn connect<A>(addr: A) -> Self
    where
        A: ToSocketAddrs,
    {
        // TODO handle Err
        Self {
            stream: TcpStream::connect(addr).unwrap(),
        }
    }

    pub fn get(
        &mut self,
        key: &str,
    ) {
        let request = Request::Get(key);
        self.send_request(request);
        self.receive_response().unwrap();
    }

    pub fn set(
        &mut self,
        key: &str,
        value: &str,
    ) {
        let request = Request::Set { key, value };
        self.send_request(request);
    }

    pub fn delete(
        &mut self,
        key: &str,
    ) {
        let request = Request::Delete(key);
        self.send_request(request);
    }

    pub fn flush(&mut self) {
        let request = Request::Flush;
        self.send_request(request);
    }

    fn send_request(
        &mut self,
        request: Request,
    ) {
        let request_bytes = serialize_request(request);
        self.stream.write_all(&request_bytes).unwrap();
        self.stream.flush().unwrap();
    }

    fn receive_response(&mut self) -> Result<(), ()> {
        let mut buffer = vec![0; INITIAL_BUFFER_SIZE];
        loop {
            let bytes_read = self.stream.read(&mut buffer).map_err(|_| ())?;
            if let Some(response) = parse_response(&buffer) {
                println!("response: {:?}", response);
                return Ok(());
            }
            if bytes_read == 0 {
                // Connection reset by peer:
                // No more bytes were read but we still could not parse the response
                return Err(());
            }
            if buffer.len() == buffer.capacity() {
                buffer.resize(buffer.capacity() * 2, 0);
            }
            if buffer.len() >= MAX_BUFFER_SIZE {
                return Err(());
            }
        }
    }
}
