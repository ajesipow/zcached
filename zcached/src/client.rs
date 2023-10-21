use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::net::ToSocketAddrs;

use crate::serialize_request;
use crate::Request;

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
        let mut response_bytes = [0; 2];
        let _ = self.stream.read(&mut response_bytes).unwrap();
    }
}
