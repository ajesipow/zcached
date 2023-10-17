use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::net::ToSocketAddrs;
use std::str::from_utf8;

use zcached::serialize_request;
use zcached::Request;

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
        let n_bytes_read = self.stream.read(&mut response_bytes).unwrap();
        println!("Read {} bytes", n_bytes_read);
        println!("response: {}", from_utf8(&response_bytes).unwrap());
    }
}

fn main() {
    let mut client = Client::connect("127.0.0.1:7891");

    client.get("abc");
    client.set("abc", "ghi");
    client.set("123", "This is some longer text that did not fit into a single TCP request. This is an even longer text for testing the resizing of the buffer. There is even more data in here now. Look at that");
}
