use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::net::ToSocketAddrs;

use crate::error::ClientError;
use crate::error::Result;
use crate::parse_response;
use crate::serialize_request;
use crate::Request;

pub struct Client {
    stream: TcpStream,
    init_buffer_size: usize,
    // The buffer can be resized as long as it is < max_buffer_size.
    // If the server sends too much data, we reject the response.
    max_buffer_size: usize,
}

impl Client {
    pub fn connect<A>(addr: A) -> Self
    where
        A: ToSocketAddrs,
    {
        Self {
            stream: TcpStream::connect(addr).unwrap(),
            init_buffer_size: 4096,
            max_buffer_size: 1024 * 1024,
        }
    }

    pub fn connect_with_max_buffer_size<A>(
        addr: A,
        max_buffer_size: usize,
    ) -> Self
    where
        A: ToSocketAddrs,
    {
        Self {
            stream: TcpStream::connect(addr).unwrap(),
            init_buffer_size: 4096,
            max_buffer_size,
        }
    }

    pub fn get(
        &mut self,
        key: &str,
        // TODO return proper response
    ) -> Result<()> {
        let request = Request::Get(key);
        self.send_request(request);
        receive_response(
            &mut self.stream,
            self.init_buffer_size,
            self.max_buffer_size,
        )
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
}

fn receive_response<R: Read>(
    stream: &mut R,
    init_buffer_size: usize,
    max_buffer_size: usize,
) -> Result<()> {
    let mut buffer = vec![0; init_buffer_size];
    loop {
        let bytes_read = stream.read(&mut buffer)?;
        if let Some(response) = parse_response(&buffer)? {
            println!("response: {:?}", response);
            return Ok(());
        }
        if bytes_read == 0 {
            // Connection reset by peer:
            // No more bytes were read but we still could not parse the response
            return Err(ClientError::ConnectionResetByPeer.into());
        }
        if buffer.len() == buffer.capacity() {
            buffer.resize(buffer.capacity() * 2, 0);
        }
        if buffer.len() >= max_buffer_size {
            return Err(ClientError::TooMuchData.into());
        }
    }
}
