use std::collections::HashMap;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::ToSocketAddrs;

use crate::parse_request;
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

type DB = HashMap<String, String>;

#[derive(Debug)]
enum Response<'a> {
    Get(Option<&'a String>),
    Set,
    Delete,
    Flush,
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
enum ServerError {
    TooMuchData,
    ConnectionResetByPeer,
}

/// A basic in-memory database server.
pub struct Server {
    listener: TcpListener,
    db: DB,
}

impl Server {
    /// Creates a new server.
    /// Panics when it cannot bind to `addr`.
    pub fn new<A>(addr: A) -> Self
    where
        A: ToSocketAddrs,
    {
        let listener = TcpListener::bind(addr).expect("to be able to bind to address");
        Self {
            listener,
            db: HashMap::with_capacity(1024),
        }
    }

    /// Runs the server.
    pub fn run(mut self) {
        for stream in self.listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    let _ = handle_connection(&mut stream, &mut self.db);
                }
                Err(e) => {
                    println!("Could not read incoming stream: {:?}", e);
                }
            }
        }
    }
}

fn handle_connection<RW>(
    stream: &mut RW,
    db: &mut DB,
) -> Result<(), ServerError>
where
    RW: Read + Write + ?Sized,
{
    let mut buffer = vec![0; INITIAL_BUFFER_SIZE];
    let mut cursor = 0;

    loop {
        if let Some((request, n_parsed_bytes)) = parse_request(&buffer[0..cursor]) {
            let _response = match request {
                Request::Get(key) => {
                    let v = db.get(key);
                    Response::Get(v)
                }
                Request::Set { key, value } => {
                    db.insert(key.to_string(), value.to_string());
                    Response::Set
                }
                Request::Delete(key) => {
                    db.remove(key);
                    Response::Delete
                }
                Request::Flush => {
                    db.clear();
                    Response::Flush
                }
            };
            if n_parsed_bytes <= cursor {
                // We parsed less data than there is in the buffer.
                // Move the remaining bytes in the buffer that were not parsed yet to the front.
                // This way we don't have to resize the buffer more than necessary when more data is sent.
                // Since we have a maximum buffer size, this prevents running into it for repeated sends.
                buffer.copy_within(n_parsed_bytes..cursor, 0);
                cursor -= n_parsed_bytes;
            }
            send_response(stream);
            continue;
        }

        if buffer.len() >= MAX_BUFFER_SIZE {
            return Err(ServerError::TooMuchData);
        }

        if buffer.len() == cursor {
            buffer.resize(buffer.capacity() * 2, 0);
        }

        // Handle the case where there is still a frame in the buffer
        let read_end = buffer.capacity();
        let n_bytes_read = stream.read(&mut buffer[cursor..read_end]).unwrap();
        if n_bytes_read == 0 {
            return if cursor == 0 {
                Ok(())
            } else {
                Err(ServerError::ConnectionResetByPeer)
            };
        } else {
            cursor += n_bytes_read;
        }
    }
}

fn send_response<W>(stream: &mut W)
where
    W: Write + ?Sized,
{
    stream.write_all("OK".as_bytes()).unwrap();
    stream.flush().unwrap();
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::io::Cursor;

    use super::handle_connection;
    use super::ServerError;
    use super::INITIAL_BUFFER_SIZE;
    use super::MAX_BUFFER_SIZE;

    #[test]
    fn test_read_request_single_request_in_stream() {
        let mut db: HashMap<String, String> = HashMap::new();
        let raw_data = vec![2, 0, 0, 0, 3, 97, 98, 99, 0, 0, 0, 3, 103, 104, 105];
        assert!(raw_data.len() < INITIAL_BUFFER_SIZE);
        assert!(raw_data.len() < 2 * MAX_BUFFER_SIZE);
        let mut stream = Cursor::new(raw_data);
        let _ = handle_connection(&mut stream, &mut db);
        assert_eq!(db.get("abc").unwrap(), "ghi");
    }

    #[test]
    fn test_read_request_multiple_requests_in_stream() {
        let mut db: HashMap<String, String> = HashMap::new();
        // Two concatenated requests
        let raw_data = vec![
            2, 0, 0, 0, 3, 97, 98, 99, 0, 0, 0, 3, 103, 104, 105, 2, 0, 0, 0, 3, 49, 50, 51, 0, 0,
            0, 3, 52, 53, 54,
        ];
        assert!(raw_data.len() < INITIAL_BUFFER_SIZE);
        assert!(raw_data.len() < 2 * MAX_BUFFER_SIZE);
        let mut stream = Cursor::new(raw_data);
        let _ = handle_connection(&mut stream, &mut db);
        assert_eq!(db.get("abc").unwrap(), "ghi");
        assert_eq!(db.get("123").unwrap(), "456");
    }

    #[test]
    fn test_read_single_request_larger_than_initial_buffer() {
        let mut db: HashMap<String, String> = HashMap::new();
        // Two concatenated requests
        let raw_data = vec![
            2, 0, 0, 0, 3, 49, 50, 51, 0, 0, 0, 67, 84, 104, 105, 115, 32, 105, 115, 32, 115, 111,
            109, 101, 32, 108, 111, 110, 103, 101, 114, 32, 116, 101, 120, 116, 32, 116, 104, 97,
            116, 32, 100, 105, 100, 32, 110, 111, 116, 32, 102, 105, 116, 32, 105, 110, 116, 111,
            32, 97, 32, 115, 105, 110, 103, 108, 101, 32, 84, 67, 80, 32, 114, 101, 113, 117, 101,
            115, 116,
        ];
        assert!(raw_data.len() > INITIAL_BUFFER_SIZE);
        assert!(raw_data.len() < 2 * MAX_BUFFER_SIZE);
        let mut stream = Cursor::new(raw_data);
        let _ = handle_connection(&mut stream, &mut db);
        assert_eq!(
            db.get("123").unwrap(),
            "This is some longer text that did not fit into a single TCP request"
        );
    }

    #[test]
    fn test_max_buffer_resize_is_respected() {
        let mut db: HashMap<String, String> = HashMap::new();
        // Two concatenated requests
        let raw_data = vec![
            2, 0, 0, 0, 3, 49, 50, 51, 0, 0, 0, 186, 84, 104, 105, 115, 32, 105, 115, 32, 115, 111,
            109, 101, 32, 108, 111, 110, 103, 101, 114, 32, 116, 101, 120, 116, 32, 116, 104, 97,
            116, 32, 100, 105, 100, 32, 110, 111, 116, 32, 102, 105, 116, 32, 105, 110, 116, 111,
            32, 97, 32, 115, 105, 110, 103, 108, 101, 32, 84, 67, 80, 32, 114, 101, 113, 117, 101,
            115, 116, 46, 32, 84, 104, 105, 115, 32, 105, 115, 32, 97, 110, 32, 101, 118, 101, 110,
            32, 108, 111, 110, 103, 101, 114, 32, 116, 101, 120, 116, 32, 102, 111, 114, 32, 116,
            101, 115, 116, 105, 110, 103, 32, 116, 104, 101, 32, 114, 101, 115, 105, 122, 105, 110,
            103, 32, 111, 102, 32, 116, 104, 101, 32, 98, 117, 102, 102, 101, 114, 46, 32, 84, 104,
            101, 114, 101, 32, 105, 115, 32, 101, 118, 101, 110, 32, 109, 111, 114, 101, 32, 100,
            97, 116, 97, 32, 105, 110, 32, 104, 101, 114, 101, 32, 110, 111, 119, 46, 32, 76, 111,
            111, 107, 32, 97, 116, 32, 116, 104, 97, 116,
        ];
        assert!(raw_data.len() > INITIAL_BUFFER_SIZE);
        assert!(
            raw_data.len() > 2 * MAX_BUFFER_SIZE,
            "data len is {}",
            raw_data.len()
        );
        let mut stream = Cursor::new(raw_data);
        assert_eq!(
            handle_connection(&mut stream, &mut db).err(),
            Some(ServerError::TooMuchData)
        );
    }
}
