use std::collections::HashMap;
use std::io;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

use tracing::error;

use crate::error::Result;
use crate::error::ServerError;
use crate::parse_request;
use crate::serialize_response;
use crate::RawResponse;
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

/// A basic in-memory database server.
pub struct Server {
    listener: TcpListener,
    db: Arc<Mutex<DB>>,
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
            db: Arc::new(Mutex::new(HashMap::with_capacity(1024))),
        }
    }

    /// Runs the server.
    pub fn run(&self) {
        for stream in self.listener.incoming() {
            let db_clone = self.db.clone();
            thread::spawn(move || match stream {
                Ok(mut stream) => {
                    let _ = handle_connection(&mut stream, db_clone);
                }
                Err(e) => {
                    error!("Could not read incoming stream: {:?}", e);
                }
            });
        }
    }
}

fn handle_connection<RW>(
    stream: &mut RW,
    db: Arc<Mutex<DB>>,
) -> Result<()>
where
    RW: Read + Write + ?Sized,
{
    let mut buffer = vec![0; INITIAL_BUFFER_SIZE];
    let mut cursor = 0;

    loop {
        if let Some((request, n_parsed_bytes)) = parse_request(&buffer[0..cursor]).unwrap() {
            let mut db_lock = db.try_lock().map_err(|_| ServerError::DbLock)?;
            let response = match request {
                Request::Get(key) => {
                    let v = db_lock.get(key);
                    RawResponse::Get(v.map(|s| s.as_str()))
                }
                Request::Set { key, value } => {
                    db_lock.insert(key.to_string(), value.to_string());
                    RawResponse::Set
                }
                Request::Delete(key) => {
                    db_lock.remove(key);
                    RawResponse::Delete
                }
                Request::Flush => {
                    db_lock.clear();
                    RawResponse::Flush
                }
            };
            send_response(stream, response).map_err(|_| ServerError::IO)?;
            drop(db_lock);

            if n_parsed_bytes <= cursor {
                // We parsed less data than there is in the buffer.
                // Move the remaining bytes in the buffer that were not parsed yet to the front.
                // This way we don't have to resize the buffer more than necessary when more data is sent.
                // Since we have a maximum buffer size, this prevents running into it for repeated sends.
                buffer.copy_within(n_parsed_bytes..cursor, 0);
                cursor -= n_parsed_bytes;
            }
            continue;
        }

        if buffer.len() >= MAX_BUFFER_SIZE {
            return Err(ServerError::TooMuchData.into());
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
                Err(ServerError::ConnectionResetByPeer.into())
            };
        } else {
            cursor += n_bytes_read;
        }
    }
}

fn send_response<W>(
    stream: &mut W,
    response: RawResponse,
) -> io::Result<()>
where
    W: Write + ?Sized,
{
    let bytes = serialize_response(response);
    stream.write_all(&bytes)?;
    stream.flush()
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::sync::Arc;
    use std::sync::Mutex;

    use super::handle_connection;
    use super::ServerError;
    use super::INITIAL_BUFFER_SIZE;
    use super::MAX_BUFFER_SIZE;
    use crate::error::Error;

    #[test]
    fn test_read_request_single_request_in_stream() {
        let db: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
        let raw_data = vec![2, 0, 0, 0, 3, 97, 98, 99, 0, 0, 0, 3, 103, 104, 105];
        assert!(raw_data.len() < INITIAL_BUFFER_SIZE);
        assert!(raw_data.len() < 2 * MAX_BUFFER_SIZE);
        let mut stream = Cursor::new(raw_data);
        let _ = handle_connection(&mut stream, db.clone());
        assert_eq!(db.lock().unwrap().get("abc").unwrap(), "ghi");
    }

    #[test]
    fn test_read_request_multiple_requests_in_stream() {
        let db: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
        // Two concatenated requests
        let raw_data = vec![
            2, 0, 0, 0, 3, 97, 98, 99, 0, 0, 0, 3, 103, 104, 105, 2, 0, 0, 0, 3, 49, 50, 51, 0, 0,
            0, 3, 52, 53, 54,
        ];
        assert!(raw_data.len() < INITIAL_BUFFER_SIZE);
        assert!(raw_data.len() < 2 * MAX_BUFFER_SIZE);
        let mut stream = Cursor::new(raw_data);
        let _ = handle_connection(&mut stream, db.clone());
        assert_eq!(db.lock().unwrap().get("abc").unwrap(), "ghi");
        assert_eq!(db.lock().unwrap().get("123").unwrap(), "456");
    }

    #[test]
    fn test_read_single_request_larger_than_initial_buffer() {
        let db: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
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
        let _ = handle_connection(&mut stream, db.clone());
        assert_eq!(
            db.lock().unwrap().get("123").unwrap(),
            "This is some longer text that did not fit into a single TCP request"
        );
    }

    #[test]
    fn test_max_buffer_resize_is_respected() {
        let db: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
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
        assert!(matches!(
            handle_connection(&mut stream, db).err(),
            Some(Error::Server(ServerError::TooMuchData))
        ));
    }
}
