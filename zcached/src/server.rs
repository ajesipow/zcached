use std::io;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::ToSocketAddrs;
use std::thread;

use tracing::error;

use crate::db::Database;
use crate::db::DB;
use crate::error::Result;
use crate::error::ServerError;
use crate::parse_request;
use crate::serialize_response;
use crate::Request;
use crate::Response;

/// A basic in-memory database server.
pub struct Server {
    listener: TcpListener,
    db: DB,
    initial_buffer_size: InitialBufferSize,
    // If the client requests too much data, we reject the request.
    max_buffer_size: MaxBufferSize,
}
/// A `ServerBuilder` can be used to create a `Server` with custom configuration.
#[derive(Debug)]
pub struct ServerBuilder<A> {
    addr: Option<A>,
    initial_db_size: Option<usize>,
    initial_buffer_size: Option<InitialBufferSize>,
    max_buffer_size: Option<MaxBufferSize>,
}

impl<A> Default for ServerBuilder<A> {
    fn default() -> Self {
        Self {
            addr: None,
            initial_db_size: None,
            initial_buffer_size: None,
            max_buffer_size: None,
        }
    }
}

impl<A: ToSocketAddrs> ServerBuilder<A> {
    /// Creates a new `ServerBuilder`
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the address the `Server` listens at.
    /// The validity of `addr` is not verified here, but only when [`build`]ing the server.
    ///
    /// [`build`]: ServerBuilder::build
    pub fn address(
        mut self,
        addr: A,
    ) -> Self {
        self.addr = Some(addr);
        self
    }

    /// Sets the initial memory allocation of the database in bytes.
    pub fn initial_db_size(
        mut self,
        initial_db_size: usize,
    ) -> Self {
        self.initial_db_size = Some(initial_db_size);
        self
    }

    /// Sets the initial buffer size in bytes for every new incoming connection to the server.
    pub fn initial_buffer_size(
        mut self,
        initial_buffer_size: usize,
    ) -> Self {
        self.initial_buffer_size = Some(InitialBufferSize(initial_buffer_size));
        self
    }

    /// Sets the maximum buffer size in bytes for every new incoming connection to the server.
    /// If the client sends more than this number of byes, the request will be rejected.
    pub fn max_buffer_size(
        mut self,
        max_buffer_size: usize,
    ) -> Self {
        self.max_buffer_size = Some(MaxBufferSize(max_buffer_size));
        self
    }

    /// Starts a server from this `ServerBuilder`.
    ///
    /// # Errors
    /// If no [`address`] was set then an error is returned.
    ///
    /// [`address`]: ServerBuilder::address
    ///
    /// # Panics
    /// Panics if the server cannot bind to the specified `address`.
    pub fn build(self) -> Result<Server> {
        let Some(addr) = self.addr else {
            return Err(ServerError::NoAddress.into());
        };
        let listener = TcpListener::bind(addr).expect("to be able to bind to address");
        Ok(Server {
            listener,
            initial_buffer_size: self.initial_buffer_size.unwrap_or_default(),
            max_buffer_size: self.max_buffer_size.unwrap_or_default(),
            db: DB::with_capacity(self.initial_db_size.unwrap_or(1024 * 1024)),
        })
    }
}

impl Server {
    /// Creates a new `Server` listening on `addr`.
    ///
    /// # Panics
    /// Panics if it cannot bind to `addr`.
    pub fn bind<A: ToSocketAddrs>(addr: A) -> Self {
        let listener = TcpListener::bind(addr).expect("to be able to bind to address");
        Self {
            listener,
            db: DB::with_capacity(1024),
            initial_buffer_size: InitialBufferSize::default(),
            max_buffer_size: MaxBufferSize::default(),
        }
    }

    /// Returns a `ServerBuilder` that can be used to build a `Server`.
    pub fn builder<A: ToSocketAddrs>() -> ServerBuilder<A> {
        ServerBuilder::new()
    }

    /// Runs the server.
    pub fn run(&self) {
        for stream in self.listener.incoming() {
            let db_clone = self.db.clone();
            let init_buffer_size = self.initial_buffer_size;
            let max_buffer_size = self.max_buffer_size;
            thread::spawn(move || match stream {
                Ok(mut stream) => {
                    // TODO handle err
                    let _ =
                        handle_connection(&mut stream, db_clone, init_buffer_size, max_buffer_size);
                }
                Err(e) => {
                    error!("Could not read incoming stream: {:?}", e);
                }
            });
        }
    }

    /// Returns the port the server is listening on.
    pub fn port(&self) -> Result<u16> {
        let addr = self.listener.local_addr().map_err(ServerError::IO)?;
        Ok(addr.port())
    }
}

#[derive(Debug, Copy, Clone)]
struct InitialBufferSize(usize);

impl Default for InitialBufferSize {
    fn default() -> Self {
        // 4kB
        Self(4096)
    }
}

#[derive(Debug, Copy, Clone)]
struct MaxBufferSize(usize);

impl Default for MaxBufferSize {
    fn default() -> Self {
        // 1MB
        Self(1024 * 1024)
    }
}

fn handle_connection<RW, DB>(
    stream: &mut RW,
    db: DB,
    initial_buffer_size: InitialBufferSize,
    max_buffer_size: MaxBufferSize,
) -> Result<()>
where
    RW: Read,
    RW: Write,
    RW: ?Sized,
    DB: Database,
{
    let mut buffer = vec![0; initial_buffer_size.0];
    let mut cursor = 0;

    loop {
        if let Some((request, n_parsed_bytes)) = parse_request(&buffer[0..cursor]).unwrap() {
            let response = match request {
                Request::Get(key) => {
                    let v = db.get(key)?;
                    Response::Get(v)
                }
                Request::Set { key, value } => {
                    db.insert(key.to_string(), value.to_string())?;
                    Response::Set
                }
                Request::Delete(key) => {
                    db.remove(key)?;
                    Response::Delete
                }
                Request::Flush => {
                    db.clear()?;
                    Response::Flush
                }
            };
            send_response(stream, response).map_err(ServerError::IO)?;

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

        if buffer.len() >= max_buffer_size.0 {
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

fn send_response<W: Write + ?Sized>(
    stream: &mut W,
    response: Response,
) -> io::Result<()> {
    let bytes = serialize_response(response);
    stream.write_all(&bytes)?;
    stream.flush()
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use super::*;
    use crate::error::Error;
    use crate::server::InitialBufferSize;
    use crate::server::MaxBufferSize;

    const INITIAL_BUFFER_SIZE: usize = 32;
    const MAX_BUFFER_SIZE: usize = 93;

    #[test]
    fn test_read_request_single_request_in_stream() {
        let db = DB::new();
        let raw_data = vec![2, 0, 0, 0, 3, 97, 98, 99, 0, 0, 0, 3, 103, 104, 105];
        assert!(raw_data.len() < INITIAL_BUFFER_SIZE);
        assert!(raw_data.len() < 2 * MAX_BUFFER_SIZE);
        let mut stream = Cursor::new(raw_data);
        let _ = handle_connection(
            &mut stream,
            db.clone(),
            InitialBufferSize(INITIAL_BUFFER_SIZE),
            MaxBufferSize(MAX_BUFFER_SIZE),
        );
        assert_eq!(db.read().unwrap().get("abc").unwrap(), "ghi");
    }

    #[test]
    fn test_read_request_multiple_requests_in_stream() {
        let db = DB::new();
        // Two concatenated requests
        let raw_data = vec![
            2, 0, 0, 0, 3, 97, 98, 99, 0, 0, 0, 3, 103, 104, 105, 2, 0, 0, 0, 3, 49, 50, 51, 0, 0,
            0, 3, 52, 53, 54,
        ];
        assert!(raw_data.len() < INITIAL_BUFFER_SIZE);
        assert!(raw_data.len() < 2 * MAX_BUFFER_SIZE);
        let mut stream = Cursor::new(raw_data);
        let _ = handle_connection(
            &mut stream,
            db.clone(),
            InitialBufferSize(INITIAL_BUFFER_SIZE),
            MaxBufferSize(MAX_BUFFER_SIZE),
        );
        assert_eq!(db.read().unwrap().get("abc").unwrap(), "ghi");
        assert_eq!(db.read().unwrap().get("123").unwrap(), "456");
    }

    #[test]
    fn test_read_single_request_larger_than_initial_buffer() {
        let db = DB::new();
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
        let _ = handle_connection(
            &mut stream,
            db.clone(),
            InitialBufferSize(INITIAL_BUFFER_SIZE),
            MaxBufferSize(MAX_BUFFER_SIZE),
        );
        assert_eq!(
            db.read().unwrap().get("123").unwrap(),
            "This is some longer text that did not fit into a single TCP request"
        );
    }

    #[test]
    fn test_max_buffer_resize_is_respected() {
        let db = DB::new();
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
            handle_connection(
                &mut stream,
                db,
                InitialBufferSize(INITIAL_BUFFER_SIZE),
                MaxBufferSize(MAX_BUFFER_SIZE),
            )
            .err(),
            Some(Error::Server(ServerError::TooMuchData))
        ));
    }
}
