use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Parsing(#[from] ParsingError),
    #[error(transparent)]
    Server(#[from] ServerError),
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error(transparent)]
    IO(#[from] std::io::Error),
}

#[derive(Debug, Error)]
pub enum ParsingError {
    #[error("cannot convert Utf8")]
    Utf8Error(#[from] std::str::Utf8Error),
    #[error("other parsing error")]
    Other,
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("no address provided for starting server")]
    NoAddress,
    #[error("received too much data")]
    TooMuchData,
    #[error("connection reset by peer")]
    ConnectionResetByPeer,
    #[error("database error")]
    Database(#[from] DatabaseError),
    #[error("database IO issue")]
    IO(#[from] std::io::Error),
}

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("database locking issue")]
    DbLock,
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("connection reset by peer")]
    ConnectionResetByPeer,
    #[error("received too much data")]
    TooMuchData,
}

pub(crate) type Result<T> = std::result::Result<T, Error>;
