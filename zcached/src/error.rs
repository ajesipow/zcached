use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Parsing(#[from] ParsingError),
}

#[derive(Debug, Error)]
pub enum ParsingError {
    #[error("cannot convert Utf8")]
    Utf8Error(#[from] std::str::Utf8Error),
    #[error("other parsing error")]
    Other,
}

pub(crate) type Result<T> = std::result::Result<T, Error>;
