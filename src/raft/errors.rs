use std::{result, io, fmt};
use std::error;

use protobuf::ProtobufError;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    NotFound,
    Store(StorageError),
    Protobuf(ProtobufError),
    Other(String),
}

pub enum StorageError {
    Compacted,
    Unavailable,
}


impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Ok(self.description)
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match self {
            // not sure that cause should be included in message
            &Error::Io(ref e) => e.description(),
            &Error::Store(ref e) => e.description(),
            &Error::Protobuf(ref e) => e.description(),
            &Error::Other(ref e) => &e,
            &Error::NotFound => "not found",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match self {
            &Error::Io(ref e) => Some(e),
            &Error::Protobuf(ref e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<ProtobufError> for Error {
    fn from(err: ProtobufError) -> Error {
        Error::Protobuf(err)
    }
}

pub type Result<T> = result::Result<T, Error>;
