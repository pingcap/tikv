use std::error;
use std::fmt;

#[derive(Debug)]
pub enum ProfError {
    MemProfilingNotEnabled,
    IOError(std::io::Error),
    JemallocError(i32),
    PathEncodingError(std::ffi::OsString), // When temp files are in a non-unicode directory, OsString.into_string() will cause this error,
    PtahError(std::ffi::NulError),
}

pub type ProfResult<T> = std::result::Result<T, ProfError>;

impl fmt::Display for ProfError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProfError::MemProfilingNotEnabled => write!(f, "mem-profiling was not enabled"),
            ProfError::IOError(e) => write!(f, "io error occurred {:?}", e),
            ProfError::JemallocError(e) => write!(f, "jemalloc error {}", e),
            ProfError::PathEncodingError(path) => {
                write!(f, "Dump target path {:?} is non-unicode", path)
            }
            ProfError::PtahError(path) => write!(f, "Dump target path {:?} is invalid", path),
        }
    }
}

impl From<std::io::Error> for ProfError {
    fn from(e: std::io::Error) -> Self {
        ProfError::IOError(e)
    }
}

impl From<std::ffi::NulError> for ProfError {
    fn from(e: std::ffi::NulError) -> Self {
        ProfError::PtahError(e)
    }
}

impl error::Error for ProfError {}
