use lapin::Error as LapinError;

use std::{io::Error as IoError, result::Result as StdResult};
use tokio::sync::oneshot::error::RecvError;

pub type Result<T> = StdResult<T, Error>;

#[derive(Debug)]
pub enum Error {
    Lapin(LapinError),
    Io(IoError),
    Recv(RecvError),
    Reply(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Lapin(e) => write!(f, "Lapin error: {}", e),
            Error::Io(e) => write!(f, "IO error: {}", e),
            Error::Recv(e) => write!(f, "Async receive error: {}", e),
            Error::Reply(e) => write!(f, "Reply error: {}", e),
        }
    }
}

impl std::error::Error for Error {}

impl From<LapinError> for Error {
    fn from(err: LapinError) -> Self {
        Error::Lapin(err)
    }
}

impl From<RecvError> for Error {
    fn from(err: RecvError) -> Self {
        Error::Recv(err)
    }
}
