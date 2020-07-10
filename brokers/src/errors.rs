use lapin::Error as LapinError;

use std::{io::Error as IoError, result::Result as StdResult};
use tokio::sync::oneshot::error::RecvError;

pub type Result<T> = StdResult<T, Error>;

#[derive(Debug)]
pub enum Error {
    Lapin(LapinError),
    Io(IoError),
    Recv(RecvError),
    Reply,
}

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
