use lapin::Error as LapinError;

use std::{io::Error as IoError, result::Result as StdResult};
use thiserror::Error;
use tokio::sync::oneshot::error::RecvError;

pub type Result<T> = StdResult<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Lapin error")]
    Lapin(#[from] LapinError),
    #[error("IO error")]
    Io(#[from] IoError),
    #[error("Async receive error")]
    Recv(#[from] RecvError),
    #[error("Reply error")]
    Reply(String),
}
