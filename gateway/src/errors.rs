use std::{
    io::Error as IoError,
    result::Result as StdResult,
};

use async_tungstenite::tungstenite::error::Error as TungsteniteError;

pub type Result<T> = StdResult<T, Error>;

pub enum Error {
    Tungstenite(TungsteniteError)
}

impl From<TungsteniteError> for Error {
    fn from(err: TungsteniteError) -> Self { Error::Tungstenite(err) }
}