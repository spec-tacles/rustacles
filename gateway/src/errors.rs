use std::{
    io::Error as IoError,
    result::Result as StdResult,
};

use async_tungstenite::tungstenite::error::Error as TungsteniteError;
use futures::channel::mpsc::SendError;
use serde_json::Error as JsonError;

pub type Result<T> = StdResult<T, Error>;

#[derive(Debug)]
pub enum Error {
    Tungstenite(TungsteniteError),
    Json(JsonError),
    SendError,
}

impl From<TungsteniteError> for Error {
    fn from(err: TungsteniteError) -> Self { Error::Tungstenite(err) }
}

impl From<JsonError> for Error {
    fn from(err: JsonError) -> Self { Error::Json(err) }
}

impl From<SendError> for Error {
    fn from(_err: SendError) -> Self { Error::SendError }
}