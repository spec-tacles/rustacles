#[cfg(feature = "redis-broker")]
use deadpool_redis::{redis::RedisError, PoolError};
#[cfg(feature = "amqp-broker")]
use lapin::Error as LapinError;
#[cfg(feature = "amqp-broker")]
use tokio::sync::oneshot::error::RecvError;
use std::{io::Error as IoError, result::Result as StdResult};
use thiserror::Error;

pub type Result<T, E = Error> = StdResult<T, E>;

#[derive(Error, Debug)]
pub enum Error {
    #[cfg(feature = "amqp-broker")]
    #[error("Lapin error")]
    Lapin(#[from] LapinError),
    #[error("IO error")]
    Io(#[from] IoError),
    #[cfg(feature = "amqp-broker")]
    #[error("Async receive error")]
    Recv(#[from] RecvError),
    #[error("Reply error")]
    Reply(String),
    #[cfg(feature = "redis-broker")]
    #[error("Redis error")]
    Redis(#[from] RedisError),
    #[cfg(feature = "redis-broker")]
    #[error("Pool error")]
    Deadpool(#[from] PoolError),
    #[error("MessagePack encode error")]
    MsgpackEncode(#[from] rmp_serde::encode::Error),
    #[error("MessagePack decode error")]
    MsgpackDecode(#[from] rmp_serde::decode::Error),
}
