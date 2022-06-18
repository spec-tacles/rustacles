use std::{
    fmt::Debug,
    io::Write,
    str::from_utf8,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use redust::model::stream::{read::Entry, Id};
use serde::{de::DeserializeOwned, Serialize};
use tokio::net::ToSocketAddrs;

use crate::error::Result;

use super::{RedisBroker, STREAM_DATA_KEY, STREAM_TIMEOUT_KEY};

/// A message received from the broker.
#[derive(Debug, Clone)]
pub struct Message<A, V>
where
    A: ToSocketAddrs + Clone + Send + Sync + Debug,
{
    /// The group this message belongs to.
    pub group: Bytes,
    /// The event this message signals.
    pub event: Bytes,
    /// The ID of this message (generated by Redis).
    pub id: Id,
    /// The data of this message. Always present unless there is a bug with a client implementation.
    pub data: Option<V>,
    /// When this message times out. Clients should cancel work if it is still in progress after
    /// this instant.
    pub timeout_at: Option<SystemTime>,
    broker: RedisBroker<A>,
}

impl<A, V> PartialEq for Message<A, V>
where
    A: ToSocketAddrs + Clone + Send + Sync + Debug,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<A, V> Eq for Message<A, V> where A: ToSocketAddrs + Clone + Send + Sync + Debug {}

impl<A, V> Message<A, V>
where
    A: ToSocketAddrs + Clone + Send + Sync + Debug,
    V: DeserializeOwned,
{
    pub(crate) fn new(id: Id, entry: Entry, event: Bytes, broker: RedisBroker<A>) -> Self {
        let data = entry
            .get(&STREAM_DATA_KEY)
            .and_then(|value| rmp_serde::from_read_ref(&value.0).ok());

        let timeout_at = entry
            .get(&STREAM_TIMEOUT_KEY)
            .and_then(|value| from_utf8(&value.0).ok()?.parse().ok())
            .map(|timeout| UNIX_EPOCH + Duration::from_nanos(timeout));

        Message {
            group: broker.group.clone(),
            event,
            id,
            data,
            timeout_at,
            broker,
        }
    }
}

impl<A, V> Message<A, V>
where
    A: ToSocketAddrs + Clone + Send + Sync + Debug,
{
    /// Acknowledge receipt of the message. This should always be called, since un-acked messages
    /// will be reclaimed by other clients.
    pub async fn ack(&self) -> Result<()> {
        self.broker
            .pool
            .get()
            .await?
            .cmd([
                b"xack",
                &*self.event,
                &*self.group,
                self.id.to_string().as_bytes(),
            ])
            .await?;

        Ok(())
    }

    /// Reply to this message.
    pub async fn reply(&self, data: &impl Serialize) -> Result<()> {
        let mut key = Vec::new();
        key.copy_from_slice(&self.event);
        write!(key, ":{}", self.id)?;

        let serialized = rmp_serde::to_vec(data)?;
        self.broker
            .pool
            .get()
            .await?
            .cmd([b"publish", key.as_slice(), serialized.as_slice()])
            .await?;

        Ok(())
    }
}
