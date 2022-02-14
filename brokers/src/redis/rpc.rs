use futures::TryStreamExt;
use serde::de::DeserializeOwned;

use crate::error::Result;

use super::RedisBroker;

/// A Remote Procedure Call. Poll the future returned by `response` to get the response value.
#[derive(Debug, Clone)]
pub struct Rpc<'broker> {
    pub(crate) name: String,
    pub(crate) broker: &'broker RedisBroker,
}

impl<'broker> PartialEq for Rpc<'broker> {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl<'broker> Eq for Rpc<'broker> {}

impl<'broker> Rpc<'broker> {
    pub async fn response<V>(&self) -> Result<Option<V>>
    where
        V: DeserializeOwned,
    {
        Ok(self
            .broker
            .pubsub
            .subscribe(self.name.clone())
            .await?
            .try_next()
            .await?
            .map(|msg| rmp_serde::from_read(msg.as_bytes()))
            .transpose()?)
    }
}
