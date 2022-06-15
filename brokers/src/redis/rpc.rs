use redust::resp::from_data;
use serde::de::DeserializeOwned;
use serde_bytes::Bytes;
use tokio::net::ToSocketAddrs;

use crate::error::Result;

use super::RedisBroker;

/// A Remote Procedure Call. Poll the future returned by `response` to get the response value.
#[derive(Debug, Clone)]
pub struct Rpc<A>
where
    A: ToSocketAddrs + Clone + Send + Sync,
{
    pub(crate) name: String,
    pub(crate) broker: RedisBroker<A>,
}

impl<A> PartialEq for Rpc<A>
where
    A: ToSocketAddrs + Clone + Send + Sync,
{
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl<A> Eq for Rpc<A> where A: ToSocketAddrs + Clone + Send + Sync {}

impl<A> Rpc<A>
where
    A: ToSocketAddrs + Clone + Send + Sync,
{
    pub async fn response<V>(&self) -> Result<Option<V>>
    where
        V: DeserializeOwned,
    {
        let mut conn = self.broker.pool.get().await?;

        conn.cmd(["subscribe", &self.name]).await?;
        let data = conn.read_cmd().await?;

        let bytes = from_data::<&Bytes>(data)?;
        Ok(rmp_serde::from_read_ref(bytes)?)
    }
}
