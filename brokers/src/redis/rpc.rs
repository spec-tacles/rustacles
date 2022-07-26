use std::fmt::Debug;

use async_trait::async_trait;
use redust::{model::pubsub, resp::from_data};
use serde::de::DeserializeOwned;
use tokio::net::ToSocketAddrs;
use tracing::instrument;

use crate::{common, error::Result};

use super::RedisBroker;

/// A Remote Procedure Call. Poll the future returned by `response` to get the response value.
#[derive(Debug, Clone)]
pub struct Rpc<A>
where
    A: ToSocketAddrs + Clone + Send + Sync + Debug + 'static,
{
    pub(crate) name: String,
    pub(crate) broker: RedisBroker<A>,
}

impl<A> PartialEq for Rpc<A>
where
    A: ToSocketAddrs + Clone + Send + Sync + Debug,
{
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl<A> Eq for Rpc<A> where A: ToSocketAddrs + Clone + Send + Sync + Debug {}

#[async_trait]
impl<A> common::Rpc for Rpc<A>
where
    A: ToSocketAddrs + Clone + Send + Sync + Debug,
{
    #[instrument(level = "debug", ret, err)]
    async fn response<V>(&self) -> Result<Option<V>>
    where
        V: DeserializeOwned + Debug,
    {
        let mut conn = self.broker.pool.get().await?;
        conn.cmd(["SUBSCRIBE", &self.name]).await?;

        loop {
            let response = from_data::<pubsub::Response>(conn.read_cmd().await?)?;

            if let pubsub::Response::Message(msg) = response {
                conn.cmd(["UNSUBSCRIBE", &self.name]).await?;
                break Ok(rmp_serde::from_read_ref(&msg.data)?);
            }
        }
    }
}
