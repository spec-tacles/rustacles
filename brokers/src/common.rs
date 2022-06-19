use std::fmt::Debug;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

use crate::error::Result;

#[async_trait]
pub trait Message {
    async fn ack(&self) -> Result<()>;
    async fn reply(&self, data: &(impl Serialize + Debug + Send + Sync)) -> Result<()>;
}

#[async_trait]
pub trait Rpc {
    async fn response<V>(&self) -> Result<Option<V>>
    where
        V: DeserializeOwned + Debug;
}
