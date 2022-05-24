use std::{
    borrow::Cow,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use futures::{
    stream::{iter, select, select_all},
    StreamExt, TryStream, TryStreamExt,
};
use nanoid::nanoid;
use redust::{
    model::stream::{
        claim::AutoclaimResponse,
        read::{Field, ReadResponse},
        Id,
    },
    pool::Pool,
    resp::from_data,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    error::{Error, Result},
    util::stream::repeat_fn,
};

use self::{message::Message, rpc::Rpc};

pub mod message;
pub mod rpc;

const DEFAULT_MAX_CHUNK: &[u8] = b"10";
const DEFAULT_BLOCK_INTERVAL: &[u8] = b"5000";
const STREAM_DATA_KEY: Field<'static> = Field(Cow::Borrowed(b"data"));
const STREAM_TIMEOUT_KEY: Field<'static> = Field(Cow::Borrowed(b"timeout_at"));

/// RedisBroker is internally reference counted and can be safely cloned.
#[derive(Debug, Clone)]
pub struct RedisBroker {
    /// The consumer name of this broker. Should be unique to the container/machine consuming
    /// messages.
    pub name: Bytes,
    /// The consumer group name.
    pub group: Bytes,
    pool: Pool,
}

impl RedisBroker {
    /// Creates a new broker with sensible defaults.
    pub fn new(group: impl Into<Bytes>, pool: Pool) -> Self {
        let group = group.into();
        let name = nanoid!();

        Self {
            name: name.into(),
            group,
            pool,
        }
    }

    /// Publishes an event to the broker. Returned value is the ID of the message.
    pub async fn publish(&self, event: impl AsRef<[u8]>, data: &impl Serialize) -> Result<Id> {
        let serialized_data = rmp_serde::to_vec(data)?;
        let mut conn = self.pool.get().await?;

        let data = conn
            .cmd([
                b"xadd",
                event.as_ref(),
                b"*",
                &STREAM_DATA_KEY.0,
                &serialized_data,
            ])
            .await?;

        Ok(from_data(data)?)
    }

    pub async fn call(
        &self,
        event: &str,
        data: &impl Serialize,
        timeout: Option<SystemTime>,
    ) -> Result<Rpc> {
        let id = if let Some(timeout) = timeout {
            self.publish_timeout(event, data, timeout).await?
        } else {
            self.publish(event, data).await?
        };

        let name = format!("{}:{}", event, id);

        Ok(Rpc {
            name,
            broker: self.clone(),
        })
    }

    pub async fn publish_timeout(
        &self,
        event: impl AsRef<[u8]>,
        data: &impl Serialize,
        timeout: SystemTime,
    ) -> Result<Id> {
        let serialized_data = rmp_serde::to_vec(data)?;
        let mut conn = self.pool.get().await?;

        let timeout_bytes = timeout
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .to_string()
            .into_bytes();

        let data = conn
            .cmd([
                b"xadd",
                event.as_ref(),
                b"*",
                &STREAM_DATA_KEY.0,
                &serialized_data,
                &STREAM_TIMEOUT_KEY.0,
                &timeout_bytes,
            ])
            .await?;

        Ok(from_data(data)?)
    }

    pub async fn subscribe(&self, events: impl Iterator<Item = &Bytes>) -> Result<()> {
        let mut conn = self.pool.get().await?;

        for event in events {
            let cmd: &[&[u8]] = &[
                b"xgroup",
                b"create",
                &*event,
                &*self.group,
                b"$",
                b"mkstream",
            ];

            match conn.cmd(cmd).await {
                Ok(_) => (),
                Err(redust::Error::Redis(err)) if err.starts_with("BUSYGROUP") => (),
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }

    /// Consume events from the broker.
    pub fn consume<V>(&self, events: Vec<Bytes>) -> impl TryStream<Ok = Message<V>, Error = Error>
    where
        V: DeserializeOwned + 'static,
    {
        let autoclaim = self
            .autoclaim_all::<V>(events.clone())
            .into_stream()
            .boxed();
        let claim = self.claim::<V>(events).into_stream().boxed();

        select(autoclaim, claim)
    }

    fn claim<V>(&self, events: Vec<Bytes>) -> impl TryStream<Ok = Message<V>, Error = Error>
    where
        V: DeserializeOwned,
    {
        let this = self.clone();
        let fut_fn = move || {
            let this = this.clone();
            let events = events.clone();

            async move { Some(this.get_messages(&events).await) }
        };

        repeat_fn(fut_fn).try_flatten()
    }

    async fn get_messages<V>(
        &self,
        events: &[Bytes],
    ) -> Result<impl TryStream<Ok = Message<V>, Error = Error>>
    where
        V: DeserializeOwned,
    {
        let this = self.clone();
        let read = self.xreadgroup(events).await?;

        let messages = read.0.into_iter().flat_map(move |(event, entries)| {
            let this = this.clone();
            entries.0.into_iter().map(move |(id, entry)| {
                Ok(Message::<V>::new(
                    id,
                    entry,
                    Bytes::copy_from_slice(&event.0),
                    this.clone(),
                ))
            })
        });

        Ok::<_, Error>(iter(messages))
    }

    async fn xreadgroup(&self, events: &[Bytes]) -> Result<ReadResponse<'static>, Error> {
        let ids = vec![&b">"[..]; events.len()];
        let mut cmd: Vec<&[u8]> = vec![
            b"xreadgroup",
            b"group",
            &*self.group,
            &*self.name,
            b"count",
            DEFAULT_MAX_CHUNK,
            b"block",
            DEFAULT_BLOCK_INTERVAL,
            b"streams",
        ];
        cmd.extend(events.iter().map(|b| &b[..]));
        cmd.extend_from_slice(&ids);

        let data = self.pool.get().await?.cmd(cmd).await?;
        Ok(from_data(data)?)
    }

    async fn xautoclaim(&self, event: &[u8]) -> Result<AutoclaimResponse<'static>, Error> {
        let cmd = [
            b"xautoclaim",
            event,
            &*self.group,
            &*self.name,
            DEFAULT_BLOCK_INTERVAL,
            b"0-0",
        ];

        let mut conn = self.pool.get().await?;

        let res = conn.cmd(cmd).await?;
        Ok(from_data(res)?)
    }

    fn autoclaim_all<V>(&self, events: Vec<Bytes>) -> impl TryStream<Ok = Message<V>, Error = Error>
    where
        V: DeserializeOwned,
    {
        let futs = events
            .into_iter()
            .map(|event| {
                let this = self.clone();
                move || {
                    let this = this.clone();
                    let event = event.clone();

                    let messages = async move {
                        let read = this.xautoclaim(&event).await?;

                        let messages = read.1 .0.into_iter().map(move |(id, data)| {
                            Ok::<_, Error>(Message::<V>::new(id, data, event.clone(), this.clone()))
                        });

                        Ok::<_, Error>(iter(messages))
                    };

                    async move { Some(messages.await) }
                }
            })
            .map(repeat_fn)
            .map(|iter| iter.try_flatten());

        select_all(futs)
    }
}

#[cfg(test)]
mod test {
    use std::time::{Duration, SystemTime};

    use bytes::Bytes;
    use futures::TryStreamExt;
    use redust::pool::{Manager, Pool};
    use tokio::{spawn, try_join};

    use super::RedisBroker;

    #[tokio::test]
    async fn consumes_messages() {
        let group = "foo";
        let manager = Manager::new(([127, 0, 0, 1], 6379).into());
        let pool = Pool::builder(manager).build().expect("pool builder");
        let broker = RedisBroker::new(group, pool);

        let events = [Bytes::from("abc")];

        broker.subscribe(events.iter()).await.expect("subscribed");
        broker
            .publish("abc", &[1u8, 2, 3])
            .await
            .expect("published");

        let mut consumer = broker.consume::<Vec<u8>>(events.to_vec());
        let msg = consumer
            .try_next()
            .await
            .expect("read message")
            .expect("read message");
        msg.ack().await.expect("ack");

        assert_eq!(msg.data.expect("data"), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn rpc_timeout() {
        let group = "foo";
        let manager = Manager::new(([127, 0, 0, 1], 6379).into());
        let pool = Pool::builder(manager).build().expect("pool builder");

        let broker1 = RedisBroker::new(group, pool);
        let broker2 = broker1.clone();

        let events = [Bytes::from("def")];
        broker1.subscribe(events.iter()).await.expect("subscribed");

        let timeout = Some(SystemTime::now() + Duration::from_millis(500));

        let call_fut = spawn(async move {
            broker2
                .call("def", &[1u8, 2, 3], timeout)
                .await
                .expect("published");
        });

        let consume_fut = spawn(async move {
            let mut consumer = broker1.consume::<Vec<u8>>(events.to_vec());
            let msg = consumer
                .try_next()
                .await
                .expect("message")
                .expect("message");

            msg.ack().await.expect("ack");

            assert_eq!(msg.data.as_ref().expect("data"), &[1, 2, 3]);
            assert_eq!(msg.timeout_at, timeout);
        });

        try_join!(consume_fut, call_fut).expect("cancelation futures");
    }
}
