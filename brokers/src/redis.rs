use std::{borrow::Cow, ops::DerefMut};

pub use deadpool_redis;
use deadpool_redis::{
    redis::{
        streams::{StreamId, StreamRangeReply, StreamReadOptions, StreamReadReply},
        AsyncCommands, FromRedisValue, RedisError, Value,
    },
    Connection, Pool,
};
use futures::{
    stream::{iter, select_all},
    StreamExt, TryStream, TryStreamExt,
};
use nanoid::nanoid;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    error::{Error, Result},
    util::stream::repeat_fn,
};

const DEFAULT_MAX_CHUNK: usize = 10;
const DEFAULT_BLOCK_INTERVAL: usize = 5000;
const STREAM_DATA_KEY: &'static str = "data";

/// A message received from the broker.
#[derive(Clone)]
pub struct Message<'a, V> {
    /// The group this message belongs to.
    pub group: &'a str,
    /// The event this message signals.
    pub event: Cow<'a, str>,
    /// The ID of this message (generated by Redis).
    pub id: String,
    /// The data of this message. Always present unless there is a bug with a client implementation.
    pub data: Option<V>,
    pool: &'a Pool,
}

impl<'a, V> Message<'a, V>
where
    V: DeserializeOwned,
{
    fn new(id: StreamId, group: &'a str, event: Cow<'a, str>, pool: &'a Pool) -> Self {
        let data = id
            .get(STREAM_DATA_KEY)
            .and_then(|data: Vec<u8>| rmp_serde::from_read_ref(&data).ok());

        Message {
            group,
            event,
            id: id.id,
            pool,
            data,
        }
    }
}

impl<'a, V> Message<'a, V> {
    /// Acknowledge receipt of the message. This should always be called, since un-acked messages
    /// will be reclaimed by other clients.
    pub async fn ack(&self) -> Result<()> {
        let _: Value = self
            .pool
            .get()
            .await?
            .xack(&*self.event, self.group, &[&self.id])
            .await?;

        Ok(())
    }

    /// Reply to this message.
    pub async fn reply(&self, data: &impl Serialize) -> Result<()> {
        let key = format!("{}:{}", self.event, self.id);
        let serialized = rmp_serde::to_vec(data)?;
        let _: Value = self.pool.get().await?.publish(key, serialized).await?;

        Ok(())
    }
}

/// The result of consuming events.
#[derive(Debug, Clone)]
pub struct ConsumeResult<T, U> {
    /// A stream that, when polled, claims messages from other clients that have failed to ack
    /// within the configured time period.
    pub autoclaim: T,
    /// A stream that, when polled, consumes messages from the group for this client.
    pub claim: U,
}

// #[derive(Debug)]
pub struct RedisBroker<'a> {
    /// The consumer name of this broker. Should be unique to the container/machine consuming
    /// messages.
    pub name: Cow<'a, str>,
    /// The consumer group name.
    pub group: Cow<'a, str>,
    /// The largest chunk to consume from Redis. This is only exposed for tuning purposes and
    /// doesn't affect the public API at all.
    pub max_chunk: usize,
    /// The maximum time that a broker is assumed to be alive (ms). Messages pending after this
    /// time period will be reclaimed by other clients.
    pub max_operation_time: usize,
    pool: Pool,
    read_opts: StreamReadOptions,
}

impl<'a> RedisBroker<'a> {
    /// Creates a new broker with sensible defaults.
    pub fn new(group: impl Into<Cow<'a, str>>, pool: Pool) -> RedisBroker<'a> {
        let group = group.into();
        let name = nanoid!();
        let read_opts = StreamReadOptions::default()
            .group(&*group, &name)
            .count(DEFAULT_MAX_CHUNK)
            .block(DEFAULT_BLOCK_INTERVAL);

        Self {
            name: Cow::Owned(name),
            group,
            max_chunk: DEFAULT_MAX_CHUNK,
            max_operation_time: DEFAULT_BLOCK_INTERVAL,
            pool,
            read_opts,
        }
    }

    /// Publishes an event to the broker. Returned value is the ID of the message.
    pub async fn publish(&self, event: &str, data: &impl Serialize) -> Result<String> {
        let serialized = rmp_serde::to_vec(data)?;
        Ok(self
            .get()
            .await?
            .xadd(event, "*", &[(STREAM_DATA_KEY, serialized)])
            .await?)
    }

    pub async fn call<V>(&self, event: &str, data: &impl Serialize) -> Result<Option<V>>
    where
        V: DeserializeOwned,
    {
        let id = self.publish(event, data).await?;
        let name = format!("{}:{}", event, id);

        let mut conn = Connection::take(self.get().await?).into_pubsub();
        conn.subscribe(&name).await?;

        let mut stream = conn.on_message();
        Ok(stream
            .next()
            .await
            .map(|msg| msg.get_payload::<Vec<u8>>())
            .transpose()?
            .map(|payload| rmp_serde::from_read_ref(&payload))
            .transpose()?)
    }

    pub async fn subscribe(&self, events: &[&str]) -> Result<()> {
        for event in events {
            let _: Result<Value, RedisError> = self
                .get()
                .await?
                .xgroup_create_mkstream(*event, &*self.group, 0)
                .await;
        }

        Ok(())
    }

    async fn get(&self) -> Result<Connection> {
        Ok(self.pool.get().await?)
    }

    /// Consume events from the broker.
    pub fn consume<'consume, V>(
        &'consume self,
        events: &'consume [&str],
    ) -> ConsumeResult<
        impl TryStream<Ok = Message<'consume, V>, Error = Error>,
        impl TryStream<Ok = Message<'consume, V>, Error = Error>,
    >
    where
        V: DeserializeOwned,
    {
        let ids = vec![">"; events.len()];

        let pool = &self.pool;
        let group = &self.group;
        let name = &self.name;
        let time = self.max_operation_time;

        let autoclaim_futs = events
            .iter()
            .map(|event| {
                move || async move {
                    let messages = async move {
                        let mut conn = pool.get().await?;
                        let mut cmd = redis::cmd("xautoclaim");

                        cmd.arg(event)
                            .arg(&**group)
                            .arg(&**name)
                            .arg(time)
                            .arg("0-0");

                        let res: Vec<Value> = cmd.query_async(conn.deref_mut()).await?;
                        let read = StreamRangeReply::from_redis_value(&res[1])?;

                        let messages = read.ids.into_iter().map(move |id| {
                            Ok::<_, Error>(Message::<V>::new(
                                id,
                                &group,
                                Cow::Borrowed(event),
                                pool,
                            ))
                        });

                        Ok::<_, Error>(iter(messages))
                    };

                    Some(messages.await)
                }
            })
            .map(repeat_fn)
            .map(|iter| iter.try_flatten());

        let claim_fut = move || {
            let opts = &self.read_opts;
            let ids = ids.clone();

            async move {
                let messages =
                    async move {
                        let read: Option<StreamReadReply> =
                            pool.get().await?.xread_options(&events, &ids, opts).await?;

                        dbg!(&read);
                        let messages = read.map(|reply| reply.keys).into_iter().flatten().flat_map(
                            move |event| {
                                let key = Cow::from(event.key);
                                event.ids.into_iter().map(move |id| {
                                    Ok(Message::<V>::new(id, group, key.clone(), pool))
                                })
                            },
                        );

                        Ok::<_, Error>(iter(messages))
                    };

                Some(messages.await)
            }
        };

        let autoclaim = select_all(autoclaim_futs);
        let claim = repeat_fn(claim_fut).try_flatten();

        ConsumeResult { autoclaim, claim }
    }
}

#[cfg(test)]
mod test {
    use deadpool_redis::{Manager, Pool};
    use futures::TryStreamExt;

    use super::RedisBroker;

    #[tokio::test]
    async fn consumes_messages() {
        let group = "foo".to_string();
        let manager = Manager::new("redis://localhost:6379").expect("create manager");
        let pool = Pool::new(manager, 32);
        let broker = RedisBroker::new(group, pool);

        let events = ["abc"];

        broker.subscribe(&events).await.expect("subscribed");
        broker
            .publish("abc", &[1u8, 2, 3])
            .await
            .expect("published");

        let mut consumer = broker.consume::<Vec<u8>>(&events);
        let msg = consumer
            .claim
            .try_next()
            .await
            .expect("message")
            .expect("message");
        msg.ack().await.expect("ack");

        assert_eq!(msg.data.expect("data"), vec![1, 2, 3]);
    }
}
