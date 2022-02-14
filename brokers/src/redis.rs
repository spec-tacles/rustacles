use std::{
    fmt::{self, Debug},
    time::{SystemTime, UNIX_EPOCH},
};

use arcstr::ArcStr;
pub use deadpool_redis;
use deadpool_redis::{
    redis::{
        streams::{StreamRangeReply, StreamReadOptions, StreamReadReply},
        AsyncCommands, FromRedisValue, RedisError, Value,
    },
    Connection, Pool,
};
use futures::{
    stream::{iter, select_all},
    stream_select, TryStream, TryStreamExt,
};
use nanoid::nanoid;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    error::{Error, Result},
    util::stream::repeat_fn,
};

use self::{message::Message, pubsub::BroadcastSub, rpc::Rpc};

pub mod message;
pub mod pubsub;
pub mod rpc;

const DEFAULT_MAX_CHUNK: usize = 10;
const DEFAULT_BLOCK_INTERVAL: usize = 5000;
const STREAM_DATA_KEY: &'static str = "data";
const STREAM_TIMEOUT_KEY: &'static str = "timeout_at";

/// RedisBroker is internally reference counted and can be safely cloned.
pub struct RedisBroker {
    /// The consumer name of this broker. Should be unique to the container/machine consuming
    /// messages.
    pub name: ArcStr,
    /// The consumer group name.
    pub group: ArcStr,
    /// The largest chunk to consume from Redis. This is only exposed for tuning purposes and
    /// doesn't affect the public API at all.
    pub max_chunk: usize,
    /// The maximum time that a broker is assumed to be alive (ms). Messages pending after this
    /// time period will be reclaimed by other clients.
    pub max_operation_time: usize,
    pool: Pool,
    pubsub: BroadcastSub,
    read_opts: StreamReadOptions,
}

impl Clone for RedisBroker {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            group: self.group.clone(),
            max_chunk: self.max_chunk,
            max_operation_time: self.max_operation_time,
            pool: self.pool.clone(),
            pubsub: self.pubsub.clone(),
            read_opts: Self::make_read_opts(&*self.group, &*self.name),
        }
    }
}

impl Debug for RedisBroker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisBroker")
            .field("name", &self.name)
            .field("group", &self.group)
            .field("max_chunk", &self.max_chunk)
            .field("max_operation_time", &self.max_operation_time)
            .field("pubsub", &self.pubsub)
            .field("read_opts", &self.read_opts)
            .finish_non_exhaustive()
    }
}

impl RedisBroker {
    fn make_read_opts(group: &str, name: &str) -> StreamReadOptions {
        StreamReadOptions::default()
            .group(group, name)
            .count(DEFAULT_MAX_CHUNK)
            .block(DEFAULT_BLOCK_INTERVAL)
    }

    /// Creates a new broker with sensible defaults.
    pub fn new(group: impl Into<ArcStr>, pool: Pool, address: &str) -> RedisBroker {
        let group = group.into();
        let name = nanoid!();
        let read_opts = RedisBroker::make_read_opts(&*group, &name);

        let pubsub = BroadcastSub::new(address);

        Self {
            name: name.into(),
            group,
            max_chunk: DEFAULT_MAX_CHUNK,
            max_operation_time: DEFAULT_BLOCK_INTERVAL,
            pool,
            pubsub,
            read_opts,
        }
    }

    /// Publishes an event to the broker. Returned value is the ID of the message.
    pub async fn publish(&self, event: &str, data: &impl Serialize) -> Result<String> {
        self.publish_timeout(event, data, None).await
    }

    pub async fn call(
        &self,
        event: &str,
        data: &impl Serialize,
        timeout: Option<SystemTime>,
    ) -> Result<Rpc<'_>> {
        let id = self.publish_timeout(event, data, timeout).await?;
        let name = format!("{}:{}", event, id);

        Ok(Rpc {
            name,
            broker: &self,
        })
    }

    async fn publish_timeout(
        &self,
        event: &str,
        data: &impl Serialize,
        maybe_timeout: Option<SystemTime>,
    ) -> Result<String> {
        let serialized_data = rmp_serde::to_vec(data)?;
        let mut conn = self.get_conn().await?;

        let args = match maybe_timeout {
            Some(timeout) => vec![
                (STREAM_DATA_KEY, serialized_data),
                (
                    STREAM_TIMEOUT_KEY,
                    timeout
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                        .to_string()
                        .into_bytes(),
                ),
            ],
            None => vec![(STREAM_DATA_KEY, serialized_data)],
        };

        Ok(conn.xadd(event, "*", &args).await?)
    }

    pub async fn subscribe(&self, events: &[&str]) -> Result<()> {
        for event in events {
            let _: Result<Value, RedisError> = self
                .get_conn()
                .await?
                .xgroup_create_mkstream(*event, &*self.group, 0)
                .await;
        }

        Ok(())
    }

    async fn get_conn(&self) -> Result<Connection> {
        Ok(self.pool.get().await?)
    }

    /// Consume events from the broker.
    pub fn consume<'consume, V>(
        &'consume self,
        events: &'consume [&str],
    ) -> impl TryStream<Ok = Message<V>, Error = Error> + 'consume
    where
        V: DeserializeOwned,
    {
        let ids = vec![">"; events.len()];

        let pool = &self.pool;
        let group = self.group.clone();
        let name = self.name.clone();
        let time = self.max_operation_time;

        let autoclaim_futs = events
            .iter()
            .copied()
            .map(|event| {
                let event = ArcStr::from(event);
                let group = group.clone();
                let name = name.clone();

                move || {
                    let event = event.clone();
                    let group = group.clone();
                    let name = name.clone();

                    async move {
                        let messages = async move {
                            let mut conn = pool.get().await?;
                            let mut cmd = redis::cmd("xautoclaim");

                            cmd.arg(&*event)
                                .arg(&*group)
                                .arg(&*name)
                                .arg(time)
                                .arg("0-0");

                            let res: Vec<Value> = cmd.query_async(&mut conn).await?;
                            let read = StreamRangeReply::from_redis_value(&res[1])?;

                            let messages = read.ids.into_iter().map(move |id| {
                                Ok::<_, Error>(Message::<V>::new(
                                    id,
                                    group.clone(),
                                    event.clone(),
                                    self.clone(),
                                ))
                            });

                            Ok::<_, Error>(iter(messages))
                        };

                        Some(messages.await)
                    }
                }
            })
            .map(repeat_fn)
            .map(|iter| iter.try_flatten());

        let group = group.clone();
        let claim_fut = move || {
            let opts = &self.read_opts;
            let ids = ids.clone();
            let group = group.clone();

            async move {
                let messages =
                    async move {
                        let read: Option<StreamReadReply> =
                            pool.get().await?.xread_options(&events, &ids, opts).await?;

                        let messages = read.map(|reply| reply.keys).into_iter().flatten().flat_map(
                            move |event| {
                                let group = group.clone();
                                let key = ArcStr::from(event.key);
                                event.ids.into_iter().map(move |id| {
                                    Ok(Message::<V>::new(
                                        id,
                                        group.clone(),
                                        key.clone(),
                                        self.clone(),
                                    ))
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

        stream_select!(autoclaim, claim)
    }
}

#[cfg(test)]
mod test {
    use std::time::{Duration, SystemTime};

    use deadpool_redis::{Manager, Pool};
    use futures::TryStreamExt;
    use redis::cmd;
    use tokio::{spawn, try_join};

    use super::RedisBroker;

    #[tokio::test]
    async fn consumes_messages() {
        let group = "foo";
        let manager = Manager::new("redis://localhost:6379").expect("create manager");
        let pool = Pool::new(manager, 32);
        let broker = RedisBroker::new(group, pool, "redis://localhost:6379");

        let events = ["abc"];

        broker.subscribe(&events).await.expect("subscribed");
        broker
            .publish("abc", &[1u8, 2, 3])
            .await
            .expect("published");

        let mut consumer = broker.consume::<Vec<u8>>(&events);
        let msg = consumer
            .try_next()
            .await
            .expect("message")
            .expect("message");
        msg.ack().await.expect("ack");

        assert_eq!(msg.data.expect("data"), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn rpc_timeout() {
        let group = "foo";
        let manager = Manager::new("redis://localhost:6379").expect("create manager");
        let pool = Pool::new(manager, 32);

        let _: () = cmd("FLUSHDB")
            .query_async(&mut pool.get().await.expect("redis connection"))
            .await
            .expect("flush db");

        let broker1 = RedisBroker::new(group, pool, "localhost:6379");
        let broker2 = broker1.clone();

        let events = ["def"];
        broker1.subscribe(&events).await.expect("subscribed");

        let timeout = Some(SystemTime::now() + Duration::from_millis(500));

        let call_fut = spawn(async move {
            broker2
                .call("def", &[1u8, 2, 3], timeout)
                .await
                .expect("published");
        });

        let consume_fut = spawn(async move {
            let mut consumer = broker1.consume::<Vec<u8>>(&events);
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
