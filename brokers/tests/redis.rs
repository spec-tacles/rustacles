use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures::TryStreamExt;
use redust::pool::{Manager, Pool};
use tokio::{spawn, try_join};

use rustacles_brokers::{common::Message, redis::RedisBroker};

#[tokio::test]
async fn consumes_messages() {
    let group = "foo";
    let manager = Manager::new("localhost:6379");
    let pool = Pool::builder(manager).build().expect("pool builder");
    let broker = RedisBroker::new(group, pool);

    let events = [Bytes::from("abc")];

    broker
        .ensure_events(events.iter())
        .await
        .expect("subscribed");
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
    let event = "def";
    let events = [Bytes::from(event)];

    let manager = Manager::new("localhost:6379");
    let pool = Pool::builder(manager).build().expect("pool builder");

    let broker1 = RedisBroker::new(group, pool);
    let broker2 = broker1.clone();

    broker1
        .ensure_events(events.iter())
        .await
        .expect("subscribed");

    let timeout = Some(SystemTime::now() + Duration::from_millis(500));

    let call_fut = spawn(async move {
        broker2
            .call(event, &[1u8, 2, 3], timeout)
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
