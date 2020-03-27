use crate::amqp::AmqpBroker;
use futures_await_test::*;
use std::env;

#[async_test]
async fn connect() {
    let env = env::var("AMQP_URI");
    assert!(env.is_ok());
    let broker = AmqpBroker::new(
        env.unwrap(),
        String::from("foo"),
        None
    ).await;
    assert!(broker.is_ok());
}