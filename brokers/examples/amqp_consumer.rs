use std::env;

use futures::StreamExt;

use rustacles_brokers::amqp::AmqpBroker;

#[tokio::main]
async fn main() {
    env_logger::init();
    let amqp_uri = env::var("AMQP_URI").expect("No AMQP URI provided");
    let broker = AmqpBroker::new(amqp_uri, "foo".to_string(), None)
        .await
        .expect("Failed to initialize broker");
    let mut consumer = broker
        .consume("foobar")
        .await
        .expect("Failed to consume event");
    println!("I'm now listening for messages!");
    tokio::spawn(async move {
        while let Some(payload) = consumer.next().await {
            let string = std::str::from_utf8(&payload).expect("Failed to decode string");
            println!("Message received: {}", string);
        }
    });
}
