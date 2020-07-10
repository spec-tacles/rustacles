use std::env;

use rustacles_brokers::amqp::AmqpBroker;
use tokio::stream::StreamExt;

#[tokio::main]
async fn main() {
    env_logger::init();
    let amqp_uri = env::var("AMQP_URI").unwrap_or("amqp://localhost:5672/%2f".into());
    let broker = AmqpBroker::new(&amqp_uri, "foo".to_string(), None)
        .await
        .expect("Failed to initialize broker");
    let mut consumer = broker
        .consume("foobar")
        .await
        .expect("Failed to consume event");
    println!("I'm now listening for messages!");
    while let Some(payload) = consumer.next().await {
        let string = std::str::from_utf8(&payload.data).expect("Failed to decode string");
        println!("Message received: {}", string);
        broker.reply_to(&payload, string.as_bytes().to_vec()).await.expect("Unable to send reply");
    }
}
