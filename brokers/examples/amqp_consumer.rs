use std::env;

use rustacles_brokers::amqp::AmqpBroker;

#[tokio::main]
async fn main() {
    env_logger::init();
    let amqp_uri = env::var("AMQP_URI").unwrap_or("amqp://127.0.0.1:5672/%2f".into());
    let broker = AmqpBroker::new(&amqp_uri, "foo".to_string(), None)
        .await
        .expect("Failed to initialize broker");
    let mut consumer = broker
        .consume("foobar")
        .await
        .expect("Failed to consume event");
    println!("I'm now listening for messages!");
    while let Some(message) = consumer.recv().await {
        message.ack().await.expect("Unable to ack message");
        let string = std::str::from_utf8(&message.data).expect("Failed to decode string");
        println!("Message received: {}", string);
    }

    println!("Finished consumption");
}
