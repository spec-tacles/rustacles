use std::env;

use rustacles_brokers::amqp::{AmqpBroker, AmqpProperties};

#[tokio::main]
async fn main() {
    let amqp_uri = env::var("AMQP_URI").unwrap_or("amqp://127.0.0.1:5672/%2f".into());
    let broker = AmqpBroker::new(&amqp_uri, "foo".to_string(), None)
        .await
        .expect("Failed to initialize broker");
    match broker
        .publish(
            "foobar",
            b"{'message': 'hello'}".to_vec(),
            AmqpProperties::default(),
        )
        .await
    {
        Ok(_) => println!("Message successfully published."),
        Err(e) => panic!("Failed to publish message: {:?}", e),
    };
}
