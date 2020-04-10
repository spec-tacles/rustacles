# Rustacles Brokers

Message brokers which allow for simple communication between Spectacles services.

## Available Brokers
- AMQP - An interface to connect to an AMQP-compliant server.

## Example: AMQP Publisher
```rust,norun
use std::env;
use futures::StreamExt;
use rustacles_brokers::amqp::{AmqpBroker, AmqpProperties};

#[tokio::main]
async fn run() {
    let amqp_uri = env::var("AMQP_URI").expect("No AMQP URL provided");
    let broker = AmqpBroker::new(&amqp_uri, "foo".to_string(), None).await.expect("Failed to initialize broker");
    let json = b"{'foo': 'bar'}";
    match broker.publish("foobar", json.to_vec(), AmqpProperties::default()).await {
        Ok(_) => println!("Message successfully published."),
        Err(e) => panic!("Failed to publish message: {}", e)
    };
}
```


More examples can be found in the [`examples`] directory.