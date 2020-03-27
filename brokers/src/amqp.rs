use lapin::{
    Connection,
    Consumer as ConsumerStream,
    ConnectionProperties,
    BasicProperties,
    Channel,
    ExchangeKind,
    types::FieldTable,
    options::*
};
use std::sync::Arc;
use crate::errors::*;

pub type AmqpProperties = BasicProperties;

/// A stream of messages that are being consumed in the message queue.
/*pub struct AmqpConsumer {
    recv: UnboundedReceiver<Vec<u8>>
}

impl AmqpConsumer {
    fn new(recv: UnboundedReceiver<Vec<u8>>) -> Self {
        Self {
            recv
        }
    }
}

*/

#[derive(Clone)]
pub struct AmqpBroker {
    consumer: Consumer,
    producer: Producer,
    group: String,
    subgroup: Option<String>
}

#[derive(Clone)]
pub struct Consumer(Connection);

#[derive(Clone)]
pub struct Producer {
    connection: Connection,
    channel: Channel
}

impl AmqpBroker {
    pub async fn new(amqp_uri: String, group: String, subgroup: Option<String>) -> Result<AmqpBroker> {
        let producer = Connection::connect(amqp_uri.as_str().into(), ConnectionProperties::default()).await?;
        let consumer = Connection::connect(amqp_uri.as_str().into(), ConnectionProperties::default()).await?;
        let channel = producer.create_channel().await?;

        Ok(Self {
            consumer: Consumer(consumer),
            producer: Producer {
                connection: producer,
                channel,
            },
            group,
            subgroup
        })
    }

    pub async fn publish(&self, evt: &str, payload: Vec<u8>, properties: AmqpProperties) -> Result<()> {
        debug!("[Rustacles_BROKER] Publishing event: {} to the AMQP server.", evt);

        self.producer.channel.basic_publish(
            self.group.as_str(),
            evt,
            BasicPublishOptions::default(),
            payload,
            properties
        ).await?;

        Ok(())
    }

    pub async fn consume(&self, evt: &str) -> Result<ConsumerStream> {
        let queue_name = match &self.subgroup {
            Some(g) => format!("{}:{}:{}", self.group, g, evt),
            None => format!("{}:{}", self.group, evt)
        };
        let group = self.group.clone();
        let event = evt.to_string();
        let channel: Channel = self.consumer.0.create_channel().await?;
        channel.exchange_declare(
            &group,
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default()
        ).await?;
        channel.queue_bind(
            &queue_name,
            &group,
            &event,
            QueueBindOptions::default(),
            FieldTable::default()
        ).await?;
        let queue = channel.queue_declare(
            &queue_name,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default()
        );
        let consumer: ConsumerStream = channel.basic_consume(
            &queue_name,
            "",
            BasicConsumeOptions::default(),
            FieldTable::default()
        ).await?;

        Ok(consumer)
    }
}

