use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

use lapin::{
    message::Delivery, options::*, types::FieldTable, BasicProperties, Channel, Connection,
    ConnectionProperties, ExchangeKind,
};
use log::debug;
use nanoid::nanoid;
pub use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_stream::StreamExt;

use crate::error::*;

pub type AmqpProperties = BasicProperties;

#[derive(Debug, Clone)]
pub struct Message {
    pub channel: Weak<Channel>,
    pub data: Vec<u8>,
    pub properties: BasicProperties,
    pub delivery_tag: u64,
    pub routing_key: String,
    pub exchange: String,
    pub redelivered: bool,
}

impl From<(Weak<Channel>, Delivery)> for Message {
    fn from(data: (Weak<Channel>, Delivery)) -> Self {
        Self {
            channel: data.0,
            data: data.1.data,
            properties: data.1.properties,
            delivery_tag: data.1.delivery_tag,
            routing_key: data.1.routing_key.to_string(),
            exchange: data.1.exchange.to_string(),
            redelivered: data.1.redelivered,
        }
    }
}

impl Message {
    pub async fn ack(&self) -> Result<()> {
        match self.channel.upgrade() {
            Some(channel) => Ok(channel
                .basic_ack(self.delivery_tag, BasicAckOptions::default())
                .await?),
            None => Ok(()),
        }
    }

    pub async fn reject(&self) -> Result<()> {
        match self.channel.upgrade() {
            Some(channel) => Ok(channel
                .basic_reject(self.delivery_tag, BasicRejectOptions::default())
                .await?),
            None => Ok(()),
        }
    }

    pub async fn nack(&self) -> Result<()> {
        match self.channel.upgrade() {
            Some(channel) => Ok(channel
                .basic_nack(self.delivery_tag, BasicNackOptions::default())
                .await?),
            None => Ok(()),
        }
    }

    pub async fn reply(&self, payload: Vec<u8>) -> Result<()> {
        if let Some(channel) = self.channel.upgrade() {
            let reply_to = self
                .properties
                .reply_to()
                .as_ref()
                .ok_or(Error::Reply("missing reply_to property".into()))?
                .as_str();
            let correlation_id = self
                .properties
                .correlation_id()
                .as_ref()
                .ok_or(Error::Reply("missing correlation_id property".into()))?;
            let props = AmqpProperties::default().with_correlation_id(correlation_id.clone());

            channel
                .basic_publish("", reply_to, Default::default(), payload, props)
                .await?;
        }

        Ok(())
    }
}

pub struct AmqpBroker {
    connection: Connection,
    publisher: Channel,
    group: String,
    subgroup: Option<String>,
    replies: Arc<Mutex<HashMap<String, oneshot::Sender<Message>>>>,
    callback_queue: Option<String>,
}

impl AmqpBroker {
    pub async fn new(
        amqp_uri: &str,
        group: String,
        subgroup: Option<String>,
    ) -> Result<AmqpBroker> {
        let connection = Connection::connect(amqp_uri, ConnectionProperties::default()).await?;
        let publisher = connection.create_channel().await?;

        publisher
            .exchange_declare(
                &group,
                ExchangeKind::Direct,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        Ok(Self {
            connection,
            publisher,
            group,
            subgroup,
            replies: Default::default(),
            callback_queue: None,
        })
    }

    /// Setup a consumer to send responses back to a publisher. When the consumer receives a
    /// message, it should use the `AmqpBroker::reply_to` function to send a response.
    pub async fn with_rpc(mut self) -> Result<Self> {
        let channel = Arc::new(self.connection.create_channel().await?);
        let callback = channel
            .queue_declare(
                "",
                QueueDeclareOptions {
                    exclusive: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await?;
        self.callback_queue = Some(callback.name().to_string());

        let mut callback_consumer = channel
            .basic_consume(
                callback.name().as_str(),
                "",
                BasicConsumeOptions {
                    no_ack: true,
                    ..BasicConsumeOptions::default()
                },
                FieldTable::default(),
            )
            .await?;

        let replies = Arc::clone(&self.replies);
        tokio::spawn(async move {
            while let Some(Ok((_, delivery))) = callback_consumer.next().await {
                if let Some(correlation_id) = delivery.properties.correlation_id() {
                    if let Some(sender) = replies.lock().await.remove(correlation_id.as_str()) {
                        let _ = sender.send((Arc::downgrade(&channel), delivery).into());
                    }
                }
            }
        });

        Ok(self)
    }

    pub async fn publish(
        &self,
        evt: &str,
        payload: Vec<u8>,
        properties: AmqpProperties,
    ) -> Result<()> {
        debug!(
            "[Rustacles_BROKER] Publishing event: {} to the AMQP server.",
            evt
        );

        self.publisher
            .basic_publish(
                self.group.as_str(),
                evt,
                BasicPublishOptions {
                    mandatory: true,
                    immediate: false,
                },
                payload,
                properties,
            )
            .await?;

        Ok(())
    }

    pub async fn call(
        &self,
        evt: &str,
        payload: Vec<u8>,
        properties: AmqpProperties,
    ) -> Result<Message> {
        let call_id = nanoid!();
        let properties = properties
            .with_correlation_id(call_id.as_str().into())
            .with_reply_to(
                self.callback_queue
                    .clone()
                    .ok_or(Error::Reply("RPC is not configured for this client".into()))?
                    .into(),
            );

        let (tx, rx) = oneshot::channel();
        self.replies.lock().await.insert(call_id, tx);

        self.publisher
            .basic_publish(
                self.group.as_str(),
                evt,
                Default::default(),
                payload,
                properties,
            )
            .await?;

        Ok(rx.await?)
    }

    pub async fn consume(&self, evt: &str) -> Result<mpsc::UnboundedReceiver<Message>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let queue_name = match &self.subgroup {
            Some(g) => format!("{}:{}:{}", self.group, g, evt),
            None => format!("{}:{}", self.group, evt),
        };
        let channel = Arc::new(self.connection.create_channel().await?);
        let queue = channel
            .queue_declare(
                &queue_name,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;
        channel
            .queue_bind(
                &queue_name,
                &self.group,
                evt,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;

        let mut consumer = channel
            .basic_consume(
                queue.name().as_str(),
                "",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        tokio::spawn(async move {
            while let Some(delivery) = consumer.next().await {
                let (_, delivery) = delivery.expect("Error in consumer");
                tx.send((Arc::downgrade(&channel), delivery).into())
                    .expect("Failed to send message to stream");
            }
        });

        Ok(rx)
    }
}

#[cfg(test)]
mod test {
    use super::AmqpBroker;
    use crate::error::*;
    use tokio::{
        spawn,
        time::{timeout, Duration},
    };

    #[tokio::test]
    async fn makes_rpc_call() -> Result<()> {
        let client = AmqpBroker::new("amqp://localhost:5672/%2f", "foo".into(), None).await?;
        let rpc_client = AmqpBroker::new("amqp://localhost:5672/%2f", "foo".into(), None)
            .await?
            .with_rpc()
            .await?;

        println!("Connected both clients");

        let mut consumer = client
            .consume("bar")
            .await
            .expect("Unable to begin message consumption");

        spawn(async move {
            let message = consumer.recv().await.expect("Consumer closed unexpectedly");

            println!("Received message: {:?}", message);
            message
                .reply("def".as_bytes().to_vec())
                .await
                .expect("Unable to send response");
        });

        println!("Attempting RPC call");

        let response = timeout(
            Duration::from_secs(5),
            rpc_client.call("bar", "abc".as_bytes().to_vec(), Default::default()),
        )
        .await
        .expect("Call timed out")?;

        assert_eq!(response.data.as_slice(), "def".as_bytes());
        Ok(())
    }
}
