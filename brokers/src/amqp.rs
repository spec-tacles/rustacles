use std::{collections::HashMap, sync::Arc};

pub use lapin::message::Delivery;
use lapin::{
    options::*, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties,
    ExchangeKind,
};
use nanoid::nanoid;
pub use tokio::{
    stream::StreamExt,
    sync::{mpsc, oneshot, Mutex},
};

use crate::errors::*;

pub type AmqpProperties = BasicProperties;

pub struct AmqpBroker {
    connection: Connection,
    publisher: Channel,
    group: String,
    subgroup: Option<String>,
    replies: Arc<Mutex<HashMap<String, oneshot::Sender<Delivery>>>>,
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
        let channel = self.connection.create_channel().await?;
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
            let _ = channel; // channel needs to be moved here so that it isn't immediately dropped and closed
            while let Some(Ok((_, delivery))) = callback_consumer.next().await {
                if let Some(correlation_id) = delivery.properties.correlation_id() {
                    if let Some(sender) = replies.lock().await.remove(correlation_id.as_str()) {
                        let _ = sender.send(delivery);
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
                BasicPublishOptions::default(),
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
    ) -> Result<Delivery> {
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

    pub async fn reply_to(&self, msg: &Delivery, payload: Vec<u8>) -> Result<()> {
        let reply_to = msg
            .properties
            .reply_to()
            .as_ref()
            .ok_or(Error::Reply("missing reply_to property".into()))?
            .as_str();
        let correlation_id = msg
            .properties
            .correlation_id()
            .as_ref()
            .ok_or(Error::Reply("missing correlation_id property".into()))?;
        let props = AmqpProperties::default().with_correlation_id(correlation_id.clone());

        self.publisher
            .basic_publish("", reply_to, Default::default(), payload, props)
            .await?;

        Ok(())
    }

    pub async fn consume(&self, evt: &str) -> Result<mpsc::UnboundedReceiver<Delivery>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let queue_name = match &self.subgroup {
            Some(g) => format!("{}:{}:{}", self.group, g, evt),
            None => format!("{}:{}", self.group, evt),
        };
        let channel = self.connection.create_channel().await?;
        channel
            .exchange_declare(
                &self.group,
                ExchangeKind::Direct,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;
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
                channel
                    .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                    .await
                    .expect("Failed to acknowledge message.");
                tx.send(delivery).expect("Failed to send message to stream");
            }
        });

        Ok(rx)
    }
}
