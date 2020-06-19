use std::borrow::Borrow;
use std::pin::Pin;

use futures::channel::mpsc::{unbounded, UnboundedReceiver};
use futures::task::{Context, Poll};
use futures::{Stream, StreamExt};
use lapin::{
    options::*, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties,
    Consumer as ConsumerStream, ExchangeKind,
};

use crate::errors::*;

pub type AmqpProperties = BasicProperties;

/// A stream of messages that are being consumed in the message queue.
pub struct AmqpConsumer {
    recv: UnboundedReceiver<Vec<u8>>,
}

impl AmqpConsumer {
    fn new(recv: UnboundedReceiver<Vec<u8>>) -> Self {
        Self { recv }
    }
}

impl Stream for AmqpConsumer {
    type Item = Vec<u8>;
    fn poll_next(mut self: Pin<&mut Self>, waker: &mut Context) -> Poll<Option<Self::Item>> {
        self.recv.poll_next_unpin(waker)
    }
}

pub struct AmqpBroker {
    consumer: Consumer,
    producer: Producer,
    group: String,
    subgroup: Option<String>,
}

pub struct Consumer(Connection);

pub struct Producer {
    channel: Channel,
}

impl AmqpBroker {
    pub async fn new(
        amqp_uri: String,
        group: String,
        subgroup: Option<String>,
    ) -> Result<AmqpBroker> {
        let producer =
            Connection::connect(amqp_uri.as_str().into(), ConnectionProperties::default()).await?;
        let consumer =
            Connection::connect(amqp_uri.as_str().into(), ConnectionProperties::default()).await?;
        let channel = producer.create_channel().await?;

        Ok(Self {
            consumer: Consumer(consumer),
            producer: Producer { channel },
            group,
            subgroup,
        })
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

        self.producer
            .channel
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

    pub async fn consume(&self, evt: &str) -> Result<AmqpConsumer> {
        let (tx, rx) = unbounded();
        let queue_name = match &self.subgroup {
            Some(g) => format!("{}:{}:{}", self.group, g, evt),
            None => format!("{}:{}", self.group, evt),
        };
        let group = self.group.clone();
        let event = evt.to_string();
        let channel: Channel = self.consumer.0.create_channel().await?;
        channel
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
        channel
            .queue_bind(
                &queue_name,
                &group,
                &event,
                QueueBindOptions::default(),
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
        let mut consumer: ConsumerStream = channel
            .basic_consume(
                queue.borrow(),
                "",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;
        tokio::spawn(async move {
            while let Some(Ok((channel, delivery))) = consumer.next().await {
                tx.unbounded_send(delivery.data)
                    .expect("Failed to send message to stream");
                channel
                    .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                    .await
                    .expect("Failed to acknowledge message.");
            }
        });

        Ok(AmqpConsumer::new(rx))
    }
}
