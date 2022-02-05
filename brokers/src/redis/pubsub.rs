use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{future::ready, Stream, StreamExt, TryStream, TryStreamExt};
use pin_project::{pin_project, pinned_drop};
use redis_subscribe::{Message, RedisSub};
use tokio::{spawn, sync::broadcast};
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};

use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct BroadcastSub {
    pubsub: Arc<RedisSub>,
    pubsub_msgs: broadcast::Sender<Arc<Message>>,
}

impl BroadcastSub {
    pub fn new(addr: &str) -> Self {
        let pubsub = Arc::new(RedisSub::new(addr));

        let (tx, _) = broadcast::channel(1);
        let task_pubsub = Arc::clone(&pubsub);
        let task_tx = tx.clone();
        spawn(async move {
            let mut stream = task_pubsub.listen().await.unwrap();
            while let Some(msg) = stream.next().await {
                let _ = task_tx.send(Arc::new(msg));
            }
        });

        Self {
            pubsub,
            pubsub_msgs: tx,
        }
    }

    pub async fn subscribe(
        &self,
        channel: String,
    ) -> Result<impl TryStream<Ok = String, Error = Error>> {
        self.pubsub.subscribe(channel.clone()).await?;

        let stream = SubStream {
            pubsub: Arc::clone(&self.pubsub),
            channel: channel.clone(),
            msgs: BroadcastStream::new(self.pubsub_msgs.subscribe()),
        };

        Ok(stream.err_into::<Error>().try_filter_map(move |msg| {
            ready(match &*msg {
                Message::Message {
                    channel: new_ch,
                    message,
                } if &channel == new_ch => Ok(Some(message.clone())),
                _ => Ok(None),
            })
        }))
    }
}

#[derive(Debug)]
#[pin_project(PinnedDrop)]
struct SubStream<T> {
    pubsub: Arc<RedisSub>,
    channel: String,
    #[pin]
    msgs: BroadcastStream<T>,
}

impl<T> Stream for SubStream<T>
where
    T: 'static + Clone + Send,
{
    type Item = Result<T, BroadcastStreamRecvError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.msgs.poll_next(cx)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for SubStream<T> {
    fn drop(self: Pin<&mut Self>) {
        let client = Arc::clone(&self.pubsub);
        let channel = self.channel.clone();
        spawn(async move {
            client.unsubscribe(channel).await.unwrap();
        });
    }
}
