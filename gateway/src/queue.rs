use std::pin::Pin;
use std::sync::Arc;

use async_tungstenite::tungstenite::{Error as TungsteniteError, Message as TungsteniteMessage};
use futures::{channel::mpsc::{SendError, UnboundedSender}, Sink};
use futures::task::{Context, Poll};
use parking_lot::Mutex;

use crate::Shard;

pub(crate) struct MessageSink {
    pub shard: Arc<Mutex<Shard>>,
    pub sender: UnboundedSender<(Arc<Mutex<Shard>>, TungsteniteMessage)>,
}

impl Sink<TungsteniteMessage> for MessageSink {
    type Error = MessageSinkError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.sender.poll_ready(cx).map_err(From::from)
    }

    fn start_send(mut self: Pin<&mut Self>, item: TungsteniteMessage) -> Result<(), Self::Error> {
        let sd = self.shard.clone();
        UnboundedSender::start_send(
            &mut self.sender.clone(),
            (sd, item),
        ).map_err(From::from)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.sender.disconnect();
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
pub enum MessageSinkError {
    MpscSend(SendError),
    Tungstenite(TungsteniteError),
}


impl From<SendError> for MessageSinkError {
    fn from(e: SendError) -> Self {
        MessageSinkError::MpscSend(e)
    }
}

impl From<TungsteniteError> for MessageSinkError {
    fn from(e: TungsteniteError) -> Self {
        MessageSinkError::Tungstenite(e)
    }
}