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

    fn poll_ready(&mut self, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.sender.poll_ready().from_err()
    }

    fn start_send(&mut self, item: TungsteniteMessage) -> Result<(), Self::Error> {
        self.sender.start_send((self.shard.clone(), item)).from_err()
    }

    fn poll_flush(&mut self, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.sender.poll_flush().from_err()
    }


    fn poll_close(&mut self, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.sender.poll_close().from_err()
    }
}

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