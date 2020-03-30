use std::{
    io::{Error as IoError, ErrorKind},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use async_native_tls::TlsStream;
use async_std::{
    net::TcpStream,
    stream,
};
use async_std::prelude::*;
use async_tungstenite::{
    stream::Stream as TungsteniteStream,
    tungstenite::protocol::{Message as WebsocketMessage, WebSocketConfig},
    WebSocketStream,
};
use futures::{channel::mpsc::{self, UnboundedSender}, Sink, stream::SplitStream, StreamExt};
use parking_lot::Mutex;

use rustacles_model::{
    gateway::{
        GatewayEvent,
        HeartbeatPacket,
        HelloPacket,
        IdentifyPacket,
        IdentifyProperties,
        Opcodes,
        ReadyPacket,
        ReceivePacket,
        ResumeSessionPacket,
        SendablePacket,
    },
    presence::{ClientActivity, ClientPresence, Status},
};

use crate::{
    constants::GATEWAY_VERSION,
    errors::{Error, Result},
};

pub type ShardStream = SplitStream<WebSocketStream<TungsteniteStream<TcpStream, TlsStream<TcpStream>>>>;

/// Various actions that a shard can perform.
enum ShardAction {
    None,
    AutoReconnect,
    Reconnect,
    Identify,
    Resume,
}

/// A shard's heartbeat information.
#[derive(Debug, Copy, Clone)]
pub struct Heartbeat {
    pub acknowledged: bool,
    pub seq: u64,
}

impl Heartbeat {
    fn new() -> Heartbeat {
        Self { acknowledged: false, seq: 0 }
    }
}


#[derive(Clone)]
pub struct Shard {
    /// The bot token that this shard will use.
    pub token: String,
    /// The shard's info. Includes the shard's ID and the total amount of shards.
    pub info: [usize; 2],
    /// The currently active presence for this shard.
    pub presence: ClientPresence,
    /// The session ID of this shard, if applicable.
    pub session_id: Option<String>,
    /// The interval at which a heartbeat is made.
    pub interval: Option<u64>,
    /// The websocket stream of the shard.
    pub stream: Arc<Mutex<Option<ShardStream>>>,
    /// The channel used to send shard messages to Discord..
    pub sender: Arc<Mutex<UnboundedSender<WebsocketMessage>>>,
    /// The current heartbeat of the shard.
    pub heartbeat: Arc<Mutex<Heartbeat>>,
    current_state: Arc<Mutex<String>>,
    ws_uri: String,
}

impl Shard {
    /// Creates a new Discord shard with the provided token.
    pub async fn new(token: String, info: [usize; 2], ws_uri: String) -> Result<Self> {
        let (sender, stream) = Shard::connect(&ws_uri).await?;
        Ok(Shard {
            current_state: Arc::new(Mutex::new(String::from("handshake"))),
            token,
            session_id: None,
            presence: ClientPresence { status: String::from("online"), ..Default::default() },
            info,
            interval: None,
            heartbeat: Arc::new(Mutex::new(Heartbeat::new())),
            sender: Arc::new(Mutex::new(sender)),
            stream: Arc::new(Mutex::new(Some(stream))),
            ws_uri,
        })
    }

    /*pub fn identify(&self) -> Result<()> {

    }

    pub async fn autoreconnect(&self) -> Result<()> {

    }

    pub async fn resume(&mut self) -> Result<()> {

    }
    */


    async fn connect(ws: &str) -> Result<(UnboundedSender<WebsocketMessage>, ShardStream)> {
        let (wstream, _) = async_tungstenite::async_std::connect_async_with_config(ws, Some(WebSocketConfig {
            max_message_size: Some(usize::max_value()),
            max_frame_size: Some(usize::max_value()),
            ..Default::default()
        })).await?;
        let (tx, rx) = mpsc::unbounded();
        let (sink, stream) = wstream.split();

        async_std::task::spawn(async {
            rx.forward(sink).await.expect("Failed to send message to sink.");
        });

        Ok((tx, stream))
    }
}