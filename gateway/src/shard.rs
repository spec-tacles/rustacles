use std::{
    io::{Error as IoError, ErrorKind},
    sync::Arc,
    time::Duration,
};

use async_tungstenite::{
    stream::Stream as TungsteniteStream,
    tokio::TokioAdapter,
    tungstenite::{
        protocol::{Message as WebsocketMessage, WebSocketConfig},
        Error as TungsteniteError,
    },
    WebSocketStream,
};
use futures::{
    channel::mpsc::{self, UnboundedSender},
    stream::SplitStream,
    FutureExt, StreamExt, TryStreamExt,
};
use parking_lot::Mutex;
use tokio::net::TcpStream;
use tokio::time;
use tokio_native_tls::TlsStream;

use rustacles_model::{
    gateway::{
        GatewayEvent, HeartbeatPacket, HelloPacket, IdentifyPacket, IdentifyProperties, Opcodes,
        ReadyPacket, ReceivePacket, ResumeSessionPacket, SendablePacket,
    },
    presence::{ClientActivity, ClientPresence, Status},
};

use crate::{
    constants::GATEWAY_VERSION,
    errors::{Error, Result},
};

pub type ShardStream = SplitStream<
    WebSocketStream<
        TungsteniteStream<
            TokioAdapter<TcpStream>,
            TokioAdapter<TlsStream<TokioAdapter<TokioAdapter<TcpStream>>>>,
        >,
    >,
>;

/// Various actions that a shard can perform.
pub(crate) enum ShardAction {
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
        Self {
            acknowledged: false,
            seq: 0,
        }
    }
}

/// A Spectacles gateway shard, representing a connection to the Discord Websocket.
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
    pub sender: Arc<Mutex<UnboundedSender<Result<WebsocketMessage>>>>,
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
            presence: ClientPresence {
                status: String::from("online"),
                ..Default::default()
            },
            info,
            interval: None,
            heartbeat: Arc::new(Mutex::new(Heartbeat::new())),
            sender: Arc::new(Mutex::new(sender)),
            stream: Arc::new(Mutex::new(Some(stream))),
            ws_uri,
        })
    }

    /// Attempts to automatically reconnect the shard to Discord.
    pub async fn autoreconnect(&mut self) -> Result<()> {
        if self.session_id.is_some() && self.heartbeat.lock().seq > 0 {
            self.resume().await
        } else {
            self.reconnect().await
        }
    }

    /// Makes a request to reconnect the shard.
    pub async fn reconnect(&mut self) -> Result<()> {
        debug!(
            "[Shard {}] Attempting to reconnect to gateway.",
            &self.info[0]
        );
        self.reset_values()
            .expect("[Shard] Failed to reset this shard for autoreconnecting.");
        self.dial_gateway().await?;

        Ok(())
    }

    /// Resumes a shard's past session.
    pub async fn resume(&mut self) -> Result<()> {
        debug!(
            "[Shard {}] Attempting to resume gateway connection.",
            &self.info[0]
        );
        let seq = self.heartbeat.lock().seq;
        let token = self.token.clone();
        let session = self.session_id.clone();
        let sender = self.sender.clone();
        match self.dial_gateway().await {
            Ok(_) => {
                let payload = ResumeSessionPacket {
                    session_id: session.unwrap(),
                    seq,
                    token,
                };
                Shard::send(&sender, WebsocketMessage::text(payload.to_json()?))
            }
            Err(e) => Err(e),
        }
    }

    /// Change the status of the current shard.
    pub fn change_status(&mut self, status: Status) -> Result<()> {
        self.presence.status = status.to_string();
        let oldpresence = self.presence.clone();

        self.change_presence(oldpresence)
    }

    /// Change the activity of the current shard.
    pub fn change_activity(&mut self, activity: ClientActivity) -> Result<()> {
        self.presence.game = Some(activity);
        let oldpresence = self.presence.clone();

        self.change_presence(oldpresence)
    }

    /// Change the presence of the current shard.
    pub fn change_presence(&mut self, presence: ClientPresence) -> Result<()> {
        debug!(
            "[Shard {}] Sending a presence change payload. {:?}",
            self.info[0],
            presence.clone()
        );
        self.send_payload(presence.clone())?;
        self.presence = presence;

        Ok(())
    }

    /// Sends an IDENTIFY payload to the Discord Gateway.
    pub fn identify(&mut self) -> Result<()> {
        self.send_payload(IdentifyPacket {
            large_threshold: 250,
            token: self.token.clone(),
            shard: self.info.clone(),
            compress: false,
            presence: Some(self.presence.clone()),
            version: GATEWAY_VERSION,
            properties: IdentifyProperties {
                os: std::env::consts::OS.to_string(),
                browser: String::from("rustacles"),
                device: String::from("rustacles"),
            },
        })
    }

    /// Resolves a Websocket message into a ReceivePacket struct.
    pub fn resolve_packet(&self, mess: &WebsocketMessage) -> Result<ReceivePacket> {
        match mess {
            WebsocketMessage::Binary(v) => serde_json::from_slice(v),
            WebsocketMessage::Text(v) => serde_json::from_str(v),
            _ => unreachable!("Invalid type detected."),
        }
        .map_err(Error::from)
    }

    /// Sends a packet to the Discord Gateway.
    pub fn send_payload<T: SendablePacket>(&self, payload: T) -> Result<()> {
        let json = payload.to_json()?;
        Shard::send(&self.sender, WebsocketMessage::text(json))
    }

    async fn begin_interval(mut shard: Shard, duration: Duration) {
        let mut interval = time::interval(duration);
        let info = shard.info.clone();
        loop {
            interval.tick().await;
            if let Err(r) = shard.heartbeat() {
                warn!("[Shard {}] Failed to perform heartbeat. {:?}", info[0], r);
            }
        }
    }

    async fn connect(ws: &str) -> Result<(UnboundedSender<Result<WebsocketMessage>>, ShardStream)> {
        let (wstream, _) = async_tungstenite::tokio::connect_async_with_config(
            ws,
            Some(WebSocketConfig {
                max_message_size: Some(usize::max_value()),
                max_frame_size: Some(usize::max_value()),
                ..Default::default()
            }),
        )
        .await?;
        let (tx, rx) = mpsc::unbounded();
        let (sink, stream) = wstream.split();

        tokio::spawn(async {
            rx.map_err(|err| {
                error!("Failed to select sink. {:?}", err);
                TungsteniteError::Io(IoError::new(
                    ErrorKind::Other,
                    "Error whilst attempting to select sink.",
                ))
            })
            .forward(sink)
            .map(|_| ())
            .await;
        });

        Ok((tx, stream))
    }

    async fn dial_gateway(&mut self) -> Result<()> {
        *self.current_state.lock() = String::from("connected");
        let state = self.current_state.clone();
        let orig_sender = self.sender.clone();
        let orig_stream = self.stream.clone();
        let heartbeat = self.heartbeat.clone();

        let (sender, stream) = Shard::connect(&self.ws_uri).await?;
        *orig_sender.lock() = sender;
        *heartbeat.lock() = Heartbeat::new();
        *state.lock() = String::from("handshake");
        *orig_stream.lock() = Some(stream);

        Ok(())
    }

    pub(crate) fn handle_packet(&mut self, pkt: ReceivePacket) -> Result<ShardAction> {
        let info = self.info.clone();
        let current_state = self.current_state.lock().clone();

        match pkt.op {
            Opcodes::Dispatch => {
                if let Some(GatewayEvent::READY) = pkt.t {
                    let ready: ReadyPacket = serde_json::from_str(pkt.d.get())?;
                    *self.current_state.lock() = "connected".to_string();
                    self.session_id = Some(ready.session_id.clone());
                    trace!(
                        "[Shard {}] Received ready, set session ID as {}",
                        &info[0],
                        ready.session_id
                    )
                };

                Ok(ShardAction::None)
            }
            Opcodes::HeartbeatAck => {
                let mut hb = self.heartbeat.lock().clone();
                hb.acknowledged = true;

                Ok(ShardAction::None)
            }
            Opcodes::Hello => {
                if self.current_state.lock().clone() == "resume".to_string() {
                    return Ok(ShardAction::None);
                };
                let hello: HelloPacket = serde_json::from_str(pkt.d.get()).unwrap();
                if hello.heartbeat_interval > 0 {
                    self.interval = Some(hello.heartbeat_interval);
                }
                if current_state == "handshake".to_string() {
                    let dn = Duration::from_millis(hello.heartbeat_interval);
                    let shard = self.clone();
                    tokio::spawn(Shard::begin_interval(shard, dn));
                    return Ok(ShardAction::Identify);
                }

                Ok(ShardAction::AutoReconnect)
            }
            Opcodes::Reconnect => Ok(ShardAction::Reconnect),
            Opcodes::InvalidSession => {
                let invalid: bool = serde_json::from_str(pkt.d.get())?;
                if !invalid {
                    Ok(ShardAction::Identify)
                } else {
                    Ok(ShardAction::Resume)
                }
            }
            _ => Ok(ShardAction::None),
        }
    }

    fn heartbeat(&mut self) -> Result<()> {
        debug!("[Shard {}] Sending heartbeat.", self.info[0]);
        let seq = self.heartbeat.lock().seq;

        self.send_payload(HeartbeatPacket { seq })
    }

    fn reset_values(&mut self) -> Result<()> {
        self.session_id = None;
        *self.current_state.lock() = "disconnected".to_string();

        let mut hb = self.heartbeat.lock();
        hb.acknowledged = true;
        hb.seq = 0;

        Ok(())
    }

    fn send(
        sender: &Arc<Mutex<UnboundedSender<Result<WebsocketMessage>>>>,
        mess: WebsocketMessage,
    ) -> Result<()> {
        sender
            .lock()
            .start_send(Ok(mess))
            .map(|_| ())
            .map_err(From::from)
    }
}
