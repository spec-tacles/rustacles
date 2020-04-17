use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_tungstenite::tungstenite::Message as TungsteniteMessage;
use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    StreamExt, TryStreamExt,
};
use futures::Stream;
use futures::task::{Context, Poll};
use hashbrown::HashMap;
use parking_lot::{Mutex, RwLock};

use rustacles_model::gateway::{GatewayBot, Opcodes, ReceivePacket};

use crate::{
    constants::API_BASE,
    errors::*,
    queue::*,
    shard::{Shard, ShardAction},
};

/// The strategy in which you would like to spawn shards.
#[derive(Clone)]
pub enum ShardStrategy {
    /// The spawner will automatically spawn shards based on the amount recommended by Discord.
    Recommended,
    /// Spawns shards according to the amount specified, starting from shard 0.
    Spawn(usize),
}

#[derive(Clone)]
/// Information about a Discord Gateway event received for a shard.
pub struct ShardEvent {
    /// The shard which emitted this event.
    pub shard: ManagerShard,
    /// The Discord Gateway packet that the event contains.
    pub packet: ReceivePacket,
}

/// An alias for a shard spawned with the sharding manager.
pub type ManagerShard = Arc<Mutex<Shard>>;
type MessageStream = UnboundedReceiver<(ManagerShard, TungsteniteMessage)>;

/// A collection of shards, keyed by shard ID.
pub type ShardMap = HashMap<usize, ManagerShard>;

// A stream of shards being spawned and emitting the ready event.
pub struct Spawner {
    inner: UnboundedReceiver<ManagerShard>
}

impl Spawner {
    fn new(receiver: UnboundedReceiver<ManagerShard>) -> Self {
        Spawner { inner: receiver }
    }
}

impl Stream for Spawner {
    type Item = ManagerShard;
    fn poll_next(mut self: Pin<&mut Self>, waker: &mut Context) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(waker)
    }
}

/// A stream which serves as an event handler for a shard.
pub struct EventHandler {
    inner: UnboundedReceiver<ShardEvent>
}

impl EventHandler {
    fn new(receiver: UnboundedReceiver<ShardEvent>) -> Self {
        EventHandler { inner: receiver }
    }
}

impl Stream for EventHandler {
    type Item = ShardEvent;
    fn poll_next(mut self: Pin<&mut Self>, waker: &mut Context) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(waker)
    }
}

/// The central hub for all shards, where shards are spawned and maintained.
pub struct ShardManager {
    /// The token used by this manager to spawn shards.
    pub token: String,
    /// The total amount of shards that this manager will attempt to spawn.
    pub total_shards: usize,
    /// A collection of shards that have been spawned.
    pub shards: Arc<RwLock<ShardMap>>,
    pub spawner: Option<Spawner>,
    pub events: Option<EventHandler>,
    event_sender: Option<UnboundedSender<ShardEvent>>,
    message_stream: Option<MessageStream>,
    ws_uri: String,
}

impl ShardManager {
    /// Creates a new shard manager, with the provided Discord token and strategy.
    pub async fn new(token: String, strategy: ShardStrategy) -> Result<ShardManager> {
        let token = if token.starts_with("Bot ")
        { token } else { format!("Bot {}", token) };

        let res = reqwest::Client::new().get(&format!("{}/gateway/bot", API_BASE))
            .header("Authorization", token.clone())
            .send()
            .await?;

        let gb: GatewayBot = res.json().await?;
        let total_shards = match strategy {
            ShardStrategy::Recommended => gb.shards,
            ShardStrategy::Spawn(int) => int
        };

        Ok(ShardManager {
            token,
            total_shards,
            shards: Arc::new(RwLock::new(HashMap::new())),
            spawner: None,
            event_sender: None,
            events: None,
            message_stream: None,
            ws_uri: gb.url,
        })
    }

    /// Starts the shard spawning process. Returns two streams, one for successfully spawned shards, and one for shard events.
    pub fn spawn(&mut self) {
        let (sender, receiver) = unbounded();
        let (tx, rx) = unbounded();
        self.message_stream = Some(receiver);
        self.spawner = Some(Spawner::new(rx));
        self.events = Some(self.handle_events());

        let shard_count = self.total_shards.clone();
        let token = self.token.clone();
        let ws = self.ws_uri.clone();
        let shard_map = self.shards.clone();
        debug!("Preparing to spawn {} shards.", &shard_count);

        tokio::spawn(async move {
            for id in 0..shard_count {
                debug!("CREATING Shard {}", id);
                tokio::time::delay_for(Duration::from_secs(6)).await;
                let count = shard_count.clone();
                let shard = Shard::new(token.clone(), [id, count], ws.clone()).await;
                let shard = Arc::new(Mutex::new(shard.expect("Failed to create shard")));
                shard_map.write().insert(id, shard.clone());

                let sink = MessageSink {
                    shard: shard.clone(),
                    sender: sender.clone(),
                };
                let split = shard.lock().stream.lock().take().unwrap().map_err(MessageSinkError::from);
                tokio::spawn(async {
                    split.forward(sink).await.expect("Failed to forward shard message to sink");
                });

                tx.unbounded_send(shard).expect("Failed to send shard to stream");
            };
            info!("Sharder has completed spawning shards.");
        });
    }

    fn handle_events(&mut self) -> EventHandler {
        let mut stream = self.message_stream.take().unwrap();
        let (sender, receiver) = unbounded();
        self.event_sender = Some(sender.clone());

        let handler = async move {
            while let Some((shard, message)) = stream.next().await {
                let current_shard = shard.clone();
                let mut shard = current_shard.lock().clone();
                trace!("Websocket message received: {:?}", &message.clone());
                let event = shard.resolve_packet(&message.clone()).expect("Failed to parse the shard message");
                if let Opcodes::Dispatch = event.op {
                    sender.unbounded_send(ShardEvent {
                        packet: event.clone(),
                        shard: current_shard.clone(),
                    }).expect("Failed to send shard event to stream");
                };
                let action = shard.handle_packet(event.clone());
                if let Ok(ShardAction::AutoReconnect) = action {
                    shard.autoreconnect().await.expect("Shard failed to autoreconnect.");
                } else if let Ok(ShardAction::Identify) = action {
                    debug!("[Shard {}] Identifying with the gateway.", &shard.info[0]);
                    if let Err(e) = shard.identify() {
                        warn!("[Shard {}] Failed to identify with gateway. {:?}", &shard.info[0], e);
                    };
                } else if let Ok(ShardAction::Reconnect) = action {
                    shard.reconnect().await.expect("Failed to reconnect shard.");
                    info!("[Shard {}] Reconnection successful.", &shard.info[0]);
                } else if let Ok(ShardAction::Resume) = action {
                    shard.resume().await.expect("Failed to resume shard session.");
                    info!("[Shard {}] Successfully resumed session.", &shard.info[0]);
                };
            }
        };
        tokio::spawn(handler);

        EventHandler::new(receiver)
    }
}

