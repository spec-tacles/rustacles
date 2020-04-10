# Rustacles Gateway

Rustacles Gateway provides a Spectacles-based interface for the Discord Websocket Gateway.

## Features
- Asynchronous websocket message handling
- Zero-Downtime shard spawning
- Shard session resuming

## How to Use

Shards can be easily spawned with the `ShardManager` struct.

```rust
use rustacles_gateway::{ShardManager, ShardStrategy};
use std::env::var;


#[tokio::main]
async fn main() {
    let token = var("DISCORD_TOKEN").expect("No Discord token provided");
    let strategy = match env::var("SHARD_COUNT") {
        Ok(num) => ShardStrategy::Spawn(num.parse::<usize>().expect("Invalid integer provided")),
        Err(_) => ShardStrategy::Recommended
    };
    let manager = ShardManager::new(token, strategy).await.expect("Failed to initialize shard manager");    
    let (mut spawner, mut events) = manager.spawn();

    // Handle shards and events here
}
```

