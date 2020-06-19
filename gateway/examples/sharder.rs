extern crate log;

use std::env;

use futures::StreamExt;

use rustacles_gateway::{ShardManager, ShardStrategy};

#[tokio::main]
async fn main() {
    env_logger::init();
    let token = env::var("DISCORD_TOKEN").expect("No Discord token provided");
    let strategy = match env::var("SHARD_COUNT") {
        Ok(num) => ShardStrategy::Spawn(num.parse::<usize>().expect("Invalid integer provided")),
        Err(_) => ShardStrategy::Recommended,
    };
    let mut manager = ShardManager::new(token, strategy)
        .await
        .expect("Failed to create shard manager");

    manager.spawn();

    let mut events = manager.events.take().unwrap();
    let mut spawner = manager.spawner.take().unwrap();

    tokio::spawn(async move {
        while let Some(shard) = spawner.next().await {
            println!("Shard {:?} spawned.", shard.lock().info);
        }
    });

    tokio::spawn(async move {
        while let Some(event) = events.next().await {
            if let Some(evt) = event.packet.t {
                println!(
                    "Received event from Shard {:?}: {:?}",
                    event.shard.lock().info,
                    evt
                );
            }
        }
    })
    .await
    .unwrap();
}
