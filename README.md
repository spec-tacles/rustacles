# Rustacles

Rustacles is a distributed Discord library, a Rust port included in the Spectacles family of libraries.

## Introduction
Rustacles allows you to create components which interact with the Discord API using a microservices-like architecture. All components are usually unified through a message brokering system, which means they can be fully distributed (spread out acress servers).

Imagine the following: You want to push a major update in your bot, but you fear the downtime that it would bring. With Rustacles, you can have your application split up into "workers", each of whom will consume Discord events from the message broker. So if you take down one worker to update it, the other worker can still receive events, thus acheiving zero downtime.

The microservices architecture is also very beneficial in the sense that you can scale your bot's components with ease. If your two workers are receiving a lot of load, simply add a third worker, for improved load balancing. If you so choose, you may even have your bot use different programming languages for each service. For example, you could have your gateway in Rust, and your workers in Golang. They will all come together with help from the message broker.

## Getting started
This library features several important crates to help you get started.

[Rustacles Brokers](brokers/) - Message brokers which allow for powerful communication between services.

[Rustacles Gateway](gateway/) - A Spectacles gateway implementation for Discord with enables stateless sharding.

[Rustacles Models](models/) - A collection of data types than are used for serializing and deserializing data from the Discord API.

[Rustacles REST](rest/) - A Spectacles implementation of Discord's REST API methods.

See each crate for more details on how to get started with your application.