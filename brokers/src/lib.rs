#[macro_use] extern crate log;

pub mod amqp;
mod errors;

#[cfg(test)]
#[path = "./amqp_test.rs"]
mod amqp_test;