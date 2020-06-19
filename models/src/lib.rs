//! A collection of data types for working with various Spectacles modules.

#[macro_use]
extern crate serde_derive;

pub use snowflake::*;
pub use user::User;

#[derive(Serialize)]
pub struct NoQuery;

pub mod channel;
pub mod gateway;
pub mod guild;
pub mod invite;
pub mod message;
pub mod presence;
pub mod snowflake;
mod user;
pub mod voice;
