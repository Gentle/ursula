//! Shared wire types.
//!
//! Module map:
//!
//! - [`durable`]: generated stream and storage protobuf messages.
//! - [`admin`]: shared administrative HTTP requests, responses and identity preconditions.
//! - [`headers`]: client-plane response headers shared by the server and the gateway.

pub mod admin;
pub mod headers;

pub mod durable {
    include!(concat!(env!("OUT_DIR"), "/ursula.durable.v1.rs"));
}

pub use durable::*;
