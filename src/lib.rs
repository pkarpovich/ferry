//! ferry: a call bus between two hosts over one ssh stream.

pub mod cli;
pub mod client;
pub mod config;
pub mod daemon;
pub mod dial;
pub mod frame;
pub mod handlers;
pub mod local;
pub mod name;
pub mod paths;
pub mod peer;
pub mod runner;
pub mod session;
pub mod writer;
