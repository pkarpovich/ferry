//! ferry: a call bus between two hosts over one ssh stream.

pub mod cli;
pub mod config;
pub mod frame;
pub mod handlers;
pub mod name;
pub mod paths;
pub mod runner;
pub mod session;
pub mod writer;
