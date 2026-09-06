//! Physical streaming replication: primary WAL feeder + read-only replica.
//!
//! Protocol (see `protocol.rs`): `[u32 BE len][0x52][type][payload]` frames
//! over a dedicated TCP port. The primary authenticates replicas via
//! `auth.bin`, feeds durable WAL bytes in `(generation, offset)` space,
//! and serves whole snapshots (checkpoint + HDBB archive) whenever a
//! replica is fresh or diverged. The replica applies committed batches and
//! snapshots to its live database and serves reads; writes are rejected in
//! the engine (`Error::ReadOnlyReplica`).

pub mod primary;
pub mod protocol;
pub mod replica;

#[cfg(test)]
mod tests;
