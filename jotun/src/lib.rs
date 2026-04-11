//! Jotun — batteries-included Raft node runtime built on
//! [`jotun_core`].
//!
//! This crate is the answer for users who want a clustered service
//! without writing the engine integration themselves. You implement
//! [`StateMachine`] (your application's apply/snapshot/restore) and
//! call [`Node::start`]; the runtime owns the engine, the network
//! transport, the on-disk persistence, the tick driver, and the
//! action dispatcher.
//!
//! For users who want different transport, storage, or concurrency,
//! [`Storage`] and [`Transport`] are traits — the defaults are
//! TCP + a hand-rolled segmented log on disk, but anything that
//! satisfies the trait works.
//!
//! Internals (the engine state machine, log entries, message types)
//! live in [`jotun_core`] and are re-exported here for convenience.

#![cfg_attr(
    test,
    allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing,)
)]

pub mod state_machine;
pub mod storage;
pub mod transport;

pub use state_machine::{DecodeError, StateMachine};
pub use storage::{DiskStorage, DiskStorageError, Storage, StoredHardState, StoredSnapshot};
pub use transport::{TcpTransport, TcpTransportError, Transport};

// Convenience re-exports from jotun-core. Saves users from having to
// `use jotun_core::...` for the basics; the engine itself stays
// addressable via that crate for power-user integrations.
pub use jotun_core::{ConfigChange, LogIndex, NodeId, Term};
