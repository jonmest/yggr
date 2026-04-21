//! Jotun — a Raft node runtime built on
//! [`yggr_core`].
//!
//! Users implement [`StateMachine`] (your application's apply/restore logic) and call
//! [`Node::start`]; the runtime owns the engine, the network
//! transport, the on-disk persistence, the tick driver, and the
//! action dispatcher. Incoming snapshots are installed and restored
//! automatically. By default the runtime also reacts to snapshot hints
//! from the engine by calling [`StateMachine::snapshot`] and feeding
//! the resulting bytes back into Raft; the serializer runs on a
//! separate task so a slow [`StateMachine::snapshot`] does not stall
//! ticks, heartbeats, or [`Node::status`]. `snapshot` is fallible
//! ([`SnapshotError`]); returning `Err` just skips this hint and the
//! engine retries on the next one. Set
//! [`Config::snapshot_hint_threshold_entries`] to `0` (and
//! [`Config::max_log_entries`]) if you want to disable host-initiated
//! compaction entirely.
//!
//! Observability is pull-model: [`Node::metrics`] returns a snapshot
//! of counters and gauges covering elections, replication, commits,
//! applies, reads, and snapshots.
//!
//! For users who want different transport, storage, or concurrency,
//! [`Storage`] and [`Transport`] are traits — the defaults are
//! TCP + a hand-rolled segmented log on disk, but anything that
//! satisfies the trait works.
//!
//! Internals (the engine state machine, log entries, message types)
//! live in [`yggr_core`] and are re-exported here for convenience.

#![cfg_attr(
    test,
    allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing,)
)]

pub mod node;
pub mod state_machine;
pub mod storage;
pub mod transport;

pub use node::{
    Bootstrap, Config, ConfigError, Node, NodeStartError, NodeStatus, ProposeError, ReadError,
    Role, TransferLeadershipError,
};
pub use state_machine::{DecodeError, SnapshotError, StateMachine};
pub use storage::{DiskStorage, DiskStorageError, Storage, StoredHardState, StoredSnapshot};
pub use transport::{TcpTransport, TcpTransportError, Transport};

// Convenience re-exports from yggr-core. Saves users from having to
// `use yggr_core::...` for the basics; the engine itself stays
// addressable via that crate for power-user integrations.
pub use yggr_core::{ConfigChange, LogIndex, NodeId, Term};
