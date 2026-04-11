//! Trait for the on-disk side of a [`crate::Node`].
//!
//! The engine owns *what* must be persisted (via `Action::PersistHardState`,
//! `Action::PersistLogEntries`, `Action::PersistSnapshot`); this trait
//! is the host's *how*. Implementations decide on serialisation, file
//! layout, fsync cadence — within the contract that durability is
//! atomic per call (a crash during one of these methods either keeps
//! the prior state or persists the new one, never half-applied).
//!
//! The default impl ([`crate::storage::DiskStorage`], not yet built)
//! uses a segmented append-only log + a small hard-state file. Users
//! who want different storage (sled, rocksdb, foundationdb, S3-tiered)
//! plug in their own [`Storage`].

use std::future::Future;

use jotun_core::{LogEntry, LogIndex, NodeId, Term};

/// The §5.1 hard state persisted across crashes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredHardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
}

/// A snapshot persisted on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSnapshot {
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub bytes: Vec<u8>,
}

/// State recovered from disk on startup. All fields are `None` /
/// empty for a node that has never run before.
#[derive(Debug, Clone, Default)]
pub struct RecoveredState<C> {
    pub hard_state: Option<StoredHardState>,
    pub snapshot: Option<StoredSnapshot>,
    /// Log entries with index strictly past `snapshot.last_included_index`
    /// (or starting at 1 if no snapshot). Indices are dense.
    pub log: Vec<LogEntry<C>>,
}

/// The host's interface to durable storage. Every method MUST commit
/// to disk before returning success — the runtime depends on this for
/// the §5.1 "respond after persisting" contract.
pub trait Storage<C>: Send + 'static
where
    C: Send + 'static,
{
    /// One-shot error type. Implementations decide granularity; the
    /// runtime treats every error as fatal and shuts the node down.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Read everything we previously persisted, or default-empty if
    /// this is a brand-new node. Called once at startup.
    fn recover(&mut self) -> impl Future<Output = Result<RecoveredState<C>, Self::Error>> + Send;

    /// Durably write the new hard state. Must replace any prior
    /// hard state atomically.
    fn persist_hard_state(
        &mut self,
        state: StoredHardState,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Durably append `entries` to the log. The runtime promises
    /// indices are contiguous with whatever's already on disk; an
    /// implementation may rely on that to pre-allocate.
    fn append_log(
        &mut self,
        entries: Vec<LogEntry<C>>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Durably truncate the log to entries with index strictly less
    /// than `from`. Used by followers reconciling against a leader.
    fn truncate_log(
        &mut self,
        from: LogIndex,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Durably install a snapshot, replacing any prior snapshot AND
    /// dropping log entries with index ≤ `snapshot.last_included_index`.
    /// Atomic — recovery never sees a half-installed snapshot.
    fn persist_snapshot(
        &mut self,
        snapshot: StoredSnapshot,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
