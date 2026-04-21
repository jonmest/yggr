//! Pull-model observability for the engine. Counters tick as the
//! engine processes events; gauges reflect current state. Snapshot
//! via [`crate::engine::engine::Engine::metrics`].
//!
//! Every counter is a `u64` that only moves forward within a single
//! `Engine` instance. Wraparound is not modelled — a Raft node that
//! processes 2^64 AEs has bigger problems. The struct is
//! `#[non_exhaustive]` so fields can be added without a breaking
//! change.

use crate::types::{index::LogIndex, term::Term};

/// A snapshot of the engine's metric counters and gauges. Produced by
/// [`crate::engine::engine::Engine::metrics`].
///
/// Counters only increase over the lifetime of the engine; gauges
/// track the current value of the underlying state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct EngineMetrics {
    // -------- election counters --------
    /// Elections this node has started. Counts both classic
    /// `Candidate` bumps and §9.6 `PreCandidate` probes — a
    /// pre-election that promotes to a real election counts once.
    pub elections_started: u64,
    /// Pre-vote probes this node has granted as a responder.
    pub pre_votes_granted: u64,
    /// Pre-vote probes this node has denied as a responder.
    pub pre_votes_denied: u64,
    /// Real `RequestVote`s this node has granted as a responder.
    pub votes_granted: u64,
    /// Real `RequestVote`s this node has denied as a responder.
    pub votes_denied: u64,
    /// Elections this node won (transitioned to `Leader`).
    pub leader_elections_won: u64,
    /// Times this node stepped down because it saw a higher term.
    pub higher_term_stepdowns: u64,

    // -------- replication counters --------
    /// `AppendEntriesRequest`s this node has emitted as leader
    /// (including heartbeats, which are just empty AEs).
    pub append_entries_sent: u64,
    /// `AppendEntriesRequest`s this node has received as follower.
    pub append_entries_received: u64,
    /// `AppendEntriesResponse`s this node has emitted with
    /// `success=false` (log mismatch, stale leader, or log gap).
    pub append_entries_rejected: u64,
    /// Total log entries appended to the local log across every
    /// accepted AE.
    pub entries_appended: u64,
    /// Cumulative `commit_index` advance.
    pub entries_committed: u64,
    /// Cumulative `last_applied` advance.
    pub entries_applied: u64,

    // -------- snapshot counters --------
    /// `InstallSnapshotRequest`s emitted by this node as leader (one
    /// per chunk).
    pub snapshots_sent: u64,
    /// `InstallSnapshotRequest`s accepted by this node as follower
    /// (one per completed snapshot, not per chunk).
    pub snapshots_installed: u64,

    // -------- read counters --------
    /// `ReadIndex` proposals accepted into the pending queue.
    pub read_index_started: u64,
    /// Pending reads that completed with `Action::ReadReady`.
    pub reads_completed: u64,
    /// Pending reads that ended with `Action::ReadFailed` for any
    /// reason.
    pub reads_failed: u64,

    // -------- gauges --------
    /// Current term.
    pub current_term: Term,
    /// Current commit index.
    pub commit_index: LogIndex,
    /// Current last-applied index.
    pub last_applied: LogIndex,
    /// Number of live log entries (above the snapshot floor).
    pub log_len: u64,
    /// Role encoded for easy export: 0=follower, 1=precandidate,
    /// 2=candidate, 3=leader.
    pub role_code: u8,
}

/// Internal counter pack that the engine mutates. Kept separate from
/// [`EngineMetrics`] so the public struct can grow gauges that are
/// computed from engine state at read time.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct MetricsCounters {
    pub(crate) elections_started: u64,
    pub(crate) pre_votes_granted: u64,
    pub(crate) pre_votes_denied: u64,
    pub(crate) votes_granted: u64,
    pub(crate) votes_denied: u64,
    pub(crate) leader_elections_won: u64,
    pub(crate) higher_term_stepdowns: u64,

    pub(crate) append_entries_sent: u64,
    pub(crate) append_entries_received: u64,
    pub(crate) append_entries_rejected: u64,
    pub(crate) entries_appended: u64,
    pub(crate) entries_committed: u64,
    pub(crate) entries_applied: u64,

    pub(crate) snapshots_sent: u64,
    pub(crate) snapshots_installed: u64,

    pub(crate) read_index_started: u64,
    pub(crate) reads_completed: u64,
    pub(crate) reads_failed: u64,
}
