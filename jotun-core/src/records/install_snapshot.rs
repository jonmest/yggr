//! `InstallSnapshot` RPC types (§7).
//!
//! Sent by a leader to a follower whose `nextIndex` has fallen below
//! the leader's log floor — replication via `AppendEntries` can no
//! longer construct a valid `prev_log_id` from entries that have been
//! compacted into the snapshot.

use std::collections::BTreeSet;

use crate::types::{index::LogIndex, log::LogId, node::NodeId, term::Term};

/// Leader → follower: "here is a snapshot of the application state
/// up to `last_included`. Replace your log up to and including that
/// index, hand the bytes to your state machine, and resume from
/// `last_included.index + 1`."
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestInstallSnapshot {
    /// Sender's term.
    pub term: Term,
    /// Sender's id. Used by the receiver to record the current leader.
    pub leader_id: NodeId,
    /// `(index, term)` at the snapshot's tail.
    pub last_included: LogId,
    /// Opaque application bytes. The receiver hands these to its
    /// state machine via `Action::ApplySnapshot` after persisting.
    /// The protocol never inspects these bytes; any compression is a
    /// host-side concern applied inside `StateMachine::snapshot`.
    pub data: Vec<u8>,
    /// Byte offset this chunk starts at within the logical snapshot
    /// stream. `0` starts a fresh transfer; non-zero values resume an
    /// in-progress install.
    pub offset: u64,
    /// True when this chunk reaches the end of the logical snapshot
    /// stream. Followers install only once they have received every
    /// byte up to and including this chunk.
    pub done: bool,
    /// Sender's `commit_index` at the time of send. The follower
    /// advances its own `commit_index` to at least
    /// `min(leader_commit, last_included.index)` on success.
    pub leader_commit: LogIndex,
    /// Cluster membership as of `last_included.index`. Ships with
    /// the snapshot so committed `AddPeer` / `RemovePeer` entries
    /// that got snapshotted survive install on the receiver — without
    /// it, the follower would revert to its bootstrap config and
    /// compute the wrong majority.
    pub peers: BTreeSet<NodeId>,
}

/// Follower → leader: "I observed your snapshot offer at `term`."
///
/// No conflict shape: a snapshot is take-it-or-leave-it. Either the
/// follower accepted it (and its term equals or exceeds the leader's),
/// or it has a strictly higher term and the leader must step down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstallSnapshotResponse {
    /// Receiver's term.
    pub term: Term,
    /// Snapshot tail this acknowledgement refers to. On a completed
    /// install this is the follower's currently-installed snapshot
    /// tail, which may be newer than the leader's offered tail if the
    /// follower had already compacted further.
    pub last_included: LogId,
    /// Number of contiguous bytes the follower now holds for
    /// `last_included`. Meaningful only when `done == false`.
    pub next_offset: u64,
    /// True once the follower has installed a complete snapshot at
    /// `last_included` and is ready to resume with `AppendEntries`.
    pub done: bool,
}
