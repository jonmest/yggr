use std::collections::BTreeSet;

use crate::{
    records::{log_entry::LogEntry, message::Message},
    types::{index::LogIndex, node::NodeId, term::Term},
};

/// The engine's only output, in vector form per `step()` call.
///
/// The engine never performs I/O directly. Instead, every effect it
/// wants the host to carry out becomes an `Action`. The host fulfils
/// the actions however it likes — sockets, async runtimes, in-memory
/// simulators. This is what makes the engine purely synchronous and
/// testable without a network or filesystem.
/// The engine emits actions in causal order: every action that must reach
/// stable storage before any subsequent network send appears earlier in the
/// vector. Hosts MUST process actions in order and MUST flush
/// [`Action::PersistHardState`] / [`Action::PersistLogEntries`] to disk
/// before performing any [`Action::Send`] that follows them. This is what
/// keeps Raft crash-safe (Figure 2: "respond to RPCs only after updating
/// stable storage").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action<C> {
    /// Persist the §5.1 hard state — the values the engine must recover
    /// after a crash to remain safe. Emitted whenever `current_term` or
    /// `voted_for` changes.
    PersistHardState {
        current_term: Term,
        voted_for: Option<NodeId>,
    },
    /// Persist these log entries. Emitted in the same `step()` that
    /// appended them (follower receiving `AppendEntries`, leader
    /// proposing or appending its election no-op). Entries are in index
    /// order and contiguous with whatever the host already has.
    PersistLogEntries(Vec<LogEntry<C>>),
    /// Send `message` to peer `to`. The host owns the network
    /// transport; the engine just describes who and what.
    Send { to: NodeId, message: Message<C> },
    /// These entries (in index order, contiguous, all newly committed)
    /// are now safe to feed to the application state machine. The
    /// engine advances `last_applied` to the last index in the slice
    /// when emitting this; the host must not skip the action.
    Apply(Vec<LogEntry<C>>),
    /// A client proposal landed on a non-leader; the host should
    /// retarget the client at this peer (the leader for the current
    /// term, as last observed via `AppendEntries`).
    Redirect { leader_hint: NodeId },
    /// Persist `bytes` to durable storage as the latest snapshot. The
    /// engine has already truncated its in-memory log up to and
    /// including `last_included_index` and recorded the snapshot
    /// floor; the host MUST flush these bytes before any subsequent
    /// `Send` referring to indices at or below the floor (in
    /// practice, before any `InstallSnapshot` reply).
    ///
    /// `peers` is the cluster membership as of `last_included_index` —
    /// the host stores it alongside the bytes so membership survives
    /// restart and snapshot-based catch-up. Without this, committed
    /// `AddPeer` / `RemovePeer` entries that get snapshotted would be
    /// lost on recovery and the node would compute the wrong majority.
    PersistSnapshot {
        last_included_index: LogIndex,
        last_included_term: Term,
        peers: BTreeSet<NodeId>,
        bytes: Vec<u8>,
    },
    /// Restore the application's state machine from `bytes`. Emitted
    /// only when the engine just installed a snapshot it received
    /// from a leader; the host's state machine should call its
    /// `restore` method with these bytes before consuming any
    /// subsequent `Apply`.
    ApplySnapshot { bytes: Vec<u8> },
}
