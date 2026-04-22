use crate::types::{log::LogId, node::NodeId};

/// A cluster-membership change carried by a [`LogPayload::ConfigChange`]
/// log entry.
///
/// Implements §4.3 of the Raft thesis (single-server changes): exactly
/// one change per entry, never a batch. The leader is responsible
/// for refusing to append a new `ConfigChange` while another is still
/// uncommitted, which keeps the old and new majorities overlapping.
///
/// Voter vs learner semantics:
///  - Voters count toward quorum and can campaign.
///  - Learners receive replication but never count toward quorum and
///    never campaign (§4.2.1 non-voting servers).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigChange {
    /// Add `peer` to the cluster as a voter. Takes effect on every
    /// node the instant the entry hits its log (pre-commit), per §4.3.
    AddPeer(NodeId),
    /// Remove `peer` from the cluster. Same pre-commit semantics as
    /// `AddPeer`. A leader removing itself steps down once the entry
    /// commits. Removes either a voter or a learner.
    RemovePeer(NodeId),
    /// Add `peer` to the cluster as a non-voting learner. Receives
    /// replication but doesn't count toward quorum or campaign. No-op
    /// if the peer is already a voter.
    AddLearner(NodeId),
    /// Promote `peer` from learner to voter. No-op if the peer is
    /// already a voter. Fails silently (no state change) if the peer
    /// isn't a learner — the leader is expected to gate this at
    /// propose time.
    PromoteLearner(NodeId),
}

/// Payload carried by a [`LogEntry`].
///
/// Three flavors:
///  - [`LogPayload::Command`] wraps an application-defined command. The
///    state machine consumes it once the entry commits.
///  - [`LogPayload::Noop`] is a leader-emitted placeholder appended on
///    every leadership transition. §5.4.2 requires a leader to commit at
///    least one entry from its current term before counting prior-term
///    entries as committed; the no-op makes that possible without waiting
///    for a client proposal.
///  - [`LogPayload::ConfigChange`] is a single-server membership change
///    (§4.3). Engine processes these internally; the application state
///    machine never sees them via Apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogPayload<C> {
    /// Empty entry that exists solely to advance the current-term
    /// commit boundary (§5.4.2).
    Noop,
    /// Application-defined command. Opaque to the consensus layer.
    Command(C),
    /// Cluster membership change. Mutates the active configuration
    /// the moment it lands in any node's log (committed or not).
    ConfigChange(ConfigChange),
}

/// A single entry in the replicated log: a payload tagged with its
/// position and the term in which a leader assigned it.
///
/// Once committed, every node in the cluster will see the exact same
/// sequence of entries with the exact same `id`s — that's the durable
/// outcome the rest of Raft is engineered to deliver. The application
/// state machine consumes each [`LogPayload::Command`] in order;
/// [`LogPayload::Noop`] and [`LogPayload::ConfigChange`] are engine-internal
/// and skipped during Apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry<C> {
    /// Position and term assigned by the leader that first appended this
    /// entry (§5.3 Log Matching).
    pub id: LogId,
    /// What this entry carries: a no-op, an application command, or a
    /// configuration change.
    pub payload: LogPayload<C>,
}
