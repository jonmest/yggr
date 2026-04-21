use std::collections::{BTreeMap, BTreeSet};

use crate::{
    engine::peer_progress::PeerProgress,
    types::{index::LogIndex, log::LogId, node::NodeId, term::Term},
};

/// Per-role state the follower carries while in the Follower role.
///
/// Tracks the current leader — learned from any `AppendEntries` accepted
/// in the current term — so the host can redirect client proposals that
/// land on this node. `None` means "we haven't heard from a leader this
/// term yet" (just booted, or just stepped down from candidate/leader).
#[derive(Default, Copy, Clone, Debug)]
pub struct FollowerState {
    pub(crate) leader_id: Option<NodeId>,
}

impl FollowerState {
    /// The leader this follower last accepted an `AppendEntries` from in
    /// the current term, if any. `None` when freshly booted or just
    /// stepped down (no leader trusted yet for the new term).
    #[must_use]
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }
}

/// Per-role state the engine carries while probing peers for a
/// pre-vote (§9.6). A pre-candidate behaves like a follower in every
/// outward respect (does not grant `RequestVote`, does not broadcast) —
/// it only collects `PreVoteResponse`s against the next-higher term.
/// Once a majority grant, it transitions to a real `Candidate` and
/// actually bumps the term.
#[derive(Default, Clone, Debug)]
pub struct PreCandidateState {
    /// The term the pre-candidate would enter if promoted. Always
    /// equal to the engine's `current_term + 1` at the moment the
    /// pre-election began. Stored so stale responses (for an earlier
    /// pre-election or a different term) can be discarded.
    pub(crate) proposed_term: Term,
    /// Node ids that have granted a pre-vote for `proposed_term`,
    /// including self. Promotion happens at cluster majority.
    pub(crate) grants: BTreeSet<NodeId>,
}

impl PreCandidateState {
    /// The term we would enter on promotion.
    pub fn proposed_term(&self) -> Term {
        self.proposed_term
    }
    /// Grants received so far, including self.
    #[must_use]
    pub fn grants(&self) -> &BTreeSet<NodeId> {
        &self.grants
    }
}

/// Per-role state the engine carries while in the Candidate role (§5.2).
///
/// Tracks which peers have granted us their vote this term. Self always
/// votes for self (inserted by `become_candidate`); set semantics make
/// duplicate grants from the same peer harmless.
#[derive(Default, Clone, Debug)]
pub struct CandidateState {
    /// Node ids that have granted us a vote this term — including self.
    /// Election wins when `votes_granted.len() >= cluster_majority()`.
    pub(crate) votes_granted: BTreeSet<NodeId>,
}

impl CandidateState {
    /// The set of nodes (including self) that have granted us a vote
    /// this term.
    #[must_use]
    pub fn votes_granted(&self) -> &BTreeSet<NodeId> {
        &self.votes_granted
    }
}

/// Per-role state the engine carries while in the Leader role
/// (§5.3 Figure 2).
///
/// All leader bookkeeping lives in [`PeerProgress`]: per-peer `nextIndex`
/// and `matchIndex`, plus the median calculation that drives commit
/// advancement. Membership changes (§6) eventually mutate this same
/// structure.
#[derive(Default, Clone, Debug)]
pub struct LeaderState {
    /// Per-peer replication state for every other node in the cluster.
    pub(crate) progress: PeerProgress,
    /// Per-peer outbound snapshot transfer progress. `next_offset` is
    /// the first byte the follower has not yet acknowledged for the
    /// snapshot at `last_included`.
    pub(crate) snapshot_transfers: BTreeMap<NodeId, SnapshotTransfer>,
    /// Leadership transfer target, if the current leader is trying to
    /// hand off authority.
    pub(crate) transfer_target: Option<NodeId>,
    /// Monotonic counter bumped on each replication broadcast. Each
    /// outgoing `AppendEntries` is stamped with the current value via
    /// `peer_sent_seq`; a successful ack copies that value into
    /// `peer_acked_seq`. `ReadIndex` uses majority acked-seq to confirm
    /// still-leader for a given request.
    pub(crate) heartbeat_seq: u64,
    /// Per-peer highest `heartbeat_seq` stamped on a message we sent
    /// to that peer. Used to translate a successful response back to
    /// the seq it implicitly acks.
    pub(crate) peer_sent_seq: BTreeMap<NodeId, u64>,
    /// Per-peer highest `heartbeat_seq` the peer has acked in the
    /// current term.
    pub(crate) peer_acked_seq: BTreeMap<NodeId, u64>,
    /// Pending `ReadIndex` requests, in submission order.
    pub(crate) pending_reads: Vec<PendingRead>,
}

/// A `ReadIndex` request awaiting confirmation that we're still leader.
///
/// A request is ready to serve once:
///  - a majority of the cluster (self included) has acked a broadcast
///    at or after `required_seq`, AND
///  - `last_applied >= read_index` on this leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PendingRead {
    pub(crate) id: u64,
    pub(crate) read_index: LogIndex,
    pub(crate) required_seq: u64,
}

impl LeaderState {
    /// Per-peer replication state. Read-only externally — the engine
    /// owns mutation through the `AppendEntries` response handlers.
    #[must_use]
    pub fn progress(&self) -> &PeerProgress {
        &self.progress
    }
}

/// Leader-side progress for one chunked snapshot install.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SnapshotTransfer {
    pub(crate) last_included: LogId,
    pub(crate) next_offset: u64,
}

/// The Raft role. Every node is exactly one of these at any given time.
///
/// Transitions are tightly constrained:
///  - Anyone → `Follower` on observing a higher term (§5.1).
///  - `Follower` → `PreCandidate` on election timeout when pre-vote
///    is enabled. Without pre-vote, `Follower` → `Candidate` directly.
///  - `PreCandidate` → `Candidate` on collecting a majority of
///    `PreVoteResponse { granted: true }`.
///  - `PreCandidate` → `Follower` on accepting a current-term
///    `AppendEntries`, observing a higher term, or (with the election
///    timer re-firing) starting a fresh pre-election at the next
///    proposed term.
///  - `Candidate` → `Follower` on receiving a current-term
///    `AppendEntries` (a peer won the election) or a higher-term
///    message.
///  - `Candidate` → `Leader` on receiving votes from a majority.
///  - `Leader` → `Follower` only on observing a higher term.
#[derive(Debug, Clone)]
pub enum RoleState {
    Follower(FollowerState),
    PreCandidate(PreCandidateState),
    Candidate(CandidateState),
    Leader(LeaderState),
}
