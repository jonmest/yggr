use crate::{
    records::log_entry::LogEntry,
    types::{index::LogIndex, log::LogId, node::NodeId, term::Term},
};

/// The leader's workhorse RPC (§5.3, Figure 2).
///
/// Carries three concerns in one shape:
///  - **Replication**: `entries` are new log records to append.
///  - **Consistency check**: `prev_log_id` lets the follower verify its
///    log matches the leader's prefix before accepting; mismatches are
///    rejected via [`AppendEntriesResult::Conflict`].
///  - **Commit propagation**: `leader_commit` tells the follower how
///    far the leader believes is committed cluster-wide.
///
/// A heartbeat is just an `RequestAppendEntries` with empty `entries`.
/// Leaders send these periodically to prevent followers from starting
/// elections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestAppendEntries<C> {
    /// Leader's term.
    pub term: Term,
    /// Leader's id, so followers know who to redirect clients to.
    pub leader_id: NodeId,
    /// `(index, term)` of the entry immediately preceding `entries`, or
    /// `None` if `entries[0]` is at index 1 (or `entries` is empty and
    /// the follower is being told "I'm the leader, no preconditions").
    /// The follower's prefix-match check (§5.3 rule 2) keys off this.
    pub prev_log_id: Option<LogId>,
    /// New entries to append. Empty for pure heartbeats.
    pub entries: Vec<LogEntry<C>>,
    /// Leader's `commit_index`. Followers may advance their own commit
    /// up to `min(leader_commit, last_new_entry_index)` (§5.3 rule 5).
    pub leader_commit: LogIndex,
}

/// Response to a [`RequestAppendEntries`] (§5.3, Figure 2).
///
/// Whether the leader sees `Success` or `Conflict`, the response always
/// carries the responder's `term` so a stale leader can step down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    /// Responder's `current_term`, for the leader to update itself.
    pub term: Term,
    /// Outcome of the consistency check and append.
    pub result: AppendEntriesResult,
}

/// Outcome variant of an [`AppendEntriesResponse`].
///
/// The Raft paper uses a `bool` here; we carry richer information so
/// the leader can advance bookkeeping without extra round-trips:
///  - On success, `last_appended` lets the leader update `matchIndex`
///    without guessing.
///  - On conflict, `next_index_hint` lets the leader skip ahead in its
///    log-recovery probing rather than decrementing one entry at a time
///    (the optional optimization in §5.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendEntriesResult {
    /// The follower accepted the append (or found nothing to append in
    /// the heartbeat case). `last_appended` is the highest log index
    /// the follower now holds, or `None` if its log remains empty.
    Success {
        /// Highest index in the follower's log after the append, if any.
        last_appended: Option<LogIndex>,
    },
    /// The follower rejected the append: either the leader is stale
    /// (term too low) or the prefix-match check failed.
    /// `next_index_hint` suggests where the leader should retry.
    Conflict {
        /// Suggested `nextIndex` for the leader's next attempt.
        next_index_hint: LogIndex,
    },
}
