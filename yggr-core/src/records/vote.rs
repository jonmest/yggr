use crate::types::{log::LogId, node::NodeId, term::Term};

/// RPC sent by a candidate soliciting a vote (§5.2, Figure 2).
///
/// A receiver grants the vote if all of:
///  - `term >= current_term` (after term catch-up).
///  - It hasn't already voted for someone else this term.
///  - The candidate's log is at least as up-to-date as its own
///    (§5.4.1; see [`crate::engine::log::Log::is_superseded_by`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestVote {
    /// Candidate's term.
    pub term: Term,
    /// Candidate asking for the vote.
    pub candidate_id: NodeId,
    /// Candidate's last log entry, or `None` if its log is empty. Drives
    /// the §5.4.1 election restriction.
    pub last_log_id: Option<LogId>,
}

/// Response to a [`RequestVote`] (§5.2, Figure 2).
///
/// The `term` lets the candidate detect that it is stale and step down;
/// `result` carries the actual decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteResponse {
    /// Responder's `current_term`, for the candidate to update itself.
    pub term: Term,
    /// Whether the vote was granted.
    pub result: VoteResult,
}

/// Decision in a [`VoteResponse`].
///
/// The Raft paper uses a `bool`; we use an enum so the value is
/// self-describing in logs and traces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteResult {
    /// Voter granted the vote to the candidate.
    Granted,
    /// Voter rejected — stale term, already voted, or candidate's log
    /// is not up-to-date.
    Rejected,
}
