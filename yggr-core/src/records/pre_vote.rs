//! Pre-vote RPCs (§9.6).
//!
//! A disrupted follower whose election timer has expired sends
//! `RequestPreVote` before actually incrementing its term. Peers grant
//! only if they also believe the current leader has disappeared. A
//! would-be candidate that can't collect a majority of grants stays in
//! its current term, so a flapping node can no longer force the rest
//! of the cluster to step down.
//!
//! `PreVote` messages are protocol-local: receiving one does NOT update
//! the recipient's term, `voted_for`, or role. That is what makes them
//! non-disruptive.

use crate::types::{log::LogId, node::NodeId, term::Term};

/// Candidate → peer. Proposes `term` as the term the candidate would
/// bump to if the pre-vote succeeded. The recipient's term is not
/// updated by the receipt of this message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestPreVote {
    /// The term the candidate would enter if the pre-vote passed.
    /// Always `candidate.current_term + 1`.
    pub term: Term,
    /// Sender id. Must equal the `from` on the wrapping envelope.
    pub candidate_id: NodeId,
    /// Candidate's last log entry, for the §5.4.1 up-to-date check.
    pub last_log_id: Option<LogId>,
}

/// Peer → candidate. Carries the responder's real term (so a pre-vote
/// against a stale term discovers the gap) and whether the responder
/// would vote for this candidate if asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreVoteResponse {
    /// The responder's current term. A candidate observing a higher
    /// term here must step down to follower and advance its own term.
    pub term: Term,
    /// True iff the responder would grant a real vote right now.
    pub granted: bool,
}
