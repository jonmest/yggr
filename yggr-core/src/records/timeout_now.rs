//! `TimeoutNow` leadership-transfer RPC.

use crate::types::{node::NodeId, term::Term};

/// Leader → follower: "start an election immediately."
///
/// Used for leadership transfer after the current leader has caught
/// the target follower up to its own log tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeoutNow {
    /// Sender's current term.
    pub term: Term,
    /// Sender's id. The receiver accepts only when this matches the
    /// leader it already trusts for `term`.
    pub leader_id: NodeId,
}
