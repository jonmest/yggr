use crate::types::{log::LogId, node::NodeId, term::Term};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestVote {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_id: Option<LogId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteResponse {
    pub term: Term,
    pub result: VoteResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteResult {
    Granted,
    Rejected,
}
