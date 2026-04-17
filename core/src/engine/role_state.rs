use crate::types::{index::LogIndex, node::NodeId};
use std::collections::HashMap;

#[derive(Default, Copy, Clone, Debug)]
pub struct FollowerState {}

#[derive(Default, Copy, Clone, Debug)]
pub struct CandidateState {
    pub votes_granted: usize,
}

#[derive(Default, Clone, Debug)]
pub struct LeaderState {
    pub next_index: HashMap<NodeId, LogIndex>,
    pub match_index: HashMap<NodeId, LogIndex>,
}

#[derive(Debug, Clone)]
pub enum RoleState {
    Follower(FollowerState),
    Candidate(CandidateState),
    Leader(LeaderState),
}
