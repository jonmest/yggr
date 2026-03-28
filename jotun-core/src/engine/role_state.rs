use std::collections::BTreeSet;

use crate::{engine::peer_progress::PeerProgress, types::node::NodeId};

#[derive(Default, Copy, Clone, Debug)]
pub struct FollowerState {}

#[derive(Default, Clone, Debug)]
pub struct CandidateState {
    pub votes_granted: BTreeSet<NodeId>,
}

#[derive(Default, Clone, Debug)]
pub struct LeaderState {
    pub progress: PeerProgress,
}

#[derive(Debug, Clone)]
pub enum RoleState {
    Follower(FollowerState),
    Candidate(CandidateState),
    Leader(LeaderState),
}
