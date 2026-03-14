#![allow(
    dead_code,
    unused_variables,
    unreachable_code,
    clippy::needless_pass_by_value
)]
use crate::engine::log::Log;
use crate::records::message::Message::*;
use crate::records::vote::{RequestVote, VoteResponse, VoteResult};
use crate::{
    engine::{
        action::Action,
        event::Event,
        incoming::Incoming,
        role_state::{CandidateState, FollowerState, LeaderState, RoleState},
    },
    types::{index::LogIndex, node::NodeId, term::Term},
};

#[derive(Debug)]
pub struct RaftState<C> {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Log<C>,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    pub role: RoleState,
}

#[derive(Debug)]
pub struct Engine<C> {
    id: NodeId,
    state: RaftState<C>,
}

impl<C> Engine<C> {
    pub fn step(&mut self, event: Event<C>) -> Vec<Action<C>> {
        match event {
            Event::Tick => self.on_tick(),
            Event::Incoming(incoming) => self.on_incoming(incoming),
            Event::ClientProposal(command) => self.on_client_proposal(command),
        }
    }

    fn on_tick(&mut self) -> Vec<Action<C>> {
        todo!()
    }

    fn on_incoming(&mut self, incoming: Incoming<C>) -> Vec<Action<C>> {
        match incoming.message {
            VoteRequest(request_vote) => vec![self.on_vote_request(request_vote)],
            VoteResponse(vote_response) => vec![self.on_vote_response(vote_response)],
            AppendEntriesRequest(request_append_entries) => todo!(),
            AppendEntriesResponse(append_entries_response) => todo!(),
        }
    }

    fn on_client_proposal(&mut self, command: C) -> Vec<Action<C>> {
        todo!()
    }

    fn on_vote_response(&mut self, request: VoteResponse) -> Action<C> {
        todo!()
    }
    
    fn on_vote_request(&mut self, request: RequestVote) -> Action<C> {        
        if request.term > self.state.current_term {
            self.become_follower(request.term);
        }

        let is_valid_term = request.term >= self.state.current_term;
        let is_vote_available = self.state.voted_for.is_none_or(|v| v == request.candidate_id);
        let candidate_log_valid = request.last_log_id.is_some_and(|log| {
            log.term >= self.state.current_term && 
            log.index >= self.state.commit_index 
        });
        
        let msg = if is_valid_term && is_vote_available && candidate_log_valid {
            VoteResponse { term: self.state.current_term, result: VoteResult::Granted }
        } else {
            VoteResponse { term: self.state.current_term, result: VoteResult::Rejected }
        };

        Action::Send { to: request.candidate_id, message: VoteResponse(msg) }
    }

    fn become_follower(&mut self, term: Term) {
        self.state.current_term = term;
        self.state.voted_for = None;
        self.state.role = RoleState::Follower(FollowerState::default());
    }

    fn become_candidate(&mut self) {
        self.state.current_term = self.state.current_term.next();
        self.state.voted_for = Some(self.id);
        self.state.role = RoleState::Candidate(CandidateState { votes_granted: 1 });
    }

    fn become_leader(&mut self) {
        let next_index = todo!();
        let match_index = todo!();

        self.state.role = RoleState::Leader(LeaderState {
            next_index,
            match_index,
        });
    }
}
