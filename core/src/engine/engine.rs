// Scaffolding: remove these allows once the engine is implemented.
#![allow(
    dead_code,
    unused_variables,
    unreachable_code,
    clippy::needless_pass_by_value
)]

use crate::{
    engine::{
        action::Action,
        event::Event,
        incoming::Incoming,
        role_state::{CandidateState, FollowerState, LeaderState, RoleState},
    },
    records::log_entry::LogEntry,
    types::{index::LogIndex, node::NodeId, term::Term},
};

#[derive(Debug)]
pub struct RaftState<C> {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry<C>>,
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
        todo!()
    }

    fn on_client_proposal(&mut self, command: C) -> Vec<Action<C>> {
        todo!()
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
