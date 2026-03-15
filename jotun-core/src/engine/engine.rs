#![allow(
    dead_code,
    unused_variables,
    unreachable_code,
    clippy::needless_pass_by_value,
    clippy::unused_self
)]
use crate::engine::log::Log;
use crate::engine::telemetry;
use crate::records::append_entries::{
    AppendEntriesResponse, AppendEntriesResult, RequestAppendEntries,
};
#[allow(clippy::enum_glob_use)] // match-heavy file; variants are used unqualified throughout
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
use tracing::instrument;

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
    /// Create a fresh follower in term 0 with an empty log and no recorded vote.
    #[must_use]
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            state: RaftState {
                current_term: Term::ZERO,
                voted_for: None,
                log: Log::new(),
                commit_index: LogIndex::ZERO,
                last_applied: LogIndex::ZERO,
                role: RoleState::Follower(FollowerState::default()),
            },
        }
    }

    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn current_term(&self) -> Term {
        self.state.current_term
    }

    #[must_use]
    pub fn voted_for(&self) -> Option<NodeId> {
        self.state.voted_for
    }

    #[must_use]
    pub fn role(&self) -> &RoleState {
        &self.state.role
    }

    pub fn commit_index(&self) -> LogIndex {
        self.state.commit_index
    }

    #[must_use]
    pub fn log(&self) -> &Log<C> {
        &self.state.log
    }

    #[cfg(test)]
    pub(crate) fn state_mut(&mut self) -> &mut RaftState<C> {
        &mut self.state
    }

    #[instrument(
        target = "jotun::engine",
        skip_all,
        fields(
            node_id = %self.id,
            term = self.state.current_term.get(),
        ),
    )]
    pub fn step(&mut self, event: Event<C>) -> Vec<Action<C>> {
        match event {
            Event::Tick => self.on_tick(),
            Event::Incoming(incoming) => self.on_incoming(incoming),
            Event::ClientProposal(command) => self.on_client_proposal(command),
        }
    }

    #[instrument(target = "jotun::engine", skip_all)]
    fn on_tick(&mut self) -> Vec<Action<C>> {
        todo!()
    }

    #[instrument(target = "jotun::engine", skip_all)]
    fn on_incoming(&mut self, incoming: Incoming<C>) -> Vec<Action<C>> {
        match incoming.message {
            VoteRequest(request_vote) => vec![self.on_vote_request(request_vote)],
            VoteResponse(vote_response) => vec![self.on_vote_response(vote_response)],
            AppendEntriesRequest(request_append_entries) => {
                vec![self.on_append_entries_request(request_append_entries)]
            }
            AppendEntriesResponse(append_entries_response) => todo!(),
        }
    }

    #[instrument(target = "jotun::engine", skip_all)]
    fn on_client_proposal(&mut self, command: C) -> Vec<Action<C>> {
        todo!()
    }

    #[instrument(target = "jotun::engine", skip_all)]
    fn on_vote_response(&mut self, request: VoteResponse) -> Action<C> {
        todo!()
    }

    #[instrument(
        target = "jotun::engine",
        skip_all,
        fields(
            leader = %request.leader_id,
            term = %request.term
        )
    )]
    fn on_append_entries_request(&mut self, request: RequestAppendEntries<C>) -> Action<C> {
        if request.term > self.state.current_term
            || (request.term == self.state.current_term
                && matches!(self.state.role, RoleState::Candidate(_)))
        {
            self.become_follower(request.term);
        }
        if request.term < self.state.current_term {
            return self.conflict(request.leader_id, LogIndex::ZERO);
        }

        if request.prev_log_id.is_some_and(|id| {
            self.state
                .log
                .entry_at(id.index)
                .is_none_or(|e| e.id.term != id.term)
        }) {
            let hint = self
                .state
                .log
                .last_log_id()
                .map_or(LogIndex::new(1), |l| l.index.next());
            return self.conflict(request.leader_id, hint);
        }

        for entry in request.entries {
            match self.state.log.entry_at(entry.id.index) {
                None => self.state.log.append(entry),
                Some(existing) => {
                    if existing.id.term != entry.id.term {
                        // §5.4.1 guarantees a correct leader never asks us to
                        // overwrite a committed entry. Defend against malformed
                        // or buggy senders rather than corrupt committed state.
                        if entry.id.index <= self.state.commit_index {
                            let hint = self
                                .state
                                .log
                                .last_log_id()
                                .map_or(LogIndex::new(1), |l| l.index.next());
                            return self.conflict(request.leader_id, hint);
                        }
                        self.state.log.truncate_from(entry.id.index);
                        self.state.log.append(entry);
                    }
                }
            }
        }

        let last_appended = self
            .state
            .log
            .last_log_id()
            .map_or(LogIndex::ZERO, |l| l.index);

        if request.leader_commit > self.state.commit_index {
            self.state.commit_index = request.leader_commit.min(last_appended);
        }

        Action::Send {
            to: request.leader_id,
            message: AppendEntriesResponse(AppendEntriesResponse {
                term: self.current_term(),
                result: AppendEntriesResult::Success {
                    last_appended: Some(last_appended),
                },
            }),
        }
    }

    fn conflict(&self, leader_id: NodeId, hint: LogIndex) -> Action<C> {
        Action::Send {
            to: leader_id,
            message: AppendEntriesResponse(AppendEntriesResponse {
                term: self.current_term(),
                result: AppendEntriesResult::Conflict {
                    next_index_hint: hint,
                },
            }),
        }
    }

    #[instrument(
        target = "jotun::engine",
        skip_all,
        fields(
            candidate = %request.candidate_id,
            request_term = request.term.get(),
            decision = tracing::field::Empty,
        ),
    )]
    fn on_vote_request(&mut self, request: RequestVote) -> Action<C> {
        if request.term > self.state.current_term {
            self.become_follower(request.term);
        }

        let is_valid_term = request.term == self.state.current_term;
        let is_vote_available = self
            .state
            .voted_for
            .is_none_or(|v| v == request.candidate_id);
        let candidate_log_valid = self.state.log.is_superseded_by(request.last_log_id);

        let granted = is_valid_term && is_vote_available && candidate_log_valid;
        if granted {
            self.state.voted_for = Some(request.candidate_id);
        }
        tracing::Span::current().record(
            telemetry::fields::DECISION,
            if granted { "granted" } else { "rejected" },
        );

        let msg = VoteResponse {
            term: self.state.current_term,
            result: if granted {
                VoteResult::Granted
            } else {
                VoteResult::Rejected
            },
        };

        Action::Send {
            to: request.candidate_id,
            message: VoteResponse(msg),
        }
    }

    fn become_follower(&mut self, term: Term) {
        let from_term = self.state.current_term;
        self.state.current_term = term;
        self.state.voted_for = None;
        self.state.role = RoleState::Follower(FollowerState::default());
        if from_term != term {
            telemetry::term_advanced(self.id, from_term, term);
        }
        telemetry::became_follower(self.id, term);
    }

    fn become_candidate(&mut self) {
        let from_term = self.state.current_term;
        self.state.current_term = self.state.current_term.next();
        self.state.voted_for = Some(self.id);
        self.state.role = RoleState::Candidate(CandidateState { votes_granted: 1 });
        telemetry::term_advanced(self.id, from_term, self.state.current_term);
        telemetry::became_candidate(self.id, self.state.current_term);
    }

    fn become_leader(&mut self) {
        let next_index = todo!();
        let match_index = todo!();

        self.state.role = RoleState::Leader(LeaderState {
            next_index,
            match_index,
        });
        telemetry::became_leader(self.id, self.state.current_term);
    }
}
