#![allow(
    dead_code,
    unused_variables,
    unreachable_code,
    clippy::needless_pass_by_value,
    clippy::unused_self
)]
use std::collections::BTreeSet;

use crate::engine::env::Env;
use crate::engine::log::Log;
use crate::engine::peer_progress::PeerProgress;
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
    pub election_timeout_ticks: u64,
    pub election_elapsed: u64,
    pub heartbeat_elapsed: u64,
}

#[derive(Debug)]
pub struct Engine<C> {
    id: NodeId,
    peers: BTreeSet<NodeId>,
    env: Box<dyn Env>,
    heartbeat_interval_ticks: u64,
    state: RaftState<C>,
}

impl<C> Engine<C> {
    /// Create a fresh follower in term 0 with an empty log and no recorded vote.
    ///
    /// `peers` is any iterable of peer node ids. Self is automatically
    /// excluded, so the caller can pass the full cluster membership without
    /// filtering.
    #[must_use]
    pub fn new(
        id: NodeId,
        peers: impl IntoIterator<Item = NodeId>,
        mut env: Box<dyn Env>,
        heartbeat_interval_ticks: u64,
    ) -> Self {
        let peers = peers.into_iter().filter(|p| *p != id).collect();
        let election_timeout_ticks = env.next_election_timeout();

        Self {
            id,
            peers,
            env,
            heartbeat_interval_ticks,
            state: RaftState {
                current_term: Term::ZERO,
                voted_for: None,
                log: Log::new(),
                commit_index: LogIndex::ZERO,
                last_applied: LogIndex::ZERO,
                role: RoleState::Follower(FollowerState::default()),
                election_elapsed: 0,
                heartbeat_elapsed: 0,
                election_timeout_ticks,
            },
        }
    }

    fn reset_election_timer(&mut self) {
        self.state.election_elapsed = 0;
        self.state.election_timeout_ticks = self.env.next_election_timeout();
    }

    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    #[must_use]
    pub fn peers(&self) -> &BTreeSet<NodeId> {
        &self.peers
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
    pub fn election_elapsed(&self) -> u64 {
        self.state.election_elapsed
    }

    #[must_use]
    pub fn election_timeout_ticks(&self) -> u64 {
        self.state.election_timeout_ticks
    }

    #[must_use]
    pub fn heartbeat_elapsed(&self) -> u64 {
        self.state.heartbeat_elapsed
    }

    #[must_use]
    pub fn heartbeat_interval_ticks(&self) -> u64 {
        self.heartbeat_interval_ticks
    }

    #[must_use]
    pub fn log(&self) -> &Log<C> {
        &self.state.log
    }

    /// Number of votes required to win an election in this cluster.
    /// `cluster_size = peers + self`; majority = `cluster_size / 2 + 1`.
    #[must_use]
    pub fn cluster_majority(&self) -> usize {
        self.peers.len().div_ceil(2) + 1
    }

    #[cfg(test)]
    pub(crate) fn state_mut(&mut self) -> &mut RaftState<C> {
        &mut self.state
    }

    /// Structural invariants the engine must never violate.
    /// Panics in debug builds when a transition breaks one; no-op in release.
    ///
    /// Derived from Figure 2 and §5 of the Raft paper:
    ///  - `commit_index <= last log index` (§5.3 rule 5 — can't commit what isn't there).
    ///  - `last_applied <= commit_index` (Figure 2 — can't apply what isn't committed).
    ///  - if role is Candidate, `voted_for == Some(self.id)` (§5.2 — candidates
    ///    always vote for themselves).
    #[cfg(debug_assertions)]
    fn check_invariants(&self) {
        let last_log_index = self
            .state
            .log
            .last_log_id()
            .map_or(LogIndex::ZERO, |l| l.index);

        debug_assert!(
            self.state.commit_index <= last_log_index,
            "commit_index {:?} exceeds last log index {last_log_index:?} (§5.3)",
            self.state.commit_index,
        );
        debug_assert!(
            self.state.last_applied <= self.state.commit_index,
            "last_applied {:?} exceeds commit_index {:?}",
            self.state.last_applied,
            self.state.commit_index,
        );
        if matches!(self.state.role, RoleState::Candidate(_)) {
            debug_assert_eq!(
                self.state.voted_for,
                Some(self.id),
                "Candidate must have voted for itself (§5.2)",
            );
        }

        self.state.log.check_invariants();
    }

    #[cfg(not(debug_assertions))]
    fn check_invariants(&self) {}

    #[instrument(
        target = "jotun::engine",
        skip_all,
        fields(
            node_id = %self.id,
            term = self.state.current_term.get(),
        ),
    )]
    pub fn step(&mut self, event: Event<C>) -> Vec<Action<C>> {
        #[cfg(debug_assertions)]
        let prev_term = self.state.current_term;

        let actions = match event {
            Event::Tick => self.on_tick(),
            Event::Incoming(incoming) => self.on_incoming(incoming),
            Event::ClientProposal(command) => self.on_client_proposal(command),
        };

        #[cfg(debug_assertions)]
        {
            // §5.1: a server's current term only ever increases.
            debug_assert!(
                self.state.current_term >= prev_term,
                "current_term went backward: {prev_term:?} -> {:?}",
                self.state.current_term,
            );
            self.check_invariants();
        }

        actions
    }

    #[instrument(target = "jotun::engine", skip_all)]
    fn on_tick(&mut self) -> Vec<Action<C>> {
        match &self.state.role {
            RoleState::Follower(_) | RoleState::Candidate(_) => {
                self.state.election_elapsed += 1;
                if self.state.election_elapsed >= self.state.election_timeout_ticks {
                    return self.start_election();
                }
                vec![]
            }
            RoleState::Leader(_) => {
                self.state.heartbeat_elapsed += 1;
                if self.state.heartbeat_elapsed >= self.heartbeat_interval_ticks {
                    self.state.heartbeat_elapsed = 0;
                    return self.send_heartbeats();
                }
                vec![]
            }
        }
    }

    fn start_election(&mut self) -> Vec<Action<C>> {
        self.become_candidate();
        if self.has_majority_votes() {
            return self.become_leader();
        }

        let request = RequestVote {
            term: self.state.current_term,
            candidate_id: self.id(),
            last_log_id: self.state.log.last_log_id(),
        };

        self.peers
            .iter()
            .map(|&peer| Action::Send {
                to: peer,
                message: VoteRequest(request),
            })
            .collect()
    }

    fn send_heartbeats(&mut self) -> Vec<Action<C>> {
        // TODO: actually build and send AppendEntries heartbeats to every peer.
        // Leader-side work is pending (§5.2). Returning empty keeps on_tick's
        // leader branch reachable and harmless until then.
        vec![]
    }

    #[instrument(target = "jotun::engine", skip_all)]
    fn on_incoming(&mut self, incoming: Incoming<C>) -> Vec<Action<C>> {
        match incoming.message {
            VoteRequest(request_vote) => vec![self.on_vote_request(request_vote)],
            VoteResponse(vote_response) => self.on_vote_response(incoming.from, vote_response),
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

    fn has_majority_votes(&self) -> bool {
        match self.role() {
            RoleState::Candidate(state) => state.votes_granted.len() >= self.cluster_majority(),
            _ => false,
        }
    }

    #[instrument(target = "jotun::engine", skip_all)]
    fn on_vote_response(&mut self, voter_id: NodeId, response: VoteResponse) -> Vec<Action<C>> {
        if response.term > self.state.current_term {
            self.become_follower(response.term);
            return vec![];
        }
        if response.term < self.state.current_term {
            return vec![];
        }

        // `c` borrow ends after the insert; NLL releases it before
        // has_majority_votes() and become_leader() are called.
        let RoleState::Candidate(c) = &mut self.state.role else {
            return vec![];
        };

        if response.result == VoteResult::Granted {
            c.votes_granted.insert(voter_id);
        }

        if self.has_majority_votes() {
            return self.become_leader();
        }

        vec![]
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

        self.reset_election_timer();

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
            self.reset_election_timer();
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

    /// Transition to follower. Does NOT reset the election timer — per §5.2,
    /// the timer resets only on accepting `AppendEntries` from the current
    /// leader or granting a vote. Callers that need both should do so
    /// explicitly after this returns.
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

        let mut votes_granted = BTreeSet::new();
        votes_granted.insert(self.id());

        self.state.role = RoleState::Candidate(CandidateState { votes_granted });
        self.reset_election_timer();

        telemetry::term_advanced(self.id, from_term, self.state.current_term);
        telemetry::became_candidate(self.id, self.state.current_term);
    }

    fn become_leader(&mut self) -> Vec<Action<C>> {
        let last_log_index = self.log().last_log_id().map_or(LogIndex::ZERO, |l| l.index);

        let progress = PeerProgress::new(self.peers.iter().copied(), last_log_index);
        self.state.role = RoleState::Leader(LeaderState { progress });
        telemetry::became_leader(self.id, self.state.current_term);

        self.send_heartbeats()
    }
}
