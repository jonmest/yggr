use std::collections::BTreeSet;

use crate::engine::env::Env;
use crate::engine::log::Log;
use crate::engine::peer_progress::PeerProgress;
use crate::engine::telemetry;
use crate::records::append_entries::{
    AppendEntriesResponse, AppendEntriesResult, RequestAppendEntries,
};
use crate::records::log_entry::{LogEntry, LogPayload};
use crate::types::log::LogId;
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

/// All mutable state of a single Raft node (Figure 2).
///
/// Split out from [`Engine`] so the surrounding configuration
/// (`id`, `peers`, `env`, `heartbeat_interval_ticks`) — which never
/// changes after construction — stays clearly distinct from the
/// per-step running state.
///
/// Persistent vs volatile (per Figure 2):
///  - `current_term` and `voted_for` are *persistent* — must survive a
///    crash for safety. The host is responsible for writing them
///    durably before responding to any RPC that mutated them.
///  - `log` is also persistent.
///  - Everything else (`commit_index`, `last_applied`, `role`, the
///    election/heartbeat counters) is volatile and reconstructed on
///    restart.
#[derive(Debug)]
pub(crate) struct RaftState<C> {
    /// Highest term we've ever seen. Monotonically non-decreasing (§5.1).
    pub(crate) current_term: Term,
    /// Candidate this node voted for in `current_term`, if any.
    pub(crate) voted_for: Option<NodeId>,
    /// The replicated log itself.
    pub(crate) log: Log<C>,
    /// Highest log index known to be committed cluster-wide.
    pub(crate) commit_index: LogIndex,
    /// Highest log index applied to the local state machine. Always
    /// `<= commit_index`.
    pub(crate) last_applied: LogIndex,
    /// Current role (Follower / Candidate / Leader) and its per-role
    /// bookkeeping.
    pub(crate) role: RoleState,
    /// Threshold at which the election timer fires, in ticks. Re-rolled
    /// from [`crate::engine::env::Env`] on every reset (§5.2 randomization).
    pub(crate) election_timeout_ticks: u64,
    /// Ticks elapsed since the last election-timer reset.
    pub(crate) election_elapsed: u64,
    /// Ticks elapsed since the leader last broadcast `AppendEntries`.
    pub(crate) heartbeat_elapsed: u64,
}

/// The Raft state machine — a single node's complete consensus engine.
///
/// Pure: no I/O, no async, no clock of its own. Driven by
/// [`Engine::step`] with [`crate::engine::event::Event`]s and emits
/// [`crate::engine::action::Action`]s describing what the host should
/// do (send messages, persist state, apply committed entries). The
/// host owns sockets, disk, and time; the engine owns correctness.
///
/// `C` is the application command type. Must be `Clone` because leaders
/// hand copies of log entries to peers while keeping the originals.
#[derive(Debug)]
pub struct Engine<C> {
    /// This node's id. Stable for the lifetime of the engine.
    id: NodeId,
    /// Other nodes in the cluster. Self is excluded by the constructor.
    /// Iteration order is deterministic (`BTreeSet`) for reproducible tests.
    peers: BTreeSet<NodeId>,
    /// Source of nondeterministic inputs — currently just election
    /// timeout randomization. Boxed for object safety; Send so the
    /// engine can be moved between threads.
    env: Box<dyn Env>,
    /// How often the leader emits heartbeats, in ticks. Must be smaller
    /// than the election timeout (§5.2).
    heartbeat_interval_ticks: u64,
    /// All mutable per-node state.
    state: RaftState<C>,
}

impl<C: Clone> Engine<C> {
    // =========================================================================
    // Construction
    // =========================================================================

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

    // =========================================================================
    // Accessors — read-only views of state and config
    // =========================================================================

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
    pub fn log(&self) -> &Log<C> {
        &self.state.log
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn election_elapsed(&self) -> u64 {
        self.state.election_elapsed
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn election_timeout_ticks(&self) -> u64 {
        self.state.election_timeout_ticks
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn heartbeat_elapsed(&self) -> u64 {
        self.state.heartbeat_elapsed
    }

    /// Number of votes required to win an election in this cluster.
    /// `cluster_size = peers + self`; majority = `cluster_size / 2 + 1`.
    #[must_use]
    pub fn cluster_majority(&self) -> usize {
        self.peers.len().div_ceil(2) + 1
    }

    // =========================================================================
    // Test-only mutators
    // =========================================================================

    #[cfg(test)]
    pub(crate) fn state_mut(&mut self) -> &mut RaftState<C> {
        &mut self.state
    }

    // =========================================================================
    // Invariants — debug-only correctness checks called after every step()
    // =========================================================================

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

    // =========================================================================
    // Step — the public entry point. The host calls this exactly once per
    // event; everything else in the file is reachable from here.
    // =========================================================================

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

    // =========================================================================
    // Persistence helpers
    // =========================================================================

    /// Build an `Action::PersistHardState` snapshot of the §5.1 hard state.
    /// Callers emit this after any state change that touched
    /// `current_term` or `voted_for`, before any subsequent `Send`.
    fn persist_hard_state(&self) -> Action<C> {
        Action::PersistHardState {
            current_term: self.state.current_term,
            voted_for: self.state.voted_for,
        }
    }

    // =========================================================================
    // Top-level dispatch — one handler per Event variant.
    // =========================================================================

    /// Time has passed. Drives the election timer (followers/candidates) and
    /// the heartbeat interval (leaders).
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
                    return self.broadcast_append_entries();
                }
                vec![]
            }
        }
    }

    /// A peer's RPC arrived. Demultiplex by message type.
    ///
    /// Drops messages from any node that isn't a configured peer. The
    /// host transport authenticated `incoming.from`; we additionally
    /// require that sender to be a cluster member, so a stray
    /// `VoteResponse` from a non-member can't be counted toward
    /// `votes_granted` and stray `AppendEntriesResponse` from non-members
    /// can't poison `matchIndex`. For the request variants we also
    /// require the body-level id (`candidate_id` / `leader_id`) to
    /// equal `from` — the wire-authenticated identity is the source of
    /// truth, body fields are advisory.
    #[instrument(target = "jotun::engine", skip_all)]
    fn on_incoming(&mut self, incoming: Incoming<C>) -> Vec<Action<C>> {
        if !self.peers.contains(&incoming.from) {
            return vec![];
        }
        match incoming.message {
            VoteRequest(request) => {
                if request.candidate_id != incoming.from {
                    return vec![];
                }
                self.on_vote_request(request)
            }
            VoteResponse(response) => self.on_vote_response(incoming.from, response),
            AppendEntriesRequest(request) => {
                if request.leader_id != incoming.from {
                    return vec![];
                }
                self.on_append_entries_request(request)
            }
            AppendEntriesResponse(response) => {
                self.on_append_entries_response(incoming.from, response)
            }
        }
    }

    /// The application is asking us to replicate a command.
    ///
    ///  - Leader: append the command at `(last+1, current_term)` and
    ///    broadcast — replication kicks off immediately rather than
    ///    waiting for the next heartbeat.
    ///  - Follower with a known leader: emit `Action::Redirect` so the
    ///    host can forward the client to the right node.
    ///  - Otherwise (candidate, or follower that hasn't heard from a
    ///    leader this term): drop — the host should retry.
    #[instrument(target = "jotun::engine", skip_all)]
    fn on_client_proposal(&mut self, command: C) -> Vec<Action<C>> {
        match &self.state.role {
            RoleState::Leader(_) => {}
            RoleState::Follower(f) => {
                return match f.leader_id {
                    Some(leader) => vec![Action::Redirect { leader_hint: leader }],
                    None => vec![],
                };
            }
            RoleState::Candidate(_) => return vec![],
        }

        let next_index = self
            .state
            .log
            .last_log_id()
            .map_or(LogIndex::new(1), |l| l.index.next());
        let entry = LogEntry {
            id: LogId::new(next_index, self.state.current_term),
            payload: LogPayload::Command(command),
        };
        self.state.log.append(entry.clone());

        let mut out = vec![Action::PersistLogEntries(vec![entry])];
        out.extend(self.broadcast_append_entries());
        out
    }

    // =========================================================================
    // Election — RequestVote + VoteResponse + the candidate machinery (§5.2)
    // =========================================================================

    #[instrument(
        target = "jotun::engine",
        skip_all,
        fields(
            candidate = %request.candidate_id,
            request_term = request.term.get(),
            decision = tracing::field::Empty,
        ),
    )]
    fn on_vote_request(&mut self, request: RequestVote) -> Vec<Action<C>> {
        let prior_term = self.state.current_term;
        let prior_voted_for = self.state.voted_for;

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

        let mut out = Vec::new();
        if self.state.current_term != prior_term || self.state.voted_for != prior_voted_for {
            out.push(self.persist_hard_state());
        }
        out.push(Action::Send {
            to: request.candidate_id,
            message: VoteResponse(msg),
        });
        out
    }

    #[instrument(target = "jotun::engine", skip_all)]
    fn on_vote_response(&mut self, voter_id: NodeId, response: VoteResponse) -> Vec<Action<C>> {
        if response.term > self.state.current_term {
            self.become_follower(response.term);
            return vec![self.persist_hard_state()];
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

    /// True iff we're a candidate whose tally has reached cluster majority.
    fn has_majority_votes(&self) -> bool {
        match self.role() {
            RoleState::Candidate(state) => state.votes_granted.len() >= self.cluster_majority(),
            _ => false,
        }
    }

    /// Begin a new election: bump term, vote for self, send `RequestVote` to
    /// every peer. Single-node clusters self-elect immediately.
    fn start_election(&mut self) -> Vec<Action<C>> {
        self.become_candidate();
        // become_candidate bumped current_term and set voted_for to self.
        // Persist before any Send: a crashed-and-recovered candidate must
        // remember it already voted in this term.
        let mut out = vec![self.persist_hard_state()];

        if self.has_majority_votes() {
            out.extend(self.become_leader());
            return out;
        }

        let request = RequestVote {
            term: self.state.current_term,
            candidate_id: self.id(),
            last_log_id: self.state.log.last_log_id(),
        };

        out.extend(self.peers.iter().map(|&peer| Action::Send {
            to: peer,
            message: VoteRequest(request),
        }));
        out
    }

    // =========================================================================
    // Replication — AppendEntries in both directions (§5.3)
    // =========================================================================

    #[instrument(
        target = "jotun::engine",
        skip_all,
        fields(
            leader = %request.leader_id,
            term = %request.term
        )
    )]
    fn on_append_entries_request(&mut self, request: RequestAppendEntries<C>) -> Vec<Action<C>> {
        let prior_term = self.state.current_term;
        let prior_voted_for = self.state.voted_for;

        if request.term > self.state.current_term
            || (request.term == self.state.current_term
                && matches!(self.state.role, RoleState::Candidate(_)))
        {
            self.become_follower(request.term);
        }
        if request.term < self.state.current_term {
            return vec![self.conflict(request.leader_id, LogIndex::ZERO)];
        }

        // We're Follower at `request.term`. Record the leader so client
        // proposals landing here can be redirected.
        if let RoleState::Follower(f) = &mut self.state.role {
            f.leader_id = Some(request.leader_id);
        }

        self.reset_election_timer();

        let hard_state_changed =
            self.state.current_term != prior_term || self.state.voted_for != prior_voted_for;

        if let Some(prev) = request.prev_log_id
            && self
                .state
                .log
                .entry_at(prev.index)
                .is_none_or(|e| e.id.term != prev.term)
        {
            // See §5.3 conflict-hint comment elsewhere: cap at prev.index.
            let our_last_next = self
                .state
                .log
                .last_log_id()
                .map_or(LogIndex::new(1), |l| l.index.next());
            let hint = our_last_next.min(prev.index);
            let mut out = Vec::new();
            if hard_state_changed {
                out.push(self.persist_hard_state());
            }
            out.push(self.conflict(request.leader_id, hint));
            return out;
        }

        let mut newly_persisted: Vec<LogEntry<C>> = Vec::new();
        for entry in request.entries {
            match self.state.log.entry_at(entry.id.index) {
                None => {
                    newly_persisted.push(entry.clone());
                    self.state.log.append(entry);
                }
                Some(existing) => {
                    if existing.id.term != entry.id.term {
                        if entry.id.index <= self.state.commit_index {
                            let hint = entry.id.index;
                            let mut out = Vec::new();
                            if hard_state_changed {
                                out.push(self.persist_hard_state());
                            }
                            out.push(self.conflict(request.leader_id, hint));
                            return out;
                        }
                        self.state.log.truncate_from(entry.id.index);
                        newly_persisted.push(entry.clone());
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

        let mut out = Vec::new();
        if hard_state_changed {
            out.push(self.persist_hard_state());
        }
        if !newly_persisted.is_empty() {
            out.push(Action::PersistLogEntries(newly_persisted));
        }
        out.extend(self.drain_apply());
        out.push(Action::Send {
            to: request.leader_id,
            message: AppendEntriesResponse(AppendEntriesResponse {
                term: self.current_term(),
                result: AppendEntriesResult::Success {
                    last_appended: Some(last_appended),
                },
            }),
        });
        out
    }

    /// Process a peer's `AppendEntries` response (§5.3).
    ///
    /// On Success: record the peer's new `matchIndex`, then attempt to
    /// advance `commit_index` — but only to an entry from the leader's
    /// current term (§5.4.2; see `become_leader`'s no-op for why).
    /// When commit advances, emit an `Action::Apply` for the newly
    /// committed range and bump `last_applied` so we don't replay it.
    ///
    /// On Conflict: rewind the peer's `nextIndex` to the hint and
    /// immediately re-send to that peer alone — waiting a full heartbeat
    /// interval just to retry a known-broken peer is wasteful.
    #[instrument(target = "jotun::engine", skip_all)]
    fn on_append_entries_response(
        &mut self,
        peer: NodeId,
        response: AppendEntriesResponse,
    ) -> Vec<Action<C>> {
        if response.term > self.current_term() {
            self.become_follower(response.term);
            return vec![self.persist_hard_state()];
        }
        if response.term < self.current_term() {
            return vec![];
        }
        let RoleState::Leader(leader) = &mut self.state.role else {
            return vec![];
        };

        let leader_last = self
            .state
            .log
            .last_log_id()
            .map_or(LogIndex::ZERO, |l| l.index);

        match response.result {
            AppendEntriesResult::Success { last_appended } => {
                let last_appended = last_appended.unwrap_or(LogIndex::ZERO);
                // A correct follower never acks beyond what we sent, which
                // is bounded by our own log. Reject impossible acks rather
                // than poisoning matchIndex with values we can't even look
                // up — see §5.3 commit safety.
                if last_appended > leader_last {
                    return vec![];
                }
                leader.progress.record_success(peer, last_appended);

                let RoleState::Leader(leader) = &self.state.role else {
                    unreachable!("we just confirmed Leader above");
                };
                let n = leader.progress.majority_index(leader_last);

                if n > self.state.commit_index
                    && self.state.log.term_at(n) == Some(self.state.current_term)
                {
                    self.state.commit_index = n;
                    return self.drain_apply();
                }
                vec![]
            }
            AppendEntriesResult::Conflict { next_index_hint } => {
                // Clamp the hint to our own log: nextIndex must never point
                // past leader_last + 1. A buggy or malicious follower could
                // otherwise push us into synthesising prev_log_id from a
                // region of our log that doesn't exist.
                let cap = leader_last.next();
                let hint = next_index_hint.min(cap);
                leader.progress.record_conflict(peer, hint);
                vec![self.append_entries_to(peer)]
            }
        }
    }

    /// Slice newly-committed-but-not-yet-applied entries out of the log
    /// into an `Action::Apply`, then bump `last_applied` to the new
    /// commit point. Returns an empty vec if there's nothing to apply.
    fn drain_apply(&mut self) -> Vec<Action<C>> {
        let from = LogIndex::new(self.state.last_applied.get() + 1);
        let to = self.state.commit_index;
        if from > to {
            return vec![];
        }
        let entries: Vec<LogEntry<C>> = (from.get()..=to.get())
            .filter_map(|i| self.state.log.entry_at(LogIndex::new(i)).cloned())
            .collect();
        if entries.is_empty() {
            return vec![];
        }
        self.state.last_applied = to;
        vec![Action::Apply(entries)]
    }

    /// Send one `AppendEntries` per peer carrying every log entry from that
    /// peer's `nextIndex` onward. When the peer is caught up the slice is
    /// empty (effective heartbeat); when behind it carries the missing tail.
    /// Called from `on_tick` (heartbeat), `become_leader` (initial broadcast),
    /// and `on_client_proposal` (post-append replication).
    fn broadcast_append_entries(&self) -> Vec<Action<C>> {
        if !matches!(self.state.role, RoleState::Leader(_)) {
            return vec![];
        }
        self.peers
            .iter()
            .copied()
            .map(|peer| self.append_entries_to(peer))
            .collect()
    }

    /// Build the `AppendEntries` we'd send to a single peer right now,
    /// based on its current `nextIndex`. On non-leaders we fall back to
    /// nextIndex = 1 since there's no progress map; in practice this is
    /// only called from leader paths.
    fn append_entries_to(&self, peer: NodeId) -> Action<C> {
        let next = match &self.state.role {
            RoleState::Leader(l) => l.progress.next_for(peer).unwrap_or(LogIndex::new(1)),
            _ => LogIndex::new(1),
        };
        debug_assert!(next.get() >= 1, "nextIndex floor is 1");
        let prev_log_index = LogIndex::new(next.get() - 1);
        let prev_log_id = self.state.log.entry_at(prev_log_index).map(|e| e.id);
        let entries = self.state.log.entries_from(next).to_vec();

        Action::Send {
            to: peer,
            message: AppendEntriesRequest(RequestAppendEntries {
                term: self.state.current_term,
                leader_id: self.id,
                prev_log_id,
                entries,
                leader_commit: self.state.commit_index,
            }),
        }
    }

    /// Build an `AppendEntries` Conflict response. Helper for the rejection
    /// paths in [`Engine::on_append_entries_request`].
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

    // =========================================================================
    // Role transitions and the timer reset they delegate to
    // =========================================================================

    /// Transition to follower. Does NOT reset the election timer — per §5.2,
    /// the timer resets only on accepting `AppendEntries` from the current
    /// leader or granting a vote. Callers that need both should do so
    /// explicitly after this returns.
    fn become_follower(&mut self, term: Term) {
        let from_term = self.state.current_term;
        self.state.current_term = term;
        // §5.1: voted_for is per-term. Only clear it when the term actually
        // advances — a same-term step-down (e.g. a candidate discovering
        // the legitimate current-term leader via AppendEntries) must
        // preserve the self-vote already cast in this term, otherwise a
        // delayed RequestVote from another candidate could be granted and
        // we'd violate "at most one vote per term".
        if term > from_term {
            self.state.voted_for = None;
        }
        self.state.role = RoleState::Follower(FollowerState::default());

        if from_term != term {
            telemetry::term_advanced(self.id, from_term, term);
        }
        telemetry::became_follower(self.id, term);
    }

    /// Transition to candidate: bump term, vote for self, reset the election
    /// timer (a fresh election starts a fresh deadline).
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

    /// Transition to leader: initialize per-peer progress, append a no-op
    /// entry at the new leader's term (§5.4.2), and emit the initial
    /// `AppendEntries` broadcast.
    ///
    /// The no-op exists to make prior-term entries committable. §5.4.2
    /// forbids a leader from counting an entry as committed by majority
    /// replication alone unless the entry is from the leader's current
    /// term; the no-op guarantees there is always such an entry available
    /// without waiting for a client proposal.
    fn become_leader(&mut self) -> Vec<Action<C>> {
        let next_index = self
            .log()
            .last_log_id()
            .map_or(LogIndex::new(1), |l| l.index.next());
        let noop = LogEntry {
            id: LogId::new(next_index, self.state.current_term),
            payload: LogPayload::Noop,
        };
        self.state.log.append(noop.clone());

        let last_log_index = self.log().last_log_id().map_or(LogIndex::ZERO, |l| l.index);

        let progress = PeerProgress::new(self.peers.iter().copied(), last_log_index);
        self.state.role = RoleState::Leader(LeaderState { progress });
        telemetry::became_leader(self.id, self.state.current_term);

        // The no-op is now in our log; persist before we announce it via
        // the initial broadcast.
        let mut out = vec![Action::PersistLogEntries(vec![noop])];
        out.extend(self.broadcast_append_entries());
        out
    }

    /// Re-roll the election timeout from `Env` and zero the elapsed counter.
    /// §5.2 says to invoke this on the two events that prove the cluster is
    /// alive: granting a vote, and accepting `AppendEntries` from a current-
    /// term leader. Also called by `become_candidate` so a fresh election
    /// starts with a fresh deadline.
    fn reset_election_timer(&mut self) {
        self.state.election_elapsed = 0;
        self.state.election_timeout_ticks = self.env.next_election_timeout();
    }
}
