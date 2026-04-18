use std::collections::BTreeSet;

use crate::engine::env::Env;
use crate::engine::log::Log;
use crate::engine::peer_progress::PeerProgress;
use crate::engine::telemetry;
use crate::records::append_entries::{
    AppendEntriesResponse, AppendEntriesResult, RequestAppendEntries,
};
use crate::records::install_snapshot::{InstallSnapshotResponse, RequestInstallSnapshot};
use crate::records::log_entry::{ConfigChange, LogEntry, LogPayload};
#[allow(clippy::enum_glob_use)] // match-heavy file; variants are used unqualified throughout
use crate::records::message::Message::*;
use crate::records::vote::{RequestVote, VoteResponse, VoteResult};
use crate::types::log::LogId;
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
    /// Other nodes in the cluster (self excluded). Mutated by every
    /// [`crate::records::log_entry::ConfigChange`] entry the moment it
    /// hits our log, committed or not — that's the §4.3 single-server
    /// rule that keeps old-and-new majorities overlapping.
    pub(crate) peers: BTreeSet<NodeId>,
    /// The peer set the engine was constructed with. Snapshotted so we
    /// can recompute the active config from scratch after a log truncation
    /// rolls back uncommitted `ConfigChange` entries.
    pub(crate) initial_peers: BTreeSet<NodeId>,
    /// Bytes of the most recent snapshot, if any. Held in memory so a
    /// leader can include them in `InstallSnapshot` RPCs without going
    /// back to the host. Pass-1: just stored after `SnapshotTaken`;
    /// pass-2 will read from here to populate outbound RPCs.
    pub(crate) snapshot_bytes: Option<Vec<u8>>,
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
    /// Source of nondeterministic inputs — currently just election
    /// timeout randomization. Boxed for object safety; Send so the
    /// engine can be moved between threads.
    env: Box<dyn Env>,
    /// How often the leader emits heartbeats, in ticks. Must be smaller
    /// than the election timeout (§5.2).
    heartbeat_interval_ticks: u64,
    /// All mutable per-node state. Includes the active peer set, which
    /// the §4.3 single-server membership rule mutates from log entries.
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
        let peers: BTreeSet<NodeId> = peers.into_iter().filter(|p| *p != id).collect();
        let initial_peers = peers.clone();
        let election_timeout_ticks = env.next_election_timeout();

        Self {
            id,
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
                peers,
                initial_peers,
                snapshot_bytes: None,
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
        &self.state.peers
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
        self.state.peers.len().div_ceil(2) + 1
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
            Event::ProposeConfigChange(change) => self.on_propose_config_change(change),
            Event::SnapshotTaken {
                last_included_index,
                bytes,
            } => self.on_snapshot_taken(last_included_index, bytes),
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
        if !self.state.peers.contains(&incoming.from) {
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
            InstallSnapshotRequest(request) => {
                if request.leader_id != incoming.from {
                    return vec![];
                }
                self.on_install_snapshot_request(request)
            }
            InstallSnapshotResponse(response) => {
                self.on_install_snapshot_response(incoming.from, response)
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
                    Some(leader) => vec![Action::Redirect {
                        leader_hint: leader,
                    }],
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
        // Single-node clusters won't get a peer ack to drive commit;
        // try advancing now so propose can complete locally. No-op
        // when peers exist (their matchIndex is still < the new entry).
        out.extend(self.try_advance_commit_as_leader());
        out
    }

    /// Operator-initiated membership change (§4.3 single-server rule).
    ///
    /// Non-leaders: redirect or drop, same shape as [`Self::on_client_proposal`].
    /// Leaders apply these pre-acceptance checks:
    ///  - refuse if an uncommitted `ConfigChange` is already in the log
    ///    (at most one in flight at a time — the §4.3 safety condition);
    ///  - refuse no-ops (adding an existing member, removing a non-member).
    ///
    /// On accept: append a `LogPayload::ConfigChange` entry at
    /// `(last+1, current_term)`, mutate our own active config
    /// immediately (pre-commit), persist + broadcast.
    #[instrument(target = "jotun::engine", skip_all)]
    fn on_propose_config_change(&mut self, change: ConfigChange) -> Vec<Action<C>> {
        match &self.state.role {
            RoleState::Leader(_) => {}
            RoleState::Follower(f) => {
                return match f.leader_id {
                    Some(leader) => vec![Action::Redirect {
                        leader_hint: leader,
                    }],
                    None => vec![],
                };
            }
            RoleState::Candidate(_) => return vec![],
        }

        // §4.3 safety: at most one uncommitted ConfigChange in the log.
        if self.has_uncommitted_config_change() {
            return vec![];
        }
        // No-op refusal.
        if self.is_config_change_noop(change) {
            return vec![];
        }

        let next_index = self
            .state
            .log
            .last_log_id()
            .map_or(LogIndex::new(1), |l| l.index.next());
        let entry = LogEntry {
            id: LogId::new(next_index, self.state.current_term),
            payload: LogPayload::ConfigChange(change),
        };
        self.state.log.append(entry.clone());
        // §4.3: the config change takes effect the instant the entry is
        // in the log, committed or not.
        self.apply_config_change(change);

        let mut out = vec![Action::PersistLogEntries(vec![entry])];
        out.extend(self.broadcast_append_entries());
        out.extend(self.try_advance_commit_as_leader());
        out
    }

    // =========================================================================
    // Snapshot — host-driven log compaction (§7)
    // =========================================================================

    /// The host has finished producing a snapshot of its state machine
    /// covering everything applied up to `last_included_index`. Truncate
    /// our in-memory log up to and including that index, record the
    /// snapshot floor, stash the bytes for future `InstallSnapshot`
    /// RPCs, and emit a `PersistSnapshot` for the host to flush.
    ///
    /// Silently dropped if `last_included_index > commit_index` (the
    /// host can only snapshot committed state) or if it doesn't
    /// advance the floor (a stale snapshot, harmless to ignore).
    #[instrument(target = "jotun::engine", skip_all)]
    fn on_snapshot_taken(
        &mut self,
        last_included_index: LogIndex,
        bytes: Vec<u8>,
    ) -> Vec<Action<C>> {
        if last_included_index > self.state.commit_index {
            return vec![];
        }
        if last_included_index <= self.state.log.snapshot_last().index {
            return vec![];
        }
        // Term lookup must happen *before* install_snapshot moves the
        // floor — install_snapshot will overwrite snapshot_last and
        // potentially drop the entry from in-memory storage.
        let Some(last_included_term) = self.state.log.term_at(last_included_index) else {
            // Index sits past the in-memory tail or in some other
            // unreachable spot — refuse rather than fabricate a term.
            return vec![];
        };
        self.state
            .log
            .install_snapshot(last_included_index, last_included_term);
        self.state.snapshot_bytes = Some(bytes.clone());
        vec![Action::PersistSnapshot {
            last_included_index,
            last_included_term,
            bytes,
        }]
    }

    // =========================================================================
    // Membership helpers (§4.3)
    // =========================================================================

    /// True iff any entry past `commit_index` is a `ConfigChange`. A leader
    /// uses this to enforce "at most one in flight" before accepting a new
    /// membership proposal.
    fn has_uncommitted_config_change(&self) -> bool {
        let from = self.state.commit_index.get() + 1;
        let last = self.state.log.last_log_id().map_or(0, |l| l.index.get());
        for i in from..=last {
            if let Some(entry) = self.state.log.entry_at(LogIndex::new(i))
                && matches!(entry.payload, LogPayload::ConfigChange(_))
            {
                return true;
            }
        }
        false
    }

    /// True iff `change` would have no effect on the current active config:
    /// adding a member that's already present, or removing one that isn't.
    /// Self-add / self-remove are evaluated against the engine's own id.
    fn is_config_change_noop(&self, change: ConfigChange) -> bool {
        match change {
            ConfigChange::AddPeer(id) => id == self.id || self.state.peers.contains(&id),
            ConfigChange::RemovePeer(id) => id != self.id && !self.state.peers.contains(&id),
        }
    }

    /// Mutate the active peer set for one `ConfigChange`. Called once per
    /// entry appended to the log (leader or follower), and once per entry
    /// walked during [`Self::recompute_active_config`] after a truncation.
    ///
    /// Also keeps the leader's `PeerProgress` in sync: a fresh `AddPeer`
    /// initialises tracking; `RemovePeer` drops it. Without this the leader
    /// would either ignore the new peer entirely (no replication) or keep
    /// computing majority over a peer that no longer exists.
    ///
    /// Self-adds and self-removes toggle the engine's *own* membership,
    /// which the peer set doesn't track (self is always implicit), so
    /// those are no-ops for the set itself — stepping down on self-removal
    /// is a separate concern handled at commit time.
    fn apply_config_change(&mut self, change: ConfigChange) {
        match change {
            ConfigChange::AddPeer(id) => {
                if id != self.id {
                    self.state.peers.insert(id);
                    if let RoleState::Leader(l) = &mut self.state.role {
                        let leader_last = self
                            .state
                            .log
                            .last_log_id()
                            .map_or(LogIndex::ZERO, |l| l.index);
                        l.progress.add_peer(id, leader_last);
                    }
                }
            }
            ConfigChange::RemovePeer(id) => {
                if id != self.id {
                    self.state.peers.remove(&id);
                    if let RoleState::Leader(l) = &mut self.state.role {
                        l.progress.remove_peer(id);
                    }
                }
            }
        }
    }

    /// True iff the log range `(prior_commit, new_commit]` contains a
    /// `RemovePeer(self)` entry. Used after a commit advance to detect
    /// "the leader just committed its own removal" — at which point §4.3
    /// requires it to step down.
    fn committed_self_removal(&self, prior_commit: LogIndex, new_commit: LogIndex) -> bool {
        let from = prior_commit.get() + 1;
        for i in from..=new_commit.get() {
            if let Some(entry) = self.state.log.entry_at(LogIndex::new(i))
                && let LogPayload::ConfigChange(ConfigChange::RemovePeer(id)) = entry.payload
                && id == self.id
            {
                return true;
            }
        }
        false
    }

    /// Reset the active peer set to the initial config, then replay every
    /// `ConfigChange` entry currently in the log. Called after a
    /// `Log::truncate_from` that may have rolled back uncommitted
    /// membership changes — we can't undo individual changes reliably
    /// (`AddPeer`'s inverse depends on whether the peer was there before),
    /// so we recompute from scratch.
    fn recompute_active_config(&mut self) {
        self.state.peers.clone_from(&self.state.initial_peers);
        let last = self.state.log.last_log_id().map_or(0, |l| l.index.get());
        for i in 1..=last {
            if let Some(entry) = self.state.log.entry_at(LogIndex::new(i))
                && let LogPayload::ConfigChange(change) = entry.payload
            {
                self.apply_config_change(change);
            }
        }
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

        out.extend(self.state.peers.iter().map(|&peer| Action::Send {
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
                .term_at(prev.index)
                .is_none_or(|t| t != prev.term)
        {
            // See §5.3 conflict-hint comment elsewhere: cap at prev.index.
            // term_at handles the snapshot floor: prev_log at exactly the
            // snapshot's tail returns the snapshot's term, not None, so a
            // follower with floor=N can validate prev=(N, snapshot_term).
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
        let mut truncated = false;
        let snapshot_floor = self.state.log.snapshot_last().index;
        for entry in request.entries {
            // Entries at or below the snapshot floor are already
            // captured by the snapshot — silently skip them. Happens
            // when the leader's view of nextIndex lags our snapshot
            // install (we haven't acked InstallSnapshot yet).
            if entry.id.index <= snapshot_floor {
                continue;
            }
            match self.state.log.entry_at(entry.id.index) {
                None => {
                    // Plain append: incrementally apply any ConfigChange.
                    if let LogPayload::ConfigChange(change) = entry.payload {
                        self.apply_config_change(change);
                    }
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
                        truncated = true;
                        newly_persisted.push(entry.clone());
                        self.state.log.append(entry);
                    }
                }
            }
        }
        // After a truncate the active config may be stale (rolled-back CC
        // entries are still in `peers`). Recompute from the post-truncate
        // log state — covers both the truncate itself and any CC entries
        // that landed in the same batch.
        if truncated {
            self.recompute_active_config();
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
                self.try_advance_commit_as_leader()
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

    /// Receive an `InstallSnapshot` from the current leader (§7).
    ///
    /// Term checks first (same shape as `on_append_entries_request`).
    /// On accept: install the snapshot floor in our log, persist the
    /// snapshot bytes, advance `commit_index` and `last_applied` to
    /// the snapshot's tail, and emit `ApplySnapshot` for the host's
    /// state machine. The `PersistSnapshot` action is emitted before
    /// the final response Send to honour the persist-before-send rule.
    #[instrument(target = "jotun::engine", skip_all)]
    fn on_install_snapshot_request(&mut self, request: RequestInstallSnapshot) -> Vec<Action<C>> {
        let prior_term = self.state.current_term;
        let prior_voted_for = self.state.voted_for;

        if request.term > self.state.current_term
            || (request.term == self.state.current_term
                && matches!(self.state.role, RoleState::Candidate(_)))
        {
            self.become_follower(request.term);
        }
        if request.term < self.state.current_term {
            // Stale leader. Reply with our (higher) term so they step
            // down. No state mutation.
            return vec![Action::Send {
                to: request.leader_id,
                message: InstallSnapshotResponse(InstallSnapshotResponse {
                    term: self.state.current_term,
                }),
            }];
        }

        // Record the leader for client redirects.
        if let RoleState::Follower(f) = &mut self.state.role {
            f.leader_id = Some(request.leader_id);
        }
        self.reset_election_timer();

        let hard_state_changed =
            self.state.current_term != prior_term || self.state.voted_for != prior_voted_for;

        // Stale snapshot at or below our existing floor: ack but do
        // nothing else.
        if request.last_included.index <= self.state.log.snapshot_last().index {
            let mut out = Vec::new();
            if hard_state_changed {
                out.push(self.persist_hard_state());
            }
            out.push(Action::Send {
                to: request.leader_id,
                message: InstallSnapshotResponse(InstallSnapshotResponse {
                    term: self.state.current_term,
                }),
            });
            return out;
        }

        // Install the snapshot floor. Log::install_snapshot keeps
        // post-floor entries iff our log already had a matching
        // boundary entry; otherwise it wipes the in-memory tail.
        self.state
            .log
            .install_snapshot(request.last_included.index, request.last_included.term);
        self.state.snapshot_bytes = Some(request.data.clone());

        // Advance commit / applied past the snapshot. The snapshot
        // captures everything up to `last_included.index`, so we treat
        // it as if those entries had already been applied.
        if request.last_included.index > self.state.commit_index {
            self.state.commit_index = request.last_included.index;
        }
        if request.last_included.index > self.state.last_applied {
            self.state.last_applied = request.last_included.index;
        }
        // leader_commit may also exceed the snapshot's tail — propagate.
        if request.leader_commit > self.state.commit_index {
            // Cap at our last log index, same rule as on_append_entries.
            let last_log = self
                .state
                .log
                .last_log_id()
                .map_or(LogIndex::ZERO, |l| l.index);
            self.state.commit_index = request.leader_commit.min(last_log);
        }
        // Recompute active config from scratch — entries below the
        // floor are gone, so we need to start over from initial_peers.
        // The snapshot is opaque to us (host's state-machine bytes);
        // we trust the leader that whatever membership it implies is
        // captured in the post-snapshot log entries we still have or
        // will receive.
        self.recompute_active_config();

        let mut out = Vec::new();
        if hard_state_changed {
            out.push(self.persist_hard_state());
        }
        out.push(Action::PersistSnapshot {
            last_included_index: request.last_included.index,
            last_included_term: request.last_included.term,
            bytes: request.data.clone(),
        });
        out.push(Action::ApplySnapshot {
            bytes: request.data,
        });
        out.push(Action::Send {
            to: request.leader_id,
            message: InstallSnapshotResponse(InstallSnapshotResponse {
                term: self.state.current_term,
            }),
        });
        out
    }

    /// Process a peer's `InstallSnapshot` reply.
    ///
    /// On higher term: step down. Otherwise: assume the peer caught
    /// up to our snapshot's tail and bump `matchIndex` / `nextIndex`
    /// accordingly. The snapshot's `last_included.index` is the
    /// highest entry the peer can be holding; treat it as a Success
    /// at that index.
    #[instrument(target = "jotun::engine", skip_all)]
    fn on_install_snapshot_response(
        &mut self,
        peer: NodeId,
        response: InstallSnapshotResponse,
    ) -> Vec<Action<C>> {
        if response.term > self.state.current_term {
            self.become_follower(response.term);
            return vec![self.persist_hard_state()];
        }
        if response.term < self.state.current_term {
            return vec![];
        }
        let RoleState::Leader(leader) = &mut self.state.role else {
            return vec![];
        };
        // The peer just installed a snapshot whose tail is at our log's
        // current floor (which is what we sent). record_success only
        // advances when last_appended > matchIndex; an idempotent
        // InstallSnapshot ack at exactly the floor would otherwise
        // leave nextIndex stuck at floor, looping the leader forever.
        // Push nextIndex to floor + 1 unconditionally so the next
        // broadcast switches back to AppendEntries.
        let snapshot_floor = self.state.log.snapshot_last().index;
        leader.progress.record_success(peer, snapshot_floor);
        leader
            .progress
            .ensure_next_at_least(peer, snapshot_floor.next());
        vec![]
    }

    /// Run the §5.3 commit-advance check from a leader's perspective.
    /// `majority_index` across (peer `matchIndex`, leader's own
    /// `last_log_id`) — if past `commit_index` AND the entry at that
    /// index is from `current_term` (§5.4.2), advance commit, drain
    /// `Apply`, and step down on self-removal. Idempotent.
    ///
    /// Single-node clusters need this called proactively after every
    /// leader-side append: there are no peers to ack, so the only
    /// commit driver is the leader's own append moving `leader_last`
    /// past the prior commit point.
    fn try_advance_commit_as_leader(&mut self) -> Vec<Action<C>> {
        if !matches!(self.state.role, RoleState::Leader(_)) {
            return vec![];
        }
        let leader_last = self
            .state
            .log
            .last_log_id()
            .map_or(LogIndex::ZERO, |l| l.index);
        let RoleState::Leader(leader) = &self.state.role else {
            unreachable!("just matched");
        };
        let n = leader.progress.majority_index(leader_last);
        if n > self.state.commit_index && self.state.log.term_at(n) == Some(self.state.current_term)
        {
            let prior_commit = self.state.commit_index;
            self.state.commit_index = n;
            if self.committed_self_removal(prior_commit, n) {
                let prior_term = self.state.current_term;
                // Same-term step-down preserves voted_for; no Persist
                // needed beyond what drain_apply might emit.
                self.become_follower(prior_term);
            }
            return self.drain_apply();
        }
        vec![]
    }

    /// Slice newly-committed-but-not-yet-applied entries out of the log
    /// into an `Action::Apply`, then bump `last_applied` to the new
    /// commit point. Returns an empty vec if there's nothing to apply.
    ///
    /// Engine-internal payloads (`Noop`, `ConfigChange`) are filtered
    /// out — `last_applied` still advances past them, but the host's
    /// state machine never sees them. Only `Command` entries reach
    /// the application.
    fn drain_apply(&mut self) -> Vec<Action<C>> {
        let from = LogIndex::new(self.state.last_applied.get() + 1);
        let to = self.state.commit_index;
        if from > to {
            return vec![];
        }
        let entries: Vec<LogEntry<C>> = (from.get()..=to.get())
            .filter_map(|i| self.state.log.entry_at(LogIndex::new(i)).cloned())
            .filter(|e| matches!(e.payload, LogPayload::Command(_)))
            .collect();
        // Always advance last_applied to the commit boundary, even when
        // every entry in the range was filtered out — Noop/ConfigChange
        // entries are "applied" the moment they commit, just invisibly.
        self.state.last_applied = to;
        if entries.is_empty() {
            return vec![];
        }
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
        self.state
            .peers
            .iter()
            .copied()
            .map(|peer| self.append_entries_to(peer))
            .collect()
    }

    /// Build the next replication message for a single peer, based on
    /// its current `nextIndex`. Usually an `AppendEntries`, but if the
    /// peer's `nextIndex` has fallen below the leader's snapshot floor
    /// we need an `InstallSnapshot` instead — there's no `prev_log_id`
    /// to construct from log entries that have been compacted away.
    /// On non-leaders we fall back to `nextIndex = 1`; in practice
    /// this is only called from leader paths.
    fn append_entries_to(&self, peer: NodeId) -> Action<C> {
        let next = match &self.state.role {
            RoleState::Leader(l) => l.progress.next_for(peer).unwrap_or(LogIndex::new(1)),
            _ => LogIndex::new(1),
        };
        debug_assert!(next.get() >= 1, "nextIndex floor is 1");

        // §7: if the peer's nextIndex is at or below the snapshot floor,
        // we can't address the prev entry — send the snapshot instead.
        let snapshot_floor = self.state.log.snapshot_last().index;
        if snapshot_floor != LogIndex::ZERO && next.get() <= snapshot_floor.get() {
            return self.install_snapshot_to(peer);
        }

        let prev_log_index = LogIndex::new(next.get() - 1);
        // term_at handles the snapshot floor: at index == snapshot_last,
        // it returns the snapshot's term (no in-memory entry exists).
        let prev_log_id = self
            .state
            .log
            .term_at(prev_log_index)
            .map(|t| LogId::new(prev_log_index, t))
            .filter(|_| prev_log_index != LogIndex::ZERO);
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

    /// Build an `InstallSnapshot` for a peer that has fallen behind
    /// the leader's log floor. Caller must have already checked that
    /// `nextIndex[peer] ≤ snapshot_last_index` and that we have
    /// snapshot bytes in memory.
    fn install_snapshot_to(&self, peer: NodeId) -> Action<C> {
        let last_included = self.state.log.snapshot_last();
        let bytes = self.state.snapshot_bytes.clone().unwrap_or_default();
        Action::Send {
            to: peer,
            message: InstallSnapshotRequest(RequestInstallSnapshot {
                term: self.state.current_term,
                leader_id: self.id,
                last_included,
                data: bytes,
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

        let progress = PeerProgress::new(self.state.peers.iter().copied(), last_log_index);
        self.state.role = RoleState::Leader(LeaderState { progress });
        telemetry::became_leader(self.id, self.state.current_term);

        // The no-op is now in our log; persist before we announce it via
        // the initial broadcast.
        let mut out = vec![Action::PersistLogEntries(vec![noop])];
        out.extend(self.broadcast_append_entries());
        // Single-node clusters: no peers to ack, so the noop's commit
        // has to be driven from the leader's own appended state.
        out.extend(self.try_advance_commit_as_leader());
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
