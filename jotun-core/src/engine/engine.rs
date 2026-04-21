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
use crate::records::pre_vote::{PreVoteResponse, RequestPreVote};
use crate::records::timeout_now::TimeoutNow;
use crate::records::vote::{RequestVote, VoteResponse, VoteResult};
use crate::types::log::LogId;
use crate::{
    engine::{
        action::{Action, ReadFailure},
        event::Event,
        incoming::Incoming,
        role_state::{
            CandidateState, FollowerState, LeaderState, PendingRead, PreCandidateState, RoleState,
            SnapshotTransfer,
        },
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
    /// back to the host.
    pub(crate) snapshot_bytes: Option<Vec<u8>>,
    /// Cluster membership as of the most recent snapshot's tail.
    /// Shipped alongside `snapshot_bytes` in outbound
    /// `InstallSnapshot` RPCs so receivers can restore the right
    /// config on install.
    pub(crate) snapshot_peers: Option<BTreeSet<NodeId>>,
    /// Follower-side partial snapshot assembly, if we're in the
    /// middle of a chunked `InstallSnapshot` transfer.
    pub(crate) pending_snapshot_install: Option<PendingSnapshotInstall>,
    /// Highest threshold band we have already hinted compaction for.
    /// Compared against the number of applied entries past the
    /// current snapshot floor.
    pub(crate) snapshot_hint_band: u64,
    /// `true` once the live-log guardrail has fired for the current
    /// (above-threshold) live-log window. Reset to `false` when the
    /// snapshot floor advances (which shrinks the live log back below
    /// threshold).
    pub(crate) max_log_hint_armed: bool,
    /// Monotonic tick counter, incremented exactly once per
    /// [`Event::Tick`]. Drives the leader-lease expiry check — the
    /// lease is derived from the *arrival* tick of the most recent
    /// majority ack, not from wall-clock.
    pub(crate) current_tick: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct PendingSnapshotInstall {
    pub(crate) last_included: LogId,
    pub(crate) leader_commit: LogIndex,
    pub(crate) peers: BTreeSet<NodeId>,
    pub(crate) bytes: Vec<u8>,
}

impl PendingSnapshotInstall {
    fn next_offset(&self) -> u64 {
        u64::try_from(self.bytes.len()).unwrap_or(u64::MAX)
    }
}

/// What the host hands back to [`Engine::recover_from`] at boot:
/// everything Raft requires to be durable (§5.1 + §7).
#[derive(Debug, Clone)]
pub struct RecoveredHardState<C> {
    /// The term the engine was at when the last `PersistHardState`
    /// hit disk, or `Term::ZERO` for a fresh node.
    pub current_term: Term,
    /// Whoever we last granted our vote to in `current_term`, if any.
    /// Restoring this is critical: §5.1 "at most one vote per term"
    /// is only enforceable if a crashed-and-recovered node remembers
    /// the vote it already cast.
    pub voted_for: Option<NodeId>,
    /// Most recent snapshot, or `None` for a fresh node.
    pub snapshot: Option<RecoveredSnapshot>,
    /// Log entries persisted past the snapshot floor (or the full
    /// log if there's no snapshot). Indices must be contiguous and
    /// start at `snapshot.last_included_index + 1` when a snapshot
    /// is present, or at 1 otherwise.
    pub post_snapshot_log: Vec<LogEntry<C>>,
}

/// The durable snapshot piece of [`RecoveredHardState`].
///
/// `peers` is the cluster membership as of `last_included_index`,
/// encoded alongside the state-machine bytes so membership survives
/// snapshot install and compaction. Without it, a node that snapshots
/// after a committed `AddPeer` / `RemovePeer` would revert to the
/// bootstrap config on restart and compute the wrong majority.
#[derive(Debug, Clone)]
pub struct RecoveredSnapshot {
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub peers: BTreeSet<NodeId>,
    pub bytes: Vec<u8>,
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
    /// Maximum size of one outbound snapshot chunk.
    snapshot_chunk_size_bytes: usize,
    /// Applied-entry threshold for emitting `Action::SnapshotHint`.
    snapshot_hint_threshold_entries: u64,
    /// Live-log guardrail: max entries above the snapshot floor before
    /// we force a `SnapshotHint`, even if apply is lagging.
    max_log_entries: u64,
    /// §9.6 pre-vote enabled. When on, election-timer expiry enters
    /// `PreCandidate` instead of bumping the term directly.
    pre_vote: bool,
    /// Leader-lease window (§9) in ticks. See [`EngineConfig::lease_duration_ticks`].
    lease_duration_ticks: u64,
    /// Pull-model observability counters. Gauges are reconstructed
    /// from `state` on read; counters are updated in-place here.
    metrics: crate::engine::metrics::MetricsCounters,
    /// All mutable per-node state. Includes the active peer set, which
    /// the §4.3 single-server membership rule mutates from log entries.
    state: RaftState<C>,
}

/// Optional engine behavior knobs beyond the core Raft constructor
/// parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct EngineConfig {
    /// Maximum bytes to place in one `InstallSnapshot` chunk.
    pub snapshot_chunk_size_bytes: usize,
    /// Emit `Action::SnapshotHint` once the number of applied entries
    /// past the current floor crosses this threshold. `0` disables the
    /// hint.
    pub snapshot_hint_threshold_entries: u64,
    /// Emit `Action::SnapshotHint` whenever the number of live log
    /// entries (entries above the current snapshot floor) crosses this
    /// threshold — independent of how many are applied. `0` disables
    /// the guardrail.
    ///
    /// This exists so a stuck follower whose apply path is slow but
    /// whose log keeps growing doesn't run the node out of disk. The
    /// `snapshot_hint_threshold_entries` path is about "compact once
    /// we've applied enough to reclaim space"; this one is about
    /// "compact before the log itself gets out of hand."
    pub max_log_entries: u64,
    /// Enable the §9.6 pre-vote extension. When on, a follower whose
    /// election timer fires first enters `RoleState::PreCandidate`
    /// and asks peers whether they would vote for it. The term is
    /// only bumped if a majority grant. Default: `true`.
    ///
    /// Turning pre-vote off reverts to the textbook §5.2 behaviour:
    /// election timeout bumps the term immediately. Only do this for
    /// reproducibility of older test traces.
    pub pre_vote: bool,
    /// Leader-lease window, in ticks (§9). When a leader has observed
    /// majority AE-acks within the last `lease_duration_ticks`, it may
    /// serve a linearizable read synchronously without the usual
    /// `ReadIndex` heartbeat round-trip.
    ///
    /// Safety requires
    /// `lease_duration_ticks < election_timeout_min - heartbeat_interval_ticks`
    /// so no peer can time out and win an election inside our lease
    /// window. `0` disables the lease entirely (every read goes
    /// through `ReadIndex`).
    pub lease_duration_ticks: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            snapshot_chunk_size_bytes: 64 * 1024,
            snapshot_hint_threshold_entries: 0,
            max_log_entries: 0,
            pre_vote: true,
            lease_duration_ticks: 10,
        }
    }
}

impl EngineConfig {
    #[must_use]
    pub fn new(snapshot_chunk_size_bytes: usize, snapshot_hint_threshold_entries: u64) -> Self {
        Self {
            snapshot_chunk_size_bytes,
            snapshot_hint_threshold_entries,
            max_log_entries: 0,
            pre_vote: true,
            lease_duration_ticks: 10,
        }
    }

    /// Builder-style override for `pre_vote`. Useful for callers
    /// that start from `Default::default()` and want to change the
    /// one knob without naming the other fields (`EngineConfig` is
    /// `#[non_exhaustive]`).
    #[must_use]
    pub fn with_pre_vote(mut self, pre_vote: bool) -> Self {
        self.pre_vote = pre_vote;
        self
    }

    /// Builder-style override for `max_log_entries`. `0` disables the
    /// live-log guardrail.
    #[must_use]
    pub fn with_max_log_entries(mut self, max: u64) -> Self {
        self.max_log_entries = max;
        self
    }

    /// Builder-style override for `lease_duration_ticks`. `0` disables
    /// the leader-lease fast path.
    #[must_use]
    pub fn with_lease_duration_ticks(mut self, ticks: u64) -> Self {
        self.lease_duration_ticks = ticks;
        self
    }
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
        env: Box<dyn Env>,
        heartbeat_interval_ticks: u64,
    ) -> Self {
        Self::with_config(
            id,
            peers,
            env,
            heartbeat_interval_ticks,
            EngineConfig::default(),
        )
    }

    /// Create a fresh follower with explicit snapshot-transfer and
    /// snapshot-hint behavior knobs.
    #[must_use]
    pub fn with_config(
        id: NodeId,
        peers: impl IntoIterator<Item = NodeId>,
        mut env: Box<dyn Env>,
        heartbeat_interval_ticks: u64,
        config: EngineConfig,
    ) -> Self {
        let peers: BTreeSet<NodeId> = peers.into_iter().filter(|p| *p != id).collect();
        let initial_peers = peers.clone();
        let election_timeout_ticks = env.next_election_timeout();

        Self {
            id,
            env,
            heartbeat_interval_ticks,
            snapshot_chunk_size_bytes: config.snapshot_chunk_size_bytes.max(1),
            snapshot_hint_threshold_entries: config.snapshot_hint_threshold_entries,
            max_log_entries: config.max_log_entries,
            pre_vote: config.pre_vote,
            lease_duration_ticks: config.lease_duration_ticks,
            metrics: crate::engine::metrics::MetricsCounters::default(),
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
                snapshot_peers: None,
                pending_snapshot_install: None,
                snapshot_hint_band: 0,
                max_log_hint_armed: false,
                current_tick: 0,
            },
        }
    }

    /// Restore an engine from previously-persisted state, as emerges
    /// from [`Action::PersistHardState`], [`Action::PersistLogEntries`],
    /// and [`Action::PersistSnapshot`] (host reads them back at boot).
    ///
    /// After this call the engine holds:
    ///  - `current_term` and `voted_for` from the supplied
    ///    [`RecoveredHardState`] (§5.1 hard state — a safely-cast vote
    ///    survives restart);
    ///  - a snapshot floor at `(last_included_index, last_included_term)`
    ///    with the supplied snapshot bytes stashed for outbound
    ///    `InstallSnapshot` RPCs;
    ///  - any post-snapshot log entries in `post_snapshot_log`;
    ///  - `commit_index == last_applied == snapshot.last_included_index`
    ///    (we only know snapshotted state is committed; everything past
    ///    the snapshot floor is uncommitted until a current-term leader
    ///    tells us via `AppendEntries`).
    ///
    /// Peer set stays at the one passed to `new()` for the moment —
    /// the snapshot-carries-membership fix lands in a follow-up commit.
    pub fn recover_from(&mut self, recovered: RecoveredHardState<C>) {
        self.state.current_term = recovered.current_term;
        self.state.voted_for = recovered.voted_for;

        // §7: a snapshot's peer set is the cluster config as of
        // `last_included_index`. Restore it before replaying any
        // post-snapshot CCs so `apply_config_change` deltas land
        // on the right base.
        let restored_peers = recovered.snapshot.as_ref().map(|s| s.peers.clone());
        if let Some(snap) = recovered.snapshot {
            self.state
                .log
                .install_snapshot(snap.last_included_index, snap.last_included_term);
            self.state.snapshot_bytes = Some(snap.bytes);
            self.state.snapshot_peers = Some(snap.peers);
            self.state.commit_index = snap.last_included_index;
            self.state.last_applied = snap.last_included_index;
        }
        if let Some(mut peers) = restored_peers {
            peers.remove(&self.id); // self is implicit
            self.state.peers = peers;
        }

        // Replay post-snapshot CCs against the restored config so the
        // engine's active set reflects everything known at boot
        // (committed or not, per §4.3).
        for entry in recovered.post_snapshot_log {
            if let LogPayload::ConfigChange(change) = &entry.payload {
                let change = *change;
                self.apply_config_change(change);
            }
            self.state.log.append(entry);
        }

        // Reset role to Follower regardless of what we were before the
        // crash. Volatile state (role, election/heartbeat counters) is
        // rebuilt by the host's tick loop, which will detect a missing
        // leader and start a new election at `current_term + 1`.
        self.state.role = RoleState::Follower(FollowerState::default());
        self.state.election_elapsed = 0;
        self.state.heartbeat_elapsed = 0;
        self.state.pending_snapshot_install = None;
        self.state.snapshot_hint_band = 0;
        self.state.max_log_hint_armed = false;
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

    /// Snapshot of the engine's counters and current gauges. Pull it
    /// on whatever cadence the caller likes — reading never blocks
    /// the engine's event processing.
    #[must_use]
    pub fn metrics(&self) -> crate::engine::metrics::EngineMetrics {
        let role_code = match &self.state.role {
            RoleState::Follower(_) => 0,
            RoleState::PreCandidate(_) => 1,
            RoleState::Candidate(_) => 2,
            RoleState::Leader(_) => 3,
        };
        let last_index = self.state.log.last_log_id().map_or(0, |id| id.index.get());
        let snapshot_index = self.state.log.snapshot_last().index.get();
        let log_len = last_index.saturating_sub(snapshot_index);
        crate::engine::metrics::EngineMetrics {
            elections_started: self.metrics.elections_started,
            pre_votes_granted: self.metrics.pre_votes_granted,
            pre_votes_denied: self.metrics.pre_votes_denied,
            votes_granted: self.metrics.votes_granted,
            votes_denied: self.metrics.votes_denied,
            leader_elections_won: self.metrics.leader_elections_won,
            higher_term_stepdowns: self.metrics.higher_term_stepdowns,
            append_entries_sent: self.metrics.append_entries_sent,
            append_entries_received: self.metrics.append_entries_received,
            append_entries_rejected: self.metrics.append_entries_rejected,
            entries_appended: self.metrics.entries_appended,
            entries_committed: self.metrics.entries_committed,
            entries_applied: self.metrics.entries_applied,
            snapshots_sent: self.metrics.snapshots_sent,
            snapshots_installed: self.metrics.snapshots_installed,
            read_index_started: self.metrics.read_index_started,
            reads_completed: self.metrics.reads_completed,
            reads_failed: self.metrics.reads_failed,
            current_term: self.state.current_term,
            commit_index: self.state.commit_index,
            last_applied: self.state.last_applied,
            log_len,
            role_code,
        }
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

        let mut actions = match event {
            Event::Tick => self.on_tick(),
            Event::Incoming(incoming) => self.on_incoming(incoming),
            Event::ClientProposal(command) => self.on_client_proposal(command),
            Event::ClientProposalBatch(commands) => self.on_client_proposal_batch(commands),
            Event::ProposeConfigChange(change) => self.on_propose_config_change(change),
            Event::TransferLeadership { target } => self.on_transfer_leadership(target),
            Event::ProposeRead { id } => self.on_propose_read(id),
            Event::SnapshotTaken {
                last_included_index,
                bytes,
            } => self.on_snapshot_taken(last_included_index, bytes),
        };
        self.maybe_emit_snapshot_hint(&mut actions);

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

    /// Emit at most one compaction hint per applied-entry threshold
    /// band. Hints are advisory and appended after the transition's
    /// real Raft actions so hosts observe a state machine that has
    /// already applied everything through `last_included_index`.
    fn maybe_emit_snapshot_hint(&mut self, actions: &mut Vec<Action<C>>) {
        let floor = self.state.log.snapshot_last().index;
        let applied_past_floor = self.state.last_applied.get().saturating_sub(floor.get());
        let last_log_index = self
            .state
            .log
            .last_log_id()
            .map_or(0, |id| id.index.get());
        let live_log_entries = last_log_index.saturating_sub(floor.get());

        // Live-log guardrail (`max_log_entries`): fire once as the live
        // log crosses the threshold from below, re-arm once the floor
        // advances past the threshold again (i.e. live log shrinks
        // back under it).
        let max_log = self.max_log_entries;
        if max_log > 0 {
            if live_log_entries > max_log {
                if !self.state.max_log_hint_armed && self.state.last_applied > floor {
                    actions.push(Action::SnapshotHint {
                        last_included_index: self.state.last_applied,
                    });
                    self.state.max_log_hint_armed = true;
                }
            } else {
                self.state.max_log_hint_armed = false;
            }
        }

        // Applied-entries threshold: band edges.
        let threshold = self.snapshot_hint_threshold_entries;
        if threshold == 0 {
            return;
        }
        let band = applied_past_floor / threshold;
        if band < self.state.snapshot_hint_band {
            self.state.snapshot_hint_band = band;
        }
        if band > self.state.snapshot_hint_band && self.state.last_applied > floor {
            actions.push(Action::SnapshotHint {
                last_included_index: self.state.last_applied,
            });
            self.state.snapshot_hint_band = band;
        }
    }

    // =========================================================================
    // Top-level dispatch — one handler per Event variant.
    // =========================================================================

    /// Time has passed. Drives the election timer (followers/candidates) and
    /// the heartbeat interval (leaders).
    #[instrument(target = "jotun::engine", skip_all)]
    fn on_tick(&mut self) -> Vec<Action<C>> {
        // Saturating increment: the monotonic counter only matters for
        // relative lease comparisons, and u64 ticks at any realistic
        // cadence is centuries of uptime.
        self.state.current_tick = self.state.current_tick.saturating_add(1);
        match &self.state.role {
            RoleState::Follower(_) | RoleState::PreCandidate(_) | RoleState::Candidate(_) => {
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
            TimeoutNow(request) => {
                if request.leader_id != incoming.from {
                    return vec![];
                }
                self.on_timeout_now(request)
            }
            PreVoteRequest(request) => {
                if request.candidate_id != incoming.from {
                    return vec![];
                }
                self.on_pre_vote_request(request)
            }
            PreVoteResponse(response) => self.on_pre_vote_response(incoming.from, response),
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
        self.append_commands_as_leader(vec![command])
    }

    /// Batched equivalent of [`Self::on_client_proposal`]. Appends
    /// every command at consecutive indices with a single
    /// `PersistLogEntries` Action and a single broadcast per peer.
    #[instrument(target = "jotun::engine", skip_all)]
    fn on_client_proposal_batch(&mut self, commands: Vec<C>) -> Vec<Action<C>> {
        if commands.is_empty() {
            return Vec::new();
        }
        self.append_commands_as_leader(commands)
    }

    /// Shared leader-side append path for one or more client commands.
    /// Non-leaders redirect (known leader) or drop. On leader, appends
    /// every command at consecutive `(last+1, last+2, ...)` indices
    /// under the current term, emits one `PersistLogEntries` for the
    /// batch, broadcasts once to all peers, and tries to advance
    /// commit (single-node path).
    fn append_commands_as_leader(&mut self, commands: Vec<C>) -> Vec<Action<C>> {
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
            RoleState::PreCandidate(_) | RoleState::Candidate(_) => return vec![],
        }

        if commands.is_empty() {
            return Vec::new();
        }

        let mut next_index = self
            .state
            .log
            .last_log_id()
            .map_or(LogIndex::new(1), |l| l.index.next());
        let term = self.state.current_term;
        let mut entries: Vec<LogEntry<C>> = Vec::with_capacity(commands.len());
        for command in commands {
            let entry = LogEntry {
                id: LogId::new(next_index, term),
                payload: LogPayload::Command(command),
            };
            self.state.log.append(entry.clone());
            entries.push(entry);
            next_index = next_index.next();
        }

        let mut out = vec![Action::PersistLogEntries(entries)];
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
            RoleState::PreCandidate(_) | RoleState::Candidate(_) => return vec![],
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

    /// Operator-initiated leadership transfer to `target`.
    ///
    /// Leaders either send `TimeoutNow` immediately if `target` is
    /// already caught up, or remember `target` and keep driving normal
    /// replication until it is. Followers redirect if they know the
    /// current leader; candidates and leaderless followers drop.
    #[instrument(target = "jotun::engine", skip_all)]
    fn on_transfer_leadership(&mut self, target: NodeId) -> Vec<Action<C>> {
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
            RoleState::PreCandidate(_) | RoleState::Candidate(_) => return vec![],
        }

        if !self.state.peers.contains(&target) {
            return vec![];
        }
        if let RoleState::Leader(leader) = &mut self.state.role {
            leader.transfer_target = Some(target);
        }
        if self.transfer_target_is_caught_up(target) {
            if let RoleState::Leader(leader) = &mut self.state.role {
                leader.transfer_target = None;
            }
            return vec![self.timeout_now(target)];
        }
        vec![self.append_entries_to(target)]
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
        // §7 + §4.3: the snapshot's peer set is the committed config as
        // of `last_included_index` — initial_peers plus every CC with
        // index ≤ last_included_index. We compute this BEFORE moving
        // the floor (install_snapshot drops those entries).
        let mut peers_at_floor = self.state.initial_peers.clone();
        let first = self.state.log.first_index().get();
        for i in first..=last_included_index.get() {
            if let Some(e) = self.state.log.entry_at(LogIndex::new(i))
                && let LogPayload::ConfigChange(change) = &e.payload
            {
                apply_config_to_set(&mut peers_at_floor, *change, self.id);
            }
        }

        self.state
            .log
            .install_snapshot(last_included_index, last_included_term);
        self.state.snapshot_bytes = Some(bytes.clone());
        self.state.snapshot_peers = Some(peers_at_floor.clone());
        self.state.pending_snapshot_install = None;
        if let RoleState::Leader(leader) = &mut self.state.role {
            leader.snapshot_transfers.clear();
        }
        vec![Action::PersistSnapshot {
            last_included_index,
            peers: peers_at_floor,
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
                        l.snapshot_transfers.remove(&id);
                        if l.transfer_target == Some(id) {
                            l.transfer_target = None;
                        }
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
        // Start from the committed-config baseline. If a snapshot exists,
        // it carries the config as of `snapshot_last().index`; otherwise
        // fall back to the bootstrap config.
        //
        // Without this, a snapshot that included committed AddPeer /
        // RemovePeer entries would have those effects silently reverted
        // — entries below the floor aren't addressable via entry_at, so
        // the old "walk from index 1" approach missed them.
        let snapshot_floor = self.state.log.snapshot_last().index;
        let mut base = if snapshot_floor == LogIndex::ZERO {
            self.state.initial_peers.clone()
        } else {
            // The snapshot's peer set was restored into self.state.peers
            // when the snapshot landed (via recover_from or
            // on_install_snapshot_request). Post-snapshot CCs applied
            // before this recompute are reflected in state.peers too,
            // but there's no clean way to separate them here — callers
            // of recompute_active_config do so after a truncation that
            // invalidates some post-floor CC entries, and must arrange
            // for state.peers to reflect only the snapshot's baseline
            // before calling. See the post-truncation caller in
            // on_append_entries_request.
            self.state.peers.clone()
        };
        let first = self.state.log.first_index().get();
        let last = self.state.log.last_log_id().map_or(0, |l| l.index.get());
        for i in first..=last {
            if let Some(entry) = self.state.log.entry_at(LogIndex::new(i))
                && let LogPayload::ConfigChange(change) = &entry.payload
            {
                apply_config_to_set(&mut base, *change, self.id);
            }
        }
        self.state.peers = base;

        // Also rebuild leader progress if we're leader — add peers we
        // newly track, drop ones we no longer do.
        if let RoleState::Leader(l) = &mut self.state.role {
            let leader_last = self
                .state
                .log
                .last_log_id()
                .map_or(LogIndex::ZERO, |l| l.index);
            let tracked: BTreeSet<NodeId> = l.progress.peers().collect();
            for id in self.state.peers.difference(&tracked).copied() {
                l.progress.add_peer(id, leader_last);
            }
            for id in tracked.difference(&self.state.peers).copied() {
                l.progress.remove_peer(id);
                l.snapshot_transfers.remove(&id);
                if l.transfer_target == Some(id) {
                    l.transfer_target = None;
                }
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

        let mut stepdown: Vec<Action<C>> = Vec::new();
        if request.term > self.state.current_term {
            stepdown = self.become_follower(request.term);
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
            self.metrics.votes_granted += 1;
        } else {
            self.metrics.votes_denied += 1;
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

        let mut out = stepdown;
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
            let mut out = self.become_follower(response.term);
            out.push(self.persist_hard_state());
            return out;
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

    /// Begin a new election. With `pre_vote` enabled (the default)
    /// this enters `RoleState::PreCandidate` and sends
    /// `RequestPreVote` — the term is NOT bumped yet. Without
    /// pre-vote, we proceed straight to `Candidate`, bump the term,
    /// vote for self, and broadcast `RequestVote`.
    ///
    /// Single-node clusters self-elect immediately on either path:
    /// the pre-vote majority check and the vote majority check both
    /// pass with one grant.
    fn start_election(&mut self) -> Vec<Action<C>> {
        self.metrics.elections_started += 1;
        if self.pre_vote {
            return self.start_pre_election();
        }
        self.start_vote_election()
    }

    /// Classic §5.2 election start: bump term, vote for self,
    /// broadcast `RequestVote`.
    fn start_vote_election(&mut self) -> Vec<Action<C>> {
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

    /// §9.6 pre-election start. Enters `RoleState::PreCandidate`,
    /// proposes `current_term + 1` as the candidate term, and
    /// broadcasts `RequestPreVote`. Does NOT bump the engine's term
    /// or vote — that only happens on promotion to `Candidate` once
    /// a majority of peers grant.
    fn start_pre_election(&mut self) -> Vec<Action<C>> {
        self.become_pre_candidate();
        self.reset_election_timer();

        // Self-grant is always included in the tally; check for
        // majority before sending anything. A single-node cluster
        // has majority=1 and promotes immediately.
        if self.has_pre_vote_majority() {
            return self.promote_pre_candidate_to_candidate();
        }

        let proposed_term = match &self.state.role {
            RoleState::PreCandidate(p) => p.proposed_term,
            _ => unreachable!("just entered PreCandidate"),
        };
        let request = RequestPreVote {
            term: proposed_term,
            candidate_id: self.id(),
            last_log_id: self.state.log.last_log_id(),
        };

        self.state
            .peers
            .iter()
            .map(|&peer| Action::Send {
                to: peer,
                message: PreVoteRequest(request),
            })
            .collect()
    }

    /// Transition to `PreCandidate` with `proposed_term = current + 1`.
    /// Self-grants itself. Does NOT touch `current_term` or `voted_for`.
    fn become_pre_candidate(&mut self) {
        let proposed_term = self.state.current_term.next();
        let mut grants = BTreeSet::new();
        grants.insert(self.id());
        self.state.role = RoleState::PreCandidate(PreCandidateState {
            proposed_term,
            grants,
        });
        telemetry::became_pre_candidate(self.id, proposed_term);
    }

    /// True iff we're a `PreCandidate` whose pre-vote tally has
    /// reached cluster majority.
    fn has_pre_vote_majority(&self) -> bool {
        match self.role() {
            RoleState::PreCandidate(p) => p.grants.len() >= self.cluster_majority(),
            _ => false,
        }
    }

    /// `PreCandidate` → `Candidate` via the classic §5.2 path. The
    /// pre-vote succeeded, so we're now confident we can win the real
    /// election.
    fn promote_pre_candidate_to_candidate(&mut self) -> Vec<Action<C>> {
        // Drop pre-candidate state; `start_vote_election` expects to
        // transition from any non-Leader role into Candidate.
        self.start_vote_election()
    }

    /// Handle `RequestPreVote`. The responder's rules (§9.6):
    ///  - Grant iff the candidate's log is at least as up-to-date as
    ///    ours (§5.4.1), AND we haven't heard from a live leader in
    ///    the current election window.
    ///  - Receipt of a pre-vote NEVER updates our term or `voted_for`.
    ///    A stale-higher-term pre-vote is just metadata.
    #[instrument(target = "jotun::engine", skip_all)]
    fn on_pre_vote_request(&mut self, request: RequestPreVote) -> Vec<Action<C>> {
        let log_ok = self.state.log.is_superseded_by(request.last_log_id);
        // A leader MUST reject pre-votes — if we're the leader, a
        // challenger should not be allowed to disrupt us.
        let leader_recent = match &self.state.role {
            RoleState::Leader(_) => true,
            // A follower with a leader it heard from this term and
            // whose election timer hasn't expired yet still considers
            // the leader live.
            RoleState::Follower(f) => {
                f.leader_id.is_some()
                    && self.state.election_elapsed < self.state.election_timeout_ticks
            }
            // A PreCandidate / Candidate is already trying to elect
            // someone; reject pre-votes from other challengers.
            RoleState::PreCandidate(_) | RoleState::Candidate(_) => false,
        };
        let granted = log_ok && !leader_recent && request.term > self.state.current_term;
        if granted {
            self.metrics.pre_votes_granted += 1;
        } else {
            self.metrics.pre_votes_denied += 1;
        }

        let reply = PreVoteResponse {
            term: self.state.current_term,
            granted,
        };
        vec![Action::Send {
            to: request.candidate_id,
            message: PreVoteResponse(reply),
        }]
    }

    /// Handle `PreVoteResponse`.
    ///
    /// Three cases:
    ///  1. Responder is at a higher term than we are. Step down to
    ///     `Follower` at that term (§5.1).
    ///  2. We're not in `PreCandidate` state. The response is stale
    ///     (e.g. we already promoted, or got demoted by an AE).
    ///     Ignore.
    ///  3. We're in `PreCandidate`. If `granted`, record the grant;
    ///     on reaching cluster majority, promote to `Candidate`.
    ///
    /// The responder's `term` equal to ours is normal — pre-vote
    /// doesn't advance terms, so in a freshly-started 3-node cluster
    /// all pre-vote traffic happens at `term = 0`.
    #[instrument(target = "jotun::engine", skip_all)]
    fn on_pre_vote_response(&mut self, from: NodeId, response: PreVoteResponse) -> Vec<Action<C>> {
        if response.term > self.state.current_term {
            let mut out = self.become_follower(response.term);
            out.push(self.persist_hard_state());
            return out;
        }
        if !response.granted {
            return vec![];
        }
        if let RoleState::PreCandidate(p) = &mut self.state.role {
            p.grants.insert(from);
            if p.grants.len() >= self.cluster_majority() {
                return self.promote_pre_candidate_to_candidate();
            }
        }
        vec![]
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
    #[allow(clippy::too_many_lines)]
    fn on_append_entries_request(&mut self, request: RequestAppendEntries<C>) -> Vec<Action<C>> {
        self.metrics.append_entries_received += 1;
        let prior_term = self.state.current_term;
        let prior_voted_for = self.state.voted_for;

        let mut stepdown: Vec<Action<C>> = Vec::new();
        if request.term > self.state.current_term
            || (request.term == self.state.current_term
                && matches!(
                    self.state.role,
                    RoleState::Candidate(_) | RoleState::PreCandidate(_)
                ))
        {
            stepdown = self.become_follower(request.term);
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
            let mut out = std::mem::take(&mut stepdown);
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
                            let mut out = std::mem::take(&mut stepdown);
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
            let new_commit = request.leader_commit.min(last_appended);
            self.metrics.entries_committed += new_commit
                .get()
                .saturating_sub(self.state.commit_index.get());
            self.state.commit_index = new_commit;
        }

        let mut out = std::mem::take(&mut stepdown);
        if hard_state_changed {
            out.push(self.persist_hard_state());
        }
        if !newly_persisted.is_empty() {
            self.metrics.entries_appended += newly_persisted.len() as u64;
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
            let mut out = self.become_follower(response.term);
            out.push(self.persist_hard_state());
            return out;
        }
        if response.term < self.current_term() {
            return vec![];
        }
        let leader_last = self
            .state
            .log
            .last_log_id()
            .map_or(LogIndex::ZERO, |l| l.index);
        let snapshot_floor = self.state.log.snapshot_last().index;

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
                {
                    let RoleState::Leader(leader) = &mut self.state.role else {
                        return vec![];
                    };
                    leader.progress.record_success(peer, last_appended);
                    if last_appended >= snapshot_floor {
                        leader.snapshot_transfers.remove(&peer);
                    }
                }
                // ReadIndex: this ack implicitly confirms every seq
                // we've stamped this peer with. Copy it across.
                let majority_before = self.majority_acked_seq();
                {
                    let RoleState::Leader(leader) = &mut self.state.role else {
                        unreachable!("just matched");
                    };
                    if let Some(stamp) = leader.peer_sent_seq.get(&peer).copied() {
                        let cur = leader.peer_acked_seq.get(&peer).copied().unwrap_or(0);
                        if stamp > cur {
                            leader.peer_acked_seq.insert(peer, stamp);
                        }
                    }
                }
                // §9 lease: if this ack advanced the majority-acked
                // seq, stamp the arrival tick. Only *forward* moves
                // count — a stale ack can't extend the lease.
                let majority_after = self.majority_acked_seq();
                if majority_after > majority_before {
                    let now = self.state.current_tick;
                    if let RoleState::Leader(leader) = &mut self.state.role {
                        leader.last_majority_ack_tick = Some(now);
                    }
                }
                let mut out = self.try_advance_commit_as_leader();
                out.extend(self.maybe_complete_leadership_transfer(peer));
                out.extend(self.drain_ready_reads());
                out
            }
            AppendEntriesResult::Conflict { next_index_hint } => {
                // Clamp the hint to our own log: nextIndex must never point
                // past leader_last + 1. A buggy or malicious follower could
                // otherwise push us into synthesising prev_log_id from a
                // region of our log that doesn't exist.
                let cap = leader_last.next();
                let hint = next_index_hint.min(cap);
                let RoleState::Leader(leader) = &mut self.state.role else {
                    return vec![];
                };
                leader.progress.record_conflict(peer, hint);
                if hint > snapshot_floor {
                    leader.snapshot_transfers.remove(&peer);
                }
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
    #[allow(clippy::too_many_lines)]
    fn on_install_snapshot_request(&mut self, request: RequestInstallSnapshot) -> Vec<Action<C>> {
        let prior_term = self.state.current_term;
        let prior_voted_for = self.state.voted_for;

        let mut stepdown: Vec<Action<C>> = Vec::new();
        if request.term > self.state.current_term
            || (request.term == self.state.current_term
                && matches!(self.state.role, RoleState::Candidate(_)))
        {
            stepdown = self.become_follower(request.term);
        }
        if request.term < self.state.current_term {
            // Stale leader. Reply with our (higher) term so they step
            // down. No state mutation.
            return vec![self.snapshot_response(
                request.leader_id,
                request.last_included,
                0,
                false,
            )];
        }

        let RequestInstallSnapshot {
            leader_id,
            last_included,
            data,
            offset,
            done,
            leader_commit,
            peers,
            ..
        } = request;

        // Record the leader for client redirects.
        if let RoleState::Follower(f) = &mut self.state.role {
            f.leader_id = Some(leader_id);
        }
        self.reset_election_timer();

        let hard_state_changed =
            self.state.current_term != prior_term || self.state.voted_for != prior_voted_for;
        let response_term = self.state.current_term;
        let snapshot_ack = |last_included: LogId, next_offset: u64, done: bool| Action::Send {
            to: leader_id,
            message: InstallSnapshotResponse(InstallSnapshotResponse {
                term: response_term,
                last_included,
                next_offset,
                done,
            }),
        };

        // Stale snapshot at or below our existing floor: ack but do
        // nothing else.
        if last_included.index <= self.state.log.snapshot_last().index {
            let current_floor = self.state.log.snapshot_last();
            let mut out = std::mem::take(&mut stepdown);
            if hard_state_changed {
                out.push(self.persist_hard_state());
            }
            out.push(snapshot_ack(current_floor, 0, true));
            return out;
        }

        let mut out = std::mem::take(&mut stepdown);
        if hard_state_changed {
            out.push(self.persist_hard_state());
        }

        let reset_pending = self
            .state
            .pending_snapshot_install
            .as_ref()
            .is_none_or(|pending| pending.last_included != last_included);
        if reset_pending {
            if offset != 0 {
                out.push(snapshot_ack(last_included, 0, false));
                return out;
            }
            self.state.pending_snapshot_install = Some(PendingSnapshotInstall {
                last_included,
                leader_commit,
                peers: peers.clone(),
                bytes: Vec::new(),
            });
        }

        let Some(pending) = &mut self.state.pending_snapshot_install else {
            unreachable!("pending snapshot install initialized above");
        };

        let incompatible_metadata = pending.peers != peers && offset != 0;
        if incompatible_metadata {
            self.state.pending_snapshot_install = None;
            out.push(snapshot_ack(last_included, 0, false));
            return out;
        }

        let current_offset = pending.next_offset();
        if offset > current_offset {
            out.push(snapshot_ack(pending.last_included, current_offset, false));
            return out;
        }

        let overlap = current_offset.saturating_sub(offset);
        let overlap_len = usize::try_from(overlap)
            .unwrap_or(usize::MAX)
            .min(data.len());
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        let overlap_matches = start
            .checked_add(overlap_len)
            .and_then(|end| pending.bytes.get(start..end))
            .is_some_and(|existing| data.get(..overlap_len) == Some(existing));
        if overlap_len > 0 && !overlap_matches {
            if offset == 0 {
                pending.bytes.clear();
                pending.peers.clone_from(&peers);
            } else {
                self.state.pending_snapshot_install = None;
                out.push(snapshot_ack(last_included, 0, false));
                return out;
            }
        }

        let append_from = if overlap_len > 0 && overlap_matches {
            overlap_len
        } else {
            0
        };
        pending
            .bytes
            .extend_from_slice(data.get(append_from..).unwrap_or(&[]));
        pending.leader_commit = pending.leader_commit.max(leader_commit);
        pending.peers.clone_from(&peers);

        if !done {
            out.push(snapshot_ack(
                pending.last_included,
                pending.next_offset(),
                false,
            ));
            return out;
        }

        let Some(pending) = self.state.pending_snapshot_install.take() else {
            return out;
        };
        self.metrics.snapshots_installed += 1;

        // Install the snapshot floor. Log::install_snapshot keeps
        // post-floor entries iff our log already had a matching
        // boundary entry; otherwise it wipes the in-memory tail.
        self.state
            .log
            .install_snapshot(pending.last_included.index, pending.last_included.term);
        self.state.snapshot_bytes = Some(pending.bytes.clone());
        self.state.snapshot_peers = Some(pending.peers.clone());

        // Advance commit / applied past the snapshot. The snapshot
        // captures everything up to `last_included.index`, so we treat
        // it as if those entries had already been applied.
        if pending.last_included.index > self.state.commit_index {
            self.state.commit_index = pending.last_included.index;
        }
        if pending.last_included.index > self.state.last_applied {
            self.state.last_applied = pending.last_included.index;
        }
        // leader_commit may also exceed the snapshot's tail — propagate.
        if pending.leader_commit > self.state.commit_index {
            let last_log = self
                .state
                .log
                .last_log_id()
                .map_or(LogIndex::ZERO, |l| l.index);
            self.state.commit_index = pending.leader_commit.min(last_log);
        }
        let mut peers = pending.peers.clone();
        peers.remove(&self.id);
        self.state.peers = peers;
        self.recompute_active_config();

        let final_offset = u64::try_from(pending.bytes.len()).unwrap_or(u64::MAX);
        out.push(Action::PersistSnapshot {
            last_included_index: pending.last_included.index,
            last_included_term: pending.last_included.term,
            peers: pending.peers.clone(),
            bytes: pending.bytes.clone(),
        });
        out.push(Action::ApplySnapshot {
            bytes: pending.bytes,
        });
        out.push(snapshot_ack(
            self.state.log.snapshot_last(),
            final_offset,
            true,
        ));
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
            let mut out = self.become_follower(response.term);
            out.push(self.persist_hard_state());
            return out;
        }
        if response.term < self.state.current_term {
            return vec![];
        }
        let RoleState::Leader(leader) = &mut self.state.role else {
            return vec![];
        };

        if response.done {
            let leader_last = self
                .state
                .log
                .last_log_id()
                .map_or(LogIndex::ZERO, |l| l.index);
            let applied = response.last_included.index.min(leader_last);
            leader.progress.record_success(peer, applied);
            leader.progress.ensure_next_at_least(peer, applied.next());
            if let Some(transfer) = leader.snapshot_transfers.get(&peer)
                && response.last_included.index >= transfer.last_included.index
            {
                leader.snapshot_transfers.remove(&peer);
            }
            return self.maybe_complete_leadership_transfer(peer);
        }

        let Some(transfer) = leader.snapshot_transfers.get_mut(&peer) else {
            return vec![];
        };
        if response.last_included != transfer.last_included {
            return vec![];
        }
        let snapshot_len = self
            .state
            .snapshot_bytes
            .as_ref()
            .map_or(0, |bytes| u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        if response.next_offset > snapshot_len || response.next_offset <= transfer.next_offset {
            return vec![];
        }
        transfer.next_offset = response.next_offset;
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
            self.metrics.entries_committed += n.get().saturating_sub(prior_commit.get());
            self.state.commit_index = n;
            if self.committed_self_removal(prior_commit, n) {
                let prior_term = self.state.current_term;
                // Same-term step-down preserves voted_for; no Persist
                // needed beyond what drain_apply might emit.
                // become_follower fails every pending ReadIndex with
                // SteppedDown as it flips the role.
                let mut out = self.become_follower(prior_term);
                out.extend(self.drain_apply());
                return out;
            }
            let mut out = self.drain_apply();
            // last_applied advanced; pending reads may be ready.
            out.extend(self.drain_ready_reads());
            return out;
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
        self.metrics.entries_applied += to.get().saturating_sub(self.state.last_applied.get());
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
    fn broadcast_append_entries(&mut self) -> Vec<Action<C>> {
        if !matches!(self.state.role, RoleState::Leader(_)) {
            return vec![];
        }
        // Bump the ReadIndex heartbeat seq: every peer we address in
        // this round gets stamped with the new seq, and acks to those
        // messages confirm still-leader at or after this point.
        if let RoleState::Leader(leader) = &mut self.state.role {
            leader.heartbeat_seq = leader.heartbeat_seq.wrapping_add(1);
        }
        let peers: Vec<NodeId> = self.state.peers.iter().copied().collect();
        peers
            .into_iter()
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
    fn append_entries_to(&mut self, peer: NodeId) -> Action<C> {
        self.metrics.append_entries_sent += 1;
        let next = match &self.state.role {
            RoleState::Leader(l) => l.progress.next_for(peer).unwrap_or(LogIndex::new(1)),
            _ => LogIndex::new(1),
        };
        debug_assert!(next.get() >= 1, "nextIndex floor is 1");

        // Stamp this peer with the current heartbeat_seq. On successful
        // ack, this value is copied into peer_acked_seq so majority-
        // acked-seq tracks the ReadIndex quorum round.
        if let RoleState::Leader(leader) = &mut self.state.role {
            let stamp = leader.heartbeat_seq;
            leader.peer_sent_seq.insert(peer, stamp);
        }

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
    fn install_snapshot_to(&mut self, peer: NodeId) -> Action<C> {
        self.metrics.snapshots_sent += 1;
        let last_included = self.state.log.snapshot_last();
        let offset = match &mut self.state.role {
            RoleState::Leader(leader) => {
                let transfer = leader
                    .snapshot_transfers
                    .entry(peer)
                    .or_insert(SnapshotTransfer {
                        last_included,
                        next_offset: 0,
                    });
                if transfer.last_included != last_included {
                    *transfer = SnapshotTransfer {
                        last_included,
                        next_offset: 0,
                    };
                }
                transfer.next_offset
            }
            _ => 0,
        };
        let bytes = self.state.snapshot_bytes.as_deref().unwrap_or(&[]);
        let peers = self.state.snapshot_peers.clone().unwrap_or_default();
        let start = usize::try_from(offset)
            .unwrap_or(bytes.len())
            .min(bytes.len());
        let end = start
            .saturating_add(self.snapshot_chunk_size_bytes)
            .min(bytes.len());
        let data = bytes.get(start..end).unwrap_or(&[]).to_vec();
        Action::Send {
            to: peer,
            message: InstallSnapshotRequest(RequestInstallSnapshot {
                term: self.state.current_term,
                leader_id: self.id,
                last_included,
                data,
                offset,
                done: end == bytes.len(),
                leader_commit: self.state.commit_index,
                peers,
            }),
        }
    }

    /// Build an `AppendEntries` Conflict response. Helper for the rejection
    /// paths in [`Engine::on_append_entries_request`].
    fn conflict(&mut self, leader_id: NodeId, hint: LogIndex) -> Action<C> {
        self.metrics.append_entries_rejected += 1;
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

    /// Build an `InstallSnapshotResponse` carrying either partial
    /// progress (`done == false`) or completion (`done == true`).
    fn snapshot_response(
        &self,
        leader_id: NodeId,
        last_included: LogId,
        next_offset: u64,
        done: bool,
    ) -> Action<C> {
        Action::Send {
            to: leader_id,
            message: InstallSnapshotResponse(InstallSnapshotResponse {
                term: self.current_term(),
                last_included,
                next_offset,
                done,
            }),
        }
    }

    /// Build a `TimeoutNow` message for a leadership-transfer target.
    fn timeout_now(&self, target: NodeId) -> Action<C> {
        Action::Send {
            to: target,
            message: TimeoutNow(TimeoutNow {
                term: self.current_term(),
                leader_id: self.id,
            }),
        }
    }

    /// True once `target` has replicated through the leader's current
    /// log tail and can win a fresh election on log freshness.
    fn transfer_target_is_caught_up(&self, target: NodeId) -> bool {
        let leader_last = self
            .state
            .log
            .last_log_id()
            .map_or(LogIndex::ZERO, |l| l.index);
        let RoleState::Leader(leader) = &self.state.role else {
            return false;
        };
        leader
            .progress
            .match_for(target)
            .is_some_and(|matched| matched >= leader_last)
    }

    /// If `peer` is the in-flight transfer target and is now fully
    /// caught up, send `TimeoutNow` and clear the transfer request.
    fn maybe_complete_leadership_transfer(&mut self, peer: NodeId) -> Vec<Action<C>> {
        let RoleState::Leader(leader) = &self.state.role else {
            return vec![];
        };
        if leader.transfer_target != Some(peer) || !self.transfer_target_is_caught_up(peer) {
            return vec![];
        }
        if let RoleState::Leader(leader) = &mut self.state.role {
            leader.transfer_target = None;
        }
        vec![self.timeout_now(peer)]
    }

    // =========================================================================
    // ReadIndex — linearizable reads (§8)
    // =========================================================================

    /// Submit a linearizable read against the leader's applied state.
    ///
    /// Non-leaders redirect (if a leader is known) or fail with
    /// `NoLeader` / `NotReady`. Leaders that haven't yet committed an
    /// entry in their current term fail the read with `NotReady` — the
    /// §5.4.2 no-op fixes this shortly after election.
    ///
    /// Otherwise the read is queued. A broadcast is kicked off to
    /// confirm still-leader; on majority ack, and once `last_applied`
    /// has caught up to the recorded `read_index`, `ReadReady` is
    /// emitted. If either condition fails to hold (e.g. we step down),
    /// `ReadFailed` is emitted instead.
    #[instrument(target = "jotun::engine", skip_all)]
    fn on_propose_read(&mut self, id: u64) -> Vec<Action<C>> {
        match &self.state.role {
            RoleState::Leader(_) => {}
            RoleState::Follower(f) => {
                let reason = f.leader_id.map_or(ReadFailure::NotReady, |leader_hint| {
                    ReadFailure::NotLeader { leader_hint }
                });
                self.metrics.reads_failed += 1;
                return vec![Action::ReadFailed { id, reason }];
            }
            RoleState::PreCandidate(_) | RoleState::Candidate(_) => {
                self.metrics.reads_failed += 1;
                return vec![Action::ReadFailed {
                    id,
                    reason: ReadFailure::NotReady,
                }];
            }
        }

        // §8 ReadIndex safety: the leader must have committed at least
        // one entry in its current term before serving a linearizable
        // read. Otherwise commit_index may reflect a stale predecessor.
        // The §5.4.2 no-op appended in become_leader fills this gap as
        // soon as it commits.
        if self.state.log.term_at(self.state.commit_index) != Some(self.state.current_term) {
            self.metrics.reads_failed += 1;
            return vec![Action::ReadFailed {
                id,
                reason: ReadFailure::NotReady,
            }];
        }

        // §9 leader-lease fast path: if a majority AE-ack arrived
        // within the lease window AND the state machine has already
        // caught up to the commit index, serve the read synchronously.
        // The lease is derived from ack-arrival ticks only (never send
        // time), and is reset on every become_leader, so it is only
        // valid while still leader at the same term.
        if self.lease_duration_ticks > 0
            && self.state.last_applied >= self.state.commit_index
            && let RoleState::Leader(leader) = &self.state.role
            && let Some(ack_tick) = leader.last_majority_ack_tick
        {
            let elapsed = self.state.current_tick.saturating_sub(ack_tick);
            if elapsed <= self.lease_duration_ticks {
                self.metrics.reads_completed += 1;
                return vec![Action::ReadReady { id }];
            }
        }

        self.metrics.read_index_started += 1;
        let read_index = self.state.commit_index;
        let RoleState::Leader(leader) = &mut self.state.role else {
            unreachable!("matched above");
        };
        let required_seq = leader.heartbeat_seq + 1;
        leader.pending_reads.push(PendingRead {
            id,
            read_index,
            required_seq,
        });

        // Kick a broadcast so the next round of acks advances
        // majority_acked_seq to >= required_seq.
        let mut out = self.broadcast_append_entries();
        // Single-node clusters: no peers to ack, so the read is ready
        // as soon as it was filed. drain_ready_reads checks the
        // majority condition below.
        out.extend(self.drain_ready_reads());
        out
    }

    /// Return the highest `heartbeat_seq` that has been acked by a
    /// majority of the current cluster (self always counted as having
    /// acked the current `heartbeat_seq`).
    fn majority_acked_seq(&self) -> u64 {
        let RoleState::Leader(leader) = &self.state.role else {
            return 0;
        };
        // Collect every peer's acked seq, then add self (always at
        // the current seq). Majority is floor(n/2)+1 over the full
        // cluster (peers + self).
        let mut seqs: Vec<u64> = self
            .state
            .peers
            .iter()
            .map(|p| leader.peer_acked_seq.get(p).copied().unwrap_or(0))
            .collect();
        seqs.push(leader.heartbeat_seq);
        seqs.sort_unstable();
        // cluster_size = peers + 1 (self). Majority index from the
        // top of the sorted list is at seqs[cluster_size - majority]
        // where majority = cluster_size/2 + 1.
        let cluster_size = seqs.len();
        let majority = cluster_size / 2 + 1;
        // seqs[cluster_size - majority] is the majority-th highest.
        // cluster_size is always >= 1 (self pushed above); majority
        // is in (0, cluster_size], so the index is in bounds.
        seqs.get(cluster_size - majority).copied().unwrap_or(0)
    }

    /// Emit `ReadReady` for every pending read whose heartbeat-quorum
    /// condition and `last_applied >= read_index` condition are now
    /// both satisfied. Called after events that advance either side:
    /// commit advance (bumps `last_applied`), or AE success (bumps
    /// `majority_acked_seq`).
    fn drain_ready_reads(&mut self) -> Vec<Action<C>> {
        if !matches!(self.state.role, RoleState::Leader(_)) {
            return vec![];
        }
        let majority_seq = self.majority_acked_seq();
        let last_applied = self.state.last_applied;
        let RoleState::Leader(leader) = &mut self.state.role else {
            unreachable!("just matched");
        };
        let mut out = Vec::new();
        leader.pending_reads.retain(|r| {
            if majority_seq >= r.required_seq && last_applied >= r.read_index {
                out.push(Action::ReadReady { id: r.id });
                false
            } else {
                true
            }
        });
        self.metrics.reads_completed += out.len() as u64;
        out
    }

    /// Fail every pending read with `SteppedDown`. Called when the
    /// leader state is about to be discarded (term advance, same-term
    /// step-down after self-removal, etc.).
    fn fail_pending_reads(&mut self) -> Vec<Action<C>> {
        let RoleState::Leader(leader) = &mut self.state.role else {
            return vec![];
        };
        let drained: Vec<PendingRead> = std::mem::take(&mut leader.pending_reads);
        self.metrics.reads_failed += drained.len() as u64;
        drained
            .into_iter()
            .map(|r| Action::ReadFailed {
                id: r.id,
                reason: ReadFailure::SteppedDown,
            })
            .collect()
    }

    /// Handle an incoming `TimeoutNow` by starting an immediate
    /// election if the request came from the leader we currently trust
    /// for this term, or from a higher-term leader we just stepped
    /// down for.
    fn on_timeout_now(&mut self, request: TimeoutNow) -> Vec<Action<C>> {
        // Only accept TimeoutNow from a node we already trust as leader
        // for our current term. A stale/higher-term or unknown-source
        // request must not force us to start an election — otherwise any
        // node could dictate our term or disrupt the cluster.
        if request.term != self.state.current_term {
            return vec![];
        }
        let RoleState::Follower(follower) = &self.state.role else {
            return vec![];
        };
        if follower.leader_id != Some(request.leader_id) {
            return vec![];
        }
        self.start_election()
    }

    // =========================================================================
    // Role transitions and the timer reset they delegate to
    // =========================================================================

    /// Transition to follower. Does NOT reset the election timer — per §5.2,
    /// the timer resets only on accepting `AppendEntries` from the current
    /// leader or granting a vote. Callers that need both should do so
    /// explicitly after this returns.
    fn become_follower(&mut self, term: Term) -> Vec<Action<C>> {
        let from_term = self.state.current_term;
        if term > from_term {
            self.metrics.higher_term_stepdowns += 1;
        }
        // If we were leader, every pending ReadIndex dies with the
        // role. Collect ReadFailed{SteppedDown} before the role flip.
        let out = self.fail_pending_reads();
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
        out
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
        self.metrics.leader_elections_won += 1;
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
        self.state.role = RoleState::Leader(LeaderState {
            progress,
            snapshot_transfers: std::collections::BTreeMap::default(),
            transfer_target: None,
            heartbeat_seq: 0,
            peer_sent_seq: std::collections::BTreeMap::default(),
            peer_acked_seq: std::collections::BTreeMap::default(),
            pending_reads: Vec::new(),
            last_majority_ack_tick: None,
        });
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

/// Apply `change` to a peer set under the perspective of `self_id`
/// (self is always implicit in the set, so self-adds and self-removes
/// are no-ops for the set itself). Free function so snapshot-building
/// code can run it against a temporary `BTreeSet<NodeId>` without
/// owning an `Engine`.
fn apply_config_to_set(peers: &mut BTreeSet<NodeId>, change: ConfigChange, self_id: NodeId) {
    match change {
        ConfigChange::AddPeer(id) if id != self_id => {
            peers.insert(id);
        }
        ConfigChange::RemovePeer(id) if id != self_id => {
            peers.remove(&id);
        }
        _ => {}
    }
}
