//! The user-facing [`Node`] runtime: a long-running task that owns
//! the engine and dispatches its actions.
//!
//! Architecture: a single owner task ("the driver") holds the
//! [`yggr_core::Engine`], the user's [`StateMachine`], the
//! [`Storage`] handle, and the [`Transport`]. Public methods on
//! [`Node`] (`write`, `propose`, `add_peer`, `remove_peer`,
//! `transfer_leadership_to`, `shutdown`)
//! package a `DriverInput` into a channel and `await` a oneshot
//! reply. The driver's loop multiplexes:
//!
//!  1. A wall-clock tick (via `tokio::time::interval`).
//!  2. An inbound message from `Transport::recv`.
//!  3. A `DriverInput` from a user-facing call.
//!
//! Each of those becomes a `yggr_core::Event`; the driver calls
//! `engine.step` and dispatches every emitted [`Action`] in order:
//! Persists go to Storage, Sends go to Transport, Applies go to
//! the `StateMachine` (with replies to pending [`Node::propose`]
//! futures), Redirects fail any matching pending propose with
//! `NotLeader`. Leadership-transfer requests are validated against the
//! driver's current role and peer set, then handed to the engine as
//! `Event::TransferLeadership`. On explicit shutdown the driver also
//! asks the transport to stop any background tasks it owns before
//! exiting. The runtime restores and installs incoming snapshots
//! during recovery / `InstallSnapshot`.
//! By default it also reacts to `Action::SnapshotHint` by calling
//! [`StateMachine::snapshot`] and feeding the resulting bytes back into
//! the engine; set [`Config::snapshot_hint_threshold_entries`] to `0`
//! to disable host-initiated snapshotting.
//!
//! The driver is the only mutator of any shared state; no locks.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};
use yggr_core::{
    Action, ConfigChange, Engine, EngineConfig, Env, Event, LogEntry, LogIndex, LogPayload, NodeId,
    RandomizedEnv, RoleState, Term,
};

use crate::state_machine::StateMachine;
use crate::storage::{RecoveredState, Storage, StoredHardState, StoredSnapshot};
use crate::transport::Transport;

/// User-facing handle to a running Raft node. Cheap to clone — each
/// clone shares the same underlying driver task.
pub struct Node<S: StateMachine> {
    inputs: mpsc::Sender<DriverInput<S>>,
    background: Arc<Mutex<Option<BackgroundTasks>>>,
}

/// Operator-facing control handle for a running [`Node`].
///
/// This keeps the common client path (`write`, `read_linearizable`,
/// `status`, `metrics`) separate from membership and lifecycle
/// operations without changing the underlying runtime semantics.
pub struct AdminHandle<S: StateMachine> {
    node: Node<S>,
}

impl<S: StateMachine> std::fmt::Debug for Node<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node").finish_non_exhaustive()
    }
}

impl<S: StateMachine> Clone for Node<S> {
    fn clone(&self) -> Self {
        Self {
            inputs: self.inputs.clone(),
            background: Arc::clone(&self.background),
        }
    }
}

impl<S: StateMachine> std::fmt::Debug for AdminHandle<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminHandle").finish_non_exhaustive()
    }
}

impl<S: StateMachine> Clone for AdminHandle<S> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
        }
    }
}

struct BackgroundTasks {
    ticker: JoinHandle<()>,
    driver: JoinHandle<()>,
    apply: JoinHandle<()>,
}

/// How a node should come up at [`Node::start`] time.
///
/// Distinguishes the three legitimate boot scenarios so an operator
/// can't, for example, accidentally boot a would-be joiner as a fresh
/// single-node cluster — which would let it elect itself, commit
/// entries, and then try to re-join a real cluster it diverged from.
#[derive(Debug, Clone)]
pub enum Bootstrap {
    /// Initial cluster bring-up. The runtime trusts [`Config::peers`]
    /// as the authoritative initial membership; every founding node
    /// should be started with the same `Config::peers` set.
    ///
    /// `members` is retained for pre-0.1 API continuity; the runtime
    /// does not consult it when constructing the engine.
    NewCluster {
        /// The full initial cluster membership (self included).
        members: Vec<NodeId>,
    },
    /// Join an already-running cluster. Starts with an empty peer set
    /// regardless of [`Config::peers`]; the existing leader is
    /// expected to call `add_peer` to splice us in.
    Join,
    /// Boot from persisted state after a crash or restart. The
    /// runtime uses whatever membership the on-disk state implies,
    /// falling back to [`Config::peers`] when there is no persisted
    /// membership yet.
    Recover,
}

/// Configuration for a [`Node`].
///
/// `max_pending_proposals` bounds how many in-flight `propose` /
/// `add_peer` / `remove_peer` calls the driver will track at once.
/// Once the combined count reaches the limit, new calls fail fast
/// with [`ProposeError::Busy`] rather than queue unboundedly in the
/// driver's reply map. 1024 is plenty of headroom for normal
/// workloads; lower it if your callers can tolerate backpressure.
///
/// Snapshot creation is also configurable here: the runtime asks the
/// engine to emit `Action::SnapshotHint` once enough applied entries
/// have accumulated past the current floor, and streams outbound
/// `InstallSnapshot` RPCs in chunks of `snapshot_chunk_size_bytes`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct Config {
    pub node_id: NodeId,
    /// Initial peer set (excluding self). Membership changes go
    /// through `add_peer` / `remove_peer` after startup.
    ///
    /// Semantics depend on [`Self::bootstrap`]: `NewCluster` uses
    /// this set as the authoritative initial membership, `Join`
    /// ignores it (peer set starts empty), `Recover` falls back to
    /// this set only when there is no persisted membership.
    pub peers: Vec<NodeId>,
    /// Lower bound of the election-timeout range, in ticks (inclusive).
    /// The runtime draws each timeout uniformly from
    /// `[election_timeout_min_ticks, election_timeout_max_ticks)` —
    /// §5.2 randomization, prevents split votes.
    pub election_timeout_min_ticks: u64,
    /// Upper bound, exclusive. Must be strictly greater than min.
    pub election_timeout_max_ticks: u64,
    /// Heartbeat interval in ticks. Must be smaller than the election
    /// timeout minimum per §5.2.
    pub heartbeat_interval_ticks: u64,
    /// Wall-clock duration of one engine tick.
    pub tick_interval: Duration,
    /// How this node comes up. See [`Bootstrap`] for the three
    /// scenarios.
    pub bootstrap: Bootstrap,
    /// Maximum number of in-flight proposals + config changes before
    /// the driver rejects new calls with [`ProposeError::Busy`].
    /// Defaults to 1024.
    pub max_pending_proposals: usize,
    /// Capacity of the driver → apply-task channel. When full, the
    /// driver awaits space, providing natural backpressure on
    /// replication while the state machine catches up. Defaults to
    /// 4096.
    pub max_pending_applies: usize,
    /// Maximum ticks the driver will hold a proposal before flushing
    /// the batch. `0` disables batching — every proposal triggers its
    /// own log append and broadcast. Defaults to `0`.
    pub max_batch_delay_ticks: u64,
    /// Flush the current batch as soon as it reaches this many
    /// buffered proposals, regardless of `max_batch_delay_ticks`.
    /// Has no effect when `max_batch_delay_ticks == 0`. Defaults to 64.
    pub max_batch_entries: usize,
    /// Applied-entry threshold for automatic snapshot hints. `0`
    /// disables host-initiated snapshotting in the runtime; incoming
    /// leader snapshots still install normally.
    pub snapshot_hint_threshold_entries: u64,
    /// Disk-guard threshold on the live log. When the number of log
    /// entries above the current snapshot floor exceeds this, the
    /// engine emits a `SnapshotHint` regardless of
    /// `snapshot_hint_threshold_entries` — protects against runaway
    /// log growth when apply is lagging. `0` disables the guardrail.
    pub max_log_entries: u64,
    /// Maximum size of one outbound `InstallSnapshot` chunk in bytes.
    pub snapshot_chunk_size_bytes: usize,
    /// Enable §9.6 pre-vote. With pre-vote on (default `true`), a
    /// disrupted follower checks whether peers would vote for it
    /// BEFORE bumping its term. A node that can't reach a majority
    /// stays in its current term, so flapping nodes don't force the
    /// rest of the cluster to step down on every reconnect.
    pub pre_vote: bool,
    /// §9 leader lease. When the leader has received a majority AE
    /// ack within the last `lease_duration_ticks`, linearizable reads
    /// skip the fresh-heartbeat round-trip and return immediately.
    ///
    /// Safety constraint: must be strictly less than
    /// `election_timeout_min_ticks - heartbeat_interval_ticks`.
    /// Otherwise a peer whose election timer expired could elect a
    /// new leader inside our still-active lease window. `Config::validate`
    /// enforces this. `0` disables the lease (every linearizable read
    /// uses the classic §8 `ReadIndex` path). Default: `0`.
    pub lease_duration_ticks: u64,
}

/// Invalid [`Config`] settings rejected by [`Config::validate`] and
/// [`Node::start`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigError {
    /// `heartbeat_interval_ticks` must be strictly smaller than the
    /// election-timeout minimum per §5.2.
    HeartbeatNotLessThanElectionMin {
        heartbeat_interval_ticks: u64,
        election_timeout_min_ticks: u64,
    },
    /// The election-timeout range is empty or inverted.
    InvalidElectionTimeoutRange {
        election_timeout_min_ticks: u64,
        election_timeout_max_ticks: u64,
    },
    /// `peers` must exclude self; membership is always "other nodes".
    PeersContainSelf { node_id: NodeId },
    /// Snapshot chunks must have a positive size.
    InvalidSnapshotChunkSize { snapshot_chunk_size_bytes: usize },
    /// `lease_duration_ticks` must be strictly less than
    /// `election_timeout_min_ticks - heartbeat_interval_ticks`, so a
    /// peer can't elect a new leader inside our still-active lease
    /// window.
    LeaseDurationTooLarge {
        lease_duration_ticks: u64,
        election_timeout_min_ticks: u64,
        heartbeat_interval_ticks: u64,
    },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HeartbeatNotLessThanElectionMin {
                heartbeat_interval_ticks,
                election_timeout_min_ticks,
            } => write!(
                f,
                "heartbeat interval {heartbeat_interval_ticks} must be less than election timeout min {election_timeout_min_ticks}"
            ),
            Self::InvalidElectionTimeoutRange {
                election_timeout_min_ticks,
                election_timeout_max_ticks,
            } => write!(
                f,
                "election timeout range [{election_timeout_min_ticks}, {election_timeout_max_ticks}) is empty"
            ),
            Self::PeersContainSelf { node_id } => {
                write!(f, "peer set must not contain self ({node_id})")
            }
            Self::InvalidSnapshotChunkSize {
                snapshot_chunk_size_bytes,
            } => write!(
                f,
                "snapshot chunk size must be greater than zero (got {snapshot_chunk_size_bytes})"
            ),
            Self::LeaseDurationTooLarge {
                lease_duration_ticks,
                election_timeout_min_ticks,
                heartbeat_interval_ticks,
            } => write!(
                f,
                "lease_duration_ticks {lease_duration_ticks} must be strictly less than \
                 election_timeout_min_ticks {election_timeout_min_ticks} - \
                 heartbeat_interval_ticks {heartbeat_interval_ticks}"
            ),
        }
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    /// Sensible defaults for a development cluster.
    ///
    /// `bootstrap` defaults to [`Bootstrap::NewCluster`] when `peers`
    /// is non-empty (multi-member cluster bring-up is the common
    /// first-boot path) and to [`Bootstrap::Recover`] otherwise
    /// (single-node cluster — safe to recover from whatever's on
    /// disk, or self-elect if there's nothing there).
    pub fn new(node_id: NodeId, peers: impl IntoIterator<Item = NodeId>) -> Self {
        let peers: Vec<NodeId> = peers.into_iter().collect();
        let bootstrap = if peers.is_empty() {
            Bootstrap::Recover
        } else {
            // NewCluster.members must include self — callers passed
            // `peers` as the others; we reconstruct the full set.
            let mut members = peers.clone();
            members.push(node_id);
            Bootstrap::NewCluster { members }
        };
        Self {
            node_id,
            peers,
            election_timeout_min_ticks: 10,
            election_timeout_max_ticks: 20,
            heartbeat_interval_ticks: 3,
            tick_interval: Duration::from_millis(50),
            bootstrap,
            max_pending_proposals: 1024,
            max_pending_applies: 4096,
            max_batch_delay_ticks: 0,
            max_batch_entries: 64,
            snapshot_hint_threshold_entries: 1024,
            max_log_entries: 0,
            snapshot_chunk_size_bytes: 64 * 1024,
            pre_vote: true,
            lease_duration_ticks: 0,
        }
    }

    /// Validate invariants the runtime relies on before startup.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.heartbeat_interval_ticks >= self.election_timeout_min_ticks {
            return Err(ConfigError::HeartbeatNotLessThanElectionMin {
                heartbeat_interval_ticks: self.heartbeat_interval_ticks,
                election_timeout_min_ticks: self.election_timeout_min_ticks,
            });
        }
        if self.election_timeout_min_ticks >= self.election_timeout_max_ticks {
            return Err(ConfigError::InvalidElectionTimeoutRange {
                election_timeout_min_ticks: self.election_timeout_min_ticks,
                election_timeout_max_ticks: self.election_timeout_max_ticks,
            });
        }
        if self.peers.contains(&self.node_id) {
            return Err(ConfigError::PeersContainSelf {
                node_id: self.node_id,
            });
        }
        if self.snapshot_chunk_size_bytes == 0 {
            return Err(ConfigError::InvalidSnapshotChunkSize {
                snapshot_chunk_size_bytes: self.snapshot_chunk_size_bytes,
            });
        }
        // §9 lease safety: our lease must expire before any peer's
        // election timer could fire, giving one heartbeat interval of
        // clock-skew slack.
        if self.lease_duration_ticks > 0 {
            let safe_max = self
                .election_timeout_min_ticks
                .saturating_sub(self.heartbeat_interval_ticks);
            if self.lease_duration_ticks >= safe_max {
                return Err(ConfigError::LeaseDurationTooLarge {
                    lease_duration_ticks: self.lease_duration_ticks,
                    election_timeout_min_ticks: self.election_timeout_min_ticks,
                    heartbeat_interval_ticks: self.heartbeat_interval_ticks,
                });
            }
        }
        Ok(())
    }
}

/// The Raft role — `Follower`, `PreCandidate`, `Candidate`, or `Leader`.
///
/// Exposed via [`Node::status`]. Deliberately a flat enum (no
/// per-role payload) to keep the observability surface decoupled from
/// `yggr_core`'s internal [`RoleState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    /// §9.6 pre-vote phase. An election timer fired and we're asking
    /// peers whether they'd vote for us before bumping our term.
    PreCandidate,
    Candidate,
    Leader,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Follower => f.write_str("follower"),
            Self::PreCandidate => f.write_str("pre-candidate"),
            Self::Candidate => f.write_str("candidate"),
            Self::Leader => f.write_str("leader"),
        }
    }
}

/// A snapshot of a [`Node`]'s runtime state, returned by
/// [`Node::status`].
///
/// Consistent with what the driver sees at the moment the status
/// request is handled; not a live view — by the time the caller
/// inspects it the node may have moved on.
#[derive(Debug, Clone)]
pub struct NodeStatus {
    /// Stable shorthand for the local node id.
    pub id: NodeId,
    /// Our own node id, copied from [`Config::node_id`].
    pub node_id: NodeId,
    /// Current role.
    pub role: Role,
    /// Stable shorthand for the current term.
    pub term: Term,
    /// Current Raft term (§5.1).
    pub current_term: Term,
    /// Highest log index known to be committed cluster-wide.
    pub commit_index: LogIndex,
    /// Highest log index the driver has handed to the state machine.
    /// Always `<= commit_index`.
    pub last_applied: LogIndex,
    /// Highest log index currently present in the local log.
    pub last_log_index: LogIndex,
    /// Stable shorthand for the current leader hint.
    pub leader: Option<NodeId>,
    /// If we're a follower who has observed a current-term leader,
    /// its id; `None` otherwise (candidate, leader, or a follower
    /// that hasn't heard from anyone this term yet).
    pub leader_hint: Option<NodeId>,
    /// Runtime-facing membership view. Learners are empty until
    /// learner support lands.
    pub membership: MembershipView,
    /// Active peer set (excluding self). Mutates as membership
    /// changes land; see `Node::add_peer` / `remove_peer`.
    pub peers: Vec<NodeId>,
}

/// Runtime-facing view of the node's membership.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MembershipView {
    /// Voting peers excluding self.
    pub voters: Vec<NodeId>,
    /// Learners excluding self.
    pub learners: Vec<NodeId>,
}

/// Runtime-facing metrics snapshot that wraps the engine counters with
/// a small amount of node-level context.
#[derive(Debug, Clone)]
pub struct NodeMetrics {
    /// Current role at the instant the metrics snapshot was taken.
    pub role: Role,
    /// Current leader hint, if known.
    pub leader: Option<NodeId>,
    /// Highest log index currently present in the local log.
    pub last_log_index: LogIndex,
    /// Membership view used by the runtime.
    pub membership: MembershipView,
    /// Raw engine counters and gauges.
    pub engine: yggr_core::engine::metrics::EngineMetrics,
}

/// Errors `propose` / `add_peer` / `remove_peer` can return.
#[non_exhaustive]
#[derive(Debug)]
pub enum ProposeError {
    /// We were a follower with a known leader; redirect there.
    NotLeader { leader_hint: NodeId },
    /// We were a follower or candidate with no known leader. The
    /// caller should retry on a sensible cadence; the cluster may
    /// still be electing.
    NoLeader,
    /// The runtime is shutting down.
    Shutdown,
    /// The driver task died. Likely a bug or a fatal `Storage` /
    /// `StateMachine` error.
    DriverDead,
    /// The driver's in-flight proposal queue is full
    /// ([`Config::max_pending_proposals`] reached). Backpressure
    /// signal: retry later, or adjust the limit upward if your
    /// workload's concurrency is legitimately that high.
    Busy,
    /// A non-recoverable error hit the runtime: a `Storage` write
    /// failed, the state machine couldn't decode a committed
    /// command (corrupt log), or similar. The driver has shut
    /// itself down after failing every in-flight proposal with
    /// this error — the caller's next call will see `Shutdown` or
    /// `DriverDead`.
    Fatal { reason: &'static str },
}

impl std::fmt::Display for ProposeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLeader { leader_hint } => write!(f, "not leader; try {leader_hint}"),
            Self::NoLeader => write!(f, "no leader known yet"),
            Self::Shutdown => write!(f, "node is shutting down"),
            Self::DriverDead => write!(f, "node driver task died"),
            Self::Busy => write!(f, "too many in-flight proposals; retry later"),
            Self::Fatal { reason } => write!(f, "fatal runtime error: {reason}"),
        }
    }
}

impl std::error::Error for ProposeError {}

/// Errors from [`Node::transfer_leadership_to`].
#[non_exhaustive]
#[derive(Debug)]
pub enum TransferLeadershipError {
    NotLeader { leader_hint: NodeId },
    NoLeader,
    InvalidTarget { target: NodeId },
    Shutdown,
    DriverDead,
    Fatal { reason: &'static str },
}

impl std::fmt::Display for TransferLeadershipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLeader { leader_hint } => write!(f, "not leader; try {leader_hint}"),
            Self::NoLeader => write!(f, "no leader known yet"),
            Self::InvalidTarget { target } => write!(f, "invalid transfer target: {target}"),
            Self::Shutdown => write!(f, "node is shutting down"),
            Self::DriverDead => write!(f, "node driver task died"),
            Self::Fatal { reason } => write!(f, "fatal runtime error: {reason}"),
        }
    }
}

impl std::error::Error for TransferLeadershipError {}

/// Errors from [`Node::read_linearizable`].
#[non_exhaustive]
#[derive(Debug)]
pub enum ReadError {
    /// We are a follower that knows the current leader. Retry there.
    NotLeader { leader_hint: NodeId },
    /// No leader is currently known, or this leader hasn't committed
    /// an entry in its current term yet (§8 `ReadIndex` safety). The
    /// caller should retry after a short delay.
    NotReady,
    /// The leader stepped down before the read could be served.
    SteppedDown,
    /// The node is shutting down.
    Shutdown,
    /// The driver task died.
    DriverDead,
    /// A non-recoverable runtime error occurred.
    Fatal { reason: &'static str },
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLeader { leader_hint } => write!(f, "not leader; try {leader_hint}"),
            Self::NotReady => write!(f, "leader not ready to serve linearizable reads"),
            Self::SteppedDown => write!(f, "leader stepped down before read completed"),
            Self::Shutdown => write!(f, "node is shutting down"),
            Self::DriverDead => write!(f, "node driver task died"),
            Self::Fatal { reason } => write!(f, "fatal runtime error: {reason}"),
        }
    }
}

impl std::error::Error for ReadError {}

/// Errors from [`Node::start`].
#[non_exhaustive]
#[derive(Debug)]
pub enum NodeStartError<E> {
    Config(ConfigError),
    Storage(E),
}

impl<E: std::fmt::Display> std::fmt::Display for NodeStartError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(e) => write!(f, "invalid config: {e}"),
            Self::Storage(e) => write!(f, "storage recover failed: {e}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for NodeStartError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(e) => Some(e),
            Self::Storage(e) => Some(e),
        }
    }
}

/// Preferred name for the client write path.
pub type WriteError = ProposeError;

impl<S: StateMachine> Node<S> {
    /// Boot a node: recover any persisted state, build the engine,
    /// spawn the driver.
    pub async fn start<St, Tr>(
        config: Config,
        state_machine: S,
        mut storage: St,
        transport: Tr,
    ) -> Result<Self, NodeStartError<St::Error>>
    where
        St: Storage<Vec<u8>>,
        Tr: Transport<Vec<u8>>,
    {
        config.validate().map_err(NodeStartError::Config)?;
        let recovered = storage.recover().await.map_err(NodeStartError::Storage)?;

        // Resolve the initial peer set from the Bootstrap variant.
        //  - NewCluster: trust Config::peers. `Bootstrap::NewCluster`
        //    still carries `members` for pre-0.1 API continuity, but
        //    `Config::peers` is the authoritative engine input.
        //  - Join: empty — the existing leader will splice us in.
        //  - Recover: use config.peers as a fallback seed; any
        //    persisted ConfigChange entries will be replayed during
        //    hydrate_engine and mutate the set into its real shape.
        let initial_peers: Vec<NodeId> = match &config.bootstrap {
            Bootstrap::Join => Vec::new(),
            Bootstrap::NewCluster { .. } | Bootstrap::Recover => config.peers.clone(),
        };

        // Build a fresh engine. We'll feed it the recovered state via
        // synthetic events inside the driver before processing any
        // user inputs — same approach the sim harness uses for crash
        // recovery, and it keeps Engine's API as the only mutator.
        // Seed the RNG from (wall clock, node id) so different nodes
        // in the same cluster draw different timeouts. §5.2 relies on
        // this randomization to break split votes.
        let seed_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(0));
        let seed = seed_nanos.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ config.node_id.get();
        let env: Box<dyn Env> = Box::new(RandomizedEnv::new(
            seed,
            config.election_timeout_min_ticks,
            config.election_timeout_max_ticks,
        ));
        let engine_cfg = EngineConfig::new(
            config.snapshot_chunk_size_bytes,
            config.snapshot_hint_threshold_entries,
        )
        .with_pre_vote(config.pre_vote)
        .with_max_log_entries(config.max_log_entries)
        .with_lease_duration_ticks(config.lease_duration_ticks);
        let engine: Engine<Vec<u8>> = Engine::with_config(
            config.node_id,
            initial_peers.iter().copied(),
            env,
            config.heartbeat_interval_ticks,
            engine_cfg,
        );

        let max_pending_proposals = config.max_pending_proposals;

        // Apply task: owns the state machine, is fed committed
        // commands / confirmed reads / snapshot restores by the
        // driver. Bounded channel gives the driver backpressure if
        // the state machine falls behind.
        let (apply_tx, apply_rx) = mpsc::channel::<ApplyRequest<S>>(config.max_pending_applies);
        let apply: JoinHandle<()> = tokio::spawn(apply_loop(state_machine, apply_rx));

        let (inputs_tx, inputs_rx) = mpsc::channel::<DriverInput<S>>(1024);

        // Spawn the tick driver. Lives as long as the inputs channel
        // does (the driver's drop closes it).
        let tick_inputs = inputs_tx.clone();
        let tick_interval = config.tick_interval;
        let ticker: JoinHandle<()> = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tick_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if tick_inputs.send(DriverInput::Tick).await.is_err() {
                    return;
                }
            }
        });

        // Spawn the driver itself.
        let driver_state = Driver {
            node_id: config.node_id,
            engine,
            apply_tx,
            storage,
            transport,
            inputs: inputs_rx,
            inputs_tx: inputs_tx.clone(),
            pending_proposals: HashMap::new(),
            pending_config_changes: HashMap::new(),
            max_pending_proposals,
            pending_reads: HashMap::new(),
            next_read_id: 0,
            batch_buffer: Vec::new(),
            batch_delay_remaining: 0,
            max_batch_delay_ticks: config.max_batch_delay_ticks,
            max_batch_entries: config.max_batch_entries,
            last_applied: LogIndex::ZERO,
            snapshot_in_flight: false,
        };
        let driver: JoinHandle<()> = tokio::spawn(driver_loop(driver_state, recovered));

        Ok(Self {
            inputs: inputs_tx,
            background: Arc::new(Mutex::new(Some(BackgroundTasks {
                ticker,
                driver,
                apply,
            }))),
        })
    }

    /// Submit a command to the cluster. Returns once the command has
    /// applied locally — meaning it committed via majority replication
    /// AND the engine handed it back to the state machine — and
    /// returns whatever the state machine produced.
    ///
    /// `write()` is the preferred spelling for this client workflow;
    /// `propose()` remains as a compatibility alias.
    pub async fn write(&self, command: S::Command) -> Result<S::Response, WriteError> {
        let (tx, rx) = oneshot::channel();
        if self
            .inputs
            .send(DriverInput::Propose { command, reply: tx })
            .await
            .is_err()
        {
            return Err(ProposeError::Shutdown);
        }
        match rx.await {
            Ok(r) => r,
            Err(_) => Err(ProposeError::DriverDead),
        }
    }

    /// Compatibility alias for [`Self::write`].
    pub async fn propose(&self, command: S::Command) -> Result<S::Response, ProposeError> {
        self.write(command).await
    }

    /// Return an operator-facing handle for membership and lifecycle
    /// operations.
    #[must_use]
    pub fn admin(&self) -> AdminHandle<S> {
        AdminHandle { node: self.clone() }
    }

    /// Add a peer to the cluster (§4.3 single-server change). Returns
    /// once the membership change commits.
    pub async fn add_peer(&self, peer: NodeId) -> Result<(), ProposeError> {
        self.config_change(ConfigChange::AddPeer(peer)).await
    }

    /// Remove a peer from the cluster (§4.3 single-server change).
    /// Returns once the membership change commits. If the removed
    /// peer is self, the local node steps down on commit.
    pub async fn remove_peer(&self, peer: NodeId) -> Result<(), ProposeError> {
        self.config_change(ConfigChange::RemovePeer(peer)).await
    }

    /// Run `reader` against the state machine at a linearizable
    /// read point (Raft §8 "`ReadIndex`"). Returns whatever `reader`
    /// returns once the engine confirms this leader is still
    /// authoritative and has applied everything committed at request
    /// time.
    ///
    /// Errors if this node is not the leader, the leader hasn't yet
    /// committed an entry in its current term, or the leader steps
    /// down before the quorum round completes.
    pub async fn read_linearizable<R, F>(&self, reader: F) -> Result<R, ReadError>
    where
        R: Send + 'static,
        F: FnOnce(&S) -> R + Send + 'static,
    {
        let (ok_tx, ok_rx) = oneshot::channel::<R>();
        let (err_tx, err_rx) = oneshot::channel::<ReadError>();

        let boxed_reader: Box<dyn FnOnce(&S) + Send> = Box::new(move |sm: &S| {
            let value = reader(sm);
            let _ = ok_tx.send(value);
        });
        let boxed_on_failure: Box<dyn FnOnce(ReadError) + Send> = Box::new(move |e: ReadError| {
            let _ = err_tx.send(e);
        });

        if self
            .inputs
            .send(DriverInput::Read {
                reader: boxed_reader,
                on_failure: boxed_on_failure,
            })
            .await
            .is_err()
        {
            return Err(ReadError::Shutdown);
        }

        tokio::select! {
            biased;
            Ok(err) = err_rx => Err(err),
            Ok(val) = ok_rx => Ok(val),
            else => Err(ReadError::DriverDead),
        }
    }

    /// Ask the current leader to transfer leadership to `peer`.
    ///
    /// Returns once the local driver has accepted the request and
    /// dispatched the engine actions needed to start the transfer. It
    /// does not wait for the new leader to be elected.
    pub async fn transfer_leadership_to(
        &self,
        peer: NodeId,
    ) -> Result<(), TransferLeadershipError> {
        let (tx, rx) = oneshot::channel();
        if self
            .inputs
            .send(DriverInput::TransferLeadership {
                target: peer,
                reply: tx,
            })
            .await
            .is_err()
        {
            return Err(TransferLeadershipError::Shutdown);
        }
        match rx.await {
            Ok(r) => r,
            Err(_) => Err(TransferLeadershipError::DriverDead),
        }
    }

    async fn config_change(&self, change: ConfigChange) -> Result<(), ProposeError> {
        let (tx, rx) = oneshot::channel();
        if self
            .inputs
            .send(DriverInput::ConfigChange { change, reply: tx })
            .await
            .is_err()
        {
            return Err(ProposeError::Shutdown);
        }
        match rx.await {
            Ok(r) => r,
            Err(_) => Err(ProposeError::DriverDead),
        }
    }

    /// Snapshot the node's current runtime state. Cheap — the driver
    /// just reads from its own engine and replies.
    ///
    /// The returned status is a point-in-time view; the node may have
    /// changed role, advanced its term, or mutated membership by the
    /// time the caller inspects it. Use it for dashboards, health
    /// checks, and debugging, not for correctness-critical logic.
    pub async fn status(&self) -> Result<NodeStatus, ProposeError> {
        let (tx, rx) = oneshot::channel();
        if self
            .inputs
            .send(DriverInput::Status { reply: tx })
            .await
            .is_err()
        {
            return Err(ProposeError::Shutdown);
        }
        match rx.await {
            Ok(s) => Ok(s),
            Err(_) => Err(ProposeError::DriverDead),
        }
    }

    /// Return the current leader hint, if known, without forcing
    /// callers to inspect the full [`NodeStatus`].
    pub async fn current_leader(&self) -> Result<Option<NodeId>, ProposeError> {
        Ok(self.status().await?.leader_hint)
    }

    /// Pull a snapshot of the engine's observability counters and
    /// gauges. Counters (`*_sent`, `*_received`, `*_granted`, …) only
    /// move forward over the lifetime of the node; gauges
    /// (`current_term`, `commit_index`, …) reflect the current value.
    ///
    /// The snapshot is a point-in-time read — by the time the caller
    /// inspects it, the node may have processed more events.
    ///
    /// Returns an error if the driver has shut down.
    pub async fn metrics(&self) -> Result<yggr_core::engine::metrics::EngineMetrics, ProposeError> {
        let (tx, rx) = oneshot::channel();
        if self
            .inputs
            .send(DriverInput::Metrics { reply: tx })
            .await
            .is_err()
        {
            return Err(ProposeError::Shutdown);
        }
        match rx.await {
            Ok(m) => Ok(m),
            Err(_) => Err(ProposeError::DriverDead),
        }
    }

    /// Pull a runtime-facing metrics snapshot with a little extra
    /// node context around the raw engine counters and gauges.
    pub async fn node_metrics(&self) -> Result<NodeMetrics, ProposeError> {
        let status = self.status().await?;
        let engine = self.metrics().await?;
        Ok(NodeMetrics {
            role: status.role,
            leader: status.leader,
            last_log_index: status.last_log_index,
            membership: status.membership,
            engine,
        })
    }

    /// Initiate a graceful shutdown. Returns once the driver has
    /// drained any in-flight work, the transport has stopped its own
    /// background tasks, and the runtime's ticker/driver tasks have
    /// exited.
    pub async fn shutdown(self) -> Result<(), ProposeError> {
        let (tx, rx) = oneshot::channel();
        let shutdown_requested = self
            .inputs
            .send(DriverInput::Shutdown { reply: tx })
            .await
            .is_ok();

        let background = self
            .background
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();

        if shutdown_requested {
            match rx.await {
                Ok(()) => {}
                Err(_) => return Err(ProposeError::DriverDead),
            }
        }

        let Some(background) = background else {
            return Ok(());
        };
        let BackgroundTasks {
            ticker,
            driver,
            apply,
        } = background;

        ticker.abort();
        match ticker.await {
            Ok(()) => {}
            Err(e) if e.is_cancelled() => {}
            Err(_) => return Err(ProposeError::DriverDead),
        }
        match driver.await {
            Ok(()) => {}
            Err(_) => return Err(ProposeError::DriverDead),
        }
        // Driver's apply_tx was dropped when driver exited. The apply
        // task's recv() now returns None; it drains pending requests
        // in arrival order and exits. Await it to ensure every Apply
        // we dispatched has landed on the state machine.
        match apply.await {
            Ok(()) => Ok(()),
            Err(_) => Err(ProposeError::DriverDead),
        }
    }
}

impl<S: StateMachine> AdminHandle<S> {
    /// Add a peer to the cluster (§4.3 single-server change). Returns
    /// once the membership change commits.
    pub async fn add_peer(&self, peer: NodeId) -> Result<(), ProposeError> {
        self.node.add_peer(peer).await
    }

    /// Remove a peer from the cluster (§4.3 single-server change).
    /// Returns once the membership change commits.
    pub async fn remove_peer(&self, peer: NodeId) -> Result<(), ProposeError> {
        self.node.remove_peer(peer).await
    }

    /// Ask the current leader to transfer leadership to `peer`.
    pub async fn transfer_leadership(&self, peer: NodeId) -> Result<(), TransferLeadershipError> {
        self.node.transfer_leadership_to(peer).await
    }

    /// Initiate a graceful shutdown of the runtime.
    pub async fn shutdown(self) -> Result<(), ProposeError> {
        self.node.shutdown().await
    }
}

// ---------------------------------------------------------------------------
// Driver (private)
// ---------------------------------------------------------------------------

struct Driver<S: StateMachine, St, Tr> {
    node_id: NodeId,
    engine: Engine<Vec<u8>>,
    /// Sender to the apply task that owns the state machine. Every
    /// `Action::Apply` / `Action::ApplySnapshot` / `Action::ReadReady`
    /// becomes an `ApplyRequest` over this channel.
    apply_tx: mpsc::Sender<ApplyRequest<S>>,
    storage: St,
    transport: Tr,
    inputs: mpsc::Receiver<DriverInput<S>>,
    /// Cloneable sender end of the driver's own input channel. Used
    /// by background helper tasks (currently just the snapshot
    /// forwarder) to post results back into the driver's main loop
    /// without requiring a direct `&mut Driver`.
    inputs_tx: mpsc::Sender<DriverInput<S>>,
    /// Index in the log → user oneshot to fire when the entry applies.
    pending_proposals: HashMap<LogIndex, oneshot::Sender<Result<S::Response, ProposeError>>>,
    /// Index in the log → user oneshot to fire when a `ConfigChange`
    /// entry applies (commits).
    pending_config_changes: HashMap<LogIndex, oneshot::Sender<Result<(), ProposeError>>>,
    /// Combined cap on in-flight proposals + config changes. Keeps the
    /// reply maps from growing unboundedly under load.
    max_pending_proposals: usize,
    /// Read id → pending reader closures. Keyed on the id chosen when
    /// the runtime submitted the `ProposeRead` event.
    pending_reads: HashMap<u64, PendingRead<S>>,
    /// Monotonic counter that generates read ids.
    next_read_id: u64,
    /// Buffered (`command_bytes`, reply) pairs awaiting a batch flush.
    /// Only used when `max_batch_delay_ticks > 0`.
    batch_buffer: Vec<BatchEntry<S>>,
    /// Ticks remaining before the current batch is flushed. `0` with
    /// an empty buffer means "no batch in flight".
    batch_delay_remaining: u64,
    /// Mirror of `Config::max_batch_delay_ticks`.
    max_batch_delay_ticks: u64,
    /// Mirror of `Config::max_batch_entries`.
    max_batch_entries: usize,
    /// Highest log index the driver has fed into the state machine.
    /// Mirrors the engine's own `last_applied` but the engine doesn't
    /// expose it publicly, so we track it from the Apply actions we
    /// dispatch.
    last_applied: LogIndex,
    /// `true` while a snapshot request is sitting in the apply task
    /// or a forwarder task is waiting on its reply. A second
    /// `Action::SnapshotHint` that arrives in that window is dropped
    /// — the engine will re-issue one the next time the threshold is
    /// crossed, and serialising concurrent snapshots on the same
    /// state machine would double the work for nothing.
    snapshot_in_flight: bool,
}

struct PendingRead<S: StateMachine> {
    reader: Box<dyn FnOnce(&S) + Send>,
    on_failure: Box<dyn FnOnce(ReadError) + Send>,
}

/// One buffered proposal in the driver's batch: the encoded command
/// bytes and the oneshot that will carry the apply response back to
/// the user.
type BatchEntry<S> = (
    Vec<u8>,
    oneshot::Sender<Result<<S as StateMachine>::Response, ProposeError>>,
);

/// Unit of work the driver ships to the apply task. Every request
/// mutates or observes the state machine in the order the driver
/// emitted it.
enum ApplyRequest<S: StateMachine> {
    /// A committed command. The driver already decoded it; apply
    /// runs and (optionally) replies to the originating `propose`.
    Command {
        command: S::Command,
        reply: Option<oneshot::Sender<Result<S::Response, ProposeError>>>,
    },
    /// A linearizable read confirmed by the engine. Runs against the
    /// post-apply state machine.
    Read { reader: Box<dyn FnOnce(&S) + Send> },
    /// Restore the state machine from snapshot bytes just installed.
    Restore { bytes: Vec<u8> },
    /// Produce a snapshot of current state. Reply carries the bytes,
    /// or a `SnapshotError` if the state machine declined this attempt
    /// (e.g. ENOSPC, transient backpressure). The driver logs the
    /// error and drops the attempt — subsequent hints retry.
    /// Ordered behind any preceding `Command` so the snapshot observes
    /// every applied entry up to this request.
    TakeSnapshot {
        reply: oneshot::Sender<Result<Vec<u8>, crate::state_machine::SnapshotError>>,
    },
}

/// The apply task: owns the state machine, processes `ApplyRequest`s
/// in FIFO order. Runs until the channel closes (every sender has
/// been dropped — i.e. the driver exited).
async fn apply_loop<S: StateMachine>(
    mut state_machine: S,
    mut rx: mpsc::Receiver<ApplyRequest<S>>,
) {
    while let Some(req) = rx.recv().await {
        match req {
            ApplyRequest::Command { command, reply } => {
                let response = state_machine.apply(command);
                if let Some(reply) = reply {
                    let _ = reply.send(Ok(response));
                }
            }
            ApplyRequest::Read { reader } => {
                reader(&state_machine);
            }
            ApplyRequest::Restore { bytes } => {
                state_machine.restore(bytes);
            }
            ApplyRequest::TakeSnapshot { reply } => {
                let result = state_machine.snapshot();
                let _ = reply.send(result);
            }
        }
    }
}

enum DriverInput<S: StateMachine> {
    Tick,
    Propose {
        command: S::Command,
        reply: oneshot::Sender<Result<S::Response, ProposeError>>,
    },
    ConfigChange {
        change: ConfigChange,
        reply: oneshot::Sender<Result<(), ProposeError>>,
    },
    TransferLeadership {
        target: NodeId,
        reply: oneshot::Sender<Result<(), TransferLeadershipError>>,
    },
    /// Linearizable read. The driver hands `reader` to the engine as
    /// a `ProposeRead` event; once the engine signals `ReadReady` for
    /// this id, the driver runs the closure against the state machine
    /// and fires its own oneshot. On `ReadFailed` (or driver death),
    /// `on_failure` converts the reason to a user-visible error and
    /// fires whatever oneshot the caller is awaiting.
    Read {
        reader: Box<dyn FnOnce(&S) + Send>,
        on_failure: Box<dyn FnOnce(ReadError) + Send>,
    },
    Status {
        reply: oneshot::Sender<NodeStatus>,
    },
    Metrics {
        reply: oneshot::Sender<yggr_core::engine::metrics::EngineMetrics>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
    /// Delivered by the snapshot-forwarder task when the apply task
    /// finishes serialising state. Carries the bytes into the driver
    /// so it can feed them to the engine via `Event::SnapshotTaken`.
    /// `bytes == None` means the apply task was torn down before it
    /// could reply; the driver just clears the in-flight flag and
    /// lets the engine re-hint later.
    SnapshotReady {
        last_included_index: LogIndex,
        bytes: Option<Vec<u8>>,
    },
}

async fn driver_loop<S, St, Tr>(mut d: Driver<S, St, Tr>, recovered: RecoveredState<Vec<u8>>)
where
    S: StateMachine,
    St: Storage<Vec<u8>>,
    Tr: Transport<Vec<u8>>,
{
    // Re-hydrate the engine from persisted state by feeding synthetic
    // RPCs. Same approach the sim harness uses for crash-recover.
    hydrate_engine(&mut d, recovered).await;

    loop {
        let result: Result<(), Fatal> = tokio::select! {
            input = d.inputs.recv() => {
                let Some(input) = input else { return };
                match input {
                    DriverInput::Tick => handle_tick(&mut d).await,
                    DriverInput::Propose { command, reply } => {
                        handle_propose(&mut d, command, reply).await
                    }
                    DriverInput::ConfigChange { change, reply } => {
                        handle_config_change(&mut d, change, reply).await
                    }
                    DriverInput::Read { reader, on_failure } => {
                        handle_read(&mut d, reader, on_failure).await
                    }
                    DriverInput::TransferLeadership { target, reply } => {
                        handle_transfer_leadership(&mut d, target, reply).await
                    }
                    DriverInput::Status { reply } => {
                        let _ = reply.send(build_status(&d));
                        Ok(())
                    }
                    DriverInput::Metrics { reply } => {
                        let _ = reply.send(d.engine.metrics());
                        Ok(())
                    }
                    DriverInput::SnapshotReady { last_included_index, bytes } => {
                        handle_snapshot_ready(&mut d, last_included_index, bytes).await
                    }
                    DriverInput::Shutdown { reply } => {
                        debug!(target = "yggr::node", "shutdown requested");
                        // Best-effort flush of any buffered proposals so
                        // waiters see their result rather than dropping.
                        let _ = flush_batch(&mut d).await;
                        d.transport.shutdown().await;
                        let _ = reply.send(());
                        return;
                    }
                }
            }
            incoming = d.transport.recv() => {
                let Some(incoming) = incoming else {
                    warn!(target = "yggr::node", "transport recv returned None; shutting down");
                    return;
                };
                step_and_dispatch(&mut d, Event::Incoming(incoming)).await
            }
        };
        if let Err(Fatal { reason }) = result {
            fail_all_pending(&mut d, reason);
            return;
        }
    }
}

async fn hydrate_engine<S, St, Tr>(d: &mut Driver<S, St, Tr>, recovered: RecoveredState<Vec<u8>>)
where
    S: StateMachine,
    St: Storage<Vec<u8>>,
    Tr: Transport<Vec<u8>>,
{
    use yggr_core::{RecoveredHardState, RecoveredSnapshot};

    let RecoveredState {
        hard_state,
        snapshot,
        log,
    } = recovered;
    if hard_state.is_none() && snapshot.is_none() && log.is_empty() {
        return;
    }

    let snapshot_bytes_for_sm = snapshot.as_ref().map(|s| s.bytes.clone());

    let (current_term, voted_for) =
        hard_state.map_or((Term::ZERO, None), |hs| (hs.current_term, hs.voted_for));

    let recovered = RecoveredHardState {
        current_term,
        voted_for,
        snapshot: snapshot.map(|s| RecoveredSnapshot {
            last_included_index: s.last_included_index,
            last_included_term: s.last_included_term,
            membership: s.membership,
            bytes: s.bytes,
        }),
        post_snapshot_log: log,
    };
    d.engine.recover_from(recovered);

    // Hand the snapshot bytes to the state machine exactly once on
    // boot so it can rebuild its in-memory state. Everything past the
    // snapshot floor is uncommitted until a current-term leader
    // tells us otherwise, so there's nothing else to apply here.
    if let Some(bytes) = snapshot_bytes_for_sm {
        // Send.await won't fail: the apply task was spawned right
        // before driver_loop and hasn't had a reason to exit yet.
        let _ = d.apply_tx.send(ApplyRequest::Restore { bytes }).await;
    }
    // Mirror the engine's restored last_applied onto the driver's
    // NodeStatus cache.
    d.last_applied = d.engine.commit_index();
}

async fn handle_propose<S, St, Tr>(
    d: &mut Driver<S, St, Tr>,
    command: S::Command,
    reply: oneshot::Sender<Result<S::Response, ProposeError>>,
) -> Result<(), Fatal>
where
    S: StateMachine,
    St: Storage<Vec<u8>>,
    Tr: Transport<Vec<u8>>,
{
    // Backpressure: the reply maps are unbounded by design (we keep a
    // waiter around until the entry commits+applies), so without a
    // cap a flood of proposals could OOM the driver. Reject fast
    // before we even touch the engine.
    if d.pending_proposals.len() + d.pending_config_changes.len() >= d.max_pending_proposals {
        let _ = reply.send(Err(ProposeError::Busy));
        return Ok(());
    }

    let bytes = S::encode_command(&command);

    // Batching path: buffer until cap or tick flush. Skip when the
    // caller is a non-leader — we still need to return a redirect
    // synchronously, so dry-step the engine with the single command
    // to observe its role response.
    if d.max_batch_delay_ticks > 0 && matches!(d.engine.role(), RoleState::Leader(_)) {
        d.batch_buffer.push((bytes, reply));
        if d.batch_delay_remaining == 0 {
            d.batch_delay_remaining = d.max_batch_delay_ticks;
        }
        if d.batch_buffer.len() >= d.max_batch_entries {
            return flush_batch(d).await;
        }
        return Ok(());
    }

    // Encode the command, then drive ClientProposal. The next
    // PersistLogEntries action (if any) tells us the assigned index;
    // we register the reply oneshot at that index.
    let last_before = d
        .engine
        .log()
        .last_log_id()
        .map_or(LogIndex::ZERO, |l| l.index);
    let actions = d.engine.step(Event::ClientProposal(bytes));
    let last_after = d
        .engine
        .log()
        .last_log_id()
        .map_or(LogIndex::ZERO, |l| l.index);

    if last_after > last_before {
        // Leader appended; remember this index for apply-time reply.
        d.pending_proposals.insert(last_after, reply);
    } else {
        let leader_hint = actions.iter().find_map(|a| match a {
            Action::Redirect { leader_hint } => Some(*leader_hint),
            _ => None,
        });
        let err = leader_hint.map_or(ProposeError::NoLeader, |h| ProposeError::NotLeader {
            leader_hint: h,
        });
        let _ = reply.send(Err(err));
    }

    dispatch_actions(d, actions).await
}

/// Flush the driver's proposal batch as a single
/// `Event::ClientProposalBatch`. Replies are registered at the log
/// indices the engine assigned, in batch order. If we've since lost
/// the leader role (for example a step-down happened between the
/// last `handle_propose` call and this flush), every pending reply
/// fails with `NotLeader`/`NoLeader`.
async fn flush_batch<S, St, Tr>(d: &mut Driver<S, St, Tr>) -> Result<(), Fatal>
where
    S: StateMachine,
    St: Storage<Vec<u8>>,
    Tr: Transport<Vec<u8>>,
{
    if d.batch_buffer.is_empty() {
        d.batch_delay_remaining = 0;
        return Ok(());
    }
    let drained: Vec<BatchEntry<S>> = std::mem::take(&mut d.batch_buffer);
    d.batch_delay_remaining = 0;

    let (commands, replies): (Vec<Vec<u8>>, Vec<_>) = drained.into_iter().unzip();
    let last_before = d
        .engine
        .log()
        .last_log_id()
        .map_or(LogIndex::ZERO, |l| l.index);
    let actions = d.engine.step(Event::ClientProposalBatch(commands));
    let last_after = d
        .engine
        .log()
        .last_log_id()
        .map_or(LogIndex::ZERO, |l| l.index);

    let appended = last_after.get().saturating_sub(last_before.get());
    if appended == u64::try_from(replies.len()).unwrap_or(0) && appended > 0 {
        // Engine appended every buffered command at indices
        // [last_before+1 .. last_after]; register each reply at its
        // index in the same order.
        for (offset, reply) in replies.into_iter().enumerate() {
            let idx = LogIndex::new(
                last_before
                    .get()
                    .saturating_add(1)
                    .saturating_add(u64::try_from(offset).unwrap_or(0)),
            );
            d.pending_proposals.insert(idx, reply);
        }
    } else {
        // Engine did not accept the batch — we stepped down between
        // buffering and flush, or the batch was dropped. Fail each
        // waiter with a redirect or NoLeader.
        let leader_hint = actions.iter().find_map(|a| match a {
            Action::Redirect { leader_hint } => Some(*leader_hint),
            _ => None,
        });
        for reply in replies {
            let err = leader_hint.map_or(ProposeError::NoLeader, |h| ProposeError::NotLeader {
                leader_hint: h,
            });
            let _ = reply.send(Err(err));
        }
    }

    dispatch_actions(d, actions).await
}

async fn handle_config_change<S, St, Tr>(
    d: &mut Driver<S, St, Tr>,
    change: ConfigChange,
    reply: oneshot::Sender<Result<(), ProposeError>>,
) -> Result<(), Fatal>
where
    S: StateMachine,
    St: Storage<Vec<u8>>,
    Tr: Transport<Vec<u8>>,
{
    if d.pending_proposals.len() + d.pending_config_changes.len() >= d.max_pending_proposals {
        let _ = reply.send(Err(ProposeError::Busy));
        return Ok(());
    }
    let last_before = d
        .engine
        .log()
        .last_log_id()
        .map_or(LogIndex::ZERO, |l| l.index);
    let actions = d.engine.step(Event::ProposeConfigChange(change));
    let last_after = d
        .engine
        .log()
        .last_log_id()
        .map_or(LogIndex::ZERO, |l| l.index);

    if last_after > last_before {
        d.pending_config_changes.insert(last_after, reply);
    } else {
        let leader_hint = actions.iter().find_map(|a| match a {
            Action::Redirect { leader_hint } => Some(*leader_hint),
            _ => None,
        });
        let err = leader_hint.map_or(ProposeError::NoLeader, |h| ProposeError::NotLeader {
            leader_hint: h,
        });
        let _ = reply.send(Err(err));
    }
    dispatch_actions(d, actions).await
}

async fn handle_transfer_leadership<S, St, Tr>(
    d: &mut Driver<S, St, Tr>,
    target: NodeId,
    reply: oneshot::Sender<Result<(), TransferLeadershipError>>,
) -> Result<(), Fatal>
where
    S: StateMachine,
    St: Storage<Vec<u8>>,
    Tr: Transport<Vec<u8>>,
{
    match d.engine.role() {
        RoleState::Leader(_) => {}
        RoleState::Follower(f) => {
            let err = f
                .leader_id()
                .map_or(TransferLeadershipError::NoLeader, |leader_hint| {
                    TransferLeadershipError::NotLeader { leader_hint }
                });
            let _ = reply.send(Err(err));
            return Ok(());
        }
        RoleState::PreCandidate(_) | RoleState::Candidate(_) => {
            let _ = reply.send(Err(TransferLeadershipError::NoLeader));
            return Ok(());
        }
    }

    if target == d.node_id || !d.engine.peers().contains(&target) {
        let _ = reply.send(Err(TransferLeadershipError::InvalidTarget { target }));
        return Ok(());
    }

    let actions = d.engine.step(Event::TransferLeadership { target });
    match dispatch_actions(d, actions).await {
        Ok(()) => {
            let _ = reply.send(Ok(()));
            Ok(())
        }
        Err(fatal) => {
            let _ = reply.send(Err(TransferLeadershipError::Fatal {
                reason: fatal.reason,
            }));
            Err(fatal)
        }
    }
}

/// Feed the bytes produced by a background `StateMachine::snapshot`
/// call back into the engine as `Event::SnapshotTaken` and dispatch
/// any resulting actions (typically a `PersistSnapshot`). Clears the
/// in-flight guard first so a fatal dispatch failure doesn't wedge
/// future snapshot hints, and so a next-threshold hint arriving
/// during follow-up dispatch is free to schedule the next snapshot.
///
/// `bytes == None` means the forwarder saw the apply task drop the
/// reply oneshot (task teardown or a panic inside `snapshot`). The
/// engine isn't told anything and will re-hint when the threshold is
/// crossed next — safer than pushing an empty snapshot, which would
/// advance the engine's snapshot floor without real state behind it.
/// Empty `Some(vec)` — i.e. the user's `snapshot()` returned `Vec::new()`
/// — is treated the same way as in the old blocking path: it's the
/// documented "no snapshot this time" signal and we skip the engine
/// step, just clearing the guard.
async fn handle_snapshot_ready<S, St, Tr>(
    d: &mut Driver<S, St, Tr>,
    last_included_index: LogIndex,
    bytes: Option<Vec<u8>>,
) -> Result<(), Fatal>
where
    S: StateMachine,
    St: Storage<Vec<u8>>,
    Tr: Transport<Vec<u8>>,
{
    d.snapshot_in_flight = false;
    let Some(bytes) = bytes else {
        warn!(
            target = "yggr::node",
            "snapshot forwarder saw apply task teardown"
        );
        return Ok(());
    };
    if bytes.is_empty() {
        return Ok(());
    }
    let actions = d.engine.step(Event::SnapshotTaken {
        last_included_index,
        bytes,
    });
    dispatch_actions(d, actions).await
}

async fn handle_read<S, St, Tr>(
    d: &mut Driver<S, St, Tr>,
    reader: Box<dyn FnOnce(&S) + Send>,
    on_failure: Box<dyn FnOnce(ReadError) + Send>,
) -> Result<(), Fatal>
where
    S: StateMachine,
    St: Storage<Vec<u8>>,
    Tr: Transport<Vec<u8>>,
{
    let id = d.next_read_id;
    d.next_read_id = d.next_read_id.wrapping_add(1);
    d.pending_reads
        .insert(id, PendingRead { reader, on_failure });
    let actions = d.engine.step(Event::ProposeRead { id });
    match dispatch_actions(d, actions).await {
        Ok(()) => Ok(()),
        Err(fatal) => {
            if let Some(pending) = d.pending_reads.remove(&id) {
                (pending.on_failure)(ReadError::Fatal {
                    reason: fatal.reason,
                });
            }
            Err(fatal)
        }
    }
}

/// A non-recoverable runtime error. Caught at every layer above
/// dispatch and propagated up to [`driver_loop`], which fails every
/// in-flight waiter with [`ProposeError::Fatal`] before exiting.
#[derive(Debug, Clone, Copy)]
struct Fatal {
    reason: &'static str,
}

/// Drive the engine with `event`, dispatch every emitted action.
/// Fatal errors (Storage write failures, committed-command decode
/// failures) propagate up; [`driver_loop`] exits after failing
/// pending waiters.
async fn step_and_dispatch<S, St, Tr>(
    d: &mut Driver<S, St, Tr>,
    event: Event<Vec<u8>>,
) -> Result<(), Fatal>
where
    S: StateMachine,
    St: Storage<Vec<u8>>,
    Tr: Transport<Vec<u8>>,
{
    let actions = d.engine.step(event);
    dispatch_actions(d, actions).await
}

/// Tick handler. Also drives the proposal-batch timer: if batching
/// is enabled and the delay has elapsed, flush the buffer before
/// stepping the engine so batched proposals appear in this tick's
/// replication broadcast.
async fn handle_tick<S, St, Tr>(d: &mut Driver<S, St, Tr>) -> Result<(), Fatal>
where
    S: StateMachine,
    St: Storage<Vec<u8>>,
    Tr: Transport<Vec<u8>>,
{
    if d.batch_delay_remaining > 0 {
        d.batch_delay_remaining -= 1;
        if d.batch_delay_remaining == 0 && !d.batch_buffer.is_empty() {
            flush_batch(d).await?;
        }
    }
    step_and_dispatch(d, Event::Tick).await
}

#[allow(clippy::too_many_lines)]
async fn dispatch_actions<S, St, Tr>(
    d: &mut Driver<S, St, Tr>,
    actions: Vec<Action<Vec<u8>>>,
) -> Result<(), Fatal>
where
    S: StateMachine,
    St: Storage<Vec<u8>>,
    Tr: Transport<Vec<u8>>,
{
    let mut pending: VecDeque<Action<Vec<u8>>> = actions.into();
    while let Some(action) = pending.pop_front() {
        match action {
            Action::PersistHardState {
                current_term,
                voted_for,
            } => {
                if let Err(e) = d
                    .storage
                    .persist_hard_state(StoredHardState {
                        current_term,
                        voted_for,
                    })
                    .await
                {
                    error!(target = "yggr::node", error = %e, "fatal: persist_hard_state failed");
                    return Err(Fatal {
                        reason: "persist_hard_state failed",
                    });
                }
            }
            Action::PersistLogEntries(entries) => {
                if let Err(e) = d.storage.append_log(entries).await {
                    error!(target = "yggr::node", error = %e, "fatal: append_log failed");
                    return Err(Fatal {
                        reason: "append_log failed",
                    });
                }
            }
            Action::PersistSnapshot {
                last_included_index,
                last_included_term,
                membership,
                bytes,
            } => {
                if let Err(e) = d
                    .storage
                    .persist_snapshot(StoredSnapshot {
                        last_included_index,
                        last_included_term,
                        membership,
                        bytes,
                    })
                    .await
                {
                    error!(target = "yggr::node", error = %e, "fatal: persist_snapshot failed");
                    return Err(Fatal {
                        reason: "persist_snapshot failed",
                    });
                }
            }
            Action::Send { to, message } => {
                if let Err(e) = d.transport.send(to, message).await {
                    debug!(target = "yggr::node", peer = %to, error = %e, "send failed");
                    // Non-fatal: engine retries on heartbeat.
                }
            }
            Action::Apply(entries) => {
                apply_entries(d, entries).await?;
            }
            Action::ApplySnapshot { bytes } => {
                if d.apply_tx
                    .send(ApplyRequest::Restore { bytes })
                    .await
                    .is_err()
                {
                    return Err(Fatal {
                        reason: "apply task died",
                    });
                }
            }
            Action::SnapshotHint {
                last_included_index,
            } => {
                // Fire the snapshot request off to the apply task and
                // a forwarder task, then keep going. The forwarder
                // posts a `SnapshotReady` input when the (potentially
                // very slow) `StateMachine::snapshot` call returns;
                // the driver stays responsive to ticks, RPCs, and
                // user calls in the meantime.
                //
                // If a hint arrives while a previous snapshot is
                // still in flight we drop it: the engine re-issues
                // hints each time the applied-entries threshold is
                // crossed anew.
                if d.snapshot_in_flight {
                    continue;
                }
                let (reply_tx, reply_rx) = oneshot::channel();
                if d.apply_tx
                    .send(ApplyRequest::TakeSnapshot { reply: reply_tx })
                    .await
                    .is_err()
                {
                    return Err(Fatal {
                        reason: "apply task died",
                    });
                }
                d.snapshot_in_flight = true;
                let inputs_tx = d.inputs_tx.clone();
                tokio::spawn(async move {
                    let bytes = match reply_rx.await {
                        Ok(Ok(bytes)) => Some(bytes),
                        Ok(Err(err)) => {
                            tracing::warn!(
                                target = "yggr::node",
                                error = %err,
                                "state machine declined to produce a snapshot; \
                                 dropping this hint, engine will re-hint later",
                            );
                            None
                        }
                        Err(_) => None,
                    };
                    let _ = inputs_tx
                        .send(DriverInput::SnapshotReady {
                            last_included_index,
                            bytes,
                        })
                        .await;
                });
            }
            Action::Redirect { .. } => {
                // Already handled at the propose call site; engine
                // shouldn't emit Redirect from any other path.
            }
            Action::ReadReady { id } => {
                if let Some(pending) = d.pending_reads.remove(&id)
                    && d.apply_tx
                        .send(ApplyRequest::Read {
                            reader: pending.reader,
                        })
                        .await
                        .is_err()
                {
                    return Err(Fatal {
                        reason: "apply task died",
                    });
                }
            }
            Action::ReadFailed { id, reason } => {
                if let Some(pending) = d.pending_reads.remove(&id) {
                    let err = match reason {
                        yggr_core::ReadFailure::NotLeader { leader_hint } => {
                            ReadError::NotLeader { leader_hint }
                        }
                        yggr_core::ReadFailure::SteppedDown => ReadError::SteppedDown,
                        // NotReady and any future non-exhaustive variants
                        // both surface as NotReady; the caller retries.
                        _ => ReadError::NotReady,
                    };
                    (pending.on_failure)(err);
                }
            }
        }
    }
    // Mirror the engine's internal last_applied onto the driver's
    // status cache. This catches both visible Apply actions and
    // filtered internal entries (Noop/ConfigChange) that still advance
    // the engine's own last_applied to commit_index.
    if d.engine.commit_index() > d.last_applied {
        d.last_applied = d.engine.commit_index();
    }
    Ok(())
}

fn build_status<S, St, Tr>(d: &Driver<S, St, Tr>) -> NodeStatus
where
    S: StateMachine,
    St: Storage<Vec<u8>>,
    Tr: Transport<Vec<u8>>,
{
    let (role, leader_hint) = match d.engine.role() {
        RoleState::Follower(f) => (Role::Follower, f.leader_id()),
        RoleState::PreCandidate(_) => (Role::PreCandidate, None),
        RoleState::Candidate(_) => (Role::Candidate, None),
        RoleState::Leader(_) => (Role::Leader, None),
    };
    let membership = MembershipView {
        voters: d.engine.membership().voters().iter().copied().collect(),
        learners: d.engine.membership().learners().iter().copied().collect(),
    };
    let peers = membership.voters.clone();
    let last_log_index = d
        .engine
        .log()
        .last_log_id()
        .map_or(LogIndex::ZERO, |id| id.index);
    NodeStatus {
        id: d.node_id,
        node_id: d.node_id,
        role,
        term: d.engine.current_term(),
        current_term: d.engine.current_term(),
        commit_index: d.engine.commit_index(),
        last_applied: d.last_applied,
        last_log_index,
        leader: leader_hint,
        leader_hint,
        membership,
        peers,
    }
}

async fn apply_entries<S, St, Tr>(
    d: &mut Driver<S, St, Tr>,
    entries: Vec<LogEntry<Vec<u8>>>,
) -> Result<(), Fatal>
where
    S: StateMachine,
    St: Storage<Vec<u8>>,
    Tr: Transport<Vec<u8>>,
{
    for entry in entries {
        // ConfigChange entries shouldn't come through Apply — engine
        // filters them — but a ConfigChange we proposed locally
        // might have just committed. Fire its waiter regardless of
        // whether it shows up here, by checking against the index.
        // (The Apply itself only carries Command entries.)
        if let LogPayload::Command(bytes) = entry.payload {
            let cmd = match S::decode_command(&bytes) {
                Ok(c) => c,
                Err(e) => {
                    error!(target = "yggr::node", error = %e, index = ?entry.id.index,
                           "fatal: failed to decode committed command");
                    // Corrupt log — a committed entry must decode.
                    // Propagate so driver_loop fails pending waiters
                    // and exits.
                    return Err(Fatal {
                        reason: "failed to decode committed command",
                    });
                }
            };
            let reply = d.pending_proposals.remove(&entry.id.index);
            if d.apply_tx
                .send(ApplyRequest::Command {
                    command: cmd,
                    reply,
                })
                .await
                .is_err()
            {
                return Err(Fatal {
                    reason: "apply task died",
                });
            }
        }
    }

    // Fire any pending ConfigChange waiters whose index is now ≤
    // commit_index. The engine doesn't surface "this CC committed"
    // separately; we infer from commit_index advancing past it.
    let commit = d.engine.commit_index();
    let to_fire: Vec<LogIndex> = d
        .pending_config_changes
        .keys()
        .copied()
        .filter(|&idx| idx <= commit)
        .collect();
    for idx in to_fire {
        if let Some(reply) = d.pending_config_changes.remove(&idx) {
            let _ = reply.send(Ok(()));
        }
    }

    // Stale-pending clean-up: if our role flipped to follower, any
    // proposals we registered as leader will never apply locally —
    // surface a NotLeader to those waiters using the current leader
    // hint, or NoLeader.
    if !matches!(d.engine.role(), RoleState::Leader(_)) {
        let leader_hint = match d.engine.role() {
            RoleState::Follower(f) => f.leader_id(),
            _ => None,
        };
        let stale: Vec<LogIndex> = d.pending_proposals.keys().copied().collect();
        for idx in stale {
            if let Some(reply) = d.pending_proposals.remove(&idx) {
                let err = leader_hint.map_or(ProposeError::NoLeader, |h| ProposeError::NotLeader {
                    leader_hint: h,
                });
                let _ = reply.send(Err(err));
            }
        }
        let stale_cc: Vec<LogIndex> = d.pending_config_changes.keys().copied().collect();
        for idx in stale_cc {
            if let Some(reply) = d.pending_config_changes.remove(&idx) {
                let err = leader_hint.map_or(ProposeError::NoLeader, |h| ProposeError::NotLeader {
                    leader_hint: h,
                });
                let _ = reply.send(Err(err));
            }
        }
    }

    // unused for now — silences dead import warnings the compiler
    // might otherwise raise across feature combos.
    let _ = d.node_id;
    Ok(())
}

/// Fail every in-flight proposal and config-change waiter with
/// `ProposeError::Fatal { reason }`. Called from [`driver_loop`]
/// right before it exits on a fatal error.
fn fail_all_pending<S, St, Tr>(d: &mut Driver<S, St, Tr>, reason: &'static str)
where
    S: StateMachine,
    St: Storage<Vec<u8>>,
    Tr: Transport<Vec<u8>>,
{
    let indices: Vec<LogIndex> = d.pending_proposals.keys().copied().collect();
    for idx in indices {
        if let Some(reply) = d.pending_proposals.remove(&idx) {
            let _ = reply.send(Err(ProposeError::Fatal { reason }));
        }
    }
    let indices: Vec<LogIndex> = d.pending_config_changes.keys().copied().collect();
    for idx in indices {
        if let Some(reply) = d.pending_config_changes.remove(&idx) {
            let _ = reply.send(Err(ProposeError::Fatal { reason }));
        }
    }
    let read_ids: Vec<u64> = d.pending_reads.keys().copied().collect();
    for id in read_ids {
        if let Some(pending) = d.pending_reads.remove(&id) {
            (pending.on_failure)(ReadError::Fatal { reason });
        }
    }
}
