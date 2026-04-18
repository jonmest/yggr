//! The user-facing [`Node`] runtime: a long-running task that owns
//! the engine and dispatches its actions.
//!
//! Architecture: a single owner task ("the driver") holds the
//! [`jotun_core::Engine`], the user's [`StateMachine`], the
//! [`Storage`] handle, and the [`Transport`]. Public methods on
//! [`Node`] (`propose`, `add_peer`, `remove_peer`, `shutdown`)
//! package a [`DriverInput`] into a channel and `await` a oneshot
//! reply. The driver's loop multiplexes:
//!
//!  1. A wall-clock tick (via `tokio::time::interval`).
//!  2. An inbound message from `Transport::recv`.
//!  3. A [`DriverInput`] from a user-facing call.
//!
//! Each of those becomes a `jotun_core::Event`; the driver calls
//! `engine.step` and dispatches every emitted [`Action`] in order:
//! Persists go to Storage, Sends go to Transport, Applies go to
//! the `StateMachine` (with replies to pending [`Node::propose`] futures),
//! Redirects fail any matching pending propose with `NotLeader`.
//!
//! The driver is the only mutator of any shared state; no locks.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jotun_core::{
    Action, ConfigChange, Engine, Env, Event, LogEntry, LogIndex, LogPayload, NodeId,
    RandomizedEnv, RoleState, Term,
};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

use crate::state_machine::StateMachine;
use crate::storage::{RecoveredState, Storage, StoredHardState, StoredSnapshot};
use crate::transport::Transport;

/// User-facing handle to a running Raft node. Cheap to clone — each
/// clone shares the same underlying driver task.
pub struct Node<S: StateMachine> {
    inputs: mpsc::Sender<DriverInput<S>>,
    background: Arc<Mutex<Option<BackgroundTasks>>>,
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

struct BackgroundTasks {
    ticker: JoinHandle<()>,
    driver: JoinHandle<()>,
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
}

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
        }
    }
}

/// The Raft role — Follower, Candidate, or Leader.
///
/// Exposed via [`Node::status`]. Deliberately a flat enum (no
/// per-role payload) to keep the observability surface decoupled from
/// `jotun_core`'s internal [`RoleState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Follower => f.write_str("follower"),
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
    /// Our own node id, copied from [`Config::node_id`].
    pub node_id: NodeId,
    /// Current role.
    pub role: Role,
    /// Current Raft term (§5.1).
    pub current_term: Term,
    /// Highest log index known to be committed cluster-wide.
    pub commit_index: LogIndex,
    /// Highest log index the driver has handed to the state machine.
    /// Always `<= commit_index`.
    pub last_applied: LogIndex,
    /// If we're a follower who has observed a current-term leader,
    /// its id; `None` otherwise (candidate, leader, or a follower
    /// that hasn't heard from anyone this term yet).
    pub leader_hint: Option<NodeId>,
    /// Active peer set (excluding self). Mutates as membership
    /// changes land; see `Node::add_peer` / `remove_peer`.
    pub peers: Vec<NodeId>,
}

/// Errors `propose` / `add_peer` / `remove_peer` can return.
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
    Fatal {
        reason: &'static str,
    },
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

/// Errors from [`Node::start`].
#[derive(Debug)]
pub enum NodeStartError<E> {
    Storage(E),
}

impl<E: std::fmt::Display> std::fmt::Display for NodeStartError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(e) => write!(f, "storage recover failed: {e}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for NodeStartError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(e) => Some(e),
        }
    }
}

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
            Bootstrap::NewCluster { .. } => config.peers.clone(),
            Bootstrap::Join => Vec::new(),
            Bootstrap::Recover => config.peers.clone(),
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
        let engine: Engine<Vec<u8>> = Engine::new(
            config.node_id,
            initial_peers.iter().copied(),
            env,
            config.heartbeat_interval_ticks,
        );

        let max_pending_proposals = config.max_pending_proposals;

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
            state_machine,
            storage,
            transport,
            inputs: inputs_rx,
            pending_proposals: HashMap::new(),
            pending_config_changes: HashMap::new(),
            max_pending_proposals,
            last_applied: LogIndex::ZERO,
        };
        let driver: JoinHandle<()> = tokio::spawn(driver_loop(driver_state, recovered));

        Ok(Self {
            inputs: inputs_tx,
            background: Arc::new(Mutex::new(Some(BackgroundTasks { ticker, driver }))),
        })
    }

    /// Submit a command to the cluster. Returns once the command has
    /// applied locally — meaning it committed via majority replication
    /// AND the engine handed it back to the state machine — and
    /// returns whatever the state machine produced.
    pub async fn propose(&self, command: S::Command) -> Result<S::Response, ProposeError> {
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

    /// Initiate a graceful shutdown. Returns once the driver has
    /// drained any in-flight work and all background tasks have
    /// exited.
    pub async fn shutdown(self) -> Result<(), ProposeError> {
        let (tx, rx) = oneshot::channel();
        let shutdown_requested = self
            .inputs
            .send(DriverInput::Shutdown { reply: tx })
            .await
            .is_ok();

        let background = self.background.lock().unwrap().take();

        if shutdown_requested {
            match rx.await {
                Ok(()) => {}
                Err(_) => return Err(ProposeError::DriverDead),
            }
        }

        let Some(background) = background else {
            return Ok(());
        };
        let BackgroundTasks { ticker, driver } = background;

        ticker.abort();
        match ticker.await {
            Ok(()) => {}
            Err(e) if e.is_cancelled() => {}
            Err(_) => return Err(ProposeError::DriverDead),
        }
        match driver.await {
            Ok(()) => Ok(()),
            Err(_) => Err(ProposeError::DriverDead),
        }
    }
}

// ---------------------------------------------------------------------------
// Driver (private)
// ---------------------------------------------------------------------------

struct Driver<S: StateMachine, St, Tr> {
    node_id: NodeId,
    engine: Engine<Vec<u8>>,
    state_machine: S,
    storage: St,
    transport: Tr,
    inputs: mpsc::Receiver<DriverInput<S>>,
    /// Index in the log → user oneshot to fire when the entry applies.
    pending_proposals: HashMap<LogIndex, oneshot::Sender<Result<S::Response, ProposeError>>>,
    /// Index in the log → user oneshot to fire when a `ConfigChange`
    /// entry applies (commits).
    pending_config_changes: HashMap<LogIndex, oneshot::Sender<Result<(), ProposeError>>>,
    /// Combined cap on in-flight proposals + config changes. Keeps the
    /// reply maps from growing unboundedly under load.
    max_pending_proposals: usize,
    /// Highest log index the driver has fed into the state machine.
    /// Mirrors the engine's own `last_applied` but the engine doesn't
    /// expose it publicly, so we track it from the Apply actions we
    /// dispatch.
    last_applied: LogIndex,
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
    Status {
        reply: oneshot::Sender<NodeStatus>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
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
    hydrate_engine(&mut d, recovered);

    loop {
        let result: Result<(), Fatal> = tokio::select! {
            input = d.inputs.recv() => {
                let Some(input) = input else { return };
                match input {
                    DriverInput::Tick => step_and_dispatch(&mut d, Event::Tick).await,
                    DriverInput::Propose { command, reply } => {
                        handle_propose(&mut d, command, reply).await
                    }
                    DriverInput::ConfigChange { change, reply } => {
                        handle_config_change(&mut d, change, reply).await
                    }
                    DriverInput::Status { reply } => {
                        let _ = reply.send(build_status(&d));
                        Ok(())
                    }
                    DriverInput::Shutdown { reply } => {
                        debug!(target = "jotun::node", "shutdown requested");
                        let _ = reply.send(());
                        return;
                    }
                }
            }
            incoming = d.transport.recv() => {
                let Some(incoming) = incoming else {
                    warn!(target = "jotun::node", "transport recv returned None; shutting down");
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

fn hydrate_engine<S, St, Tr>(d: &mut Driver<S, St, Tr>, recovered: RecoveredState<Vec<u8>>)
where
    S: StateMachine,
    St: Storage<Vec<u8>>,
    Tr: Transport<Vec<u8>>,
{
    use jotun_core::{RecoveredHardState, RecoveredSnapshot};

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
            peers: s.peers,
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
        d.state_machine.restore(bytes);
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
    // Encode the command, then drive ClientProposal. The next
    // PersistLogEntries action (if any) tells us the assigned index;
    // we register the reply oneshot at that index.
    let bytes = S::encode_command(&command);
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

async fn dispatch_actions<S, St, Tr>(
    d: &mut Driver<S, St, Tr>,
    actions: Vec<Action<Vec<u8>>>,
) -> Result<(), Fatal>
where
    S: StateMachine,
    St: Storage<Vec<u8>>,
    Tr: Transport<Vec<u8>>,
{
    for action in actions {
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
                    error!(target = "jotun::node", error = %e, "fatal: persist_hard_state failed");
                    return Err(Fatal {
                        reason: "persist_hard_state failed",
                    });
                }
            }
            Action::PersistLogEntries(entries) => {
                if let Err(e) = d.storage.append_log(entries).await {
                    error!(target = "jotun::node", error = %e, "fatal: append_log failed");
                    return Err(Fatal {
                        reason: "append_log failed",
                    });
                }
            }
            Action::PersistSnapshot {
                last_included_index,
                last_included_term,
                peers,
                bytes,
            } => {
                if let Err(e) = d
                    .storage
                    .persist_snapshot(StoredSnapshot {
                        last_included_index,
                        last_included_term,
                        peers,
                        bytes,
                    })
                    .await
                {
                    error!(target = "jotun::node", error = %e, "fatal: persist_snapshot failed");
                    return Err(Fatal {
                        reason: "persist_snapshot failed",
                    });
                }
            }
            Action::Send { to, message } => {
                if let Err(e) = d.transport.send(to, message).await {
                    debug!(target = "jotun::node", peer = %to, error = %e, "send failed");
                    // Non-fatal: engine retries on heartbeat.
                }
            }
            Action::Apply(entries) => {
                apply_entries(d, entries)?;
            }
            Action::ApplySnapshot { bytes } => {
                d.state_machine.restore(bytes);
            }
            Action::Redirect { .. } => {
                // Already handled at the propose call site; engine
                // shouldn't emit Redirect from any other path.
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
        RoleState::Candidate(_) => (Role::Candidate, None),
        RoleState::Leader(_) => (Role::Leader, None),
    };
    NodeStatus {
        node_id: d.node_id,
        role,
        current_term: d.engine.current_term(),
        commit_index: d.engine.commit_index(),
        last_applied: d.last_applied,
        leader_hint,
        peers: d.engine.peers().iter().copied().collect(),
    }
}

fn apply_entries<S, St, Tr>(
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
                    error!(target = "jotun::node", error = %e, index = ?entry.id.index,
                           "fatal: failed to decode committed command");
                    // Corrupt log — a committed entry must decode.
                    // Propagate so driver_loop fails pending waiters
                    // and exits.
                    return Err(Fatal {
                        reason: "failed to decode committed command",
                    });
                }
            };
            let response = d.state_machine.apply(cmd);
            if let Some(reply) = d.pending_proposals.remove(&entry.id.index) {
                let _ = reply.send(Ok(response));
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
}
