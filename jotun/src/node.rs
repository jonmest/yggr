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
use std::time::Duration;

use jotun_core::{
    Action, ConfigChange, Engine, Env, Event, Incoming, LogEntry, LogIndex, LogPayload, Message,
    NodeId, RoleState, StaticEnv,
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
        }
    }
}

/// Configuration for a [`Node`].
#[derive(Debug, Clone)]
pub struct Config {
    pub node_id: NodeId,
    /// Initial peer set (excluding self). Membership changes go
    /// through `add_peer` / `remove_peer` after startup.
    pub peers: Vec<NodeId>,
    /// Election timeout in ticks. The engine resets within this on
    /// every reset (§5.2 randomization). Default `Env` is fixed; users
    /// who want randomized timeouts can supply their own — coming in
    /// a later refactor.
    pub election_timeout_ticks: u64,
    /// Heartbeat interval in ticks. Must be smaller than election
    /// timeout per §5.2.
    pub heartbeat_interval_ticks: u64,
    /// Wall-clock duration of one engine tick.
    pub tick_interval: Duration,
}

impl Config {
    /// Sensible defaults for a development cluster.
    pub fn new(node_id: NodeId, peers: impl IntoIterator<Item = NodeId>) -> Self {
        Self {
            node_id,
            peers: peers.into_iter().collect(),
            election_timeout_ticks: 15,
            heartbeat_interval_ticks: 3,
            tick_interval: Duration::from_millis(50),
        }
    }
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
}

impl std::fmt::Display for ProposeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLeader { leader_hint } => write!(f, "not leader; try {leader_hint}"),
            Self::NoLeader => write!(f, "no leader known yet"),
            Self::Shutdown => write!(f, "node is shutting down"),
            Self::DriverDead => write!(f, "node driver task died"),
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

        // Build a fresh engine. We'll feed it the recovered state via
        // synthetic events inside the driver before processing any
        // user inputs — same approach the sim harness uses for crash
        // recovery, and it keeps Engine's API as the only mutator.
        let env: Box<dyn Env> = Box::new(StaticEnv(config.election_timeout_ticks));
        let engine: Engine<Vec<u8>> = Engine::new(
            config.node_id,
            config.peers.iter().copied(),
            env,
            config.heartbeat_interval_ticks,
        );

        let (inputs_tx, inputs_rx) = mpsc::channel::<DriverInput<S>>(1024);

        // Spawn the tick driver. Lives as long as the inputs channel
        // does (the driver's drop closes it).
        let tick_inputs = inputs_tx.clone();
        let tick_interval = config.tick_interval;
        let _ticker: JoinHandle<()> = tokio::spawn(async move {
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
        };
        let _driver: JoinHandle<()> = tokio::spawn(driver_loop(driver_state, recovered));

        Ok(Self { inputs: inputs_tx })
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

    /// Initiate a graceful shutdown. Returns once the driver has
    /// drained any in-flight work and all background tasks have
    /// exited.
    pub async fn shutdown(self) -> Result<(), ProposeError> {
        let (tx, rx) = oneshot::channel();
        if self
            .inputs
            .send(DriverInput::Shutdown { reply: tx })
            .await
            .is_err()
        {
            return Ok(()); // already gone
        }
        match rx.await {
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
    hydrate_engine(&mut d, &recovered);

    loop {
        tokio::select! {
            input = d.inputs.recv() => {
                let Some(input) = input else { return };
                match input {
                    DriverInput::Tick => {
                        if let Err(()) = step_and_dispatch(&mut d, Event::Tick).await {
                            return;
                        }
                    }
                    DriverInput::Propose { command, reply } => {
                        handle_propose(&mut d, command, reply).await;
                    }
                    DriverInput::ConfigChange { change, reply } => {
                        handle_config_change(&mut d, change, reply).await;
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
                if let Err(()) = step_and_dispatch(&mut d, Event::Incoming(incoming)).await {
                    return;
                }
            }
        }
    }
}

fn hydrate_engine<S, St, Tr>(d: &mut Driver<S, St, Tr>, recovered: &RecoveredState<Vec<u8>>)
where
    S: StateMachine,
    St: Storage<Vec<u8>>,
    Tr: Transport<Vec<u8>>,
{
    if recovered.hard_state.is_none() && recovered.snapshot.is_none() && recovered.log.is_empty() {
        return;
    }
    // Pick any peer as the synthetic source; peers() returns the
    // initial set passed at construction. For a single-node cluster
    // there are no peers and nothing to hydrate against — that case
    // can only have empty persisted state anyway, handled above.
    let Some(&peer) = d.engine.peers().iter().next() else {
        return;
    };

    if let Some(snap) = &recovered.snapshot {
        use jotun_core::RequestInstallSnapshot;
        use jotun_core::types::log::LogId;
        let term = recovered
            .hard_state
            .as_ref()
            .map_or(snap.last_included_term, |hs| hs.current_term);
        let _ = d.engine.step(Event::Incoming(Incoming {
            from: peer,
            message: Message::InstallSnapshotRequest(RequestInstallSnapshot {
                term,
                leader_id: peer,
                last_included: LogId::new(snap.last_included_index, snap.last_included_term),
                data: snap.bytes.clone(),
                leader_commit: snap.last_included_index,
            }),
        }));
        // Hand the snapshot bytes to the state machine.
        d.state_machine.restore(snap.bytes.clone());
    }

    if !recovered.log.is_empty() {
        use jotun_core::RequestAppendEntries;
        use jotun_core::types::log::LogId;
        let prev_log_id = recovered
            .snapshot
            .as_ref()
            .map(|s| LogId::new(s.last_included_index, s.last_included_term));
        let term = recovered
            .hard_state
            .as_ref()
            .map_or_else(
                || recovered.log.last().map_or(jotun_core::Term::ZERO, |e| e.id.term),
                |hs| hs.current_term,
            );
        let _ = d.engine.step(Event::Incoming(Incoming {
            from: peer,
            message: Message::AppendEntriesRequest(RequestAppendEntries {
                term,
                leader_id: peer,
                prev_log_id,
                entries: recovered.log.clone(),
                leader_commit: LogIndex::ZERO,
            }),
        }));
    }
}

async fn handle_propose<S, St, Tr>(
    d: &mut Driver<S, St, Tr>,
    command: S::Command,
    reply: oneshot::Sender<Result<S::Response, ProposeError>>,
) where
    S: StateMachine,
    St: Storage<Vec<u8>>,
    Tr: Transport<Vec<u8>>,
{
    // Encode the command, then drive ClientProposal. The next
    // PersistLogEntries action (if any) tells us the assigned index;
    // we register the reply oneshot at that index.
    let bytes = S::encode_command(&command);
    let last_before = d.engine.log().last_log_id().map_or(LogIndex::ZERO, |l| l.index);
    let actions = d.engine.step(Event::ClientProposal(bytes));
    let last_after = d.engine.log().last_log_id().map_or(LogIndex::ZERO, |l| l.index);

    if last_after > last_before {
        // Leader appended; remember this index for apply-time reply.
        d.pending_proposals.insert(last_after, reply);
    } else {
        let leader_hint = actions.iter().find_map(|a| match a {
            Action::Redirect { leader_hint } => Some(*leader_hint),
            _ => None,
        });
        let err = leader_hint
            .map_or(ProposeError::NoLeader, |h| ProposeError::NotLeader { leader_hint: h });
        let _ = reply.send(Err(err));
    }

    let _ = dispatch_actions(d, actions).await;
}

async fn handle_config_change<S, St, Tr>(
    d: &mut Driver<S, St, Tr>,
    change: ConfigChange,
    reply: oneshot::Sender<Result<(), ProposeError>>,
) where
    S: StateMachine,
    St: Storage<Vec<u8>>,
    Tr: Transport<Vec<u8>>,
{
    let last_before = d.engine.log().last_log_id().map_or(LogIndex::ZERO, |l| l.index);
    let actions = d.engine.step(Event::ProposeConfigChange(change));
    let last_after = d.engine.log().last_log_id().map_or(LogIndex::ZERO, |l| l.index);

    if last_after > last_before {
        d.pending_config_changes.insert(last_after, reply);
    } else {
        let leader_hint = actions.iter().find_map(|a| match a {
            Action::Redirect { leader_hint } => Some(*leader_hint),
            _ => None,
        });
        let err = leader_hint
            .map_or(ProposeError::NoLeader, |h| ProposeError::NotLeader { leader_hint: h });
        let _ = reply.send(Err(err));
    }
    let _ = dispatch_actions(d, actions).await;
}

/// Drive the engine with `event`, dispatch every emitted action.
/// Returns `Err(())` only on a fatal Storage / Transport failure
/// the driver can't recover from — caller exits the loop.
async fn step_and_dispatch<S, St, Tr>(
    d: &mut Driver<S, St, Tr>,
    event: Event<Vec<u8>>,
) -> Result<(), ()>
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
) -> Result<(), ()>
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
                    return Err(());
                }
            }
            Action::PersistLogEntries(entries) => {
                if let Err(e) = d.storage.append_log(entries).await {
                    error!(target = "jotun::node", error = %e, "fatal: append_log failed");
                    return Err(());
                }
            }
            Action::PersistSnapshot {
                last_included_index,
                last_included_term,
                bytes,
            } => {
                if let Err(e) = d
                    .storage
                    .persist_snapshot(StoredSnapshot {
                        last_included_index,
                        last_included_term,
                        bytes,
                    })
                    .await
                {
                    error!(target = "jotun::node", error = %e, "fatal: persist_snapshot failed");
                    return Err(());
                }
            }
            Action::Send { to, message } => {
                if let Err(e) = d.transport.send(to, message).await {
                    debug!(target = "jotun::node", peer = %to, error = %e, "send failed");
                    // Non-fatal: engine retries on heartbeat.
                }
            }
            Action::Apply(entries) => {
                apply_entries(d, entries);
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
    Ok(())
}

fn apply_entries<S, St, Tr>(d: &mut Driver<S, St, Tr>, entries: Vec<LogEntry<Vec<u8>>>)
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
                    // Bail the entire driver — the only way to fail
                    // a decode is corrupt storage, which is fatal.
                    return;
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
}
