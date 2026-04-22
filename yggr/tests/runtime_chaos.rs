//! Full-stack runtime chaos tests.
//!
//! The sim in `yggr-sim` drives the pure engine under adversarial
//! schedules. These tests do the equivalent but drive real [`Node`]
//! instances — complete with driver task, apply task, and storage —
//! connected through an in-process chaos transport that can drop,
//! delay, reorder, and partition messages under a deterministic seed.
//!
//! We assert:
//!  - Election Safety (§5.2): at most one leader per term across the
//!    lifetime of the run.
//!  - Committed-prefix consistency: any two nodes that have committed
//!    the same index persist the same entry at that index.
//!  - Leader Completeness (§5.4): every later leader still contains
//!    every entry we have previously observed as committed.
//!  - State-machine consistency: if any two nodes apply a committed
//!    index, they apply the same command at that index.
//!  - Liveness under bounded faults: with only transient drops/delays
//!    (no permanent partition, no permanent crash), a leader emerges
//!    and proposals commit.

#![allow(
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    clippy::missing_const_for_fn,
    clippy::unwrap_used,
    clippy::expect_used,
    deprecated
)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use yggr::{
    Config, DecodeError, Node, NodeId, NodeStatus, ProposeError, Role, StateMachine, Storage,
    StoredHardState, StoredSnapshot, Transport,
};
use yggr_core::{Incoming, LogEntry, LogIndex, Message, Term};

// ---------------------------------------------------------------------------
// Deterministic RNG (xorshift64). The proptests feed us a seed, and we
// must make every chaos decision from it so shrinking works.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ChaosRng {
    state: Arc<Mutex<u64>>,
}

impl ChaosRng {
    fn new(seed: u64) -> Self {
        let s = if seed == 0 {
            0xDEAD_BEEF_CAFE_F00D
        } else {
            seed
        };
        Self {
            state: Arc::new(Mutex::new(s)),
        }
    }
    fn next_u64(&self) -> u64 {
        let mut g = self.state.lock().unwrap();
        let mut x = *g;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *g = x;
        x
    }
    fn bernoulli(&self, numerator: u32, denominator: u32) -> bool {
        (self.next_u64() as u32)
            .checked_rem(denominator)
            .unwrap_or(0)
            < numerator
    }
}

// ---------------------------------------------------------------------------
// Network: a central switch for every message sent by any ChaosTransport.
// It applies policy (drop / delay / partition) and posts to the target
// node's inbound channel.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct Policy {
    /// P(drop) * 100. 0 means never, 100 means always.
    drop_percent: u32,
    /// Delay a delivered message by this many ms.
    max_delay_ms: u64,
}

impl Policy {
    fn happy() -> Self {
        Self {
            drop_percent: 0,
            max_delay_ms: 0,
        }
    }
    fn lossy(drop_percent: u32) -> Self {
        Self {
            drop_percent,
            max_delay_ms: 0,
        }
    }
    fn chaotic(drop_percent: u32, max_delay_ms: u64) -> Self {
        Self {
            drop_percent,
            max_delay_ms,
        }
    }
}

type InboundTable = HashMap<NodeId, mpsc::Sender<Incoming<Vec<u8>>>>;

#[derive(Debug, Clone)]
struct Network {
    inbound: Arc<Mutex<InboundTable>>,
    /// `(a, b)` means a and b cannot talk. Partitions are symmetric.
    partitioned: Arc<Mutex<HashSet<(NodeId, NodeId)>>>,
    policy: Arc<Mutex<Policy>>,
    rng: ChaosRng,
}

impl Network {
    fn new(seed: u64, policy: Policy) -> Self {
        Self {
            inbound: Arc::default(),
            partitioned: Arc::default(),
            policy: Arc::new(Mutex::new(policy)),
            rng: ChaosRng::new(seed),
        }
    }

    fn register(&self, node: NodeId, inbound: mpsc::Sender<Incoming<Vec<u8>>>) {
        self.inbound.lock().unwrap().insert(node, inbound);
    }

    fn partition(&self, a: NodeId, b: NodeId) {
        let mut p = self.partitioned.lock().unwrap();
        p.insert((a, b));
        p.insert((b, a));
    }

    fn heal(&self) {
        self.partitioned.lock().unwrap().clear();
    }

    fn is_partitioned(&self, from: NodeId, to: NodeId) -> bool {
        self.partitioned.lock().unwrap().contains(&(from, to))
    }

    async fn deliver(&self, from: NodeId, to: NodeId, msg: Message<Vec<u8>>) {
        if self.is_partitioned(from, to) {
            return;
        }
        let (drop_p, max_delay) = {
            let p = self.policy.lock().unwrap();
            (p.drop_percent, p.max_delay_ms)
        };
        if drop_p > 0 && self.rng.bernoulli(drop_p, 100) {
            return;
        }
        if max_delay > 0 {
            let delay = self.rng.next_u64() % max_delay;
            tokio::time::sleep(Duration::from_millis(delay)).await;
            // After the delay, re-check partition — it may have
            // changed. (Heal mid-flight still delivers; partition
            // mid-flight drops.)
            if self.is_partitioned(from, to) {
                return;
            }
        }
        let sender = self.inbound.lock().unwrap().get(&to).cloned();
        if let Some(s) = sender {
            let _ = s.send(Incoming { from, message: msg }).await;
        }
    }
}

// ---------------------------------------------------------------------------
// ChaosTransport: one per node. Sends go through the shared Network.
// ---------------------------------------------------------------------------

struct ChaosTransport {
    me: NodeId,
    network: Network,
    inbound_rx: mpsc::Receiver<Incoming<Vec<u8>>>,
}

impl ChaosTransport {
    fn start(me: NodeId, network: Network) -> Self {
        let (tx, rx) = mpsc::channel::<Incoming<Vec<u8>>>(1024);
        network.register(me, tx);
        Self {
            me,
            network,
            inbound_rx: rx,
        }
    }
}

impl Transport<Vec<u8>> for ChaosTransport {
    type Error = Infallible;

    async fn send(&self, to: NodeId, message: Message<Vec<u8>>) -> Result<(), Self::Error> {
        // Deliver runs in the current task; for a true "spawn and
        // return" you'd detach, but since deliver itself awaits
        // (for the delay path), awaiting inline is fine in tests.
        let network = self.network.clone();
        let from = self.me;
        tokio::spawn(async move {
            network.deliver(from, to, message).await;
        });
        Ok(())
    }

    async fn recv(&mut self) -> Option<Incoming<Vec<u8>>> {
        self.inbound_rx.recv().await
    }
}

// ---------------------------------------------------------------------------
// State machine + storage: a simple counter with shared observed-applies
// tracker for consistency checks.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CountCmd(u64);

/// Per-node state machine. Records every applied `(index, cmd)` into
/// a shared observation log so the harness can assert that no two
/// nodes applied different commands at the same position in the
/// committed command stream.
#[derive(Debug)]
struct TrackedCounter {
    node_id: NodeId,
    value: u64,
    applied_commands: Arc<AtomicU64>,
    observed: Arc<Mutex<Vec<AppliedObservation>>>,
}

#[derive(Debug, Clone, Copy)]
struct AppliedObservation {
    node: NodeId,
    command_ordinal: u64,
    cmd: CountCmd,
}

impl StateMachine for TrackedCounter {
    type Command = CountCmd;
    type Response = u64;

    fn encode_command(c: &CountCmd) -> Vec<u8> {
        c.0.to_le_bytes().to_vec()
    }
    fn decode_command(bytes: &[u8]) -> Result<CountCmd, DecodeError> {
        let arr: [u8; 8] = bytes
            .try_into()
            .map_err(|_| DecodeError::new("CountCmd needs 8 bytes"))?;
        Ok(CountCmd(u64::from_le_bytes(arr)))
    }
    fn apply(&mut self, cmd: CountCmd) -> u64 {
        self.value = self.value.wrapping_add(cmd.0);
        let command_ordinal = self.applied_commands.fetch_add(1, Ordering::Relaxed) + 1;
        self.observed.lock().unwrap().push(AppliedObservation {
            node: self.node_id,
            command_ordinal,
            cmd,
        });
        self.value
    }
}

#[derive(Debug, Default, Clone)]
struct MemoryStorage {
    inner: Arc<Mutex<MemoryStorageInner>>,
}

#[derive(Debug, Default, Clone)]
struct MemoryStorageInner {
    hard_state: Option<StoredHardState>,
    snapshot: Option<StoredSnapshot>,
    log: Vec<LogEntry<Vec<u8>>>,
}

impl MemoryStorageInner {
    fn entry_at(&self, index: LogIndex) -> Option<&LogEntry<Vec<u8>>> {
        let snapshot_floor = self
            .snapshot
            .as_ref()
            .map_or(LogIndex::ZERO, |snapshot| snapshot.last_included_index);
        let offset = index.get().checked_sub(snapshot_floor.get() + 1)?;
        self.log.get(usize::try_from(offset).ok()?)
    }
}

impl Storage<Vec<u8>> for MemoryStorage {
    type Error = Infallible;

    async fn recover(&mut self) -> Result<yggr::storage::RecoveredState<Vec<u8>>, Self::Error> {
        let g = self.inner.lock().unwrap();
        Ok(yggr::storage::RecoveredState {
            hard_state: g.hard_state.clone(),
            snapshot: g.snapshot.clone(),
            log: g.log.clone(),
        })
    }
    async fn persist_hard_state(&mut self, s: StoredHardState) -> Result<(), Self::Error> {
        self.inner.lock().unwrap().hard_state = Some(s);
        Ok(())
    }
    async fn append_log(&mut self, entries: Vec<LogEntry<Vec<u8>>>) -> Result<(), Self::Error> {
        let mut g = self.inner.lock().unwrap();
        let snap_floor = g
            .snapshot
            .as_ref()
            .map_or(0, |s| s.last_included_index.get());
        let mut truncated = false;
        for entry in entries {
            let i = entry.id.index.get();
            if i <= snap_floor {
                continue;
            }
            let Ok(local) = usize::try_from(i - snap_floor - 1) else {
                continue;
            };
            if !truncated {
                let keep = local.min(g.log.len());
                g.log.truncate(keep);
                truncated = true;
            }
            g.log.push(entry);
        }
        Ok(())
    }
    async fn truncate_log(&mut self, from: LogIndex) -> Result<(), Self::Error> {
        let mut g = self.inner.lock().unwrap();
        let snap_floor = g
            .snapshot
            .as_ref()
            .map_or(0, |s| s.last_included_index.get());
        let Ok(local) = usize::try_from(from.get().saturating_sub(snap_floor + 1)) else {
            return Ok(());
        };
        if local < g.log.len() {
            g.log.truncate(local);
        }
        Ok(())
    }
    async fn persist_snapshot(&mut self, snap: StoredSnapshot) -> Result<(), Self::Error> {
        let mut g = self.inner.lock().unwrap();
        let Ok(drop_through) = usize::try_from(snap.last_included_index.get()) else {
            g.log.clear();
            g.snapshot = Some(snap);
            return Ok(());
        };
        let keep_from = drop_through.min(g.log.len());
        g.log.drain(..keep_from);
        g.snapshot = Some(snap);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Cluster: owns N nodes, the network, and the observation tracker.
// ---------------------------------------------------------------------------

struct ClusterNode {
    id: NodeId,
    node: Node<TrackedCounter>,
    storage: MemoryStorage,
    applied_commands: Arc<AtomicU64>,
}

struct Cluster {
    nodes: Vec<ClusterNode>,
    network: Network,
    observed: Arc<Mutex<Vec<AppliedObservation>>>,
}

struct ClusterObservation {
    statuses: BTreeMap<NodeId, NodeStatus>,
    storages: BTreeMap<NodeId, MemoryStorageInner>,
}

impl Cluster {
    async fn new(size: u64, seed: u64, policy: Policy) -> Self {
        let network = Network::new(seed, policy);
        let observed: Arc<Mutex<Vec<AppliedObservation>>> = Arc::default();
        let mut nodes = Vec::with_capacity(size as usize);
        let all_ids: Vec<NodeId> = (1..=size).map(|i| NodeId::new(i).unwrap()).collect();

        for &id in &all_ids {
            let peers: Vec<NodeId> = all_ids.iter().copied().filter(|&p| p != id).collect();
            let mut config = Config::new(id, peers.clone());
            config.election_timeout_min_ticks = 5;
            config.election_timeout_max_ticks = 10;
            config.heartbeat_interval_ticks = 1;
            config.tick_interval = Duration::from_millis(10);
            config.max_pending_proposals = 256;
            // Keep the full durable log visible in runtime-chaos tests
            // so the harness can compare committed prefixes directly.
            config.snapshot_hint_threshold_entries = 0;

            let applied_commands = Arc::new(AtomicU64::new(0));
            let sm = TrackedCounter {
                node_id: id,
                value: 0,
                applied_commands: Arc::clone(&applied_commands),
                observed: Arc::clone(&observed),
            };
            let transport = ChaosTransport::start(id, network.clone());
            let storage = MemoryStorage::default();

            let node = Node::start(config, sm, storage.clone(), transport)
                .await
                .unwrap();
            nodes.push(ClusterNode {
                id,
                node,
                storage,
                applied_commands,
            });
        }

        Self {
            nodes,
            network,
            observed,
        }
    }

    fn nodes(&self) -> &[ClusterNode] {
        &self.nodes
    }

    async fn observe(&self) -> ClusterObservation {
        let mut statuses = BTreeMap::new();
        let mut storages = BTreeMap::new();
        for node in &self.nodes {
            statuses.insert(
                node.id,
                node.node
                    .status()
                    .await
                    .expect("runtime-chaos node status must stay readable"),
            );
            storages.insert(node.id, node.storage.inner.lock().unwrap().clone());
        }
        ClusterObservation { statuses, storages }
    }

    /// Leader from any node's view. Returns Some(id) if every node
    /// that thinks there's a leader agrees, else None.
    async fn unique_leader(&self) -> Option<NodeId> {
        let mut leaders: HashSet<NodeId> = HashSet::new();
        for n in &self.nodes {
            if let Ok(s) = n.node.status().await
                && s.role == Role::Leader
            {
                leaders.insert(s.node_id);
            }
        }
        if leaders.len() == 1 {
            leaders.into_iter().next()
        } else {
            None
        }
    }

    async fn wait_for_leader(&self, deadline: Duration) -> Option<NodeId> {
        let start = std::time::Instant::now();
        while start.elapsed() < deadline {
            if let Some(id) = self.unique_leader().await {
                return Some(id);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        None
    }

    async fn propose_on(&self, leader: NodeId, cmd: CountCmd) -> Result<u64, ProposeError> {
        let n = self
            .nodes
            .iter()
            .find(|n| n.id == leader)
            .expect("leader id in cluster");
        n.node.propose(cmd).await
    }

    async fn shutdown(self) {
        for n in self.nodes {
            let _ = n.node.shutdown().await;
        }
    }

    async fn wait_for_all_nodes_to_apply_commands(&self, expected: u64, deadline: Duration) {
        let start = std::time::Instant::now();
        loop {
            let observation = self.observe().await;
            self.check_committed_prefix_consistency(&observation);
            self.check_apply_consistency();
            if self
                .nodes
                .iter()
                .all(|node| node.applied_commands.load(Ordering::Relaxed) >= expected)
            {
                return;
            }
            assert!(
                start.elapsed() < deadline,
                "not every node applied {expected} commands within {deadline:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Assert Raft Election Safety: at most one node ever claims the
    /// leader role for any given term. Reads the set of terms each
    /// node has seen via `status()` and tracks which have been claimed.
    fn check_election_safety(
        &self,
        observation: &ClusterObservation,
        leaders_by_term: &mut BTreeMap<Term, NodeId>,
    ) {
        let _ = self;
        for status in observation.statuses.values() {
            if status.role == Role::Leader {
                match leaders_by_term.get(&status.current_term) {
                    None => {
                        leaders_by_term.insert(status.current_term, status.node_id);
                    }
                    Some(&prior) => {
                        assert_eq!(
                            prior, status.node_id,
                            "Election Safety violated: term {:?} has leaders {} AND {}",
                            status.current_term, prior, status.node_id,
                        );
                    }
                }
            }
        }
    }

    fn check_status_monotonicity(
        &self,
        observation: &ClusterObservation,
        prior_statuses: &mut BTreeMap<NodeId, NodeStatus>,
    ) {
        let _ = self;
        for (node_id, status) in &observation.statuses {
            assert!(
                status.last_applied <= status.commit_index,
                "node {node_id} last_applied {:?} exceeded commit_index {:?}",
                status.last_applied,
                status.commit_index,
            );
            if let Some(previous) = prior_statuses.insert(*node_id, status.clone()) {
                assert!(
                    status.current_term >= previous.current_term,
                    "node {node_id} term regressed: {:?} -> {:?}",
                    previous.current_term,
                    status.current_term,
                );
                assert!(
                    status.commit_index >= previous.commit_index,
                    "node {node_id} commit index regressed: {:?} -> {:?}",
                    previous.commit_index,
                    status.commit_index,
                );
                assert!(
                    status.last_applied >= previous.last_applied,
                    "node {node_id} last_applied regressed: {:?} -> {:?}",
                    previous.last_applied,
                    status.last_applied,
                );
            }
        }
    }

    fn check_committed_prefix_consistency(&self, observation: &ClusterObservation) {
        let _ = self;
        let node_ids: Vec<NodeId> = observation.statuses.keys().copied().collect();
        for (index, left_id) in node_ids.iter().enumerate() {
            for right_id in node_ids.iter().skip(index + 1) {
                let left_status = &observation.statuses[left_id];
                let right_status = &observation.statuses[right_id];
                let left_storage = &observation.storages[left_id];
                let right_storage = &observation.storages[right_id];
                let shared_commit =
                    std::cmp::min(left_status.commit_index, right_status.commit_index);
                for raw_index in 1..=shared_commit.get() {
                    let log_index = LogIndex::new(raw_index);
                    let left_entry = left_storage.entry_at(log_index).unwrap_or_else(|| {
                        panic!(
                            "node {} missing committed entry {:?} at commit {:?}",
                            left_id, log_index, left_status.commit_index
                        )
                    });
                    let right_entry = right_storage.entry_at(log_index).unwrap_or_else(|| {
                        panic!(
                            "node {} missing committed entry {:?} at commit {:?}",
                            right_id, log_index, right_status.commit_index
                        )
                    });
                    assert_eq!(
                        left_entry, right_entry,
                        "Committed-prefix mismatch at {log_index:?} between node {left_id} and node {right_id}",
                    );
                }
            }
        }
    }

    fn extend_committed_prefix(
        &self,
        observation: &ClusterObservation,
        committed_prefix: &mut BTreeMap<LogIndex, LogEntry<Vec<u8>>>,
    ) {
        let _ = self;
        let max_recorded = committed_prefix
            .last_key_value()
            .map_or(LogIndex::ZERO, |(index, _)| *index);
        let source = observation
            .statuses
            .values()
            .max_by_key(|status| status.commit_index)
            .expect("cluster must have statuses");
        if source.commit_index <= max_recorded {
            return;
        }
        let storage = &observation.storages[&source.node_id];
        for raw_index in (max_recorded.get() + 1)..=source.commit_index.get() {
            let log_index = LogIndex::new(raw_index);
            let entry = storage.entry_at(log_index).unwrap_or_else(|| {
                panic!(
                    "node {} reports commit {:?} but has no persisted entry at {:?}",
                    source.node_id, source.commit_index, log_index
                )
            });
            if let Some(previous) = committed_prefix.insert(log_index, entry.clone()) {
                assert_eq!(
                    previous, *entry,
                    "Committed prefix changed at {log_index:?} after it was observed",
                );
            }
        }
    }

    fn check_leader_completeness(
        &self,
        observation: &ClusterObservation,
        committed_prefix: &BTreeMap<LogIndex, LogEntry<Vec<u8>>>,
    ) {
        let _ = self;
        for status in observation
            .statuses
            .values()
            .filter(|status| status.role == Role::Leader)
        {
            let storage = &observation.storages[&status.node_id];
            for (log_index, committed_entry) in committed_prefix {
                let leader_entry = storage.entry_at(*log_index).unwrap_or_else(|| {
                    panic!(
                        "Leader Completeness violated: leader {} at term {:?} lacks committed entry {:?}",
                        status.node_id, status.current_term, log_index,
                    )
                });
                assert_eq!(
                    leader_entry, committed_entry,
                    "Leader Completeness violated at {:?} for leader {} in term {:?}",
                    log_index, status.node_id, status.current_term,
                );
            }
        }
    }

    fn check_runtime_safety(
        &self,
        observation: &ClusterObservation,
        prior_statuses: &mut BTreeMap<NodeId, NodeStatus>,
        leaders_by_term: &mut BTreeMap<Term, NodeId>,
        committed_prefix: &mut BTreeMap<LogIndex, LogEntry<Vec<u8>>>,
    ) {
        self.check_status_monotonicity(observation, prior_statuses);
        self.check_election_safety(observation, leaders_by_term);
        self.check_committed_prefix_consistency(observation);
        self.extend_committed_prefix(observation, committed_prefix);
        self.check_leader_completeness(observation, committed_prefix);
    }

    /// Assert State-Machine Consistency: every observed apply at the
    /// same command position carries the same command across all nodes.
    fn check_apply_consistency(&self) {
        let obs = self.observed.lock().unwrap();
        let mut by_ordinal: BTreeMap<u64, CountCmd> = BTreeMap::new();
        for o in obs.iter() {
            match by_ordinal.get(&o.command_ordinal) {
                None => {
                    by_ordinal.insert(o.command_ordinal, o.cmd);
                }
                Some(&prior) => {
                    assert_eq!(
                        prior, o.cmd,
                        "State-Machine Consistency violated at command #{:?}: node {} applied {:?}, but earlier apply was {:?}",
                        o.command_ordinal, o.node, o.cmd, prior,
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Fixed-seed integration tests (fast) — these run in CI and pin
// regression behavior.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_cluster_elects_leader_and_commits() {
    let cluster = Cluster::new(3, 0xABCD_1234, Policy::happy()).await;
    let mut prior_statuses = BTreeMap::new();
    let mut leaders_by_term = BTreeMap::new();
    let mut committed_prefix = BTreeMap::new();

    let leader = cluster
        .wait_for_leader(Duration::from_secs(3))
        .await
        .expect("no leader elected in happy cluster");

    let v = cluster.propose_on(leader, CountCmd(5)).await.unwrap();
    assert_eq!(v, 5);
    let v = cluster.propose_on(leader, CountCmd(7)).await.unwrap();
    assert_eq!(v, 12);

    cluster
        .wait_for_all_nodes_to_apply_commands(2, Duration::from_secs(3))
        .await;
    let observation = cluster.observe().await;
    cluster.check_runtime_safety(
        &observation,
        &mut prior_statuses,
        &mut leaders_by_term,
        &mut committed_prefix,
    );
    cluster.check_apply_consistency();

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lossy_cluster_still_commits() {
    // 20% drop rate. Should still elect and commit, just takes longer.
    let cluster = Cluster::new(3, 0xDEAD_BEEF, Policy::lossy(20)).await;
    let mut prior_statuses = BTreeMap::new();
    let mut leaders_by_term = BTreeMap::new();
    let mut committed_prefix = BTreeMap::new();

    let leader = cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .expect("no leader elected under 20% drop");

    let mut succeeded = 0u32;
    for _ in 0..5 {
        if cluster.propose_on(leader, CountCmd(1)).await.is_ok() {
            succeeded += 1;
        }
    }
    assert!(succeeded >= 1, "no proposal committed under 20% drop");

    cluster
        .wait_for_all_nodes_to_apply_commands(u64::from(succeeded), Duration::from_secs(5))
        .await;
    let observation = cluster.observe().await;
    cluster.check_runtime_safety(
        &observation,
        &mut prior_statuses,
        &mut leaders_by_term,
        &mut committed_prefix,
    );
    cluster.check_apply_consistency();
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_heal_cluster_recovers_and_commits() {
    let cluster = Cluster::new(3, 0x1337_C0DE, Policy::happy()).await;
    let mut prior_statuses = BTreeMap::new();
    let mut leaders_by_term = BTreeMap::new();
    let mut committed_prefix = BTreeMap::new();
    let initial_leader = cluster
        .wait_for_leader(Duration::from_secs(3))
        .await
        .expect("no initial leader");

    // Partition the leader off from one follower. Majority still
    // possible (leader + the other follower).
    let other_ids: Vec<NodeId> = cluster
        .nodes()
        .iter()
        .map(|n| n.id)
        .filter(|&id| id != initial_leader)
        .collect();
    cluster.network.partition(initial_leader, other_ids[0]);

    // Commit through the remaining majority.
    let v = cluster.propose_on(initial_leader, CountCmd(1)).await;
    assert!(
        v.is_ok(),
        "proposal failed with one follower partitioned: {v:?}"
    );

    cluster.network.heal();
    cluster
        .wait_for_all_nodes_to_apply_commands(1, Duration::from_secs(3))
        .await;
    let observation = cluster.observe().await;
    cluster.check_runtime_safety(
        &observation,
        &mut prior_statuses,
        &mut leaders_by_term,
        &mut committed_prefix,
    );
    cluster.check_apply_consistency();
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// Proptest: hammer the runtime with random seeds under chaos. Must
// preserve Election Safety, committed-prefix consistency, Leader
// Completeness, and apply consistency across many seeds.
// ---------------------------------------------------------------------------

use proptest::prelude::*;

fn runtime_chaos_cases() -> u32 {
    match std::env::var("JOTUN_RUNTIME_CHAOS_CASES") {
        Ok(raw) => raw
            .parse::<u32>()
            .ok()
            .filter(|cases| *cases > 0)
            .expect("JOTUN_RUNTIME_CHAOS_CASES must be a positive integer"),
        Err(std::env::VarError::NotPresent) => 16,
        Err(e) => panic!("failed to read JOTUN_RUNTIME_CHAOS_CASES: {e}"),
    }
}

fn runtime_chaos_proptest_config() -> ProptestConfig {
    ProptestConfig {
        cases: runtime_chaos_cases(),
        ..ProptestConfig::default()
    }
}

proptest! {
    #![proptest_config(runtime_chaos_proptest_config())]

    /// Under moderate chaos (10% drop, some delay), safety must hold
    /// across arbitrary seeds. Liveness is not asserted — the
    /// scheduler is allowed to prevent progress.
    #[test]
    fn chaos_seeds_preserve_safety(seed in any::<u64>()) {
        // Proptest runs synchronously; drive a Tokio runtime per case.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let cluster = Cluster::new(3, seed, Policy::chaotic(10, 5)).await;
            let mut prior_statuses: BTreeMap<NodeId, NodeStatus> = BTreeMap::new();
            let mut leaders_by_term: BTreeMap<Term, NodeId> = BTreeMap::new();
            let mut committed_prefix: BTreeMap<LogIndex, LogEntry<Vec<u8>>> = BTreeMap::new();

            // Drive the cluster for ~1.5s, occasionally proposing.
            for step in 0..30u32 {
                let observation = cluster.observe().await;
                cluster.check_runtime_safety(
                    &observation,
                    &mut prior_statuses,
                    &mut leaders_by_term,
                    &mut committed_prefix,
                );
                if step % 5 == 0
                    && let Some(leader) = cluster.unique_leader().await
                {
                    let _ = cluster.propose_on(leader, CountCmd(1)).await;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let observation = cluster.observe().await;
            cluster.check_runtime_safety(
                &observation,
                &mut prior_statuses,
                &mut leaders_by_term,
                &mut committed_prefix,
            );
            cluster.check_apply_consistency();
            cluster.shutdown().await;
        });
    }
}
