//! Coverage for the newer runtime-facing `Node` surface: bootstrap
//! semantics, `status()`, and proposal/config-change backpressure.

#![allow(clippy::expect_used, clippy::missing_const_for_fn, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jotun::{
    Bootstrap, Config, ConfigError, DecodeError, Node, NodeId, NodeStartError, NodeStatus,
    ProposeError, ReadError, StateMachine, Storage, StoredHardState, StoredSnapshot,
    TransferLeadershipError, Transport,
};
use jotun_core::{
    Incoming, LogEntry, LogIndex, LogPayload, Message, Term, VoteResponse, VoteResult,
};
use tokio::sync::mpsc;

type SentMessages = Arc<Mutex<Vec<(NodeId, Message<Vec<u8>>)>>>;

#[derive(Debug, Default)]
struct Counter {
    value: u64,
}

#[derive(Debug, Clone)]
enum CountCmd {
    Inc(u64),
}

impl StateMachine for Counter {
    type Command = CountCmd;
    type Response = u64;

    fn encode_command(c: &CountCmd) -> Vec<u8> {
        match c {
            CountCmd::Inc(n) => n.to_le_bytes().to_vec(),
        }
    }

    fn decode_command(bytes: &[u8]) -> Result<CountCmd, DecodeError> {
        let arr: [u8; 8] = bytes
            .try_into()
            .map_err(|_| DecodeError::new("expected 8 bytes"))?;
        Ok(CountCmd::Inc(u64::from_le_bytes(arr)))
    }

    fn apply(&mut self, cmd: CountCmd) -> u64 {
        match cmd {
            CountCmd::Inc(n) => {
                self.value += n;
                self.value
            }
        }
    }

    fn restore(&mut self, _bytes: Vec<u8>) {}
}

#[derive(Debug)]
struct SnapshottingCounter {
    value: u64,
    snapshots_taken: Arc<AtomicUsize>,
}

impl SnapshottingCounter {
    fn new(snapshots_taken: Arc<AtomicUsize>) -> Self {
        Self {
            value: 0,
            snapshots_taken,
        }
    }
}

impl StateMachine for SnapshottingCounter {
    type Command = CountCmd;
    type Response = u64;

    fn encode_command(c: &CountCmd) -> Vec<u8> {
        Counter::encode_command(c)
    }

    fn decode_command(bytes: &[u8]) -> Result<CountCmd, DecodeError> {
        Counter::decode_command(bytes)
    }

    fn apply(&mut self, cmd: CountCmd) -> u64 {
        match cmd {
            CountCmd::Inc(n) => {
                self.value += n;
                self.value
            }
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        self.snapshots_taken.fetch_add(1, Ordering::Relaxed);
        self.value.to_le_bytes().to_vec()
    }

    fn restore(&mut self, bytes: Vec<u8>) {
        let arr: [u8; 8] = bytes.try_into().expect("snapshot bytes are well-formed");
        self.value = u64::from_le_bytes(arr);
    }
}

#[derive(Debug, Default)]
struct MemoryStorage<C> {
    hard_state: Option<StoredHardState>,
    snapshot: Option<StoredSnapshot>,
    log: Vec<LogEntry<C>>,
}

impl<C: Send + Clone + 'static> Storage<C> for MemoryStorage<C> {
    type Error = Infallible;

    async fn recover(&mut self) -> Result<jotun::storage::RecoveredState<C>, Self::Error> {
        Ok(jotun::storage::RecoveredState {
            hard_state: self.hard_state.clone(),
            snapshot: self.snapshot.clone(),
            log: self.log.clone(),
        })
    }

    async fn persist_hard_state(&mut self, state: StoredHardState) -> Result<(), Self::Error> {
        self.hard_state = Some(state);
        Ok(())
    }

    async fn append_log(&mut self, entries: Vec<LogEntry<C>>) -> Result<(), Self::Error> {
        let mut truncated = false;
        for entry in entries {
            let i = entry.id.index.get();
            let snap_floor = self
                .snapshot
                .as_ref()
                .map_or(0, |s| s.last_included_index.get());
            if i <= snap_floor {
                continue;
            }
            let Ok(local_idx) = usize::try_from(i - snap_floor - 1) else {
                continue;
            };
            if !truncated {
                let keep = local_idx.min(self.log.len());
                self.log.truncate(keep);
                truncated = true;
            }
            self.log.push(entry);
        }
        Ok(())
    }

    async fn truncate_log(&mut self, from: LogIndex) -> Result<(), Self::Error> {
        let snap_floor = self
            .snapshot
            .as_ref()
            .map_or(0, |s| s.last_included_index.get());
        let Ok(local) = usize::try_from(from.get().saturating_sub(snap_floor + 1)) else {
            return Ok(());
        };
        if local < self.log.len() {
            self.log.truncate(local);
        }
        Ok(())
    }

    async fn persist_snapshot(&mut self, snap: StoredSnapshot) -> Result<(), Self::Error> {
        let Ok(drop_through) = usize::try_from(snap.last_included_index.get()) else {
            self.log.clear();
            self.snapshot = Some(snap);
            return Ok(());
        };
        let keep_from = drop_through.min(self.log.len());
        self.log.drain(..keep_from);
        self.snapshot = Some(snap);
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
struct SharedMemoryStorage<C> {
    inner: Arc<Mutex<MemoryStorage<C>>>,
}

impl<C: Send + Clone + 'static> Storage<C> for SharedMemoryStorage<C> {
    type Error = Infallible;

    async fn recover(&mut self) -> Result<jotun::storage::RecoveredState<C>, Self::Error> {
        let guard = self.inner.lock().unwrap();
        Ok(jotun::storage::RecoveredState {
            hard_state: guard.hard_state.clone(),
            snapshot: guard.snapshot.clone(),
            log: guard.log.clone(),
        })
    }

    async fn persist_hard_state(&mut self, state: StoredHardState) -> Result<(), Self::Error> {
        self.inner.lock().unwrap().hard_state = Some(state);
        Ok(())
    }

    async fn append_log(&mut self, entries: Vec<LogEntry<C>>) -> Result<(), Self::Error> {
        let mut guard = self.inner.lock().unwrap();
        let mut truncated = false;
        for entry in entries {
            let i = entry.id.index.get();
            let snap_floor = guard
                .snapshot
                .as_ref()
                .map_or(0, |s| s.last_included_index.get());
            if i <= snap_floor {
                continue;
            }
            let Ok(local_idx) = usize::try_from(i - snap_floor - 1) else {
                continue;
            };
            if !truncated {
                let keep = local_idx.min(guard.log.len());
                guard.log.truncate(keep);
                truncated = true;
            }
            guard.log.push(entry);
        }
        Ok(())
    }

    async fn truncate_log(&mut self, from: LogIndex) -> Result<(), Self::Error> {
        let mut guard = self.inner.lock().unwrap();
        let snap_floor = guard
            .snapshot
            .as_ref()
            .map_or(0, |s| s.last_included_index.get());
        let Ok(local) = usize::try_from(from.get().saturating_sub(snap_floor + 1)) else {
            return Ok(());
        };
        if local < guard.log.len() {
            guard.log.truncate(local);
        }
        Ok(())
    }

    async fn persist_snapshot(&mut self, snap: StoredSnapshot) -> Result<(), Self::Error> {
        let mut guard = self.inner.lock().unwrap();
        let Ok(drop_through) = usize::try_from(snap.last_included_index.get()) else {
            guard.log.clear();
            guard.snapshot = Some(snap);
            return Ok(());
        };
        let keep_from = drop_through.min(guard.log.len());
        guard.log.drain(..keep_from);
        guard.snapshot = Some(snap);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailingStorageOp {
    Recover,
    PersistHardState,
    AppendLog,
    TruncateLog,
    PersistSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TestStorageError(&'static str);

impl std::fmt::Display for TestStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for TestStorageError {}

#[derive(Debug, Clone, Default)]
struct FailingStorage<C> {
    inner: SharedMemoryStorage<C>,
    failures: Arc<Mutex<Vec<FailingStorageOp>>>,
}

impl<C> FailingStorage<C> {
    fn new(failures: impl IntoIterator<Item = FailingStorageOp>) -> Self {
        Self {
            inner: SharedMemoryStorage {
                inner: Arc::new(Mutex::new(MemoryStorage {
                    hard_state: None,
                    snapshot: None,
                    log: Vec::new(),
                })),
            },
            failures: Arc::new(Mutex::new(failures.into_iter().collect())),
        }
    }

    fn should_fail(&self, op: FailingStorageOp) -> bool {
        let mut failures = self.failures.lock().unwrap();
        if let Some(index) = failures.iter().position(|candidate| *candidate == op) {
            failures.remove(index);
            true
        } else {
            false
        }
    }

    fn arm_failure(&self, op: FailingStorageOp) {
        self.failures.lock().unwrap().push(op);
    }
}

impl<C: Send + Clone + 'static> Storage<C> for FailingStorage<C> {
    type Error = TestStorageError;

    async fn recover(&mut self) -> Result<jotun::storage::RecoveredState<C>, Self::Error> {
        if self.should_fail(FailingStorageOp::Recover) {
            return Err(TestStorageError("recover failed"));
        }
        self.inner.recover().await.map_err(|never| match never {})
    }

    async fn persist_hard_state(&mut self, state: StoredHardState) -> Result<(), Self::Error> {
        if self.should_fail(FailingStorageOp::PersistHardState) {
            return Err(TestStorageError("persist_hard_state failed"));
        }
        self.inner
            .persist_hard_state(state)
            .await
            .map_err(|never| match never {})
    }

    async fn append_log(&mut self, entries: Vec<LogEntry<C>>) -> Result<(), Self::Error> {
        if self.should_fail(FailingStorageOp::AppendLog) {
            return Err(TestStorageError("append_log failed"));
        }
        self.inner
            .append_log(entries)
            .await
            .map_err(|never| match never {})
    }

    async fn truncate_log(&mut self, from: LogIndex) -> Result<(), Self::Error> {
        if self.should_fail(FailingStorageOp::TruncateLog) {
            return Err(TestStorageError("truncate_log failed"));
        }
        self.inner
            .truncate_log(from)
            .await
            .map_err(|never| match never {})
    }

    async fn persist_snapshot(&mut self, snap: StoredSnapshot) -> Result<(), Self::Error> {
        if self.should_fail(FailingStorageOp::PersistSnapshot) {
            return Err(TestStorageError("persist_snapshot failed"));
        }
        self.inner
            .persist_snapshot(snap)
            .await
            .map_err(|never| match never {})
    }
}

#[derive(Debug)]
struct TestTransport {
    inbound: mpsc::Receiver<Incoming<Vec<u8>>>,
    sent: SentMessages,
}

#[derive(Clone, Debug)]
struct TestTransportHandle {
    inbound: mpsc::Sender<Incoming<Vec<u8>>>,
    sent: SentMessages,
}

impl TestTransport {
    fn new() -> (Self, TestTransportHandle) {
        let (tx, rx) = mpsc::channel(1024);
        let sent = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                inbound: rx,
                sent: Arc::clone(&sent),
            },
            TestTransportHandle { inbound: tx, sent },
        )
    }
}

impl TestTransportHandle {
    async fn inject(&self, incoming: Incoming<Vec<u8>>) {
        self.inbound
            .send(incoming)
            .await
            .expect("driver still alive");
    }

    fn sent(&self) -> Vec<(NodeId, Message<Vec<u8>>)> {
        self.sent.lock().unwrap().clone()
    }
}

impl Transport<Vec<u8>> for TestTransport {
    type Error = Infallible;

    async fn send(&self, to: NodeId, message: Message<Vec<u8>>) -> Result<(), Self::Error> {
        self.sent.lock().unwrap().push((to, message));
        Ok(())
    }

    async fn recv(&mut self) -> Option<Incoming<Vec<u8>>> {
        self.inbound.recv().await
    }
}

fn nid(n: u64) -> NodeId {
    NodeId::new(n).unwrap()
}

fn quiet_config(node_id: NodeId, peers: impl IntoIterator<Item = NodeId>) -> Config {
    let mut config = Config::new(node_id, peers);
    config.election_timeout_min_ticks = 100;
    config.election_timeout_max_ticks = 101;
    config.tick_interval = Duration::from_secs(1);
    config
}

fn fast_single_node_config() -> Config {
    let mut config = Config::new(nid(1), std::iter::empty::<NodeId>());
    config.election_timeout_min_ticks = 3;
    config.election_timeout_max_ticks = 4;
    config.heartbeat_interval_ticks = 1;
    config.tick_interval = Duration::from_millis(10);
    config
}

fn fast_three_node_config(max_pending_proposals: usize) -> Config {
    let mut config = Config::new(nid(1), [nid(2), nid(3)]);
    config.election_timeout_min_ticks = 3;
    config.election_timeout_max_ticks = 4;
    config.heartbeat_interval_ticks = 1;
    config.tick_interval = Duration::from_millis(10);
    config.max_pending_proposals = max_pending_proposals;
    // These tests drive election manually via a forged VoteResponse;
    // skip the pre-vote round.
    config.pre_vote = false;
    config
}

#[test]
fn config_validate_rejects_heartbeat_not_less_than_election_min() {
    let mut config = Config::new(nid(1), std::iter::empty::<NodeId>());
    config.election_timeout_min_ticks = 3;
    config.election_timeout_max_ticks = 4;
    config.heartbeat_interval_ticks = 3;

    let err = config.validate().unwrap_err();
    assert!(matches!(
        err,
        ConfigError::HeartbeatNotLessThanElectionMin {
            heartbeat_interval_ticks: 3,
            election_timeout_min_ticks: 3,
        }
    ));
}

#[test]
fn config_validate_rejects_invalid_election_timeout_range() {
    let mut config = Config::new(nid(1), std::iter::empty::<NodeId>());
    config.election_timeout_min_ticks = 5;
    config.election_timeout_max_ticks = 5;

    let err = config.validate().unwrap_err();
    assert!(matches!(
        err,
        ConfigError::InvalidElectionTimeoutRange {
            election_timeout_min_ticks: 5,
            election_timeout_max_ticks: 5,
        }
    ));
}

#[test]
fn config_validate_rejects_self_in_peers() {
    let config = Config::new(nid(1), [nid(1), nid(2)]);

    let err = config.validate().unwrap_err();
    assert!(matches!(err, ConfigError::PeersContainSelf { node_id } if node_id == nid(1)));
}

#[test]
fn config_validate_rejects_zero_snapshot_chunk_size() {
    let mut config = Config::new(nid(1), std::iter::empty::<NodeId>());
    config.snapshot_chunk_size_bytes = 0;

    let err = config.validate().unwrap_err();
    assert!(matches!(
        err,
        ConfigError::InvalidSnapshotChunkSize {
            snapshot_chunk_size_bytes: 0,
        }
    ));
}

async fn wait_for_status<S, F>(node: &Node<S>, deadline: Duration, mut predicate: F) -> NodeStatus
where
    S: StateMachine,
    F: FnMut(&NodeStatus) -> bool,
{
    let start = tokio::time::Instant::now();
    loop {
        let status = node.status().await.expect("status call succeeds");
        if predicate(&status) {
            return status;
        }
        assert!(
            start.elapsed() < deadline,
            "status predicate timed out: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_status_error<S>(node: &Node<S>, deadline: Duration) -> ProposeError
where
    S: StateMachine,
{
    let start = tokio::time::Instant::now();
    loop {
        match node.status().await {
            Ok(_) => {
                assert!(
                    start.elapsed() < deadline,
                    "status never failed within {deadline:?}"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(err) => return err,
        }
    }
}

async fn wait_for_vote_request_term(handle: &TestTransportHandle, deadline: Duration) -> Term {
    let start = tokio::time::Instant::now();
    loop {
        for (_to, message) in handle.sent() {
            if let Message::VoteRequest(request) = message {
                return request.term;
            }
        }
        assert!(start.elapsed() < deadline, "vote request was never sent");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_replicated_payload<F>(
    handle: &TestTransportHandle,
    deadline: Duration,
    mut predicate: F,
) where
    F: FnMut(&LogPayload<Vec<u8>>) -> bool,
{
    let start = tokio::time::Instant::now();
    loop {
        let seen = handle
            .sent()
            .into_iter()
            .any(|(_to, message)| match message {
                Message::AppendEntriesRequest(request) => request
                    .entries
                    .iter()
                    .any(|entry| predicate(&entry.payload)),
                _ => false,
            });
        if seen {
            return;
        }
        assert!(
            start.elapsed() < deadline,
            "expected append payload was never sent"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn start_three_node_leader(
    max_pending_proposals: usize,
) -> (Node<Counter>, TestTransportHandle) {
    let (transport, handle) = TestTransport::new();
    let node = Node::start(
        fast_three_node_config(max_pending_proposals),
        Counter::default(),
        MemoryStorage::<Vec<u8>>::default(),
        transport,
    )
    .await
    .unwrap();

    let election_term = wait_for_vote_request_term(&handle, Duration::from_secs(1)).await;
    handle
        .inject(Incoming {
            from: nid(2),
            message: Message::VoteResponse(VoteResponse {
                term: election_term,
                result: VoteResult::Granted,
            }),
        })
        .await;

    let _ = wait_for_status(&node, Duration::from_secs(1), |status| {
        status.role.to_string() == "leader"
    })
    .await;

    (node, handle)
}

#[tokio::test]
async fn bootstrap_new_cluster_uses_config_peers_authoritatively() {
    let (transport, _handle) = TestTransport::new();
    let mut config = quiet_config(nid(1), [nid(2), nid(3)]);
    config.bootstrap = Bootstrap::NewCluster {
        members: vec![nid(1), nid(9)],
    };
    let node = Node::start(
        config,
        Counter::default(),
        MemoryStorage::<Vec<u8>>::default(),
        transport,
    )
    .await
    .unwrap();

    let status = node.status().await.unwrap();
    assert_eq!(status.peers, vec![nid(2), nid(3)]);

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn node_start_returns_config_error_for_invalid_config() {
    let (transport, _handle) = TestTransport::new();
    let mut config = Config::new(nid(1), std::iter::empty::<NodeId>());
    config.election_timeout_min_ticks = 5;
    config.election_timeout_max_ticks = 5;

    let err = Node::start(
        config,
        Counter::default(),
        MemoryStorage::<Vec<u8>>::default(),
        transport,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        err,
        NodeStartError::Config(ConfigError::InvalidElectionTimeoutRange {
            election_timeout_min_ticks: 5,
            election_timeout_max_ticks: 5,
        })
    ));
}

#[tokio::test]
async fn bootstrap_join_starts_with_empty_peers() {
    let (transport, _handle) = TestTransport::new();
    let mut config = quiet_config(nid(1), [nid(2), nid(3)]);
    config.bootstrap = Bootstrap::Join;
    let node = Node::start(
        config,
        Counter::default(),
        MemoryStorage::<Vec<u8>>::default(),
        transport,
    )
    .await
    .unwrap();

    let status = node.status().await.unwrap();
    assert!(status.peers.is_empty());

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn bootstrap_recover_prefers_persisted_peers_when_snapshot_exists() {
    let (transport, _handle) = TestTransport::new();
    let mut config = quiet_config(nid(1), [nid(3)]);
    config.bootstrap = Bootstrap::Recover;
    let storage = MemoryStorage::<Vec<u8>> {
        snapshot: Some(StoredSnapshot {
            last_included_index: LogIndex::new(7),
            last_included_term: Term::new(3),
            peers: BTreeSet::from([nid(2)]),
            bytes: b"snapshot".to_vec(),
        }),
        ..MemoryStorage::default()
    };

    let node = Node::start(config, Counter::default(), storage, transport)
        .await
        .unwrap();

    let status = node.status().await.unwrap();
    assert_eq!(status.peers, vec![nid(2)]);

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn bootstrap_recover_falls_back_to_config_peers_without_persisted_membership() {
    let (transport, _handle) = TestTransport::new();
    let mut config = quiet_config(nid(1), [nid(2), nid(3)]);
    config.bootstrap = Bootstrap::Recover;
    let storage = MemoryStorage::<Vec<u8>> {
        hard_state: Some(StoredHardState {
            current_term: Term::new(4),
            voted_for: Some(nid(2)),
        }),
        ..MemoryStorage::default()
    };

    let node = Node::start(config, Counter::default(), storage, transport)
        .await
        .unwrap();

    let status = node.status().await.unwrap();
    assert_eq!(status.peers, vec![nid(2), nid(3)]);

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn status_reports_boot_leader_and_post_propose_state() {
    let (transport, _handle) = TestTransport::new();
    let node = Node::start(
        fast_single_node_config(),
        Counter::default(),
        MemoryStorage::<Vec<u8>>::default(),
        transport,
    )
    .await
    .unwrap();

    let boot = node.status().await.unwrap();
    assert_eq!(boot.node_id, nid(1));
    assert_eq!(boot.current_term, Term::ZERO);
    assert_eq!(boot.commit_index, LogIndex::ZERO);
    assert_eq!(boot.last_applied, LogIndex::ZERO);
    assert_eq!(boot.leader_hint, None);
    assert!(boot.peers.is_empty());

    let leader = wait_for_status(&node, Duration::from_secs(1), |status| {
        status.role.to_string() == "leader" && status.commit_index >= LogIndex::new(1)
    })
    .await;
    assert_eq!(leader.current_term, Term::new(1));
    assert_eq!(leader.commit_index, LogIndex::new(1));
    assert_eq!(leader.last_applied, LogIndex::new(1));
    assert_eq!(leader.leader_hint, None);

    let result = node.propose(CountCmd::Inc(5)).await.unwrap();
    assert_eq!(result, 5);

    let after = node.status().await.unwrap();
    assert_eq!(after.current_term, Term::new(1));
    assert_eq!(after.commit_index, LogIndex::new(2));
    assert_eq!(after.last_applied, LogIndex::new(2));
    assert_eq!(after.leader_hint, None);
    assert!(after.peers.is_empty());

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn propose_returns_busy_when_pending_limit_is_reached() {
    let (node, handle) = start_three_node_leader(1).await;

    let first_node = node.clone();
    let first = tokio::spawn(async move { first_node.propose(CountCmd::Inc(5)).await });
    wait_for_replicated_payload(&handle, Duration::from_secs(1), |payload| {
        matches!(payload, LogPayload::Command(_))
    })
    .await;

    let err = node.propose(CountCmd::Inc(7)).await.unwrap_err();
    assert!(matches!(err, ProposeError::Busy));

    node.shutdown().await.unwrap();
    let err = first.await.unwrap().unwrap_err();
    assert!(matches!(
        err,
        ProposeError::DriverDead | ProposeError::Shutdown
    ));
}

#[tokio::test]
async fn config_change_returns_busy_when_pending_limit_is_reached() {
    let (node, handle) = start_three_node_leader(1).await;

    let first_node = node.clone();
    let first = tokio::spawn(async move { first_node.add_peer(nid(4)).await });
    wait_for_replicated_payload(&handle, Duration::from_secs(1), |payload| {
        matches!(payload, LogPayload::ConfigChange(_))
    })
    .await;

    let err = node.remove_peer(nid(2)).await.unwrap_err();
    assert!(matches!(err, ProposeError::Busy));

    node.shutdown().await.unwrap();
    let err = first.await.unwrap().unwrap_err();
    assert!(matches!(
        err,
        ProposeError::DriverDead | ProposeError::Shutdown
    ));
}

#[tokio::test]
async fn runtime_auto_snapshots_on_hint_by_default() {
    let (transport, _handle) = TestTransport::new();
    let snapshots_taken = Arc::new(AtomicUsize::new(0));
    let storage = SharedMemoryStorage::<Vec<u8>>::default();
    let snapshot_view = Arc::clone(&storage.inner);
    let mut config = fast_single_node_config();
    config.snapshot_hint_threshold_entries = 2;

    let node = Node::start(
        config,
        SnapshottingCounter::new(Arc::clone(&snapshots_taken)),
        storage,
        transport,
    )
    .await
    .unwrap();

    let _ = wait_for_status(&node, Duration::from_secs(1), |status| {
        status.role.to_string() == "leader" && status.commit_index >= LogIndex::new(1)
    })
    .await;
    assert_eq!(node.propose(CountCmd::Inc(5)).await.unwrap(), 5);

    let start = tokio::time::Instant::now();
    loop {
        let snapshot = snapshot_view.lock().unwrap().snapshot.clone();
        if let Some(snapshot) = snapshot {
            assert_eq!(snapshot.last_included_index, LogIndex::new(2));
            assert_eq!(snapshot.bytes, 5u64.to_le_bytes().to_vec());
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "auto snapshot was never persisted"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(snapshots_taken.load(Ordering::Relaxed) >= 1);

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn snapshot_hints_can_be_disabled_via_config() {
    let (transport, _handle) = TestTransport::new();
    let snapshots_taken = Arc::new(AtomicUsize::new(0));
    let storage = SharedMemoryStorage::<Vec<u8>>::default();
    let snapshot_view = Arc::clone(&storage.inner);
    let mut config = fast_single_node_config();
    config.snapshot_hint_threshold_entries = 0;

    let node = Node::start(
        config,
        SnapshottingCounter::new(Arc::clone(&snapshots_taken)),
        storage,
        transport,
    )
    .await
    .unwrap();

    let _ = wait_for_status(&node, Duration::from_secs(1), |status| {
        status.role.to_string() == "leader" && status.commit_index >= LogIndex::new(1)
    })
    .await;
    assert_eq!(node.propose(CountCmd::Inc(5)).await.unwrap(), 5);

    assert!(snapshot_view.lock().unwrap().snapshot.is_none());
    assert_eq!(snapshots_taken.load(Ordering::Relaxed), 0);

    node.shutdown().await.unwrap();
}

// ---------------------------------------------------------------------------
// Error-type Display / source plumbing. Exhaustive unit coverage for the
// public error enums — every variant's Display reads, and NodeStartError's
// Error::source returns the underlying cause.
// ---------------------------------------------------------------------------

#[test]
fn config_error_display_covers_every_variant() {
    let e = ConfigError::HeartbeatNotLessThanElectionMin {
        heartbeat_interval_ticks: 5,
        election_timeout_min_ticks: 3,
    };
    let s = e.to_string();
    assert!(s.contains('5') && s.contains('3'), "got: {s}");

    let e = ConfigError::InvalidElectionTimeoutRange {
        election_timeout_min_ticks: 10,
        election_timeout_max_ticks: 10,
    };
    assert!(e.to_string().contains("empty"));

    let e = ConfigError::PeersContainSelf { node_id: nid(1) };
    assert!(e.to_string().contains("must not contain self"));

    let e = ConfigError::InvalidSnapshotChunkSize {
        snapshot_chunk_size_bytes: 0,
    };
    assert!(e.to_string().contains("greater than zero"));
}

#[test]
fn propose_error_display_covers_every_variant() {
    let cases = [
        ProposeError::NotLeader {
            leader_hint: nid(2),
        },
        ProposeError::NoLeader,
        ProposeError::Shutdown,
        ProposeError::DriverDead,
        ProposeError::Busy,
        ProposeError::Fatal { reason: "boom" },
    ];
    for e in cases {
        assert!(!e.to_string().is_empty(), "empty display for {e:?}");
    }
    assert!(
        ProposeError::NotLeader {
            leader_hint: nid(7)
        }
        .to_string()
        .contains('7'),
        "leader_hint must surface in Display",
    );
    assert!(
        ProposeError::Fatal {
            reason: "disk on fire"
        }
        .to_string()
        .contains("disk on fire"),
    );
}

#[test]
fn transfer_leadership_error_display_covers_every_variant() {
    let cases = [
        TransferLeadershipError::NotLeader {
            leader_hint: nid(2),
        },
        TransferLeadershipError::NoLeader,
        TransferLeadershipError::InvalidTarget { target: nid(9) },
        TransferLeadershipError::Shutdown,
        TransferLeadershipError::DriverDead,
        TransferLeadershipError::Fatal { reason: "x" },
    ];
    for e in cases {
        assert!(!e.to_string().is_empty(), "empty display for {e:?}");
    }
    assert!(
        TransferLeadershipError::InvalidTarget { target: nid(9) }
            .to_string()
            .contains('9'),
    );
}

#[test]
fn read_error_display_covers_every_variant() {
    let cases = [
        ReadError::NotLeader {
            leader_hint: nid(2),
        },
        ReadError::NotReady,
        ReadError::SteppedDown,
        ReadError::Shutdown,
        ReadError::DriverDead,
        ReadError::Fatal { reason: "y" },
    ];
    for e in cases {
        assert!(!e.to_string().is_empty(), "empty display for {e:?}");
    }
}

#[test]
fn node_start_error_display_and_source_delegate_to_cause() {
    // NodeStartError is generic over the storage error; pick a real
    // one (ConfigError is an Error impl so it doubles as a stand-in).
    let inner = ConfigError::PeersContainSelf { node_id: nid(1) };
    let e: NodeStartError<std::io::Error> = NodeStartError::Config(inner);
    assert!(e.to_string().contains("invalid config"));
    let src = std::error::Error::source(&e).expect("source present");
    assert!(src.to_string().contains("must not contain self"));

    let io_err = std::io::Error::other("boom");
    let e: NodeStartError<std::io::Error> = NodeStartError::Storage(io_err);
    assert!(e.to_string().contains("storage recover failed"));
    assert!(e.to_string().contains("boom"));
    assert!(std::error::Error::source(&e).is_some());
}

// ---------------------------------------------------------------------------
// transfer_leadership_to: the runtime-level API. Engine-level coverage
// exists in jotun-core; these tests pin the driver-side role gating.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transfer_leadership_to_self_returns_invalid_target() {
    let (node, _handle) = start_three_node_leader(16).await;
    let err = node.transfer_leadership_to(nid(1)).await.unwrap_err();
    assert!(
        matches!(err, TransferLeadershipError::InvalidTarget { target } if target == nid(1)),
        "got {err:?}",
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn transfer_leadership_to_unknown_peer_returns_invalid_target() {
    let (node, _handle) = start_three_node_leader(16).await;
    let err = node.transfer_leadership_to(nid(99)).await.unwrap_err();
    assert!(
        matches!(err, TransferLeadershipError::InvalidTarget { target } if target == nid(99)),
        "got {err:?}",
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn transfer_leadership_from_follower_without_leader_returns_no_leader() {
    // Boot a node with ghost peers so it never wins an election.
    let (transport, _handle) = TestTransport::new();
    let node = Node::start(
        fast_three_node_config(16),
        Counter::default(),
        MemoryStorage::<Vec<u8>>::default(),
        transport,
    )
    .await
    .unwrap();

    // No VoteResponse injected — it stays candidate (or cycles
    // between follower/candidate). The driver hands back NoLeader
    // whichever non-leader role it happens to be in at call time.
    let err = node.transfer_leadership_to(nid(2)).await.unwrap_err();
    assert!(
        matches!(
            err,
            TransferLeadershipError::NoLeader | TransferLeadershipError::NotLeader { .. }
        ),
        "got {err:?}",
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn transfer_leadership_accepts_known_peer_on_leader() {
    let (node, _handle) = start_three_node_leader(16).await;
    // Valid known peer — the engine accepts the request and emits a
    // targeted AppendEntries. Even without a follower acking,
    // the call itself returns Ok.
    let r = node.transfer_leadership_to(nid(2)).await;
    assert!(r.is_ok(), "got {r:?}");
    node.shutdown().await.unwrap();
}

// ---------------------------------------------------------------------------
// After shutdown, public API calls fail cleanly with Shutdown/DriverDead —
// no hangs, no panics.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn post_shutdown_api_calls_return_shutdown_errors() {
    let (node, _handle) = start_three_node_leader(16).await;
    let clone = node.clone();
    node.shutdown().await.unwrap();
    let status_err = wait_for_status_error(&clone, Duration::from_secs(1)).await;
    assert!(matches!(
        status_err,
        ProposeError::Shutdown | ProposeError::DriverDead
    ));

    let err = clone.propose(CountCmd::Inc(1)).await.unwrap_err();
    assert!(
        matches!(err, ProposeError::Shutdown | ProposeError::DriverDead),
        "got {err:?}",
    );
    let err = clone.transfer_leadership_to(nid(2)).await.unwrap_err();
    assert!(
        matches!(
            err,
            TransferLeadershipError::Shutdown | TransferLeadershipError::DriverDead
        ),
        "got {err:?}",
    );
    let err = clone
        .read_linearizable(|c: &Counter| c.value)
        .await
        .unwrap_err();
    assert!(
        matches!(err, ReadError::Shutdown | ReadError::DriverDead),
        "got {err:?}",
    );
    let err = clone.add_peer(nid(7)).await.unwrap_err();
    assert!(
        matches!(err, ProposeError::Shutdown | ProposeError::DriverDead),
        "got {err:?}",
    );
    let err = clone.remove_peer(nid(2)).await.unwrap_err();
    assert!(
        matches!(err, ProposeError::Shutdown | ProposeError::DriverDead),
        "got {err:?}",
    );
}

#[tokio::test]
async fn node_start_surfaces_recover_failure_from_storage() {
    let (transport, _handle) = TestTransport::new();
    let err = Node::start(
        fast_single_node_config(),
        Counter::default(),
        FailingStorage::<Vec<u8>>::new([FailingStorageOp::Recover]),
        transport,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        err,
        NodeStartError::Storage(TestStorageError(reason)) if reason == "recover failed"
    ));
}

#[tokio::test]
async fn append_log_failure_fails_proposal_with_fatal() {
    let (transport, _handle) = TestTransport::new();
    let storage = FailingStorage::<Vec<u8>>::new([]);
    let node = Node::start(
        fast_single_node_config(),
        Counter::default(),
        storage.clone(),
        transport,
    )
    .await
    .unwrap();

    let _ = wait_for_status(&node, Duration::from_secs(1), |status| {
        status.role.to_string() == "leader" && status.commit_index >= LogIndex::new(1)
    })
    .await;

    storage.arm_failure(FailingStorageOp::AppendLog);
    let err = node.propose(CountCmd::Inc(1)).await.unwrap_err();
    assert!(
        matches!(err, ProposeError::Fatal { reason } if reason == "append_log failed"),
        "got {err:?}",
    );
}

#[tokio::test]
async fn persist_hard_state_failure_shuts_down_the_runtime() {
    let (transport, _handle) = TestTransport::new();
    let node = Node::start(
        fast_single_node_config(),
        Counter::default(),
        FailingStorage::<Vec<u8>>::new([FailingStorageOp::PersistHardState]),
        transport,
    )
    .await
    .unwrap();

    let err = wait_for_status_error(&node, Duration::from_secs(1)).await;
    assert!(matches!(
        err,
        ProposeError::Shutdown | ProposeError::DriverDead
    ));
}

#[tokio::test]
async fn persist_snapshot_failure_kills_the_node_after_snapshot_hint() {
    let (transport, _handle) = TestTransport::new();
    let snapshots_taken = Arc::new(AtomicUsize::new(0));
    let mut config = fast_single_node_config();
    config.snapshot_hint_threshold_entries = 2;

    let node = Node::start(
        config,
        SnapshottingCounter::new(Arc::clone(&snapshots_taken)),
        FailingStorage::<Vec<u8>>::new([FailingStorageOp::PersistSnapshot]),
        transport,
    )
    .await
    .unwrap();

    let _ = wait_for_status(&node, Duration::from_secs(1), |status| {
        status.role.to_string() == "leader" && status.commit_index >= LogIndex::new(1)
    })
    .await;
    assert_eq!(node.propose(CountCmd::Inc(5)).await.unwrap(), 5);

    let err = wait_for_status_error(&node, Duration::from_secs(1)).await;
    assert!(matches!(
        err,
        ProposeError::Shutdown | ProposeError::DriverDead
    ));
    assert!(
        snapshots_taken.load(Ordering::Relaxed) >= 1,
        "snapshot path never ran before the node died",
    );
}

// ---------------------------------------------------------------------------
// add_peer / remove_peer on a non-leader follower return NoLeader; on a
// candidate, same. Integration coverage of handle_config_change role gating.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn add_peer_on_leaderless_follower_returns_no_leader_or_not_leader() {
    let (transport, _handle) = TestTransport::new();
    let node = Node::start(
        fast_three_node_config(16),
        Counter::default(),
        MemoryStorage::<Vec<u8>>::default(),
        transport,
    )
    .await
    .unwrap();

    let err = node.add_peer(nid(9)).await.unwrap_err();
    assert!(
        matches!(err, ProposeError::NoLeader | ProposeError::NotLeader { .. }),
        "got {err:?}",
    );
    let err = node.remove_peer(nid(2)).await.unwrap_err();
    assert!(
        matches!(err, ProposeError::NoLeader | ProposeError::NotLeader { .. }),
        "got {err:?}",
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn add_peer_on_leader_appends_config_change() {
    let (node, handle) = start_three_node_leader(16).await;

    // add_peer submits a ConfigChange. The leader's broadcast will
    // include the new entry. Give the driver a moment to emit, then
    // inspect what was sent.
    //
    // We fire the add_peer concurrently with a tight wait: the call
    // won't return until the entry commits, which requires a peer
    // ack we don't provide — so we just check the leader emitted
    // the replication broadcast with a ConfigChange payload.
    let handle_ref = handle.clone();
    tokio::spawn(async move {
        let _ = node.add_peer(nid(9)).await;
        let _ = node.shutdown().await;
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut saw_config_change = false;
    while std::time::Instant::now() < deadline {
        for (_, msg) in handle_ref.sent.lock().unwrap().iter() {
            if let Message::AppendEntriesRequest(req) = msg {
                for e in &req.entries {
                    if matches!(e.payload, LogPayload::ConfigChange(_)) {
                        saw_config_change = true;
                        break;
                    }
                }
            }
        }
        if saw_config_change {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(saw_config_change, "leader never broadcast ConfigChange");
}
