//! Coverage for the newer runtime-facing `Node` surface: bootstrap
//! semantics, `status()`, and proposal/config-change backpressure.

#![allow(
    clippy::expect_used,
    clippy::missing_const_for_fn,
    clippy::unwrap_used
)]

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jotun::{
    Bootstrap, Config, ConfigError, DecodeError, Node, NodeId, NodeStartError, NodeStatus,
    ProposeError, StateMachine, Storage, StoredHardState, StoredSnapshot, Transport,
};
use jotun_core::{
    Incoming, LogEntry, LogIndex, LogPayload, Message, Term,
    VoteResponse, VoteResult,
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
            if let Some(slot) = self.log.get_mut(local_idx) {
                *slot = entry;
            } else {
                self.log.push(entry);
            }
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
        self.inbound.send(incoming).await.expect("driver still alive");
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
        assert!(start.elapsed() < deadline, "status predicate timed out: {status:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
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
)
where
    F: FnMut(&LogPayload<Vec<u8>>) -> bool,
{
    let start = tokio::time::Instant::now();
    loop {
        let seen = handle.sent().into_iter().any(|(_to, message)| match message {
            Message::AppendEntriesRequest(request) => request
                .entries
                .iter()
                .any(|entry| predicate(&entry.payload)),
            _ => false,
        });
        if seen {
            return;
        }
        assert!(start.elapsed() < deadline, "expected append payload was never sent");
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

    let _ = wait_for_status(&node, Duration::from_secs(1), |status| status.role.to_string() == "leader")
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
    let node = Node::start(config, Counter::default(), MemoryStorage::<Vec<u8>>::default(), transport)
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
    let node = Node::start(config, Counter::default(), MemoryStorage::<Vec<u8>>::default(), transport)
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
    assert!(matches!(err, ProposeError::DriverDead | ProposeError::Shutdown));
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
    assert!(matches!(err, ProposeError::DriverDead | ProposeError::Shutdown));
}
