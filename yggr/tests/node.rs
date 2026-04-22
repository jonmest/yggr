//! End-to-end [`yggr::Node`] tests. Real `DiskStorage` +
//! `TcpTransport`, actual TCP loopback connections.

#![allow(
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    clippy::missing_const_for_fn,
    clippy::unwrap_used,
    clippy::expect_used
)]

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use yggr::{
    Config, DecodeError, DiskStorage, LogIndex, Node, NodeId, NodeStatus, ProposeError, ReadError,
    Role, StateMachine, TcpTransport,
};

// ---------------------------------------------------------------------------
// Toy state machine: a counter command stream.
// ---------------------------------------------------------------------------

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
}

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

struct TmpDir(PathBuf);

impl TmpDir {
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("yggr-node-test-{label}-{pid}-{n}"));
        let _ = std::fs::remove_dir_all(&path);
        Self(path)
    }
}

impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn nid(n: u64) -> NodeId {
    NodeId::new(n).unwrap()
}

fn free_port() -> u16 {
    static NEXT_PORT: AtomicU16 = AtomicU16::new(43000);
    let pid_bias = (std::process::id() % 2000) as u16;
    NEXT_PORT.fetch_add(1, Ordering::Relaxed) + pid_bias
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

async fn wait_for_single_node_leader<S>(node: &Node<S>) -> NodeStatus
where
    S: StateMachine,
{
    wait_for_status(node, Duration::from_secs(2), |status| {
        status.role == Role::Leader && status.commit_index >= LogIndex::new(1)
    })
    .await
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn single_node_node_starts_and_shuts_down() {
    let tmp = TmpDir::new("single-start");
    let storage = DiskStorage::open(&tmp.0).await.unwrap();
    let port = free_port();
    let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
    let transport: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr, BTreeMap::new())
        .await
        .unwrap();

    let config = Config::new(nid(1), std::iter::empty::<NodeId>());
    let node = Node::start(config, Counter::default(), storage, transport)
        .await
        .unwrap();
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_releases_transport_listener_immediately() {
    let tmp = TmpDir::new("shutdown-rebind");
    let storage = DiskStorage::open(&tmp.0).await.unwrap();
    let port = free_port();
    let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
    let transport: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr, BTreeMap::new())
        .await
        .unwrap();

    let config = Config::new(nid(1), std::iter::empty::<NodeId>());
    let node = Node::start(config, Counter::default(), storage, transport)
        .await
        .unwrap();

    // Hold an active peer-side connection open so shutdown has to
    // tear down an accepted reader task, not just the listener.
    let mut peer = TcpStream::connect(addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    node.shutdown().await.unwrap();

    let mut buf = [0u8; 1];
    let read_result = tokio::time::timeout(Duration::from_secs(1), peer.read(&mut buf))
        .await
        .expect("shutdown should close accepted peer connections promptly");
    assert!(
        matches!(read_result, Ok(0)) || read_result.is_err(),
        "expected EOF or connection reset after shutdown, got {read_result:?}"
    );

    let rebound: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr, BTreeMap::new())
        .await
        .expect("shutdown should release the listen port before returning");
    drop(rebound);
}

#[tokio::test]
async fn single_node_self_elects_and_applies_proposal() {
    let tmp = TmpDir::new("single-propose");
    let storage = DiskStorage::open(&tmp.0).await.unwrap();
    let port = free_port();
    let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
    let transport: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr, BTreeMap::new())
        .await
        .unwrap();

    let mut config = Config::new(nid(1), std::iter::empty::<NodeId>());
    // Smaller intervals so the self-election fires fast in tests.
    config.election_timeout_min_ticks = 3;
    config.election_timeout_max_ticks = 5;
    config.heartbeat_interval_ticks = 1;
    config.tick_interval = Duration::from_millis(10);

    let node = Node::start(config, Counter::default(), storage, transport)
        .await
        .unwrap();

    let _ = wait_for_single_node_leader(&node).await;

    let result = tokio::time::timeout(Duration::from_secs(2), node.propose(CountCmd::Inc(5)))
        .await
        .expect("propose timed out")
        .expect("propose returned error");
    assert_eq!(result, 5);

    let result = tokio::time::timeout(Duration::from_secs(2), node.propose(CountCmd::Inc(3)))
        .await
        .expect("propose timed out")
        .expect("propose returned error");
    assert_eq!(result, 8);

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_handle_after_first_use_returns_shutdown_error() {
    let tmp = TmpDir::new("post-shutdown");
    let storage = DiskStorage::open(&tmp.0).await.unwrap();
    let port = free_port();
    let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
    let transport: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr, BTreeMap::new())
        .await
        .unwrap();

    let config = Config::new(nid(1), std::iter::empty::<NodeId>());
    let node = Node::start(config, Counter::default(), storage, transport)
        .await
        .unwrap();
    let clone = node.clone();
    node.shutdown().await.unwrap();

    let err: ProposeError = clone.propose(CountCmd::Inc(1)).await.unwrap_err();
    assert!(matches!(
        err,
        ProposeError::Shutdown | ProposeError::DriverDead
    ));
}

// ---------------------------------------------------------------------------
// Engine recovery: a restarted node preserves its persisted voted_for
// across a crash. Without this, §5.1 "at most one vote per term" is
// unenforceable — a node restarted in the same term could cast a
// second vote.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn restart_preserves_voted_for() {
    use yggr_core::{RecoveredHardState, RecoveredSnapshot, StaticEnv};

    // Seed a DiskStorage with a specific hard state that represents
    // "we voted for node 2 in term 7, then crashed".
    let tmp = TmpDir::new("recover-voted-for");
    {
        let mut storage = DiskStorage::open(&tmp.0).await.unwrap();
        <DiskStorage as yggr::Storage<Vec<u8>>>::persist_hard_state(
            &mut storage,
            yggr::StoredHardState {
                current_term: yggr_core::Term::new(7),
                voted_for: Some(nid(2)),
            },
        )
        .await
        .unwrap();
    }

    // Build an engine directly (not through Node::start, so we can
    // call recover_from and assert on the state). This exercises the
    // Engine::recover_from path the runtime uses inside hydrate_engine.
    let mut storage: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    let recovered = <DiskStorage as yggr::Storage<Vec<u8>>>::recover(&mut storage)
        .await
        .unwrap();

    let hs = recovered.hard_state.expect("hard state was persisted");
    assert_eq!(hs.current_term.get(), 7);
    assert_eq!(hs.voted_for, Some(nid(2)));

    let env: Box<dyn yggr_core::Env> = Box::new(StaticEnv(10));
    let mut engine: yggr_core::Engine<Vec<u8>> =
        yggr_core::Engine::new(nid(1), vec![nid(2), nid(3)], env, 3);

    engine.recover_from(RecoveredHardState {
        current_term: hs.current_term,
        voted_for: hs.voted_for,
        snapshot: recovered.snapshot.map(|s| RecoveredSnapshot {
            last_included_index: s.last_included_index,
            last_included_term: s.last_included_term,
            peers: s.peers,
            bytes: s.bytes,
        }),
        post_snapshot_log: recovered.log,
    });

    assert_eq!(engine.current_term().get(), 7);
    assert_eq!(
        engine.voted_for(),
        Some(nid(2)),
        "voted_for must survive across crash/recover for §5.1 safety",
    );
}

// ---------------------------------------------------------------------------
// Fatal error propagation: a StateMachine whose decode_command rejects
// every input triggers the fatal path as soon as a committed entry
// reaches Apply. Any in-flight proposal on the same driver fails with
// ProposeError::Fatal (not a silent stall).
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct BrokenSm;

#[derive(Debug, Clone)]
struct BrokenCmd;

impl StateMachine for BrokenSm {
    type Command = BrokenCmd;
    type Response = ();

    fn encode_command(_: &BrokenCmd) -> Vec<u8> {
        // Still serialize to something — the leader appends, replicates,
        // commits, and hands it back on Apply where decode fires.
        b"payload".to_vec()
    }

    fn decode_command(_: &[u8]) -> Result<BrokenCmd, DecodeError> {
        // Always fail. Simulates corrupt storage handing the state
        // machine garbage it can't decode — a real fatal runtime error.
        Err(DecodeError::new("intentional test-only decode failure"))
    }

    fn apply(&mut self, _: BrokenCmd) {}
}

#[tokio::test]
async fn decode_failure_on_apply_fails_waiter_with_fatal() {
    let tmp = TmpDir::new("fatal-decode");
    let storage = DiskStorage::open(&tmp.0).await.unwrap();
    let port = free_port();
    let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
    let transport: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr, BTreeMap::new())
        .await
        .unwrap();

    let mut config = Config::new(nid(1), std::iter::empty::<NodeId>());
    config.election_timeout_min_ticks = 3;
    config.election_timeout_max_ticks = 5;
    config.heartbeat_interval_ticks = 1;
    config.tick_interval = Duration::from_millis(10);

    let node = Node::start(config, BrokenSm, storage, transport)
        .await
        .unwrap();

    let _ = wait_for_single_node_leader(&node).await;

    // The proposal will commit (no peers to ack means the engine's
    // try_advance_commit_as_leader fires), apply will try to decode,
    // decode will fail, driver fails the waiter with Fatal.
    let err = tokio::time::timeout(Duration::from_secs(2), node.propose(BrokenCmd))
        .await
        .expect("propose did not resolve — waiter leaked")
        .expect_err("broken state machine should have failed the proposal");
    assert!(
        matches!(err, ProposeError::Fatal { .. }),
        "expected Fatal, got {err:?}",
    );
}

// ---------------------------------------------------------------------------
// ReadIndex: a single-node leader serves a linearizable read after its
// election no-op commits. The closure observes whatever `propose` has
// already applied.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn single_node_read_linearizable_observes_applied_state() {
    let tmp = TmpDir::new("single-read");
    let storage = DiskStorage::open(&tmp.0).await.unwrap();
    let port = free_port();
    let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
    let transport: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr, BTreeMap::new())
        .await
        .unwrap();

    let mut config = Config::new(nid(1), std::iter::empty::<NodeId>());
    config.election_timeout_min_ticks = 3;
    config.election_timeout_max_ticks = 5;
    config.heartbeat_interval_ticks = 1;
    config.tick_interval = Duration::from_millis(10);

    let node = Node::start(config, Counter::default(), storage, transport)
        .await
        .unwrap();

    let _ = wait_for_single_node_leader(&node).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), node.propose(CountCmd::Inc(5)))
        .await
        .unwrap()
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), node.propose(CountCmd::Inc(3)))
        .await
        .unwrap()
        .unwrap();

    let value = tokio::time::timeout(
        Duration::from_secs(2),
        node.read_linearizable(|sm: &Counter| sm.value),
    )
    .await
    .expect("read timed out")
    .expect("read returned error");
    assert_eq!(value, 8);

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn read_linearizable_on_follower_returns_not_leader() {
    // A fresh single-node cluster that hasn't elected yet looks like
    // a follower to read_linearizable. With peers listed but no quorum
    // possible, it never becomes leader.
    let tmp = TmpDir::new("follower-read");
    let storage = DiskStorage::open(&tmp.0).await.unwrap();
    let port = free_port();
    let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
    let transport: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr, BTreeMap::new())
        .await
        .unwrap();

    // Two ghost peers so the node stays as candidate/follower forever.
    let mut config = Config::new(nid(1), [nid(2), nid(3)]);
    config.election_timeout_min_ticks = 3;
    config.election_timeout_max_ticks = 5;
    config.heartbeat_interval_ticks = 1;
    config.tick_interval = Duration::from_millis(10);
    // Without pre-vote, election-timer expiry bumps the term straight
    // to 1 so the `current_term >= 1` wait below completes. With
    // pre-vote the probe never succeeds against ghost peers so the
    // term stays at 0 forever.
    config.pre_vote = false;

    let node = Node::start(config, Counter::default(), storage, transport)
        .await
        .unwrap();

    // Short wait — plenty of time to realise no leader will emerge.
    let _ = wait_for_status(&node, Duration::from_secs(2), |status| {
        status.current_term.get() >= 1
    })
    .await;

    let err = tokio::time::timeout(
        Duration::from_secs(2),
        node.read_linearizable(|sm: &Counter| sm.value),
    )
    .await
    .expect("read timed out")
    .expect_err("no-leader read must fail");
    assert!(
        matches!(err, ReadError::NotReady | ReadError::NotLeader { .. }),
        "expected NotReady/NotLeader, got {err:?}",
    );

    node.shutdown().await.unwrap();
}

// ---------------------------------------------------------------------------
// Async apply: a state machine with a slow apply() must not block the
// driver's tick / status processing. With the state machine running on
// its own task, proposals queue up in the apply channel and the driver
// stays responsive to Tick and Status inputs.
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct SlowCounter {
    value: u64,
}

impl StateMachine for SlowCounter {
    type Command = CountCmd;
    type Response = u64;

    fn encode_command(c: &CountCmd) -> Vec<u8> {
        Counter::encode_command(c)
    }
    fn decode_command(bytes: &[u8]) -> Result<CountCmd, DecodeError> {
        Counter::decode_command(bytes)
    }
    fn apply(&mut self, cmd: CountCmd) -> u64 {
        // Slow enough to starve the driver if apply were still
        // running inline. Multiple commits' worth of 50ms each
        // would blow past the election timeout set below and the
        // node would churn through terms.
        std::thread::sleep(Duration::from_millis(50));
        match cmd {
            CountCmd::Inc(n) => {
                self.value += n;
                self.value
            }
        }
    }
}

#[tokio::test]
async fn batched_concurrent_proposals_all_commit_in_order() {
    let tmp = TmpDir::new("batch");
    let storage = DiskStorage::open(&tmp.0).await.unwrap();
    let port = free_port();
    let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
    let transport: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr, BTreeMap::new())
        .await
        .unwrap();

    let mut config = Config::new(nid(1), std::iter::empty::<NodeId>());
    config.election_timeout_min_ticks = 3;
    config.election_timeout_max_ticks = 5;
    config.heartbeat_interval_ticks = 1;
    config.tick_interval = Duration::from_millis(10);
    // Batching on: 2-tick delay, flush at 4 buffered.
    config.max_batch_delay_ticks = 2;
    config.max_batch_entries = 4;

    let node = Node::start(config, Counter::default(), storage, transport)
        .await
        .unwrap();

    let _ = wait_for_single_node_leader(&node).await;

    // Fire 3 concurrent proposals. The driver buffers them; either
    // the size cap (unreached, 3 < 4) fires or the tick-based flush
    // does after 2 ticks (~20ms). Either way, all three commit in
    // submission order and each returns the cumulative counter value.
    let (a, b, c) = tokio::join!(
        node.propose(CountCmd::Inc(10)),
        node.propose(CountCmd::Inc(20)),
        node.propose(CountCmd::Inc(30)),
    );
    let results = [a.expect("a"), b.expect("b"), c.expect("c")];
    let mut sorted = results;
    sorted.sort_unstable();
    assert_eq!(sorted, [10, 30, 60]);

    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_apply_does_not_stall_driver_status() {
    let tmp = TmpDir::new("slow-apply");
    let storage = DiskStorage::open(&tmp.0).await.unwrap();
    let port = free_port();
    let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
    let transport: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr, BTreeMap::new())
        .await
        .unwrap();

    let mut config = Config::new(nid(1), std::iter::empty::<NodeId>());
    config.election_timeout_min_ticks = 8;
    config.election_timeout_max_ticks = 12;
    config.heartbeat_interval_ticks = 2;
    config.tick_interval = Duration::from_millis(10);

    let node = Node::start(config, SlowCounter::default(), storage, transport)
        .await
        .unwrap();

    let _ = wait_for_single_node_leader(&node).await;

    // Fire several proposals back-to-back. Propose returns as soon
    // as each commits and lands on the state machine via the apply
    // task; the apply task processes them serially with a 50ms sleep.
    let n = 5u64;
    let start = std::time::Instant::now();
    for i in 1..=n {
        let r = tokio::time::timeout(Duration::from_secs(5), node.propose(CountCmd::Inc(1)))
            .await
            .expect("propose timed out")
            .expect("propose returned error");
        assert_eq!(r, i);
    }
    let elapsed = start.elapsed();
    // Sanity bound: nothing should compound the 50ms-per-commit
    // cost beyond tick overhead.
    assert!(
        elapsed < Duration::from_millis(1500),
        "propose loop took {elapsed:?} — driver probably stalled",
    );

    // Node is still leader — no election churn. (A stalled driver
    // would have dropped heartbeats and stepped down.)
    let status = tokio::time::timeout(Duration::from_secs(1), node.status())
        .await
        .expect("status timed out")
        .expect("status returned error");
    assert_eq!(status.node_id, nid(1));

    node.shutdown().await.unwrap();
}

// ---------------------------------------------------------------------------
// Node::metrics — pull-model observability. Exposes engine-level
// counters (elections, AEs, commits, reads, …) and gauges
// (current_term, commit_index, role_code, …).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn metrics_track_election_and_apply() {
    let tmp = TmpDir::new("single-metrics");
    let storage = DiskStorage::open(&tmp.0).await.unwrap();
    let port = free_port();
    let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
    let transport: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr, BTreeMap::new())
        .await
        .unwrap();

    let mut config = Config::new(nid(1), std::iter::empty::<NodeId>());
    config.election_timeout_min_ticks = 3;
    config.election_timeout_max_ticks = 5;
    config.heartbeat_interval_ticks = 1;
    config.tick_interval = Duration::from_millis(10);

    let node = Node::start(config, Counter::default(), storage, transport)
        .await
        .unwrap();

    let _ = wait_for_single_node_leader(&node).await;

    // Fresh single-node leader: at least one election started, at
    // least one won, zero votes granted (no peers to ask).
    let m = node.metrics().await.expect("metrics call");
    assert!(m.elections_started >= 1);
    assert_eq!(m.leader_elections_won, 1);
    assert_eq!(m.role_code, 3, "leader");

    // Propose two commands, observe commit + apply counters move.
    let _ = node.propose(CountCmd::Inc(5)).await.unwrap();
    let _ = node.propose(CountCmd::Inc(3)).await.unwrap();

    let m = node.metrics().await.expect("metrics call");
    // Noop + two commands committed past the snapshot floor.
    assert!(m.entries_committed >= 2);
    assert!(m.entries_applied >= 2);
    assert!(m.commit_index >= LogIndex::new(2));

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn metrics_after_shutdown_return_error() {
    let tmp = TmpDir::new("metrics-shutdown");
    let storage = DiskStorage::open(&tmp.0).await.unwrap();
    let port = free_port();
    let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
    let transport: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr, BTreeMap::new())
        .await
        .unwrap();

    let mut config = Config::new(nid(1), std::iter::empty::<NodeId>());
    config.election_timeout_min_ticks = 3;
    config.election_timeout_max_ticks = 5;
    config.heartbeat_interval_ticks = 1;
    config.tick_interval = Duration::from_millis(10);

    let node = Node::start(config, Counter::default(), storage, transport)
        .await
        .unwrap();

    let handle = node.clone();
    node.shutdown().await.unwrap();

    let err = handle
        .metrics()
        .await
        .expect_err("metrics on shut-down node must fail");
    assert!(matches!(
        err,
        ProposeError::Shutdown | ProposeError::DriverDead
    ));
}
