//! End-to-end [`jotun::Node`] tests. Real `DiskStorage` +
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use jotun::{
    Config, DecodeError, DiskStorage, Node, NodeId, ProposeError, StateMachine, TcpTransport,
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
        let path = std::env::temp_dir().join(format!("jotun-node-test-{label}-{pid}-{n}"));
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
    let l = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
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
    node.shutdown().await.unwrap();

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

    // Give the node a moment to elect itself (single-node cluster
    // self-elects on first tick after the timeout).
    tokio::time::sleep(Duration::from_millis(100)).await;

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
    // Give the driver a tick to finish exiting.
    tokio::time::sleep(Duration::from_millis(50)).await;

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
    use jotun_core::{RecoveredHardState, RecoveredSnapshot, StaticEnv};

    // Seed a DiskStorage with a specific hard state that represents
    // "we voted for node 2 in term 7, then crashed".
    let tmp = TmpDir::new("recover-voted-for");
    {
        let mut storage = DiskStorage::open(&tmp.0).await.unwrap();
        <DiskStorage as jotun::Storage<Vec<u8>>>::persist_hard_state(
            &mut storage,
            jotun::StoredHardState {
                current_term: jotun_core::Term::new(7),
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
    let recovered = <DiskStorage as jotun::Storage<Vec<u8>>>::recover(&mut storage)
        .await
        .unwrap();

    let hs = recovered.hard_state.expect("hard state was persisted");
    assert_eq!(hs.current_term.get(), 7);
    assert_eq!(hs.voted_for, Some(nid(2)));

    let env: Box<dyn jotun_core::Env> = Box::new(StaticEnv(10));
    let mut engine: jotun_core::Engine<Vec<u8>> =
        jotun_core::Engine::new(nid(1), vec![nid(2), nid(3)], env, 3);

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

    // Wait for self-election (single-node cluster).
    tokio::time::sleep(Duration::from_millis(100)).await;

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
