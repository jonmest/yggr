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

use jotun::{Config, DecodeError, DiskStorage, Node, NodeId, ProposeError, StateMachine, TcpTransport};

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
    let transport: TcpTransport<Vec<u8>> =
        TcpTransport::start(nid(1), addr, BTreeMap::new()).await.unwrap();

    let config = Config::new(nid(1), std::iter::empty::<NodeId>());
    let node = Node::start(config, Counter::default(), storage, transport).await.unwrap();
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn single_node_self_elects_and_applies_proposal() {
    let tmp = TmpDir::new("single-propose");
    let storage = DiskStorage::open(&tmp.0).await.unwrap();
    let port = free_port();
    let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
    let transport: TcpTransport<Vec<u8>> =
        TcpTransport::start(nid(1), addr, BTreeMap::new()).await.unwrap();

    let mut config = Config::new(nid(1), std::iter::empty::<NodeId>());
    // Smaller intervals so the self-election fires fast in tests.
    config.election_timeout_ticks = 3;
    config.heartbeat_interval_ticks = 1;
    config.tick_interval = Duration::from_millis(10);

    let node = Node::start(config, Counter::default(), storage, transport).await.unwrap();

    // Give the node a moment to elect itself (single-node cluster
    // self-elects on first tick after the timeout).
    tokio::time::sleep(Duration::from_millis(100)).await;

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        node.propose(CountCmd::Inc(5)),
    )
    .await
    .expect("propose timed out")
    .expect("propose returned error");
    assert_eq!(result, 5);

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        node.propose(CountCmd::Inc(3)),
    )
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
    let transport: TcpTransport<Vec<u8>> =
        TcpTransport::start(nid(1), addr, BTreeMap::new()).await.unwrap();

    let config = Config::new(nid(1), std::iter::empty::<NodeId>());
    let node = Node::start(config, Counter::default(), storage, transport).await.unwrap();
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
