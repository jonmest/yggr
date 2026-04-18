//! Three real [`jotun::Node`]s on loopback: election, replication,
//! follower redirect, leader crash & recovery, durable restart.

#![allow(
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    clippy::missing_const_for_fn,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::redundant_pattern_matching,
    clippy::collapsible_if,
    clippy::single_match_else,
    clippy::items_after_statements,
    clippy::unnecessary_get_then_check,
    clippy::manual_let_else,
    clippy::manual_assert,
    clippy::match_wildcard_for_single_variants,
    clippy::single_match
)]

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use jotun::{
    Config, DecodeError, DiskStorage, Node, NodeId, ProposeError, StateMachine, TcpTransport,
};

// ---------------------------------------------------------------------------
// A toy KV state machine whose applied state is observable from the test
// thread via an Arc<Mutex<...>> snapshot.
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
struct KvSnapshot {
    map: BTreeMap<String, String>,
    /// Count of `apply` calls. Lets tests wait for a specific number
    /// of entries to have landed.
    applied: u64,
}

#[derive(Debug)]
struct ObservableKv {
    snap: Arc<Mutex<KvSnapshot>>,
}

impl ObservableKv {
    fn new() -> (Self, Arc<Mutex<KvSnapshot>>) {
        let snap = Arc::new(Mutex::new(KvSnapshot::default()));
        (
            Self {
                snap: Arc::clone(&snap),
            },
            snap,
        )
    }
}

#[derive(Debug, Clone)]
enum KvCmd {
    Set { key: String, value: String },
}

impl StateMachine for ObservableKv {
    type Command = KvCmd;
    type Response = Option<String>;

    fn encode_command(c: &KvCmd) -> Vec<u8> {
        match c {
            KvCmd::Set { key, value } => format!("S\n{key}\n{value}").into_bytes(),
        }
    }
    fn decode_command(bytes: &[u8]) -> Result<KvCmd, DecodeError> {
        let s = std::str::from_utf8(bytes).map_err(|e| DecodeError::new(e.to_string()))?;
        let mut parts = s.splitn(3, '\n');
        let tag = parts.next().ok_or_else(|| DecodeError::new("empty"))?;
        match tag {
            "S" => {
                let key = parts.next().ok_or_else(|| DecodeError::new("missing key"))?;
                let value = parts
                    .next()
                    .ok_or_else(|| DecodeError::new("missing value"))?;
                Ok(KvCmd::Set {
                    key: key.into(),
                    value: value.into(),
                })
            }
            other => Err(DecodeError::new(format!("unknown tag: {other}"))),
        }
    }
    fn apply(&mut self, cmd: KvCmd) -> Option<String> {
        let mut snap = self.snap.lock().unwrap();
        snap.applied += 1;
        match cmd {
            KvCmd::Set { key, value } => snap.map.insert(key, value),
        }
    }
}

// ---------------------------------------------------------------------------
// Test harness: three Nodes, each with its own data dir + loopback port.
// ---------------------------------------------------------------------------

struct TmpDir(PathBuf);

impl TmpDir {
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("jotun-multi-{label}-{pid}-{n}"));
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

#[allow(dead_code)] // addr read for diagnostics; applied via Arc snapshots
struct TestNode {
    id: NodeId,
    addr: SocketAddr,
    data_dir: TmpDir,
    node: Option<Node<ObservableKv>>,
    snap: Arc<Mutex<KvSnapshot>>,
}

impl TestNode {
    async fn start(
        id: NodeId,
        addr: SocketAddr,
        data_dir: TmpDir,
        peers: BTreeMap<NodeId, SocketAddr>,
    ) -> Self {
        let (sm, snap) = ObservableKv::new();
        let storage = DiskStorage::open(&data_dir.0).await.unwrap();
        let transport: TcpTransport<Vec<u8>> = TcpTransport::start(id, addr, peers).await.unwrap();

        let mut config = Config::new(id, sm_peer_ids(&transport_peer_set_for(id)));
        // Tuned for test speed. Heartbeat every tick; election within
        // a few. Tick every 20ms → elections in ~100ms.
        config.election_timeout_min_ticks = 5;
        config.election_timeout_max_ticks = 15;
        config.heartbeat_interval_ticks = 1;
        config.tick_interval = Duration::from_millis(20);

        let node = Node::start(config, sm, storage, transport).await.unwrap();
        Self {
            id,
            addr,
            data_dir,
            node: Some(node),
            snap,
        }
    }

    fn node(&self) -> &Node<ObservableKv> {
        self.node.as_ref().expect("node live")
    }

    async fn stop(&mut self) {
        if let Some(n) = self.node.take() {
            let _ = n.shutdown().await;
            // Give background tasks a tick to drain.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn get(&self, key: &str) -> Option<String> {
        self.snap.lock().unwrap().map.get(key).cloned()
    }

    #[allow(dead_code)]
    fn applied(&self) -> u64 {
        self.snap.lock().unwrap().applied
    }
}

/// Static peer-id list for the 3-node cluster.
fn all_ids() -> [NodeId; 3] {
    [nid(1), nid(2), nid(3)]
}

fn transport_peer_set_for(me: NodeId) -> Vec<NodeId> {
    all_ids()
        .into_iter()
        .filter(|&p| p != me)
        .collect()
}

fn sm_peer_ids(v: &[NodeId]) -> Vec<NodeId> {
    v.to_vec()
}

/// Bring up a 3-node cluster. Returns the three `TestNode`s and the
/// addr map so callers can reconstruct a restarted node against the
/// same cluster topology.
async fn start_three_node_cluster() -> (Vec<TestNode>, BTreeMap<NodeId, SocketAddr>) {
    let ids = all_ids();
    let ports: Vec<u16> = (0..3).map(|_| free_port()).collect();
    let addrs: BTreeMap<NodeId, SocketAddr> = ids
        .iter()
        .zip(ports.iter())
        .map(|(&id, &p)| (id, SocketAddr::from((Ipv4Addr::LOCALHOST, p))))
        .collect();

    let mut nodes = Vec::new();
    for &id in &ids {
        let tmp = TmpDir::new(&format!("n{}", id.get()));
        let peers: BTreeMap<NodeId, SocketAddr> = addrs
            .iter()
            .filter(|(pid, _)| **pid != id)
            .map(|(pid, addr)| (*pid, *addr))
            .collect();
        nodes.push(TestNode::start(id, addrs[&id], tmp, peers).await);
    }
    (nodes, addrs)
}

/// Poll `predicate` every 20ms until it returns true or `deadline`
/// elapses. Panics with `msg` on timeout.
async fn wait_until<F: FnMut() -> bool>(mut predicate: F, deadline: Duration, msg: &str) {
    let start = Instant::now();
    loop {
        if predicate() {
            return;
        }
        if start.elapsed() >= deadline {
            panic!("timeout after {deadline:?}: {msg}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Propose against each node in turn. If the result is `NotLeader`
/// with a hint, retry against that node. Returns on first success.
/// Used to avoid racing an election.
async fn propose_anywhere(
    nodes: &[TestNode],
    cmd: KvCmd,
    deadline: Duration,
) -> Result<Option<String>, String> {
    let start = Instant::now();
    loop {
        for n in nodes {
            match n.node().propose(cmd.clone()).await {
                Ok(r) => return Ok(r),
                Err(ProposeError::NotLeader { leader_hint }) => {
                    if let Some(target) = nodes.iter().find(|tn| tn.id == leader_hint) {
                        match target.node().propose(cmd.clone()).await {
                            Ok(r) => return Ok(r),
                            Err(_) => {}
                        }
                    }
                }
                Err(ProposeError::NoLeader) => {}
                Err(other) => return Err(format!("{other}")),
            }
        }
        if start.elapsed() >= deadline {
            return Err("no node accepted the proposal".into());
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn three_nodes_elect_a_leader_and_replicate_one_proposal() {
    let (mut nodes, _addrs) = start_three_node_cluster().await;

    // Propose through any node; harness retries against the hinted
    // leader if we land on a follower.
    let resp = propose_anywhere(
        &nodes,
        KvCmd::Set {
            key: "hello".into(),
            value: "world".into(),
        },
        Duration::from_secs(5),
    )
    .await
    .expect("propose failed");
    assert_eq!(resp, None, "set on empty map returns None");

    // Every node's applied count eventually ≥ 1 (the Set). No-ops
    // and ConfigChanges don't count (filtered by drain_apply).
    for n in &nodes {
        let id = n.id;
        let snap = Arc::clone(&n.snap);
        wait_until(
            move || snap.lock().unwrap().applied >= 1,
            Duration::from_secs(5),
            &format!("node {id} never applied the command"),
        )
        .await;
    }

    // And the value is visible on every node.
    for n in &nodes {
        assert_eq!(n.get("hello"), Some("world".into()), "node {}", n.id);
    }

    for n in &mut nodes {
        n.stop().await;
    }
}

#[tokio::test]
async fn follower_redirects_propose_to_leader() {
    let (mut nodes, _addrs) = start_three_node_cluster().await;

    // First ensure the cluster has a stable leader — election under
    // randomized timeouts can take a few rounds when all three nodes
    // come up at the same instant.
    propose_anywhere(
        &nodes,
        KvCmd::Set {
            key: "warmup".into(),
            value: "1".into(),
        },
        Duration::from_secs(5),
    )
    .await
    .expect("cluster never elected a leader");

    // Now probe every node. Exactly one must accept directly (the
    // leader); the others must redirect with a hint.
    let mut leader_idx = None;
    let mut leader_hint_seen = false;
    for (i, n) in nodes.iter().enumerate() {
        match n
            .node()
            .propose(KvCmd::Set {
                key: "probe".into(),
                value: "1".into(),
            })
            .await
        {
            Ok(_) => {
                leader_idx = Some(i);
            }
            Err(ProposeError::NotLeader { .. }) => {
                leader_hint_seen = true;
            }
            Err(ProposeError::NoLeader) => {}
            Err(e) => panic!("unexpected error: {e}"),
        }
    }

    assert!(leader_idx.is_some(), "no node accepted the proposal");
    assert!(
        leader_hint_seen,
        "at least one follower should have redirected with a hint",
    );

    for n in &mut nodes {
        n.stop().await;
    }
}

#[tokio::test]
async fn cluster_continues_after_leader_crash() {
    let (mut nodes, _addrs) = start_three_node_cluster().await;

    // Commit something through the original cluster so we know it's healthy.
    propose_anywhere(
        &nodes,
        KvCmd::Set {
            key: "before".into(),
            value: "1".into(),
        },
        Duration::from_secs(5),
    )
    .await
    .expect("initial propose failed");

    // Find and crash the leader.
    let mut leader_idx = None;
    for (i, n) in nodes.iter().enumerate() {
        if let Ok(_) = n
            .node()
            .propose(KvCmd::Set {
                key: "probe".into(),
                value: "1".into(),
            })
            .await
        {
            leader_idx = Some(i);
            break;
        }
    }
    let leader_idx = leader_idx.expect("no leader");
    nodes[leader_idx].stop().await;

    // The surviving two form a majority. Propose through them.
    let survivors: Vec<&TestNode> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != leader_idx)
        .map(|(_, n)| n)
        .collect();

    let start = Instant::now();
    let mut ok = false;
    while start.elapsed() < Duration::from_secs(10) {
        for n in &survivors {
            match n
                .node()
                .propose(KvCmd::Set {
                    key: "after".into(),
                    value: "2".into(),
                })
                .await
            {
                Ok(_) => {
                    ok = true;
                    break;
                }
                Err(ProposeError::NotLeader { leader_hint }) => {
                    if let Some(target) = survivors.iter().find(|tn| tn.id == leader_hint) {
                        if let Ok(_) = target
                            .node()
                            .propose(KvCmd::Set {
                                key: "after".into(),
                                value: "2".into(),
                            })
                            .await
                        {
                            ok = true;
                            break;
                        }
                    }
                }
                Err(_) => {}
            }
        }
        if ok {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ok, "surviving majority failed to elect and accept proposal");

    for n in &mut nodes {
        n.stop().await;
    }
}

#[tokio::test]
async fn cluster_survives_full_restart_from_disk() {
    // Boot a 3-node cluster, commit an entry, stop all three, then
    // restart all three against the same data dirs + ports. The
    // persisted log carries the committed entry; after re-election
    // the new leader's commit propagation drives every node's state
    // machine to reapply it.
    //
    // A single-node restart in isolation CAN'T recover applied state
    // by design — Raft's commit_index is volatile (Figure 2), so a
    // node coming up alone doesn't know what's committed until a
    // leader tells it. That's correct behaviour, not a bug.
    let ids = all_ids();
    let ports: Vec<u16> = (0..3).map(|_| free_port()).collect();
    let addrs: BTreeMap<NodeId, SocketAddr> = ids
        .iter()
        .zip(ports.iter())
        .map(|(&id, &p)| (id, SocketAddr::from((Ipv4Addr::LOCALHOST, p))))
        .collect();

    // Data dirs survive across both lives; clean them up at the end.
    let dir_paths: Vec<PathBuf> = (0..3)
        .map(|i| {
            let d = TmpDir::new(&format!("full-restart-n{}", i + 1));
            let p = d.0.clone();
            std::mem::forget(d); // skip cleanup; we handle it
            p
        })
        .collect();

    async fn boot_node(
        id: NodeId,
        addr: SocketAddr,
        dir: PathBuf,
        peers: BTreeMap<NodeId, SocketAddr>,
    ) -> (Node<ObservableKv>, Arc<Mutex<KvSnapshot>>) {
        let (sm, snap) = ObservableKv::new();
        let storage = DiskStorage::open(&dir).await.unwrap();
        let transport: TcpTransport<Vec<u8>> =
            TcpTransport::start(id, addr, peers).await.unwrap();
        let mut config = Config::new(
            id,
            ids_other_than(id).into_iter().collect::<Vec<_>>(),
        );
        config.election_timeout_min_ticks = 5;
        config.election_timeout_max_ticks = 15;
        config.heartbeat_interval_ticks = 1;
        config.tick_interval = Duration::from_millis(20);
        let node = Node::start(config, sm, storage, transport).await.unwrap();
        (node, snap)
    }

    // --- Life 1: bring the cluster up, commit something, stop. ---
    let peer_map = |me: NodeId| -> BTreeMap<NodeId, SocketAddr> {
        addrs
            .iter()
            .filter(|(pid, _)| **pid != me)
            .map(|(pid, addr)| (*pid, *addr))
            .collect()
    };

    let mut first_life: Vec<(Node<ObservableKv>, Arc<Mutex<KvSnapshot>>)> = Vec::new();
    for (i, &id) in ids.iter().enumerate() {
        first_life.push(boot_node(id, addrs[&id], dir_paths[i].clone(), peer_map(id)).await);
    }

    // Wrap in minimal TestNode-alikes for propose_anywhere.
    let wrappers: Vec<TestNode> = ids
        .iter()
        .enumerate()
        .map(|(i, &id)| TestNode {
            id,
            addr: addrs[&id],
            data_dir: TmpDir(std::env::temp_dir().join("placeholder-never-used")),
            node: Some(first_life[i].0.clone()),
            snap: Arc::clone(&first_life[i].1),
        })
        .collect();

    propose_anywhere(
        &wrappers,
        KvCmd::Set {
            key: "durable".into(),
            value: "yes".into(),
        },
        Duration::from_secs(5),
    )
    .await
    .expect("initial propose failed");

    // Don't run wrappers' TmpDir drops (the paths are fake).
    for w in wrappers {
        std::mem::forget(w.data_dir);
        drop(w.node);
        drop(w.snap);
    }
    for (n, _) in first_life {
        let _ = n.shutdown().await;
    }
    // Give background tasks time to release their ports.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Life 2: reopen from disk on fresh ports. Same data dirs. ---
    let new_ports: Vec<u16> = (0..3).map(|_| free_port()).collect();
    let new_addrs: BTreeMap<NodeId, SocketAddr> = ids
        .iter()
        .zip(new_ports.iter())
        .map(|(&id, &p)| (id, SocketAddr::from((Ipv4Addr::LOCALHOST, p))))
        .collect();
    let new_peer_map = |me: NodeId| -> BTreeMap<NodeId, SocketAddr> {
        new_addrs
            .iter()
            .filter(|(pid, _)| **pid != me)
            .map(|(pid, addr)| (*pid, *addr))
            .collect()
    };

    let mut second_life: Vec<(Node<ObservableKv>, Arc<Mutex<KvSnapshot>>)> = Vec::new();
    for (i, &id) in ids.iter().enumerate() {
        second_life.push(
            boot_node(id, new_addrs[&id], dir_paths[i].clone(), new_peer_map(id)).await,
        );
    }

    // Every node's state machine eventually sees the persisted entry.
    // The new cluster elects a leader, commit advances via heartbeat,
    // drain_apply replays the persisted Command entries.
    for (_, snap) in &second_life {
        let snap = Arc::clone(snap);
        wait_until(
            move || snap.lock().unwrap().map.get("durable").is_some(),
            Duration::from_secs(10),
            "restarted cluster never re-applied the persisted entry",
        )
        .await;
    }
    for (_, snap) in &second_life {
        assert_eq!(snap.lock().unwrap().map.get("durable"), Some(&"yes".into()));
    }

    for (n, _) in second_life {
        let _ = n.shutdown().await;
    }

    for p in &dir_paths {
        let _ = std::fs::remove_dir_all(p);
    }
}

fn ids_other_than(me: NodeId) -> impl IntoIterator<Item = NodeId> {
    all_ids().into_iter().filter(move |&p| p != me)
}
