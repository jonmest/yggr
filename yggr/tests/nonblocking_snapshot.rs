//! Driver-responsiveness guarantees while `StateMachine::snapshot()`
//! runs slowly. The runtime must not block its main driver loop on
//! snapshot serialization — `status()` should return promptly and
//! ticks should keep processing even if `snapshot()` sleeps for a
//! while. See `feature/nonblocking-snapshots`.

#![allow(clippy::expect_used, clippy::missing_const_for_fn, clippy::unwrap_used)]

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use proptest::prelude::*;
use tokio::sync::mpsc;
use yggr::{
    Bootstrap, Config, DecodeError, Node, NodeId, StateMachine, Storage, StoredHardState,
    StoredSnapshot, Transport,
};
use yggr_core::{Incoming, LogEntry, LogIndex, Message};

// ---------------------------------------------------------------------------
// Minimal counter state machine whose `snapshot()` sleeps.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct SlowSnapCounter {
    value: u64,
    snapshot_delay: Duration,
    snapshot_started: Arc<AtomicUsize>,
    snapshot_completed: Arc<AtomicUsize>,
}

impl SlowSnapCounter {
    fn new(
        snapshot_delay: Duration,
        started: Arc<AtomicUsize>,
        completed: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            value: 0,
            snapshot_delay,
            snapshot_started: started,
            snapshot_completed: completed,
        }
    }
}

#[derive(Debug, Clone)]
struct IncCmd(u64);

impl StateMachine for SlowSnapCounter {
    type Command = IncCmd;
    type Response = u64;

    fn encode_command(c: &IncCmd) -> Vec<u8> {
        c.0.to_le_bytes().to_vec()
    }

    fn decode_command(bytes: &[u8]) -> Result<IncCmd, DecodeError> {
        let arr: [u8; 8] = bytes
            .try_into()
            .map_err(|_| DecodeError::new("expected 8 bytes"))?;
        Ok(IncCmd(u64::from_le_bytes(arr)))
    }

    fn apply(&mut self, cmd: IncCmd) -> u64 {
        self.value += cmd.0;
        self.value
    }

    fn snapshot(&self) -> Result<Vec<u8>, yggr::SnapshotError> {
        self.snapshot_started.fetch_add(1, Ordering::SeqCst);
        // Synchronous sleep — simulates a big serialize step. A real
        // state machine can't `await` here; the whole point of this
        // feature is that the driver stays responsive in spite of
        // that.
        std::thread::sleep(self.snapshot_delay);
        self.snapshot_completed.fetch_add(1, Ordering::SeqCst);
        Ok(self.value.to_le_bytes().to_vec())
    }

    fn restore(&mut self, bytes: Vec<u8>) {
        let arr: [u8; 8] = bytes.try_into().expect("snapshot bytes are 8 bytes");
        self.value = u64::from_le_bytes(arr);
    }
}

// ---------------------------------------------------------------------------
// In-memory storage + transport stubs (single-node).
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct MemoryStorageInner<C> {
    hard_state: Option<StoredHardState>,
    snapshot: Option<StoredSnapshot>,
    log: Vec<LogEntry<C>>,
}

#[derive(Debug, Clone, Default)]
struct SharedStorage<C> {
    inner: Arc<Mutex<MemoryStorageInner<C>>>,
}

impl<C: Send + Clone + 'static> Storage<C> for SharedStorage<C> {
    type Error = Infallible;

    async fn recover(&mut self) -> Result<yggr::storage::RecoveredState<C>, Self::Error> {
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

    async fn append_log(&mut self, entries: Vec<LogEntry<C>>) -> Result<(), Self::Error> {
        let mut g = self.inner.lock().unwrap();
        let mut truncated = false;
        for entry in entries {
            let i = entry.id.index.get();
            let snap_floor = g
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
                let keep = local_idx.min(g.log.len());
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

struct NullTransport {
    inbound: mpsc::Receiver<Incoming<Vec<u8>>>,
}

impl NullTransport {
    fn new() -> (Self, mpsc::Sender<Incoming<Vec<u8>>>) {
        let (tx, rx) = mpsc::channel(16);
        (Self { inbound: rx }, tx)
    }
}

impl Transport<Vec<u8>> for NullTransport {
    type Error = Infallible;

    async fn send(&self, _to: NodeId, _message: Message<Vec<u8>>) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn recv(&mut self) -> Option<Incoming<Vec<u8>>> {
        self.inbound.recv().await
    }
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

fn nid(n: u64) -> NodeId {
    NodeId::new(n).unwrap()
}

fn single_node_cfg(threshold: u64) -> Config {
    let mut cfg = Config::new(nid(1), std::iter::empty::<NodeId>());
    cfg.election_timeout_min_ticks = 3;
    cfg.election_timeout_max_ticks = 4;
    cfg.heartbeat_interval_ticks = 1;
    cfg.tick_interval = Duration::from_millis(10);
    cfg.bootstrap = Bootstrap::NewCluster {
        members: vec![nid(1)],
    };
    cfg.snapshot_hint_threshold_entries = threshold;
    cfg
}

async fn wait_for_leader<S: StateMachine>(node: &Node<S>) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let s = node.status().await.expect("status");
        if matches!(s.role, yggr::Role::Leader) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node never became leader"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

/// Core guarantee: while `snapshot()` is running, `status()` must
/// still return in well under the snapshot delay. Before this feature
/// the driver blocks on the snapshot reply; `status()` would queue
/// behind it and take roughly the full snapshot delay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_remains_responsive_during_slow_snapshot() {
    const SNAPSHOT_DELAY: Duration = Duration::from_millis(800);
    const STATUS_BUDGET: Duration = Duration::from_millis(150);

    let started = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let storage = SharedStorage::<Vec<u8>>::default();
    let (transport, _tx) = NullTransport::new();

    let cfg = single_node_cfg(1);
    let sm = SlowSnapCounter::new(SNAPSHOT_DELAY, Arc::clone(&started), Arc::clone(&completed));
    let node = Node::start(cfg, sm, storage, transport).await.unwrap();

    wait_for_leader(&node).await;

    // The initial leader-noop entry will already have triggered one
    // snapshot at threshold=1. Wait for that to finish, then reset
    // counters so we measure ONLY the snapshot kicked off by our
    // explicit propose below.
    let settle = tokio::time::Instant::now() + Duration::from_secs(3);
    while completed.load(Ordering::SeqCst) < started.load(Ordering::SeqCst) {
        assert!(
            tokio::time::Instant::now() < settle,
            "initial snapshot never completed"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    started.store(0, Ordering::SeqCst);
    completed.store(0, Ordering::SeqCst);

    // Commit an entry → engine emits SnapshotHint → driver kicks off
    // a (slow) snapshot. Don't await here — the bug this test guards
    // against would cause propose itself to block behind the snapshot,
    // which in turn blocks status().
    let proposer = {
        let node = node.clone();
        tokio::spawn(async move { node.propose(IncCmd(1)).await })
    };

    // Wait until the state machine has actually started snapshotting.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while started.load(Ordering::SeqCst) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "snapshot never started"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    // Snapshot is in flight. The driver MUST stay responsive.
    assert_eq!(
        completed.load(Ordering::SeqCst),
        0,
        "precondition: snapshot should still be running"
    );

    // Issue several status() calls and confirm each returns quickly.
    for _ in 0..3 {
        let t0 = tokio::time::Instant::now();
        let _ = node.status().await.expect("status returns");
        let elapsed = t0.elapsed();
        assert!(
            elapsed < STATUS_BUDGET,
            "status() took {elapsed:?}, expected < {STATUS_BUDGET:?}; driver is blocked on snapshot"
        );
    }
    let _ = proposer.await.expect("proposer task").unwrap();

    // Eventually the snapshot completes and state advances.
    let deadline = tokio::time::Instant::now() + SNAPSHOT_DELAY * 3;
    while completed.load(Ordering::SeqCst) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "snapshot never completed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    node.shutdown().await.unwrap();
}

/// Overlapping snapshot hints (e.g. many commits in quick succession)
/// must not stack up into concurrent snapshot calls. The runtime
/// should serialise snapshot requests and drop hints that arrive
/// while one is already in flight — otherwise a slow snapshot
/// combined with heavy traffic would fan out unbounded work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlapping_snapshot_hints_do_not_stack() {
    const SNAPSHOT_DELAY: Duration = Duration::from_millis(300);

    let started = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let storage = SharedStorage::<Vec<u8>>::default();
    let (transport, _tx) = NullTransport::new();

    let cfg = single_node_cfg(1);
    let sm = SlowSnapCounter::new(SNAPSHOT_DELAY, Arc::clone(&started), Arc::clone(&completed));
    let node = Node::start(cfg, sm, storage, transport).await.unwrap();

    wait_for_leader(&node).await;

    // Let the initial leader-noop snapshot finish, then reset.
    let settle = tokio::time::Instant::now() + Duration::from_secs(3);
    while completed.load(Ordering::SeqCst) < started.load(Ordering::SeqCst) {
        assert!(
            tokio::time::Instant::now() < settle,
            "initial snapshot never completed"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    started.store(0, Ordering::SeqCst);
    completed.store(0, Ordering::SeqCst);

    // Kick off one proposal to start a snapshot. Don't wait for it
    // (its apply queues behind the in-flight snapshot).
    let p0 = {
        let node = node.clone();
        tokio::spawn(async move { node.propose(IncCmd(1)).await })
    };
    // Wait until the snapshot is in flight.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while started.load(Ordering::SeqCst) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "first snapshot never started"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        completed.load(Ordering::SeqCst),
        0,
        "precondition: snapshot 1 should still be running"
    );

    // Fire more proposals while the first snapshot is still running.
    // Each of their commits would normally trigger a fresh
    // SnapshotHint. The driver must coalesce them — only one
    // snapshot should be in flight at any time.
    let mut extras = Vec::new();
    for i in 0..5u64 {
        let node = node.clone();
        extras.push(tokio::spawn(
            async move { node.propose(IncCmd(i + 2)).await },
        ));
    }

    // During the window where snapshot 1 is in flight, started must
    // stay at exactly 1.
    while completed.load(Ordering::SeqCst) == 0 {
        let s = started.load(Ordering::SeqCst);
        assert!(
            s <= 1,
            "overlapping hints created concurrent snapshot; started={s}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Let everything settle.
    let _ = p0.await.unwrap();
    for h in extras {
        let _ = h.await.unwrap();
    }

    node.shutdown().await.unwrap();
}

// Proptest: regardless of how many proposals we push and how slow
// the snapshot is (within bounded ranges), `status()` response time
// stays well below the snapshot delay while a snapshot is in flight.
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 8,
        .. ProptestConfig::default()
    })]
    #[test]
    fn status_responsive_under_varied_snapshot_delays(
        snapshot_ms in 200u64..600,
        proposals in 1usize..5,
    ) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let started = Arc::new(AtomicUsize::new(0));
            let completed = Arc::new(AtomicUsize::new(0));
            let storage = SharedStorage::<Vec<u8>>::default();
            let (transport, _tx) = NullTransport::new();
            let cfg = single_node_cfg(1);
            let sm = SlowSnapCounter::new(
                Duration::from_millis(snapshot_ms),
                Arc::clone(&started),
                Arc::clone(&completed),
            );
            let node = Node::start(cfg, sm, storage, transport).await.unwrap();
            wait_for_leader(&node).await;
            for i in 0..proposals as u64 {
                let _ = node.propose(IncCmd(i + 1)).await.unwrap();
            }

            // Wait until a snapshot is in flight.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            while started.load(Ordering::SeqCst) == 0
                || completed.load(Ordering::SeqCst) == started.load(Ordering::SeqCst)
            {
                if tokio::time::Instant::now() >= deadline {
                    // No in-flight snapshot ever observed; nothing to
                    // assert. (Possible when the snapshot finishes
                    // before we look — this only happens if
                    // `snapshot_ms` is tiny, excluded by the strategy.)
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }

            let budget = Duration::from_millis(snapshot_ms / 4);
            let t0 = tokio::time::Instant::now();
            let _ = node.status().await.expect("status");
            let elapsed = t0.elapsed();
            prop_assert!(
                elapsed < budget,
                "status() took {:?} with snapshot_ms={}", elapsed, snapshot_ms,
            );

            node.shutdown().await.unwrap();
            Ok(())
        })?;
    }
}
