//! The top-level simulation driver.
//!
//! Owns every node harness, the in-flight message queue, the shared
//! RNG, and the running safety-checker. Each [`Cluster::step`] asks
//! the scheduler for one event, applies it, then runs every invariant.
//! A violation panics with the full schedule-so-far so a fuzz failure
//! is instantly a regression test.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use jotun_core::{Action, ConfigChange, Engine, Event as CoreEvent, Incoming, NodeId};
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};

use crate::env::SharedRng;
use crate::harness::{NodeHarness, PersistOrderingError};
use crate::invariants::{SafetyChecker, SafetyViolation};
use crate::network::{MessageId, Network};
use crate::schedule::Event;

/// The full simulation state: nodes, network, RNG, safety ledger,
/// plus the recorded schedule that a failing run prints back.
pub struct Cluster<C: Clone + PartialEq + 'static> {
    nodes: BTreeMap<NodeId, NodeHarness<C>>,
    network: Network<C>,
    rng: SharedRng,
    checker: SafetyChecker<C>,
    clock: u64,
    /// Every event the scheduler has applied so far, in order.
    /// Printed on a safety violation so a seed reproduces the failure
    /// exactly — or the user can paste the schedule into a regression.
    history: Vec<HistoryEntry<C>>,
    /// Knobs that gate which event classes the scheduler will pick.
    policy: Policy<C>,
    /// Whether this cluster's engines run with §9.6 pre-vote on.
    /// Threaded through `add_node` so nodes added mid-run match.
    pre_vote: bool,
}

impl<C: Clone + PartialEq + std::fmt::Debug + 'static> std::fmt::Debug for Cluster<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cluster")
            .field("clock", &self.clock)
            .field("nodes", &self.nodes.keys().collect::<Vec<_>>())
            .field("in_flight", &self.network.len())
            .field("history_len", &self.history.len())
            .finish()
    }
}

/// A single recorded scheduler decision. Displayed by
/// [`Cluster::format_schedule`] when panicking on a violation.
#[derive(Debug, Clone)]
pub(crate) struct HistoryEntry<C> {
    pub(crate) at_clock: u64,
    pub(crate) event: Event<C>,
}

/// Scheduler knobs. [`Policy::happy`] is the conservative baseline for
/// the first smoke tests; later steps turn on crashes and partitions.
#[derive(Debug, Clone)]
pub(crate) struct Policy<C> {
    pub(crate) allow_drop: bool,
    pub(crate) allow_reorder: bool,
    pub(crate) allow_partition: bool,
    pub(crate) allow_crash: bool,
    /// Chance a PersistHardState/PersistLogEntries waits for an
    /// explicit Flush rather than being applied immediately. `0.0`
    /// means every write flushes the moment it's observed — the
    /// default for the non-crash policy.
    pub(crate) partial_flush_probability: f64,
    /// When a proposal is scheduled, what command to supply.
    pub(crate) proposal_command: Option<C>,
}

impl<C: Clone> Policy<C> {
    /// No crashes, no partitions, no drops. Used by the smoke test and
    /// the first proptest layer.
    pub(crate) fn happy(proposal_command: Option<C>) -> Self {
        Self {
            allow_drop: false,
            allow_reorder: false,
            allow_partition: false,
            allow_crash: false,
            partial_flush_probability: 0.0,
            proposal_command,
        }
    }

    /// Adversarial policy: everything on. Messages can drop or
    /// reorder, nodes can crash mid-fsync and recover from a partial
    /// write, the network can partition. Liveness isn't guaranteed
    /// under this policy — only safety.
    #[cfg(test)]
    pub(crate) fn chaos(proposal_command: Option<C>) -> Self {
        Self {
            allow_drop: true,
            allow_reorder: true,
            allow_partition: true,
            allow_crash: true,
            partial_flush_probability: 0.3,
            proposal_command,
        }
    }
}

impl<C: Clone + PartialEq + std::fmt::Debug + 'static> Cluster<C> {
    /// Build a cluster of `n_nodes` nodes (ids 1..=n_nodes) with the
    /// same heartbeat interval and a single RNG seeded from `seed`.
    /// Pre-vote is off; use [`Cluster::with_pre_vote`] to exercise
    /// the §9.6 path.
    #[must_use]
    pub fn new(seed: u64, n_nodes: u64) -> Self {
        Self::build(seed, n_nodes, false)
    }

    /// Build a cluster with §9.6 pre-vote enabled on every node.
    /// Chaos tests exercise both paths so that disruption-avoidance
    /// doesn't regress on adversarial schedules.
    #[must_use]
    pub fn with_pre_vote(seed: u64, n_nodes: u64) -> Self {
        Self::build(seed, n_nodes, true)
    }

    fn build(seed: u64, n_nodes: u64, pre_vote: bool) -> Self {
        assert!(n_nodes >= 1, "cluster must have at least one node");
        let heartbeat = 3_u64;
        let rng: SharedRng = Arc::new(Mutex::new(StdRng::seed_from_u64(seed)));

        let ids: Vec<NodeId> = (1..=n_nodes)
            .map(|i| NodeId::new(i).expect("1..=n_nodes is non-zero"))
            .collect();

        let mut nodes = BTreeMap::new();
        for &id in &ids {
            let peers: Vec<NodeId> = ids.iter().copied().filter(|p| *p != id).collect();
            nodes.insert(
                id,
                NodeHarness::with_pre_vote(id, peers, heartbeat, Arc::clone(&rng), pre_vote),
            );
        }

        Self {
            nodes,
            network: Network::new(),
            rng,
            checker: SafetyChecker::new(),
            clock: 0,
            history: Vec::new(),
            policy: Policy::<C>::happy(None),
            pre_vote,
        }
    }

    /// Override the scheduler policy. `pub(crate)` so the public
    /// surface stays minimal; tests reach in through helpers.
    #[cfg(test)]
    pub(crate) fn set_policy(&mut self, policy: Policy<C>) {
        self.policy = policy;
    }

    /// Forcibly crash a node outside the scheduler. Test-only: real
    /// runs let the scheduler pick events.
    #[cfg(test)]
    pub(crate) fn crash_for_test(&mut self, id: NodeId) {
        if let Some(h) = self.nodes.get_mut(&id) {
            h.crash();
        }
    }

    /// Forcibly recover a node outside the scheduler. Test-only.
    #[cfg(test)]
    pub(crate) fn recover_for_test(&mut self, id: NodeId) {
        if let Some(h) = self.nodes.get_mut(&id) {
            h.recover(Arc::clone(&self.rng));
        }
    }

    /// Advance the cluster by one scheduler-picked event, apply every
    /// invariant, and panic if any fires.
    pub fn step(&mut self) {
        self.clock += 1;
        let event = self.pick_event();
        self.apply_event(event.clone());
        self.history.push(HistoryEntry {
            at_clock: self.clock,
            event,
        });
        if let Err(violation) = self.checker.check(&self.nodes) {
            panic!(
                "safety violation: {:?}\nschedule so far ({} events):\n{}",
                violation,
                self.history.len(),
                self.format_schedule(),
            );
        }
    }

    /// Tell node `id` to take a snapshot at its current `last_applied`,
    /// using the supplied bytes (real systems would serialize state
    /// machine state; the harness can hand any opaque bytes through).
    /// Recorded in history; passes through the safety checker.
    pub fn snapshot_to(&mut self, id: NodeId, bytes: Vec<u8>) {
        self.clock += 1;
        let last_applied = self
            .nodes
            .get(&id)
            .and_then(|h| h.engine.as_ref())
            .map_or(jotun_core::LogIndex::ZERO, Engine::commit_index);
        let event = Event::Snapshot(id, last_applied, bytes);
        self.apply_event(event.clone());
        self.history.push(HistoryEntry {
            at_clock: self.clock,
            event,
        });
        if let Err(violation) = self.checker.check(&self.nodes) {
            panic!(
                "safety violation: {:?}\nschedule so far ({} events):\n{}",
                violation,
                self.history.len(),
                self.format_schedule(),
            );
        }
    }

    /// Inject a single membership change at `id` directly, bypassing the
    /// random scheduler. The resulting event is recorded in history,
    /// passes through the safety checker, and otherwise behaves exactly
    /// like a scheduler-picked `ProposeConfigChange`. Used by scenario
    /// tests that want to script membership changes deterministically.
    pub fn propose_config_change_to(&mut self, id: NodeId, change: ConfigChange) {
        self.clock += 1;
        let event = Event::ProposeConfigChange(id, change);
        self.apply_event(event.clone());
        self.history.push(HistoryEntry {
            at_clock: self.clock,
            event,
        });
        if let Err(violation) = self.checker.check(&self.nodes) {
            panic!(
                "safety violation: {:?}\nschedule so far ({} events):\n{}",
                violation,
                self.history.len(),
                self.format_schedule(),
            );
        }
    }

    /// Add a brand-new node to the cluster harness. Used by scenario tests
    /// to model an operator bringing up a node and then issuing an
    /// `AddPeer` config change against the cluster. The new node starts
    /// with knowledge of every existing node as its peer set; the live
    /// nodes still need a committed `AddPeer(id)` to start replicating
    /// to it.
    pub fn add_node(&mut self, id: NodeId) {
        if self.nodes.contains_key(&id) {
            return;
        }
        let peers: Vec<NodeId> = self.nodes.keys().copied().collect();
        let heartbeat = 3_u64;
        let harness =
            NodeHarness::with_pre_vote(id, peers, heartbeat, Arc::clone(&self.rng), self.pre_vote);
        self.nodes.insert(id, harness);
    }

    /// Drive the cluster until `predicate` returns `true` or
    /// `max_steps` is reached. Returns the number of steps taken.
    pub fn run_until<F: FnMut(&Self) -> bool>(
        &mut self,
        mut predicate: F,
        max_steps: usize,
    ) -> usize {
        for i in 0..max_steps {
            if predicate(self) {
                return i;
            }
            self.step();
        }
        max_steps
    }

    /// Test-only accessor. The sim surface stays tiny by design, but
    /// tests need to assert on leader / log / applied state.
    #[cfg(test)]
    pub(crate) fn nodes(&self) -> &BTreeMap<NodeId, NodeHarness<C>> {
        &self.nodes
    }

    /// Number of events in `self.history`.
    #[must_use]
    pub fn history_len(&self) -> usize {
        self.history.len()
    }

    /// Current leader set across all live engines.
    #[must_use]
    pub fn leaders(&self) -> BTreeSet<NodeId> {
        crate::invariants::leaders(&self.nodes)
    }

    /// Highest `commit_index` observed across live engines. Useful for
    /// liveness predicates ("majority applied index N").
    #[must_use]
    pub fn max_commit_index(&self) -> u64 {
        self.nodes
            .values()
            .filter_map(|h| h.engine.as_ref().map(|e| e.commit_index().get()))
            .max()
            .unwrap_or(0)
    }

    /// Count of nodes whose applied log contains an entry at `index`.
    #[must_use]
    pub fn applied_majority(&self, index: u64) -> usize {
        self.nodes
            .values()
            .filter(|h| h.applied.len() as u64 >= index)
            .count()
    }

    /// `Cluster::new`-time cluster size (one `NodeHarness` per id).
    #[must_use]
    pub fn size(&self) -> usize {
        self.nodes.len()
    }

    pub(crate) fn format_schedule(&self) -> String {
        let mut out = String::new();
        for entry in &self.history {
            out.push_str(&format!("  t={:>4}  {:?}\n", entry.at_clock, entry.event));
        }
        out
    }

    // ---------- scheduler ----------

    fn pick_event(&self) -> Event<C> {
        // Category weights. Keep these explicit so future tuning is
        // local and predictable; a random-uniform-over-all-enabled
        // scheduler is fine for the fuzz target we need right now.
        let mut cats: Vec<Category> = Vec::new();
        cats.push(Category::Tick);
        if !self.network.is_empty() {
            cats.push(Category::Deliver);
            if self.policy.allow_drop {
                cats.push(Category::Drop);
            }
            if self.policy.allow_reorder && self.network.len() >= 2 {
                cats.push(Category::Reorder);
            }
        }
        if self.policy.proposal_command.is_some() {
            cats.push(Category::Propose);
        }
        if self.policy.allow_crash {
            cats.push(Category::Crash);
            cats.push(Category::Recover);
        }
        if self.policy.partial_flush_probability > 0.0 {
            cats.push(Category::Flush);
        }
        if self.policy.allow_partition {
            cats.push(Category::Partition);
            cats.push(Category::Heal);
        }

        let cat = *cats
            .as_slice()
            .choose(&mut *self.rng.lock().expect("sim RNG mutex poisoned"))
            .expect("at least Tick is always available");

        match cat {
            Category::Tick => {
                let live: Vec<NodeId> = self
                    .nodes
                    .iter()
                    .filter(|(_, h)| h.is_up())
                    .map(|(id, _)| *id)
                    .collect();
                let id = *live
                    .choose(&mut *self.rng.lock().expect("sim RNG mutex poisoned"))
                    .unwrap_or(&self.nodes.keys().next().copied().expect("non-empty"));
                Event::Tick(id)
            }
            Category::Deliver => {
                let ids = self.network.peek_ids();
                let id = *ids
                    .choose(&mut *self.rng.lock().expect("sim RNG mutex poisoned"))
                    .expect("queue non-empty");
                Event::Deliver(id)
            }
            Category::Drop => {
                let ids = self.network.peek_ids();
                let id = *ids
                    .choose(&mut *self.rng.lock().expect("sim RNG mutex poisoned"))
                    .expect("queue non-empty");
                Event::Drop(id)
            }
            Category::Reorder => {
                let n = self.network.len();
                let a = self
                    .rng
                    .lock()
                    .expect("sim RNG mutex poisoned")
                    .random_range(0..n);
                let mut b = self
                    .rng
                    .lock()
                    .expect("sim RNG mutex poisoned")
                    .random_range(0..n);
                if b == a {
                    b = (b + 1) % n;
                }
                Event::Reorder(a, b)
            }
            Category::Propose => {
                let id = *self
                    .nodes
                    .keys()
                    .copied()
                    .collect::<Vec<_>>()
                    .choose(&mut *self.rng.lock().expect("sim RNG mutex poisoned"))
                    .expect("non-empty");
                let cmd = self.policy.proposal_command.clone().expect("guarded above");
                Event::Propose(id, cmd)
            }
            Category::Crash => {
                let up: Vec<NodeId> = self
                    .nodes
                    .iter()
                    .filter(|(_, h)| h.is_up())
                    .map(|(id, _)| *id)
                    .collect();
                // Never crash the whole cluster at once — the
                // remaining-majority liveness bound becomes
                // unverifiable. If only one node is up, tick instead.
                if up.len() <= 1 {
                    return Event::Tick(
                        up.into_iter().next().unwrap_or_else(|| {
                            *self.nodes.keys().next().expect("non-empty cluster")
                        }),
                    );
                }
                let id = *up
                    .choose(&mut *self.rng.lock().expect("sim RNG mutex poisoned"))
                    .expect("non-empty");
                Event::Crash(id)
            }
            Category::Recover => {
                let down: Vec<NodeId> = self
                    .nodes
                    .iter()
                    .filter(|(_, h)| !h.is_up())
                    .map(|(id, _)| *id)
                    .collect();
                if let Some(&id) =
                    down.choose(&mut *self.rng.lock().expect("sim RNG mutex poisoned"))
                {
                    Event::Recover(id)
                } else {
                    // No crashed nodes — fall back to a tick so the
                    // step still makes progress.
                    Event::Tick(*self.nodes.keys().next().expect("non-empty cluster"))
                }
            }
            Category::Flush => {
                let up: Vec<NodeId> = self
                    .nodes
                    .iter()
                    .filter(|(_, h)| h.is_up() && !h.pending.is_empty())
                    .map(|(id, _)| *id)
                    .collect();
                let picked = {
                    let mut rng = self.rng.lock().expect("sim RNG mutex poisoned");
                    up.choose(&mut *rng).copied()
                };
                if let Some(id) = picked {
                    let pending = self.nodes[&id].pending.len();
                    let n = {
                        let mut rng = self.rng.lock().expect("sim RNG mutex poisoned");
                        rng.random_range(1..=pending)
                    };
                    Event::Flush(id, n)
                } else {
                    Event::Tick(*self.nodes.keys().next().expect("non-empty cluster"))
                }
            }
            Category::Partition => {
                // Random non-empty, non-full subset of node ids.
                let all: Vec<NodeId> = self.nodes.keys().copied().collect();
                let mut a_side = BTreeSet::new();
                for id in &all {
                    if self
                        .rng
                        .lock()
                        .expect("sim RNG mutex poisoned")
                        .random_bool(0.5)
                    {
                        a_side.insert(*id);
                    }
                }
                if a_side.is_empty() || a_side.len() == all.len() {
                    // Coerce into a valid 1-vs-rest partition.
                    a_side.clear();
                    a_side.insert(all[0]);
                }
                Event::Partition(a_side)
            }
            Category::Heal => Event::Heal,
        }
    }

    // ---------- event application ----------

    fn apply_event(&mut self, event: Event<C>) {
        match event {
            Event::Tick(id) => self.drive_engine(id, CoreEvent::Tick),
            Event::Deliver(mid) => self.deliver(mid),
            Event::Drop(mid) => {
                self.network.take(mid);
            }
            Event::Reorder(a, b) => self.network.swap(a, b),
            Event::Propose(id, cmd) => self.drive_engine(id, CoreEvent::ClientProposal(cmd)),
            Event::Crash(id) => {
                if let Some(h) = self.nodes.get_mut(&id) {
                    h.crash();
                }
            }
            Event::Recover(id) => {
                if let Some(h) = self.nodes.get_mut(&id) {
                    h.recover(Arc::clone(&self.rng));
                }
            }
            Event::Flush(id, n) => {
                if let Some(h) = self.nodes.get_mut(&id) {
                    h.flush(n);
                }
            }
            Event::Partition(a_side) => self.network.set_partition(a_side),
            Event::Heal => self.network.heal(),
            Event::ProposeConfigChange(id, change) => {
                self.drive_engine(id, CoreEvent::ProposeConfigChange(change));
            }
            Event::Snapshot(id, last_included_index, bytes) => {
                self.drive_engine(
                    id,
                    CoreEvent::SnapshotTaken {
                        last_included_index,
                        bytes,
                    },
                );
            }
        }
    }

    fn deliver(&mut self, mid: MessageId) {
        let Some(msg) = self.network.take(mid) else {
            return;
        };
        if self.network.partitioned(msg.from, msg.to) {
            return;
        }
        let event = CoreEvent::Incoming(Incoming {
            from: msg.from,
            message: msg.message,
        });
        self.drive_engine(msg.to, event);
    }

    fn drive_engine(&mut self, id: NodeId, event: CoreEvent<C>) {
        let actions: Vec<Action<C>> = {
            let Some(harness) = self.nodes.get_mut(&id) else {
                return;
            };
            let Some(engine) = harness.engine.as_mut() else {
                return;
            };
            engine.step(event)
        };
        self.process_actions(id, actions);
    }

    fn process_actions(&mut self, id: NodeId, actions: Vec<Action<C>>) {
        // Absorb (validate ordering, queue persists, record applies).
        let absorb_result = {
            let harness = self
                .nodes
                .get_mut(&id)
                .expect("drive_engine only calls for known nodes");
            harness.absorb(&actions)
        };
        if let Err(PersistOrderingError {
            send_index_in_actions,
            reason,
        }) = absorb_result
        {
            panic!(
                "safety violation: {:?}\nschedule so far ({} events):\n{}",
                SafetyViolation::PersistOrderingViolated {
                    node: id,
                    send_index_in_actions,
                    reason,
                },
                self.history.len(),
                self.format_schedule(),
            );
        }

        // Enqueue outbound sends.
        for action in &actions {
            if let Action::Send { to, message } = action {
                self.network.enqueue(id, *to, message.clone());
            }
        }

        // If the policy flushes immediately, apply every pending write
        // right now. Otherwise the scheduler must pick a Flush event.
        let skip_flush = if self.policy.partial_flush_probability > 0.0 {
            let mut rng = self.rng.lock().expect("sim RNG mutex poisoned");
            rng.random::<f64>() < self.policy.partial_flush_probability
        } else {
            false
        };
        if !skip_flush {
            let harness = self
                .nodes
                .get_mut(&id)
                .expect("drive_engine only calls for known nodes");
            harness.flush(usize::MAX);
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Category {
    Tick,
    Deliver,
    Drop,
    Reorder,
    Propose,
    Crash,
    Recover,
    Flush,
    Partition,
    Heal,
}
