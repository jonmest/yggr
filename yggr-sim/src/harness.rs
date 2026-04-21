//! Per-node state: the live [`Engine`], its durable persisted state,
//! and the pending-write queue that models partial disk flushes.
//!
//! Real hosts write to disk at fsync boundaries they choose. Bugs in
//! that choice are the kind of thing simulation exists to surface, so
//! the harness does not automatically apply every `Persist` action to
//! the durable snapshot. It queues them. The scheduler picks when a
//! prefix of that queue gets flushed, and the rest is discarded on
//! crash — exactly mirroring the "we sent before fsync completed"
//! failure mode Raft's action-ordering contract exists to catch.

use std::collections::BTreeSet;

use yggr_core::{Action, Engine, LogEntry, LogId, LogIndex, NodeId, Term};

use crate::env::SharedRng;
use crate::env::SimEnv;

/// The durable snapshot the engine recovers from on restart.
///
/// Tracks exactly what Raft Figure 2 labels "persistent state on all
/// servers": `current_term`, `voted_for`, and the log. Volatile state
/// (commit_index, last_applied, role) is rebuilt by the replacement
/// engine on `Recover`.
#[derive(Debug, Clone)]
pub(crate) struct PersistedState<C> {
    pub(crate) current_term: Term,
    pub(crate) voted_for: Option<NodeId>,
    pub(crate) log: Vec<LogEntry<C>>,
    /// Most recent snapshot, if any. Replaces every log entry with
    /// index ≤ `last_included_index`.
    pub(crate) snapshot: Option<PersistedSnapshot>,
}

/// The on-disk representation of a Raft snapshot.
#[derive(Debug, Clone)]
#[allow(dead_code)] // fields read by pass-2 InstallSnapshot hydration
pub(crate) struct PersistedSnapshot {
    pub(crate) last_included_index: LogIndex,
    pub(crate) last_included_term: Term,
    pub(crate) peers: BTreeSet<NodeId>,
    pub(crate) bytes: Vec<u8>,
}

impl<C> Default for PersistedState<C> {
    fn default() -> Self {
        Self {
            current_term: Term::ZERO,
            voted_for: None,
            log: Vec::new(),
            snapshot: None,
        }
    }
}

/// A write that has been emitted by the engine but not yet fsynced.
#[derive(Debug, Clone)]
pub(crate) enum PendingWrite<C> {
    HardState {
        current_term: Term,
        voted_for: Option<NodeId>,
    },
    LogEntries(Vec<LogEntry<C>>),
    Snapshot {
        last_included_index: LogIndex,
        last_included_term: Term,
        peers: BTreeSet<NodeId>,
        bytes: Vec<u8>,
    },
}

/// One node in the simulated cluster: a live engine, its durable
/// snapshot, the still-pending writes, and the applied-entry log the
/// state-machine-safety invariant checks against.
#[derive(Debug)]
pub(crate) struct NodeHarness<C> {
    pub(crate) id: NodeId,
    pub(crate) peers: Vec<NodeId>,
    /// `None` while the node is crashed.
    pub(crate) engine: Option<Engine<C>>,
    pub(crate) persisted: PersistedState<C>,
    pub(crate) pending: Vec<PendingWrite<C>>,
    pub(crate) applied: Vec<LogEntry<C>>,
    pub(crate) heartbeat_interval_ticks: u64,
    /// Whether this harness's engines (both initial and post-recover)
    /// run with §9.6 pre-vote enabled. Stored so `recover` rebuilds
    /// the engine with the same setting.
    pub(crate) pre_vote: bool,
    /// Every log-entry id the engine has ever asked to persist, across
    /// every step. A `Send` carrying entries must reference ids in this
    /// set — otherwise the engine violated §5.1 action ordering. Carries
    /// across crash/recover so the recovered engine's re-broadcasts of
    /// older entries don't false-fire the check.
    pub(crate) ever_persisted: BTreeSet<LogId>,
    /// Similar ledger for hard-state advances: `(term, voted_for)`
    /// pairs that have ever shown up in a PersistHardState. A Send
    /// carrying a term the engine never persisted is a violation.
    pub(crate) ever_persisted_terms: BTreeSet<Term>,
}

impl<C: Clone> NodeHarness<C> {
    /// Default constructor, pre-vote off. Kept for call sites that
    /// don't care about the pre-vote path (unit-level invariant
    /// tests).
    #[cfg(test)]
    pub(crate) fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        heartbeat_interval_ticks: u64,
        rng: SharedRng,
    ) -> Self {
        Self::with_pre_vote(id, peers, heartbeat_interval_ticks, rng, false)
    }

    pub(crate) fn with_pre_vote(
        id: NodeId,
        peers: Vec<NodeId>,
        heartbeat_interval_ticks: u64,
        rng: SharedRng,
        pre_vote: bool,
    ) -> Self {
        let env = Box::new(SimEnv::new(rng));
        let cfg = yggr_core::EngineConfig::default().with_pre_vote(pre_vote);
        let engine = Engine::with_config(
            id,
            peers.iter().copied(),
            env,
            heartbeat_interval_ticks,
            cfg,
        );
        let mut ever_persisted_terms = BTreeSet::new();
        ever_persisted_terms.insert(Term::ZERO);
        Self {
            id,
            peers,
            engine: Some(engine),
            persisted: PersistedState::default(),
            pending: Vec::new(),
            applied: Vec::new(),
            heartbeat_interval_ticks,
            pre_vote,
            ever_persisted: BTreeSet::new(),
            ever_persisted_terms,
        }
    }

    pub(crate) fn is_up(&self) -> bool {
        self.engine.is_some()
    }

    /// Scan `actions` for ordering correctness (§5.1: persist before
    /// send), then queue each persist into `pending` and append each
    /// applied entry to `applied`.
    ///
    /// Returns the index of any `Send` whose prerequisite persist was
    /// not ordered earlier in the vector, for the caller to turn into
    /// a safety violation.
    pub(crate) fn absorb(&mut self, actions: &[Action<C>]) -> Result<(), PersistOrderingError>
    where
        C: PartialEq,
    {
        for (i, action) in actions.iter().enumerate() {
            match action {
                Action::PersistHardState {
                    current_term,
                    voted_for,
                } => {
                    self.pending.push(PendingWrite::HardState {
                        current_term: *current_term,
                        voted_for: *voted_for,
                    });
                    self.ever_persisted_terms.insert(*current_term);
                }
                Action::PersistLogEntries(entries) => {
                    for entry in entries {
                        self.ever_persisted.insert(entry.id);
                    }
                    self.pending.push(PendingWrite::LogEntries(entries.clone()));
                }
                Action::Send { message, .. } => {
                    check_send_ordering(
                        i,
                        message,
                        &self.ever_persisted,
                        &self.ever_persisted_terms,
                    )?;
                }
                Action::Apply(entries) => {
                    self.applied.extend(entries.iter().cloned());
                }
                Action::PersistSnapshot {
                    last_included_index,
                    last_included_term,
                    peers,
                    bytes,
                } => {
                    self.pending.push(PendingWrite::Snapshot {
                        last_included_index: *last_included_index,
                        last_included_term: *last_included_term,
                        peers: peers.clone(),
                        bytes: bytes.clone(),
                    });
                }
                // Redirect, SnapshotHint, ReadReady, ReadFailed are
                // advisory for the sim's durability/persistence model;
                // ApplySnapshot only affects the host-side state
                // machine model.
                Action::Redirect { .. }
                | Action::ApplySnapshot { .. }
                | Action::SnapshotHint { .. }
                | Action::ReadReady { .. }
                | Action::ReadFailed { .. } => {}
            }
        }
        Ok(())
    }

    /// Flush up to `n` pending writes into the durable snapshot, or all
    /// of them when `n == usize::MAX`.
    pub(crate) fn flush(&mut self, n: usize) {
        let take = n.min(self.pending.len());
        let drained: Vec<_> = self.pending.drain(..take).collect();
        for write in drained {
            apply_write(&mut self.persisted, write);
        }
    }

    /// Drop the engine and any pending (unflushed) writes. Durable
    /// snapshot stays.
    pub(crate) fn crash(&mut self) {
        self.engine = None;
        self.pending.clear();
    }

    /// Rebuild the engine from the durable snapshot. Applied entries
    /// are *not* reset — the safety checker still needs to compare
    /// post-recovery applies against pre-crash applies.
    pub(crate) fn recover(&mut self, rng: SharedRng) {
        if self.engine.is_some() {
            return;
        }
        let env = Box::new(SimEnv::new(rng));
        let cfg = yggr_core::EngineConfig::default().with_pre_vote(self.pre_vote);
        let mut engine = Engine::with_config(
            self.id,
            self.peers.iter().copied(),
            env,
            self.heartbeat_interval_ticks,
            cfg,
        );
        hydrate_engine(&mut engine, &self.persisted);
        self.engine = Some(engine);
    }
}

/// Apply a pending write to the durable snapshot. Log entries
/// overwrite at their own indices — the engine always emits them
/// contiguous with the existing log, but a recovered engine re-sending
/// the same prefix would duplicate otherwise.
fn apply_write<C>(persisted: &mut PersistedState<C>, write: PendingWrite<C>) {
    match write {
        PendingWrite::HardState {
            current_term,
            voted_for,
        } => {
            persisted.current_term = current_term;
            persisted.voted_for = voted_for;
        }
        PendingWrite::LogEntries(entries) => {
            let snapshot_floor = persisted
                .snapshot
                .as_ref()
                .map_or(0, |snapshot| snapshot.last_included_index.get());
            let mut truncated_from = None;
            for entry in entries {
                let i = entry.id.index.get();
                if i == 0 || i <= snapshot_floor {
                    continue;
                }
                let local_idx = usize::try_from(i - snapshot_floor - 1)
                    .expect("log index above snapshot floor fits in usize");
                if truncated_from.is_none() {
                    let keep = local_idx.min(persisted.log.len());
                    persisted.log.truncate(keep);
                    truncated_from = Some(local_idx);
                }
                persisted.log.push(entry);
            }
        }
        PendingWrite::Snapshot {
            last_included_index,
            last_included_term,
            peers,
            bytes,
        } => {
            persisted.snapshot = Some(PersistedSnapshot {
                last_included_index,
                last_included_term,
                peers,
                bytes,
            });
            // Drop log entries up to and including the snapshot floor.
            // Indices are 1-based; entries[0] = log index 1.
            let drop_through = last_included_index.get() as usize;
            let keep_from = drop_through.min(persisted.log.len());
            persisted.log.drain(..keep_from);
        }
    }
}

/// Feed the durable snapshot back into a fresh engine so it resumes
/// with the same term / vote / log as before the crash. Volatile state
/// (role, commit_index, last_applied) stays at the engine's default
/// follower boot.
fn hydrate_engine<C: Clone>(engine: &mut Engine<C>, persisted: &PersistedState<C>) {
    use yggr_core::{RecoveredHardState, RecoveredSnapshot};

    // Early-exit when there's nothing persisted — a freshly-booted
    // node recovers to its default state.
    if persisted.current_term == Term::ZERO
        && persisted.log.is_empty()
        && persisted.snapshot.is_none()
    {
        return;
    }

    // Use the engine's public recovery API. Synthetic RPCs used to
    // work for non-self voted_for, but `on_incoming` drops messages
    // whose `from` isn't in the peer set — so a node that voted for
    // itself (candidate in the persisted term) would lose its
    // self-vote on recover. That broke §5.1: a peer could win the
    // same term by collecting the re-granted vote, producing two
    // leaders in one term.
    let snapshot = persisted.snapshot.as_ref().map(|s| RecoveredSnapshot {
        last_included_index: s.last_included_index,
        last_included_term: s.last_included_term,
        peers: s.peers.clone(),
        bytes: s.bytes.clone(),
    });
    engine.recover_from(RecoveredHardState {
        current_term: persisted.current_term,
        voted_for: persisted.voted_for,
        snapshot,
        post_snapshot_log: persisted.log.clone(),
    });
}

/// Something a `Send` action needs to have been persisted before it,
/// but wasn't.
#[derive(Debug, Clone)]
pub(crate) struct PersistOrderingError {
    pub(crate) send_index_in_actions: usize,
    pub(crate) reason: &'static str,
}

/// Verify that any state visible in `message` corresponds to something
/// the engine has previously emitted a `Persist*` action for. Raft
/// Figure 2 demands "respond to RPCs only after updating stable storage";
/// the sim enforces the engine's half (emit Persist before Send) and
/// leaves actual disk flushing to the scheduler via Flush/crash events.
///
/// The ledger is cumulative across steps: re-broadcasting an old entry
/// is fine because it was persisted in some earlier step.
fn check_send_ordering<C>(
    i: usize,
    message: &yggr_core::Message<C>,
    ever_persisted: &BTreeSet<LogId>,
    ever_persisted_terms: &BTreeSet<Term>,
) -> Result<(), PersistOrderingError> {
    use yggr_core::Message as M;

    // Pre-vote is explicitly non-durable (§9.6): neither the sender
    // nor the responder updates hard state as a result of a pre-vote
    // round-trip. The messages carry a *proposed* term which may
    // never become the engine's real term. Exempt them from the
    // persist-before-send check.
    let msg_term = match message {
        M::VoteRequest(r) => r.term,
        M::VoteResponse(r) => r.term,
        M::AppendEntriesRequest(r) => r.term,
        M::AppendEntriesResponse(r) => r.term,
        M::InstallSnapshotRequest(r) => r.term,
        M::InstallSnapshotResponse(r) => r.term,
        M::TimeoutNow(r) => r.term,
        M::PreVoteRequest(_) | M::PreVoteResponse(_) => return Ok(()),
    };

    // The engine must have persisted this term (or a later one) before
    // emitting any message that carries it. Term::ZERO is seeded into
    // the ledger at construction — the engine starts there.
    if !ever_persisted_terms.contains(&msg_term) {
        return Err(PersistOrderingError {
            send_index_in_actions: i,
            reason: "Send carries a term that was never covered by a PersistHardState",
        });
    }

    // Every log entry referenced in an outbound AppendEntriesRequest
    // must have been persisted previously.
    if let M::AppendEntriesRequest(r) = message {
        for entry in &r.entries {
            if !ever_persisted.contains(&entry.id) {
                return Err(PersistOrderingError {
                    send_index_in_actions: i,
                    reason: "AppendEntriesRequest carries an entry the engine never persisted",
                });
            }
        }
    }

    Ok(())
}
