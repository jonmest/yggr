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

use jotun_core::{Action, Engine, LogEntry, LogId, LogIndex, NodeId, Term};

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
    pub(crate) fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        heartbeat_interval_ticks: u64,
        rng: SharedRng,
    ) -> Self {
        let env = Box::new(SimEnv::new(rng));
        let engine = Engine::new(id, peers.iter().copied(), env, heartbeat_interval_ticks);
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
                    bytes,
                } => {
                    self.pending.push(PendingWrite::Snapshot {
                        last_included_index: *last_included_index,
                        last_included_term: *last_included_term,
                        bytes: bytes.clone(),
                    });
                }
                // Redirect: nothing for the harness to do; tests inspect
                // these directly. ApplySnapshot: pass-2 InstallSnapshot
                // path; pass-1 SnapshotTaken never emits it.
                Action::Redirect { .. } | Action::ApplySnapshot { .. } => {}
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
        let mut engine = Engine::new(
            self.id,
            self.peers.iter().copied(),
            env,
            self.heartbeat_interval_ticks,
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
            for entry in entries {
                let i = entry.id.index.get();
                if i == 0 {
                    continue;
                }
                let idx = (i - 1) as usize;
                match idx.cmp(&persisted.log.len()) {
                    std::cmp::Ordering::Less => persisted.log[idx] = entry,
                    // A gap (idx > log.len()) shouldn't happen given
                    // the engine emits entries contiguous with its
                    // in-memory log, but don't silently corrupt the
                    // snapshot — append anyway.
                    std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => {
                        persisted.log.push(entry);
                    }
                }
            }
        }
        PendingWrite::Snapshot {
            last_included_index,
            last_included_term,
            bytes,
        } => {
            persisted.snapshot = Some(PersistedSnapshot {
                last_included_index,
                last_included_term,
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
    use jotun_core::{Event, Incoming, Message, RequestAppendEntries};

    // The engine exposes no public setter for term / log. We drive it
    // through the only public mutator — `step` — using a synthetic
    // `AppendEntries` at the persisted term that carries the persisted
    // log. The engine appends entries, bumps term, and records
    // `voted_for = None`. If the persisted `voted_for` was `Some`, we
    // follow up with a `RequestVote` from that candidate at the same
    // term to restore the vote.
    //
    // If nothing persisted at all, no hydration needed.
    if persisted.current_term == Term::ZERO
        && persisted.log.is_empty()
        && persisted.snapshot.is_none()
    {
        return;
    }

    // Pick a peer as the synthetic "leader" of the hydration message
    // that is NOT the persisted voted_for — otherwise the follow-up
    // vote-restoration step has nothing to send from.
    let peer = engine
        .peers()
        .iter()
        .copied()
        .find(|p| Some(*p) != persisted.voted_for)
        .or_else(|| engine.peers().iter().copied().next());
    let Some(peer) = peer else {
        return;
    };

    // If we have a snapshot, install it first via a forged
    // InstallSnapshot RPC. That sets the floor, advances commit and
    // last_applied past it, and drops any in-memory log we'd otherwise
    // try to feed below the floor.
    let snapshot_index = if let Some(snap) = &persisted.snapshot {
        use jotun_core::RequestInstallSnapshot;
        let _ = engine.step(Event::Incoming(Incoming {
            from: peer,
            message: Message::InstallSnapshotRequest(RequestInstallSnapshot {
                term: persisted.current_term,
                leader_id: peer,
                last_included: LogId::new(snap.last_included_index, snap.last_included_term),
                data: snap.bytes.clone(),
                leader_commit: snap.last_included_index,
            }),
        }));
        snap.last_included_index
    } else {
        LogIndex::ZERO
    };

    // Feed any post-snapshot log entries via a synthetic AE. prev_log_id
    // points at the snapshot tail (or None if there's no snapshot) so
    // the engine accepts cleanly.
    let post_snapshot: Vec<_> = persisted
        .log
        .iter()
        .filter(|e| e.id.index.get() > snapshot_index.get())
        .cloned()
        .collect();
    if !post_snapshot.is_empty() || snapshot_index == LogIndex::ZERO {
        let prev_log_id = persisted
            .snapshot
            .as_ref()
            .filter(|_| snapshot_index != LogIndex::ZERO)
            .map(|snap| LogId::new(snap.last_included_index, snap.last_included_term));
        let request = RequestAppendEntries {
            term: persisted.current_term,
            leader_id: peer,
            prev_log_id,
            entries: post_snapshot,
            leader_commit: LogIndex::ZERO,
        };
        let _ = engine.step(Event::Incoming(Incoming {
            from: peer,
            message: Message::AppendEntriesRequest(request),
        }));
    }

    if let Some(voted_for) = persisted.voted_for
        && voted_for != peer
    {
        // Forge a vote request from `voted_for` so the engine records
        // its vote for that candidate at the current term. The log
        // predicate requires the candidate's log to be at least as
        // up-to-date — we pass the persisted last log id, which the
        // engine itself now holds, so the predicate passes.
        use jotun_core::RequestVote;
        let last_log_id = persisted.log.last().map(|e| e.id).or_else(|| {
            persisted
                .snapshot
                .as_ref()
                .map(|s| LogId::new(s.last_included_index, s.last_included_term))
        });
        let _ = engine.step(Event::Incoming(Incoming {
            from: voted_for,
            message: Message::VoteRequest(RequestVote {
                term: persisted.current_term,
                candidate_id: voted_for,
                last_log_id,
            }),
        }));
    }
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
    message: &jotun_core::Message<C>,
    ever_persisted: &BTreeSet<LogId>,
    ever_persisted_terms: &BTreeSet<Term>,
) -> Result<(), PersistOrderingError> {
    use jotun_core::Message as M;

    let msg_term = match message {
        M::VoteRequest(r) => r.term,
        M::VoteResponse(r) => r.term,
        M::AppendEntriesRequest(r) => r.term,
        M::AppendEntriesResponse(r) => r.term,
        M::InstallSnapshotRequest(r) => r.term,
        M::InstallSnapshotResponse(r) => r.term,
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
