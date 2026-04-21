//! Raft safety-property checkers.
//!
//! Each check runs after every [`crate::Cluster::step`]. A violation
//! becomes a panic carrying the schedule-so-far, so a fuzzer failure
//! is always a runnable regression.
//!
//! Properties (paper §5.2–5.4):
//!  - Election Safety: at most one leader per term, ever.
//!  - Log Matching: any two nodes with the same `(term, index)` agree
//!    on every earlier entry.
//!  - Leader Completeness: once an entry is committed in term T, every
//!    future leader (term > T) has it.
//!  - State Machine Safety: any two nodes that apply index N apply the
//!    same entry.
//!  - Persistence-before-Send: verified inline in
//!    [`crate::harness::NodeHarness::absorb`]; violations bubble up as
//!    this type.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use yggr_core::{LogEntry, LogIndex, NodeId, RoleState, Term};

use crate::harness::NodeHarness;

/// Which safety property was violated, with enough context to diagnose.
#[derive(Debug, Clone)]
pub enum SafetyViolation {
    /// Two different nodes were observed as leader in the same term.
    /// (§5.2 Election Safety.)
    TwoLeadersInTerm {
        term: Term,
        first: NodeId,
        second: NodeId,
    },
    /// Two nodes have different log entries at the same `(term, index)`
    /// — or, equivalently, agree on `(term, index)` but disagree on
    /// some earlier entry. (§5.3 Log Matching.)
    LogMismatch {
        index: LogIndex,
        a: NodeId,
        b: NodeId,
    },
    /// A leader in term `leader_term` is missing an entry at `index`
    /// that was already committed in `committed_term < leader_term`.
    /// (§5.4 Leader Completeness.)
    LeaderMissingCommitted {
        leader: NodeId,
        leader_term: Term,
        committed_term: Term,
        index: LogIndex,
    },
    /// Two nodes applied different entries at the same log index.
    /// (§5.4.3 State Machine Safety.)
    DivergentApply {
        index: LogIndex,
        a: NodeId,
        b: NodeId,
    },
    /// The engine emitted a `Send` whose state change had not yet been
    /// ordered as a Persist action earlier in the same `step()` vec.
    PersistOrderingViolated {
        node: NodeId,
        send_index_in_actions: usize,
        reason: &'static str,
    },
    /// A node's log holds two or more uncommitted `ConfigChange` entries
    /// at once. Violates the §4.3 single-server rule (at most one in
    /// flight); the leader-side append guard exists exactly to prevent
    /// this.
    MultipleUncommittedConfigChanges {
        node: NodeId,
        indices: Vec<LogIndex>,
    },
    /// A node's snapshot floor is past its `commit_index`. Snapshots
    /// must only cover committed state — §7 invariant.
    SnapshotPastCommit {
        node: NodeId,
        snapshot_index: LogIndex,
        commit_index: LogIndex,
    },
}

/// Running ledger used by the checks that need history across steps.
///
/// Leaders-seen ledger (term → first node observed as leader in that
/// term) drives Election Safety. Committed-entries ledger (index → the
/// entry once any node reported it as committed) drives Leader
/// Completeness. Applied-index ledger (index → entry once any node
/// applied it) drives State Machine Safety.
#[derive(Debug, Default)]
pub(crate) struct SafetyChecker<C> {
    leaders_by_term: BTreeMap<Term, NodeId>,
    committed_entries: BTreeMap<LogIndex, LogEntry<C>>,
    applied_by_index: BTreeMap<LogIndex, LogEntry<C>>,
}

impl<C: Clone + PartialEq> SafetyChecker<C> {
    pub(crate) fn new() -> Self {
        Self {
            leaders_by_term: BTreeMap::new(),
            committed_entries: BTreeMap::new(),
            applied_by_index: BTreeMap::new(),
        }
    }

    /// Run every property against the cluster's current state.
    /// Returns `Err` on the first violation; the caller translates
    /// that into a panic.
    pub(crate) fn check(
        &mut self,
        nodes: &BTreeMap<NodeId, NodeHarness<C>>,
    ) -> Result<(), SafetyViolation> {
        self.check_election_safety(nodes)?;
        self.check_log_matching(nodes)?;
        self.observe_applied(nodes)?;
        self.observe_committed(nodes);
        self.check_leader_completeness(nodes)?;
        Self::check_single_in_flight_config_change(nodes)?;
        Self::check_snapshot_within_commit(nodes)?;
        Ok(())
    }

    /// §7 invariant: a node's snapshot floor never exceeds its own
    /// `commit_index`. Snapshots are always of committed state.
    fn check_snapshot_within_commit(
        nodes: &BTreeMap<NodeId, NodeHarness<C>>,
    ) -> Result<(), SafetyViolation> {
        for harness in nodes.values() {
            let Some(engine) = harness.engine.as_ref() else {
                continue;
            };
            let snap = engine.log().snapshot_last().index;
            let commit = engine.commit_index();
            if snap > commit {
                return Err(SafetyViolation::SnapshotPastCommit {
                    node: engine.id(),
                    snapshot_index: snap,
                    commit_index: commit,
                });
            }
        }
        Ok(())
    }

    /// §4.3 invariant: no node's log contains more than one
    /// `ConfigChange` past `commit_index`. If this fires, either a
    /// leader appended a second CC while one was uncommitted, or a
    /// follower accepted contradictory entries from competing leaders.
    fn check_single_in_flight_config_change(
        nodes: &BTreeMap<NodeId, NodeHarness<C>>,
    ) -> Result<(), SafetyViolation> {
        use yggr_core::LogPayload;
        for harness in nodes.values() {
            let Some(engine) = harness.engine.as_ref() else {
                continue;
            };
            let from = engine.commit_index().get() + 1;
            let last = engine.log().last_log_id().map_or(0, |l| l.index.get());
            let mut indices: Vec<LogIndex> = Vec::new();
            for i in from..=last {
                let idx = LogIndex::new(i);
                if let Some(entry) = engine.log().entry_at(idx)
                    && matches!(entry.payload, LogPayload::ConfigChange(_))
                {
                    indices.push(idx);
                }
            }
            if indices.len() > 1 {
                return Err(SafetyViolation::MultipleUncommittedConfigChanges {
                    node: engine.id(),
                    indices,
                });
            }
        }
        Ok(())
    }

    fn check_election_safety(
        &mut self,
        nodes: &BTreeMap<NodeId, NodeHarness<C>>,
    ) -> Result<(), SafetyViolation> {
        for harness in nodes.values() {
            let Some(engine) = harness.engine.as_ref() else {
                continue;
            };
            if !matches!(engine.role(), RoleState::Leader(_)) {
                continue;
            }
            let term = engine.current_term();
            match self.leaders_by_term.get(&term) {
                None => {
                    self.leaders_by_term.insert(term, engine.id());
                }
                Some(&first) if first == engine.id() => {}
                Some(&first) => {
                    return Err(SafetyViolation::TwoLeadersInTerm {
                        term,
                        first,
                        second: engine.id(),
                    });
                }
            }
        }
        Ok(())
    }

    fn check_log_matching(
        &self,
        nodes: &BTreeMap<NodeId, NodeHarness<C>>,
    ) -> Result<(), SafetyViolation> {
        // Compare in absolute index space. Once either node has
        // snapshotted a prefix, indices below the higher floor are no
        // longer individually readable; we trust the snapshot contract
        // there and resume comparison at the first index still
        // inspectable by at least one side.
        let ids: Vec<NodeId> = nodes.keys().copied().collect();
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                let Some(a) = nodes.get(&ids[i]).and_then(|h| h.engine.as_ref()) else {
                    continue;
                };
                let Some(b) = nodes.get(&ids[j]).and_then(|h| h.engine.as_ref()) else {
                    continue;
                };

                let compare_from = a
                    .log()
                    .snapshot_last()
                    .index
                    .max(b.log().snapshot_last().index)
                    .max(LogIndex::new(1));
                let max_shared = a
                    .log()
                    .last_log_id()
                    .map_or(LogIndex::ZERO, |id| id.index)
                    .min(b.log().last_log_id().map_or(LogIndex::ZERO, |id| id.index));
                if max_shared < compare_from {
                    continue;
                }

                // Walk from the highest shared index backward; find the
                // most recent (term, index) agreement, then verify the
                // entire prefix matches.
                let mut agreement: Option<LogIndex> = None;
                for k in (compare_from.get()..=max_shared.get()).rev() {
                    let idx = LogIndex::new(k);
                    if let (Some(a_term), Some(b_term)) =
                        (a.log().term_at(idx), b.log().term_at(idx))
                        && a_term == b_term
                    {
                        agreement = Some(idx);
                        break;
                    }
                }
                if let Some(k) = agreement {
                    for step in compare_from.get()..=k.get() {
                        let idx = LogIndex::new(step);
                        let matches = match (a.log().entry_at(idx), b.log().entry_at(idx)) {
                            (Some(ea), Some(eb)) => ea.id == eb.id && ea.payload == eb.payload,
                            _ => a.log().term_at(idx) == b.log().term_at(idx),
                        };
                        if !matches {
                            return Err(SafetyViolation::LogMismatch {
                                index: idx,
                                a: a.id(),
                                b: b.id(),
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn observe_applied(
        &mut self,
        nodes: &BTreeMap<NodeId, NodeHarness<C>>,
    ) -> Result<(), SafetyViolation> {
        // Index by the entry's own log index, not the position in the
        // applied vec. The engine filters Noop and ConfigChange entries
        // out of Apply, so positions are dense but indices are sparse.
        for harness in nodes.values() {
            for entry in &harness.applied {
                let idx = entry.id.index;
                match self.applied_by_index.get(&idx) {
                    None => {
                        self.applied_by_index.insert(idx, entry.clone());
                    }
                    Some(seen) if seen.id == entry.id && seen.payload == entry.payload => {}
                    Some(seen) => {
                        // Find another node whose applied vec contains
                        // the differing entry at the same index, for the
                        // diagnostic message; any matching node works.
                        let other = nodes
                            .iter()
                            .find(|(_, h)| {
                                h.applied
                                    .iter()
                                    .any(|e| e.id == seen.id && e.payload == seen.payload)
                            })
                            .map(|(id, _)| *id)
                            .unwrap_or(harness.id);
                        return Err(SafetyViolation::DivergentApply {
                            index: idx,
                            a: other,
                            b: harness.id,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn observe_committed(&mut self, nodes: &BTreeMap<NodeId, NodeHarness<C>>) {
        for harness in nodes.values() {
            let Some(engine) = harness.engine.as_ref() else {
                continue;
            };
            let ci = engine.commit_index();
            if ci == LogIndex::ZERO {
                continue;
            }
            for k in 1..=ci.get() {
                let idx = LogIndex::new(k);
                if let Some(entry) = engine.log().entry_at(idx)
                    && !self.committed_entries.contains_key(&idx)
                {
                    self.committed_entries.insert(idx, entry.clone());
                }
            }
        }
    }

    fn check_leader_completeness(
        &self,
        nodes: &BTreeMap<NodeId, NodeHarness<C>>,
    ) -> Result<(), SafetyViolation> {
        for harness in nodes.values() {
            let Some(engine) = harness.engine.as_ref() else {
                continue;
            };
            if !matches!(engine.role(), RoleState::Leader(_)) {
                continue;
            }
            let leader_term = engine.current_term();
            let snapshot_floor = engine.log().snapshot_last().index;
            for (idx, committed_entry) in &self.committed_entries {
                if committed_entry.id.term >= leader_term {
                    continue;
                }
                // An entry at or below the leader's snapshot floor is
                // captured by the snapshot; the leader holds it
                // durably even though entry_at() returns None. Only
                // check agreement at the floor itself (term must
                // match) — below the floor we trust the snapshot
                // contract.
                if *idx < snapshot_floor {
                    continue;
                }
                if *idx == snapshot_floor {
                    if engine.log().snapshot_last().term != committed_entry.id.term {
                        return Err(SafetyViolation::LeaderMissingCommitted {
                            leader: engine.id(),
                            leader_term,
                            committed_term: committed_entry.id.term,
                            index: *idx,
                        });
                    }
                    continue;
                }
                match engine.log().entry_at(*idx) {
                    Some(held) if held.id == committed_entry.id => {}
                    _ => {
                        return Err(SafetyViolation::LeaderMissingCommitted {
                            leader: engine.id(),
                            leader_term,
                            committed_term: committed_entry.id.term,
                            index: *idx,
                        });
                    }
                }
            }
        }
        Ok(())
    }
}

/// Snapshot set used by tests to assert liveness predicates.
pub(crate) fn leaders<C: Clone>(nodes: &BTreeMap<NodeId, NodeHarness<C>>) -> BTreeSet<NodeId> {
    nodes
        .values()
        .filter_map(|h| {
            let engine = h.engine.as_ref()?;
            matches!(engine.role(), RoleState::Leader(_)).then_some(engine.id())
        })
        .collect()
}
