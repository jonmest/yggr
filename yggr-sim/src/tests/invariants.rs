//! Direct tests of the safety-invariant checkers.
//!
//! The happy-path smoke tests prove the checkers *don't* false-fire in
//! a normal run. These tests inject contradictory state into the
//! `SafetyChecker` directly and assert the right variant fires — so
//! we know the harness catches real violations, not just that
//! passing cases pass.

use std::collections::{BTreeMap, BTreeSet};

use yggr_core::{LogEntry, LogId, LogIndex, LogPayload, NodeId, Term};

use crate::cluster::Policy;
use crate::harness::{NodeHarness, PersistedSnapshot, PersistedState};
use crate::invariants::{SafetyChecker, SafetyViolation};
use crate::{Cluster, env::SharedRng};
use std::sync::{Arc, Mutex};

use rand::SeedableRng;
use rand::rngs::StdRng;

fn rng() -> SharedRng {
    Arc::new(Mutex::new(StdRng::seed_from_u64(0)))
}

fn node(id: u64) -> NodeId {
    NodeId::new(id).unwrap()
}

fn cmd_entry(index: u64, term: u64, cmd: u64) -> LogEntry<u64> {
    LogEntry {
        id: LogId::new(LogIndex::new(index), Term::new(term)),
        payload: LogPayload::Command(cmd),
    }
}

fn recovered_node(id: u64, peers: Vec<NodeId>, persisted: PersistedState<u64>) -> NodeHarness<u64> {
    let mut harness = NodeHarness::new(node(id), peers, 3, rng());
    harness.persisted = persisted;
    harness.crash();
    harness.recover(rng());
    harness
}

/// Handcrafted: node 1 applied `42` at index 1, node 2 applied `99` at
/// index 1. That's the textbook State Machine Safety violation.
#[test]
fn divergent_apply_is_detected() {
    let mut nodes: BTreeMap<NodeId, NodeHarness<u64>> = BTreeMap::new();
    let mut h1 = NodeHarness::new(node(1), vec![node(2)], 3, rng());
    h1.applied.push(cmd_entry(1, 1, 42));
    let mut h2 = NodeHarness::new(node(2), vec![node(1)], 3, rng());
    h2.applied.push(cmd_entry(1, 1, 99));
    nodes.insert(node(1), h1);
    nodes.insert(node(2), h2);

    let mut checker: SafetyChecker<u64> = SafetyChecker::new();
    let err = checker.check(&nodes).expect_err("must detect divergence");
    assert!(
        matches!(err, SafetyViolation::DivergentApply { index, .. } if index == LogIndex::new(1)),
        "wrong variant: {err:?}",
    );
}

/// Happy case: same entries at same indices across two nodes — checker
/// is silent. Regression for a prior version that false-fired.
#[test]
fn matching_applied_is_silent() {
    let mut nodes: BTreeMap<NodeId, NodeHarness<u64>> = BTreeMap::new();
    let mut h1 = NodeHarness::new(node(1), vec![node(2)], 3, rng());
    h1.applied.push(cmd_entry(1, 1, 42));
    let mut h2 = NodeHarness::new(node(2), vec![node(1)], 3, rng());
    h2.applied.push(cmd_entry(1, 1, 42));
    nodes.insert(node(1), h1);
    nodes.insert(node(2), h2);

    let mut checker: SafetyChecker<u64> = SafetyChecker::new();
    checker.check(&nodes).expect("divergence-free run");
}

/// Two different nodes applying at disjoint indices — nobody has
/// applied the same index as anybody else. Silent.
#[test]
fn disjoint_apply_is_silent() {
    let mut nodes: BTreeMap<NodeId, NodeHarness<u64>> = BTreeMap::new();
    let mut h1 = NodeHarness::new(node(1), vec![node(2)], 3, rng());
    h1.applied.push(cmd_entry(1, 1, 42));
    h1.applied.push(cmd_entry(2, 1, 43));
    let h2 = NodeHarness::new(node(2), vec![node(1)], 3, rng());
    // h2 applied nothing.
    nodes.insert(node(1), h1);
    nodes.insert(node(2), h2);

    let mut checker: SafetyChecker<u64> = SafetyChecker::new();
    checker
        .check(&nodes)
        .expect("disjoint — no overlap to disagree on");
}

/// End-to-end: run a happy cluster for a while and confirm no
/// violation is reported. The smoke test exercises this implicitly;
/// here we assert on the checker's internal state directly.
#[test]
fn happy_run_accumulates_committed_and_applied_without_violation() {
    let mut cluster: Cluster<u64> = Cluster::new(0xFACE_F00D, 3);
    cluster.set_policy(Policy::happy(Some(7)));
    // Drives to majority apply at index 2.
    let _ = cluster.run_until(|c| c.applied_majority(2) >= 2, 1500);
    // If we got here without panicking, the checker never saw a
    // violation across hundreds of steps.
    assert!(cluster.max_commit_index() >= 2);
}

#[test]
fn log_matching_checks_post_snapshot_disagreement_above_snapshot_floor() {
    let peers = BTreeSet::from([node(1), node(2)]);
    let mut nodes: BTreeMap<NodeId, NodeHarness<u64>> = BTreeMap::new();
    nodes.insert(
        node(1),
        recovered_node(
            1,
            vec![node(2)],
            PersistedState {
                current_term: Term::new(3),
                voted_for: None,
                log: vec![cmd_entry(4, 2, 40), cmd_entry(5, 3, 50)],
                snapshot: Some(PersistedSnapshot {
                    last_included_index: LogIndex::new(3),
                    last_included_term: Term::new(1),
                    peers: peers.clone(),
                    bytes: b"a".to_vec(),
                }),
            },
        ),
    );
    nodes.insert(
        node(2),
        recovered_node(
            2,
            vec![node(1)],
            PersistedState {
                current_term: Term::new(3),
                voted_for: None,
                log: vec![
                    cmd_entry(3, 1, 30),
                    cmd_entry(4, 2, 99),
                    cmd_entry(5, 3, 50),
                ],
                snapshot: Some(PersistedSnapshot {
                    last_included_index: LogIndex::new(2),
                    last_included_term: Term::new(1),
                    peers,
                    bytes: b"b".to_vec(),
                }),
            },
        ),
    );

    let mut checker: SafetyChecker<u64> = SafetyChecker::new();
    let err = checker
        .check(&nodes)
        .expect_err("must detect mismatch above the floor");
    assert!(
        matches!(err, SafetyViolation::LogMismatch { index, .. } if index == LogIndex::new(4)),
        "wrong variant: {err:?}",
    );
}

// Log Matching / Leader Completeness / Two-Leaders are harder to
// inject synthetically without running the engine — they depend on the
// *live* engine reporting a role/term/log combination, and the sim
// doesn't expose engine mutators. The happy-run test above exercises
// the silent path; real violations would fire during `Cluster::step`
// and panic, which is the integration path we care about.
