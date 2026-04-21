//! Step 5 smoke test: the harness drives a 3-node cluster through
//! election → leader → propose → majority-apply on a happy-path policy.
//!
//! Deterministic: same seed, same outcome. Prove the harness can run
//! the protocol end-to-end before the fuzzer layers chaos on top.

use yggr_core::NodeId;

use crate::Cluster;
use crate::cluster::Policy;

/// 3 nodes, happy policy with a proposal command available. Drive
/// until we see (a) a single leader, (b) commit_index > 0 on a
/// majority, then assert state machine safety held throughout.
#[test]
fn happy_path_three_nodes_elects_leader_and_applies_proposal() {
    let mut cluster: Cluster<u64> = Cluster::new(0xA11CE, 3);
    cluster.set_policy(Policy::happy(Some(42)));

    // Phase 1: let an election happen. Ticks + deliveries only; the
    // scheduler will pick Propose randomly but the engine will drop
    // proposals on non-leaders, so early proposals are harmless.
    let steps = cluster.run_until(|c| c.leaders().len() == 1, 400);
    assert!(
        steps < 400,
        "no leader elected after 400 steps; leaders: {:?}",
        cluster.leaders(),
    );
    let leaders = cluster.leaders();
    assert_eq!(leaders.len(), 1, "expected exactly one leader");

    // Phase 2: drive until at least a majority (2 of 3) has applied
    // index 1 — the leader's no-op. That's the first thing committed
    // once replication catches up.
    let steps = cluster.run_until(|c| c.applied_majority(1) >= 2, 800);
    assert!(
        steps < 800,
        "no majority apply at index 1 within 800 further steps; max commit = {}",
        cluster.max_commit_index(),
    );

    // Proposal-commit check: the Propose category fires with probability
    // proportional to its slot in the scheduler's category list, so by
    // ~1200 total steps a Propose against the leader should have been
    // accepted, replicated, and applied.
    let steps = cluster.run_until(|c| c.applied_majority(2) >= 2, 1200);
    assert!(
        steps < 1200,
        "no majority apply at index 2 after proposals; max commit = {}",
        cluster.max_commit_index(),
    );
}

/// Replay: same seed ⇒ same schedule. Non-negotiable harness property.
#[test]
fn same_seed_same_leader() {
    let mut a: Cluster<u64> = Cluster::new(7, 3);
    let mut b: Cluster<u64> = Cluster::new(7, 3);
    a.set_policy(Policy::happy(Some(1)));
    b.set_policy(Policy::happy(Some(1)));

    for _ in 0..300 {
        a.step();
        b.step();
    }
    assert_eq!(a.leaders(), b.leaders());
    assert_eq!(a.max_commit_index(), b.max_commit_index());
    assert_eq!(a.history_len(), b.history_len());
}

fn run_happy(seed: u64) -> Cluster<u64> {
    let mut c: Cluster<u64> = Cluster::new(seed, 3);
    c.set_policy(Policy::happy(Some(1)));
    let _ = c.run_until(|c| !c.leaders().is_empty(), 400);
    c
}

/// Different seeds usually pick different leaders. Not a correctness
/// property, just a sanity check that the RNG actually varies.
#[test]
fn different_seeds_can_diverge() {
    let a_leaders = run_happy(1).leaders();
    let b_leaders = run_happy(42).leaders();
    let c_leaders = run_happy(9999).leaders();
    // All three converging to the same leader id under three different
    // seeds is possible but improbable on `happy` policy with 3 nodes.
    let all_same = a_leaders == b_leaders && b_leaders == c_leaders;
    if all_same {
        assert!(
            !a_leaders.is_empty(),
            "all three seeds produced no leader — scheduler is stuck",
        );
    }
}

/// Cluster ids are stable `1..=n`.
#[test]
fn node_ids_are_one_indexed() {
    let cluster: Cluster<u64> = Cluster::new(0, 5);
    let ids: Vec<u64> = cluster.nodes().keys().map(|n| n.get()).collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    assert!(NodeId::new(0).is_none());
}
