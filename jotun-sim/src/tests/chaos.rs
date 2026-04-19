//! Step 8: adversarial-schedule tests.
//!
//! Safety properties must hold under any schedule the scheduler can
//! produce — including message loss, reorder, partitions, crashes,
//! and partial fsync windows. Liveness holds only under "favorable"
//! schedules (no permanent partition, no message storm); those get
//! their own narrower test.
//!
//! The per-step invariant checks already panic on a safety break;
//! these tests exist to drive the scheduler through enough chaos that
//! at least one crash/partition/drop is picked, then confirm the run
//! completed without panicking.

use proptest::prelude::*;

use crate::Cluster;
use crate::cluster::Policy;

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 128,
        .. ProptestConfig::default()
    })]

    /// Chaos: drops, reorder, partitions, crashes, partial flushes.
    /// Must not panic — safety holds even here. Liveness is not
    /// asserted; the scheduler is allowed to permanently partition
    /// the cluster.
    #[test]
    fn chaos_schedule_preserves_safety(seed in any::<u64>()) {
        let mut cluster: Cluster<u64> = Cluster::new(seed, 3);
        cluster.set_policy(Policy::chaos(Some(1)));
        for _ in 0..1500 {
            cluster.step();
        }
        // If we got here, no safety violation fired.
        prop_assert!(cluster.history_len() == 1500);
    }

    /// 5-node chaos: more room for elections to race, more chances
    /// for partitions to elect competing leaders. Safety still holds.
    #[test]
    fn chaos_schedule_five_nodes_preserves_safety(seed in any::<u64>()) {
        let mut cluster: Cluster<u64> = Cluster::new(seed, 5);
        cluster.set_policy(Policy::chaos(Some(1)));
        for _ in 0..1500 {
            cluster.step();
        }
        prop_assert!(cluster.history_len() == 1500);
    }

    /// 7-node chaos: majority=4, quorum-splitting partitions become
    /// more common, and more peers means more matchIndex combinations
    /// that must all satisfy the §5.3 commit condition. Safety holds.
    #[test]
    fn chaos_schedule_seven_nodes_preserves_safety(seed in any::<u64>()) {
        let mut cluster: Cluster<u64> = Cluster::new(seed, 7);
        cluster.set_policy(Policy::chaos(Some(1)));
        for _ in 0..2000 {
            cluster.step();
        }
        prop_assert!(cluster.history_len() == 2000);
    }
}

/// Sanity check: the recover path actually works end-to-end — a
/// cluster that gets crashed and recovered must still be able to
/// commit. Runs a happy-path cluster to first commit, manually crashes
/// one node and recovers it, then drives until the cluster commits
/// further entries. Safety invariants hold throughout.
#[test]
fn crash_and_recover_preserves_liveness() {
    use jotun_core::NodeId;

    let mut cluster: Cluster<u64> = Cluster::new(0x00C0_FFEE, 3);
    cluster.set_policy(Policy::happy(Some(7)));

    // Phase 1: reach first commit on majority.
    let steps = cluster.run_until(|c| c.applied_majority(1) >= 2, 1500);
    assert!(steps < 1500, "initial commit should happen");
    let start_commit = cluster.max_commit_index();

    // Phase 2: crash + recover a follower, then drive further.
    // Pick a non-leader so the leader stays up.
    let leader = cluster.leaders().into_iter().next().expect("leader");
    let target = [
        NodeId::new(1).unwrap(),
        NodeId::new(2).unwrap(),
        NodeId::new(3).unwrap(),
    ]
    .into_iter()
    .find(|n| *n != leader)
    .expect("non-leader exists");
    cluster.crash_for_test(target);
    for _ in 0..20 {
        cluster.step();
    }
    cluster.recover_for_test(target);

    // Phase 3: drive until commit advances past start_commit.
    let target_commit = start_commit + 1;
    let steps = cluster.run_until(|c| c.max_commit_index() >= target_commit, 3000);
    assert!(
        steps < 3000,
        "commit didn't advance past {start_commit} after crash+recover",
    );
}

/// Fixed-seed smoke run: chaos-mode 3-node cluster for a long run.
/// Useful when investigating a specific seed the proptest shrinker
/// landed on — `cargo test chaos_smoke_fixed_seed -- --nocapture` and
/// watch it go.
#[test]
fn chaos_smoke_fixed_seed() {
    let mut cluster: Cluster<u64> = Cluster::new(0xDEAD_BEEF, 3);
    cluster.set_policy(Policy::chaos(Some(7)));
    for _ in 0..1000 {
        cluster.step();
    }
    // No panic ⇒ safety held for 1000 chaotic steps on this seed.
}
