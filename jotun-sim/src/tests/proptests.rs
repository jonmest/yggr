//! Property-based tests driving the cluster through fuzzer-picked
//! seeds.
//!
//! The per-step invariant checks are already baked into [`Cluster::step`]
//! — any violation panics with the schedule. These proptests exist to
//! (a) generate diverse seeds and (b) assert a positive liveness
//! property (the cluster actually makes progress) so we catch silent
//! stalls as well as safety breaks.

use proptest::prelude::*;

use crate::Cluster;
use crate::cluster::Policy;

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        .. ProptestConfig::default()
    })]

    /// 3 nodes, 200 steps, no drops/partitions/crashes. Must converge
    /// (at least one leader observed at some point) without any
    /// invariant panicking.
    #[test]
    fn three_node_happy_schedules_converge(seed in any::<u64>()) {
        let mut cluster: Cluster<u64> = Cluster::new(seed, 3);
        cluster.set_policy(Policy::happy(Some(1)));

        let mut saw_leader = false;
        for _ in 0..200 {
            cluster.step();
            if !cluster.leaders().is_empty() {
                saw_leader = true;
            }
        }

        prop_assert!(
            saw_leader,
            "no leader ever elected across 200 steps for seed {seed}",
        );
    }

    /// Extended liveness: 3 nodes, 600 steps, must reach at least one
    /// committed entry on a majority. A cluster that elects a leader
    /// but never commits is a silent livelock; this test catches that.
    #[test]
    fn three_node_happy_schedules_commit_on_majority(seed in any::<u64>()) {
        let mut cluster: Cluster<u64> = Cluster::new(seed, 3);
        cluster.set_policy(Policy::happy(Some(1)));

        // 600 is generous — the happy path typically converges in
        // ~100-200 steps. If this cap is ever hit, fix the scheduler
        // rather than raising the bound.
        let mut committed = false;
        for _ in 0..600 {
            cluster.step();
            if cluster.applied_majority(1) >= 2 {
                committed = true;
                break;
            }
        }
        prop_assert!(
            committed,
            "no majority apply at index 1 within 600 steps for seed {seed}; max commit = {}",
            cluster.max_commit_index(),
        );
    }

    /// Determinism: two cluster instances with the same seed take the
    /// same path, step for step.
    #[test]
    fn determinism_holds_under_random_seed(seed in any::<u64>()) {
        let mut a: Cluster<u64> = Cluster::new(seed, 3);
        let mut b: Cluster<u64> = Cluster::new(seed, 3);
        a.set_policy(Policy::happy(Some(1)));
        b.set_policy(Policy::happy(Some(1)));

        for _ in 0..150 {
            a.step();
            b.step();
        }
        prop_assert_eq!(a.leaders(), b.leaders());
        prop_assert_eq!(a.max_commit_index(), b.max_commit_index());
        prop_assert_eq!(a.history_len(), b.history_len());
    }
}
