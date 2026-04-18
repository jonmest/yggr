//! Scripted snapshot scenarios driven through the sim. The random
//! scheduler doesn't pick `Snapshot` events on its own (snapshot rate
//! is application-policy, not load-bearing for safety fuzzing); these
//! tests drive them via `Cluster::snapshot_to`.

use jotun_core::NodeId;

use crate::Cluster;
use crate::cluster::Policy;

fn id(n: u64) -> NodeId {
    NodeId::new(n).expect("non-zero")
}

fn run_until_leader(cluster: &mut Cluster<u64>, max_steps: usize) -> NodeId {
    let steps = cluster.run_until(|c| c.leaders().len() == 1, max_steps);
    assert!(
        steps < max_steps,
        "no leader elected after {max_steps} steps; leaders: {:?}",
        cluster.leaders(),
    );
    cluster
        .leaders()
        .iter()
        .next()
        .copied()
        .expect("just asserted exactly one leader")
}

/// Leader takes a snapshot. Floor advances; subsequent broadcasts can
/// rely on the floor for prev_log_id derivation. Most importantly,
/// the SnapshotPastCommit invariant runs after every step and must
/// not fire.
#[test]
fn leader_takes_snapshot_keeps_safety() {
    let mut cluster: Cluster<u64> = Cluster::new(0xF00D, 3);
    cluster.set_policy(Policy::happy(Some(99)));
    let leader = run_until_leader(&mut cluster, 400);
    // Drive a few proposals so commit advances past the noop.
    cluster.run_until(|c| c.max_commit_index() >= 3, 600);

    let commit = cluster.max_commit_index();
    assert!(commit >= 1, "needed a non-empty committed prefix");

    cluster.snapshot_to(leader, b"snapshot-bytes".to_vec());

    // Cluster keeps making progress after the snapshot.
    let post_snap_commit = cluster.max_commit_index();
    let steps = cluster.run_until(|c| c.max_commit_index() > post_snap_commit, 3000);
    assert!(
        steps < 3000,
        "no progress after snapshot; max commit = {}",
        cluster.max_commit_index(),
    );
}

/// Crash a node, advance the cluster (committing entries past where
/// the crashed node was), take a snapshot on the leader, then recover
/// the crashed node. Without InstallSnapshot the recovered node would
/// be stuck at its old log; with it, the leader catches it up via the
/// snapshot once nextIndex falls below the floor.
#[test]
fn crashed_node_catches_up_via_install_snapshot_after_recover() {
    let mut cluster: Cluster<u64> = Cluster::new(0xC0DE, 3);
    cluster.set_policy(Policy::happy(Some(7)));
    let leader = run_until_leader(&mut cluster, 400);

    // Pick a victim that isn't the leader.
    let victim = [id(1), id(2), id(3)]
        .into_iter()
        .find(|&n| n != leader)
        .expect("at least one non-leader peer");

    // Make sure something has committed before we crash.
    cluster.run_until(|c| c.max_commit_index() >= 1, 200);
    cluster.crash_for_test(victim);

    // Cluster keeps committing without the victim. With one of three
    // down we still have a 2-of-3 majority.
    let pre_snap = cluster.max_commit_index();
    cluster.run_until(|c| c.max_commit_index() > pre_snap + 2, 1000);

    // Leader takes a snapshot.
    cluster.snapshot_to(leader, b"snap-after-crash".to_vec());

    // Recover victim.
    cluster.recover_for_test(victim);

    // Driving forward: leader's broadcast to victim should now use
    // InstallSnapshot since the victim's nextIndex is below the floor.
    // We verify via observable state: victim eventually sees
    // commit_index ≥ leader's snapshot floor.
    let snap_floor = {
        let v = cluster
            .nodes()
            .get(&leader)
            .and_then(|h| h.engine.as_ref())
            .expect("leader engine");
        v.log().snapshot_last().index.get()
    };
    assert!(snap_floor > 0, "leader should have a snapshot");

    let steps = cluster.run_until(
        |c| {
            c.nodes()
                .get(&victim)
                .and_then(|h| h.engine.as_ref())
                .is_some_and(|e| e.commit_index().get() >= snap_floor)
        },
        2000,
    );
    assert!(
        steps < 2000,
        "victim never caught up to snapshot floor {snap_floor}; \
         victim commit = {}",
        cluster
            .nodes()
            .get(&victim)
            .and_then(|h| h.engine.as_ref())
            .map_or(0, |e| e.commit_index().get()),
    );
}
