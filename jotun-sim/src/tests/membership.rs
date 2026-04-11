//! Scripted membership-change scenarios driven through the sim.
//!
//! The random scheduler doesn't inject `ProposeConfigChange` events
//! (membership rate vs convergence is awkward to balance random-uniformly
//! and isn't load-bearing yet). These tests use
//! [`Cluster::propose_config_change_to`] to drive the changes deterministically
//! while letting the scheduler pick everything else.

use jotun_core::{ConfigChange, NodeId};

use crate::Cluster;
use crate::cluster::Policy;

fn id(n: u64) -> NodeId {
    NodeId::new(n).expect("non-zero")
}

/// Drive the cluster until a single leader is elected, panicking on
/// timeout. Returns the elected leader id.
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

/// 3-node cluster: elect a leader, add a 4th node, then remove one of
/// the originals. Cluster must keep electing leaders and applying
/// proposals throughout. Safety invariants run after every step — a
/// CC violation would panic mid-run.
#[test]
fn add_then_remove_keeps_cluster_alive() {
    let mut cluster: Cluster<u64> = Cluster::new(0xCAFE, 3);
    cluster.set_policy(Policy::happy(Some(7)));

    let leader = run_until_leader(&mut cluster, 400);
    let initial_commit = cluster.max_commit_index();

    // Add node 4 to the harness so it can receive RPCs, then propose
    // AddPeer(4). The leader appends, the active config grows, and the
    // CC entry replicates to peers.
    cluster.add_node(id(4));
    cluster.propose_config_change_to(leader, ConfigChange::AddPeer(id(4)));

    // Drive until commit advances past the AddPeer entry. New majority
    // is 3 of 4 — leader plus two more must ack.
    let steps = cluster.run_until(
        |c| c.max_commit_index() > initial_commit,
        1200,
    );
    assert!(
        steps < 1200,
        "AddPeer never committed; max commit = {}",
        cluster.max_commit_index(),
    );
    let post_add_commit = cluster.max_commit_index();

    // Now remove an original peer that ISN'T the leader.
    let victim = [id(2), id(3)]
        .into_iter()
        .find(|&p| p != leader)
        .expect("self id from {2,3}");
    cluster.propose_config_change_to(leader, ConfigChange::RemovePeer(victim));

    // After RemovePeer, new majority is 2 of 3 (leader + node 4 + the
    // surviving original). Drive until commit advances past Remove.
    let steps = cluster.run_until(
        |c| c.max_commit_index() > post_add_commit,
        1200,
    );
    assert!(
        steps < 1200,
        "RemovePeer never committed; max commit = {}",
        cluster.max_commit_index(),
    );

    // Sanity: the leader from before is still leading. (A self-remove
    // would step it down, but we removed someone else.)
    assert!(
        cluster.leaders().contains(&leader),
        "non-self remove must not depose the leader",
    );

    // Drive a few more proposals to verify continued progress.
    let post_remove_commit = cluster.max_commit_index();
    let steps = cluster.run_until(
        |c| c.max_commit_index() > post_remove_commit + 1,
        800,
    );
    assert!(
        steps < 800,
        "no further commits after membership churn; max commit = {}",
        cluster.max_commit_index(),
    );
}

/// Self-removal: leader proposes its own removal, the change commits,
/// and the leader steps down. Cluster must elect a new leader from
/// the surviving members.
#[test]
fn self_remove_steps_down_and_cluster_re_elects() {
    let mut cluster: Cluster<u64> = Cluster::new(0xBEEF, 3);
    cluster.set_policy(Policy::happy(Some(11)));

    let leader = run_until_leader(&mut cluster, 400);

    cluster.propose_config_change_to(leader, ConfigChange::RemovePeer(leader));

    // Drive until either the old leader has stepped down (no longer in
    // leaders()) or a new leader from a different node has emerged.
    let steps = cluster.run_until(
        |c| {
            let ls = c.leaders();
            !ls.contains(&leader) || ls.iter().any(|&l| l != leader)
        },
        1500,
    );
    assert!(
        steps < 1500,
        "leader did not step down after self-remove; leaders={:?}",
        cluster.leaders(),
    );

    // The cluster should re-elect a new leader from the other two nodes.
    let steps = cluster.run_until(
        |c| {
            let ls = c.leaders();
            ls.len() == 1 && !ls.contains(&leader)
        },
        1500,
    );
    assert!(
        steps < 1500,
        "no replacement leader elected; leaders={:?}",
        cluster.leaders(),
    );
}
