//! Engine-side `ReadIndex` tests (Raft §8 linearizable reads).

use super::fixtures::{
    append_entries_from, append_entries_request, append_entries_success_from, collect_read_failed,
    collect_read_ready, follower, follower_with_env, node, propose_read, vote_response_from,
};
use crate::engine::action::ReadFailure;
use crate::engine::engine::Engine;
use crate::engine::env::StaticEnv;
use crate::engine::role_state::RoleState;

/// A 3-node cluster leader whose election no-op has been committed
/// and applied. After this, `ReadIndex` is safe to serve.
fn leader_with_committed_noop() -> Engine<Vec<u8>> {
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    engine.step(crate::engine::event::Event::Tick);
    engine.step(vote_response_from(2, 1, true));
    assert!(matches!(engine.role(), RoleState::Leader(_)));
    // Ack the noop (index 1) from one peer — majority = 2 (self + peer 2).
    engine.step(append_entries_success_from(2, 1, 1));
    // Noop should now be committed; term-at-commit == current_term.
    assert_eq!(engine.commit_index().get(), 1);
    engine
}

#[test]
fn leader_without_committed_current_term_entry_fails_notready() {
    // Freshly-elected leader — noop is appended but not yet acked.
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    engine.step(crate::engine::event::Event::Tick);
    engine.step(vote_response_from(2, 1, true));
    assert!(matches!(engine.role(), RoleState::Leader(_)));
    // commit_index is still 0 here; term-at(0) is None.
    assert_eq!(engine.commit_index().get(), 0);

    let actions = engine.step(propose_read(42));
    assert_eq!(
        collect_read_failed(&actions),
        vec![(42, ReadFailure::NotReady)],
    );
    assert!(collect_read_ready(&actions).is_empty());
}

#[test]
fn leader_emits_ready_after_majority_heartbeat_quorum() {
    let mut engine = leader_with_committed_noop();

    // Propose the read. This kicks a broadcast and queues the read
    // until majority acks the new heartbeat_seq.
    let actions = engine.step(propose_read(7));
    // No ready yet — peers haven't acked the post-read broadcast.
    assert!(collect_read_ready(&actions).is_empty());
    assert!(collect_read_failed(&actions).is_empty());

    // Peer 2 acks. With self + peer 2, majority (2/3) is reached.
    let actions = engine.step(append_entries_success_from(2, 1, 1));
    assert_eq!(collect_read_ready(&actions), vec![7]);
}

#[test]
fn single_node_leader_serves_read_immediately() {
    // A single-node cluster has no peers; majority is self alone.
    // The no-op commits on become_leader's try_advance path, so
    // propose_read should emit ReadReady synchronously.
    let env = StaticEnv(1);
    let mut engine = Engine::new(node(1), Vec::new(), Box::new(env), 1);
    engine.step(crate::engine::event::Event::Tick);
    assert!(matches!(engine.role(), RoleState::Leader(_)));
    assert_eq!(engine.commit_index().get(), 1);

    let actions = engine.step(propose_read(1));
    assert_eq!(collect_read_ready(&actions), vec![1]);
}

#[test]
fn follower_with_known_leader_redirects_read_request() {
    let mut engine = follower(1);
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, vec![], 0),
    ));

    let actions = engine.step(propose_read(3));
    assert_eq!(
        collect_read_failed(&actions),
        vec![(
            3,
            ReadFailure::NotLeader {
                leader_hint: node(2)
            }
        )],
    );
}

#[test]
fn follower_without_known_leader_fails_notready() {
    let mut engine = follower(1);
    let actions = engine.step(propose_read(9));
    assert_eq!(
        collect_read_failed(&actions),
        vec![(9, ReadFailure::NotReady)],
    );
}

#[test]
fn stepdown_fails_pending_reads() {
    let mut engine = leader_with_committed_noop();
    engine.step(propose_read(100));
    engine.step(propose_read(101));

    // A higher-term AE bumps us out of leader role.
    let actions = engine.step(append_entries_from(
        2,
        append_entries_request(99, 2, None, vec![], 0),
    ));

    let failed = collect_read_failed(&actions);
    // Both reads fail with SteppedDown — order matches submission.
    assert_eq!(
        failed,
        vec![
            (100, ReadFailure::SteppedDown),
            (101, ReadFailure::SteppedDown),
        ],
    );
}
