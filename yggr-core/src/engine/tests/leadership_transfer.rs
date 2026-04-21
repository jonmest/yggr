use super::fixtures::{
    append_entries_from, append_entries_request, append_entries_success_from, client_proposal,
    collect_append_entries, collect_persist_hard_state, collect_redirects, collect_timeout_now,
    collect_vote_requests, follower, follower_with_env, node, term, timeout_now_from,
    transfer_leadership, vote_response_from,
};
use crate::engine::engine::Engine;
use crate::engine::env::StaticEnv;
use crate::engine::role_state::RoleState;

fn elected_leader() -> Engine<Vec<u8>> {
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    engine.step(crate::engine::event::Event::Tick);
    engine.step(vote_response_from(2, 1, true));
    assert!(matches!(engine.role(), RoleState::Leader(_)));
    engine
}

#[test]
fn follower_with_known_leader_redirects_transfer_request() {
    let mut engine = follower(1);
    let actions = engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, vec![], 0),
    ));
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, crate::engine::action::Action::Send { .. }))
    );

    let actions = engine.step(transfer_leadership(3));
    assert_eq!(collect_redirects(&actions), vec![node(2)]);
}

#[test]
fn leader_sends_timeout_now_immediately_when_target_is_caught_up() {
    let mut engine = elected_leader();
    engine.step(append_entries_success_from(2, 1, 1));

    let actions = engine.step(transfer_leadership(2));
    assert_eq!(collect_timeout_now(&actions), vec![node(2)]);
}

#[test]
fn leader_catches_target_up_before_sending_timeout_now() {
    let mut engine = elected_leader();
    engine.step(client_proposal(b"cmd"));

    let actions = engine.step(transfer_leadership(2));
    assert!(
        collect_timeout_now(&actions).is_empty(),
        "transfer should wait for the target to catch up first",
    );
    let targeted_replication = collect_append_entries(&actions)
        .into_iter()
        .filter(|(peer, _)| *peer == node(2))
        .count();
    assert_eq!(targeted_replication, 1);

    let actions = engine.step(append_entries_success_from(2, 1, 2));
    assert_eq!(collect_timeout_now(&actions), vec![node(2)]);
}

#[test]
fn follower_starts_election_immediately_on_timeout_now_from_current_leader() {
    let mut engine = follower(1);
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, vec![], 0),
    ));

    let actions = engine.step(timeout_now_from(2, 1));

    assert!(matches!(engine.role(), RoleState::Candidate(_)));
    assert_eq!(engine.current_term(), term(2));
    let persisted = collect_persist_hard_state(&actions);
    assert_eq!(persisted.len(), 1);
    let votes = collect_vote_requests(&actions);
    assert_eq!(votes.len(), 2);
}

#[test]
fn same_term_timeout_now_from_unknown_leader_is_ignored() {
    let mut engine = follower(1);
    let actions = engine.step(timeout_now_from(2, 1));

    assert!(matches!(engine.role(), RoleState::Follower(_)));
    assert_eq!(engine.current_term(), term(0));
    assert!(actions.is_empty());
}

#[test]
fn removing_transfer_target_cancels_inflight_transfer() {
    let mut engine = elected_leader();
    engine.step(client_proposal(b"cmd"));

    let _ = engine.step(transfer_leadership(2));
    let RoleState::Leader(leader) = engine.role() else {
        panic!("expected leader");
    };
    assert_eq!(leader.transfer_target, Some(node(2)));

    let _ = engine.step(super::fixtures::propose_remove_peer(2));
    let RoleState::Leader(leader) = engine.role() else {
        panic!("expected leader");
    };
    assert_eq!(leader.transfer_target, None);
}
