//! Tests for the [`Action::PersistHardState`] / [`Action::PersistLogEntries`]
//! emission contract. The engine must tell the host what to flush before
//! any subsequent network send (Figure 2: "respond to RPCs only after
//! updating stable storage").

use super::fixtures::{
    append_entries_from, append_entries_request, append_entries_success_from, client_proposal,
    collect_persist_hard_state, collect_persist_log_entries, expect_vote_response, follower,
    follower_with_env, log_id, node, term, vote_request, vote_request_from, vote_response_from,
};
use crate::engine::action::Action;
use crate::engine::env::StaticEnv;
use crate::engine::event::Event;
use crate::engine::role_state::RoleState;
use crate::records::log_entry::LogPayload;

// ---------------------------------------------------------------------------
// PersistHardState — emitted on current_term / voted_for change
// ---------------------------------------------------------------------------

#[test]
fn granting_a_vote_emits_persist_hard_state_before_response() {
    // Fresh follower (term 0, voted_for=None) grants a vote at term 1.
    // Both current_term and voted_for changed; persist must precede send.
    let mut engine = follower(1);
    let actions = engine.step(vote_request_from(2, vote_request(2, 1, None)));

    let persists = collect_persist_hard_state(&actions);
    assert_eq!(persists.len(), 1);
    assert_eq!(persists[0], (term(1), Some(node(2))));

    // PersistHardState must come *before* the Send.
    let positions: Vec<usize> = actions
        .iter()
        .enumerate()
        .filter_map(|(i, a)| match a {
            Action::PersistHardState { .. } | Action::Send { .. } => Some(i),
            _ => None,
        })
        .collect();
    assert!(matches!(
        actions[positions[0]],
        Action::PersistHardState { .. }
    ));
    assert!(matches!(actions[positions[1]], Action::Send { .. }));

    let response = expect_vote_response(&actions);
    assert_eq!(response.term, term(1));
}

#[test]
fn rejecting_a_vote_at_same_term_emits_no_persist() {
    // Follower at term 1 already voted for node 2. A second request from
    // node 3 at the same term is rejected; nothing persisted, nothing to
    // flush — vote response only.
    let mut engine = follower(1);
    engine.step(vote_request_from(2, vote_request(2, 1, None)));
    let actions = engine.step(vote_request_from(3, vote_request(3, 1, None)));
    assert!(collect_persist_hard_state(&actions).is_empty());
}

#[test]
fn vote_request_at_higher_term_emits_persist_even_when_rejected() {
    // We have log [(1,5)]. A candidate at term 9 with a stale log
    // (last_log_id=None) is rejected, but the higher term still bumped
    // our current_term and cleared voted_for — must persist.
    let mut engine = follower(1);
    super::fixtures::seed_log(&mut engine, &[5]);

    let actions = engine.step(vote_request_from(2, vote_request(2, 9, None)));

    let persists = collect_persist_hard_state(&actions);
    assert_eq!(persists.len(), 1);
    assert_eq!(persists[0], (term(9), None));
}

#[test]
fn higher_term_append_entries_response_emits_persist() {
    // Leader sees an AE response at a higher term → step down. The new
    // term must hit disk before the host can act on anything else.
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    engine.step(Event::Tick);
    engine.step(vote_response_from(2, 1, true));
    assert!(matches!(engine.role(), RoleState::Leader(_)));

    let actions = engine.step(append_entries_success_from(2, 99, 0));
    let persists = collect_persist_hard_state(&actions);
    assert_eq!(persists.len(), 1);
    assert_eq!(persists[0], (term(99), None));
}

#[test]
fn election_start_emits_persist_for_self_vote_and_term_bump() {
    // Election timeout fires → become_candidate bumps term and votes
    // self. Persist must precede the RequestVote sends — a crashed
    // candidate that recovers without remembering its own vote could
    // re-vote in the same term.
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    let actions = engine.step(Event::Tick);

    let persists = collect_persist_hard_state(&actions);
    assert_eq!(persists.len(), 1);
    assert_eq!(persists[0], (term(1), Some(node(1))));

    // Persist comes before any Send.
    let first_persist = actions
        .iter()
        .position(|a| matches!(a, Action::PersistHardState { .. }))
        .unwrap();
    let first_send = actions
        .iter()
        .position(|a| matches!(a, Action::Send { .. }))
        .unwrap();
    assert!(first_persist < first_send);
}

// ---------------------------------------------------------------------------
// PersistLogEntries — emitted whenever the log grows
// ---------------------------------------------------------------------------

#[test]
fn follower_emits_persist_log_entries_for_appended_entries() {
    let mut engine = follower(1);
    let entries = super::fixtures::log_entries(&[(1, 1), (2, 1)]);
    let actions = engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, entries.clone(), 0),
    ));

    let persisted = collect_persist_log_entries(&actions);
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0], entries);
}

#[test]
fn follower_emits_no_persist_log_entries_when_nothing_new_appended() {
    // First call appends; second call is idempotent — nothing new on disk.
    let mut engine = follower(1);
    let entries = super::fixtures::log_entries(&[(1, 1)]);
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, entries.clone(), 0),
    ));
    let actions = engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, entries, 0),
    ));
    assert!(collect_persist_log_entries(&actions).is_empty());
}

#[test]
fn become_leader_emits_persist_log_entries_for_the_noop() {
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    engine.step(Event::Tick);
    let actions = engine.step(vote_response_from(2, 1, true));
    assert!(matches!(engine.role(), RoleState::Leader(_)));

    let persisted = collect_persist_log_entries(&actions);
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].len(), 1);
    assert_eq!(persisted[0][0].id, log_id(1, 1));
    assert!(matches!(persisted[0][0].payload, LogPayload::Noop));
}

#[test]
fn client_proposal_on_leader_emits_persist_before_broadcast() {
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    engine.step(Event::Tick);
    engine.step(vote_response_from(2, 1, true));
    assert!(matches!(engine.role(), RoleState::Leader(_)));

    let actions = engine.step(client_proposal(b"x"));

    let persisted = collect_persist_log_entries(&actions);
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].len(), 1);
    match &persisted[0][0].payload {
        LogPayload::Command(c) => assert_eq!(c, b"x"),
        other => panic!("expected Command, got {other:?}"),
    }

    // Persist precedes the Send broadcasts.
    let first_persist = actions
        .iter()
        .position(|a| matches!(a, Action::PersistLogEntries(_)))
        .unwrap();
    let first_send = actions
        .iter()
        .position(|a| matches!(a, Action::Send { .. }))
        .unwrap();
    assert!(first_persist < first_send);
}

#[test]
fn client_proposal_on_follower_emits_no_persist_log_entries() {
    let mut engine = follower(1);
    // Give us a known leader so we Redirect rather than drop.
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, vec![], 0),
    ));
    let actions = engine.step(client_proposal(b"x"));
    assert!(collect_persist_log_entries(&actions).is_empty());
}
