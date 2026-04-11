//! Tests for the cluster-membership boundary check at `on_incoming`.
//!
//! `Incoming::from` is the transport-authenticated sender; the engine
//! additionally requires that sender to be a configured peer of this
//! node, otherwise stray messages from non-members could perturb
//! safety-critical state (e.g. inflate `votes_granted` toward an
//! invalid majority, or poison `matchIndex`).

use super::fixtures::{
    append_entries_from, append_entries_request, append_entries_success_from, follower,
    follower_with_env, node, vote_request, vote_request_from, vote_response_from,
};
use crate::engine::env::StaticEnv;
use crate::engine::event::Event;
use crate::engine::role_state::RoleState;

// ---------------------------------------------------------------------------
// Non-member messages are dropped wholesale
// ---------------------------------------------------------------------------

#[test]
fn vote_request_from_non_member_is_dropped() {
    // follower(1) has peers {2, 3}. Node 99 is not a member.
    let mut engine = follower(1);
    let actions = engine.step(vote_request_from(99, vote_request(99, 1, None)));
    assert!(actions.is_empty(), "non-member request must yield no actions");
    assert_eq!(engine.voted_for(), None, "must not record a vote for a non-member");
}

#[test]
fn append_entries_request_from_non_member_is_dropped() {
    let mut engine = follower(1);
    let actions = engine.step(append_entries_from(
        99,
        append_entries_request(5, 99, None, vec![], 0),
    ));
    assert!(actions.is_empty(), "non-member request must yield no actions");
    // Crucially, the higher term in the body must NOT have been observed.
    // Otherwise a non-member could force everyone into a phantom term.
    assert_eq!(
        engine.current_term(),
        crate::types::term::Term::ZERO,
        "non-member message must not advance our term",
    );
}

#[test]
fn vote_response_from_non_member_does_not_count_toward_majority() {
    // 5-node cluster, self=1, peers={2,3,4,5}. Majority = 3.
    // Self always grants self-vote; peer 2 grants → 2 votes; a stray
    // VoteResponse from non-member 99 must NOT push us to 3.
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3, 4, 5], Box::new(env));
    engine.step(Event::Tick); // → Candidate at term 1
    engine.step(vote_response_from(2, 1, true));
    assert!(matches!(engine.role(), RoleState::Candidate(_)));

    engine.step(vote_response_from(99, 1, true));
    assert!(
        matches!(engine.role(), RoleState::Candidate(_)),
        "non-member grant must not elect us",
    );
}

#[test]
fn append_entries_response_from_non_member_does_not_update_progress() {
    // 3-node leader. Stray Success { last_appended: 1 } from non-member
    // must not poison matchIndex for any peer.
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    engine.step(Event::Tick);
    engine.step(vote_response_from(2, 1, true));
    assert!(matches!(engine.role(), RoleState::Leader(_)));

    engine.step(append_entries_success_from(99, 1, 1));
    let RoleState::Leader(s) = engine.role() else {
        panic!("expected Leader");
    };
    // Only the configured peers exist in progress; node 99 was never tracked.
    assert!(s.progress().match_for(node(99)).is_none());
}

// ---------------------------------------------------------------------------
// Body id must match the authenticated `from`
// ---------------------------------------------------------------------------

#[test]
fn vote_request_body_candidate_id_must_match_from() {
    // Authenticated sender is peer 2; body claims candidate_id = 3 (also
    // a peer, but the wire identity is the source of truth). Drop.
    let mut engine = follower(1);
    let req = vote_request(3, 1, None); // body says 3
    let actions = engine.step(vote_request_from(2, req)); // wire says 2
    assert!(actions.is_empty(), "mismatched candidate_id must be dropped");
    assert_eq!(engine.voted_for(), None);
}

#[test]
fn append_entries_request_body_leader_id_must_match_from() {
    let mut engine = follower(1);
    let req = append_entries_request(1, 3, None, vec![], 0); // body says 3
    let actions = engine.step(append_entries_from(2, req)); // wire says 2
    assert!(actions.is_empty(), "mismatched leader_id must be dropped");
}
