//! Tests for §4.2.1 learner semantics: non-voting members that
//! receive replication but never count toward quorum and never
//! campaign.

use super::fixtures::{
    append_entries_from, append_entries_request, append_entries_success_from,
    collect_append_entries, collect_persist_log_entries, follower_with_env, log_id, node,
    propose_add_learner, propose_promote_learner, propose_remove_peer, vote_request,
    vote_request_from, vote_response_from,
};
use crate::engine::engine::Engine;
use crate::engine::env::StaticEnv;
use crate::engine::event::Event;
use crate::engine::role_state::RoleState;
use crate::records::log_entry::{ConfigChange, LogPayload};
use crate::records::vote::VoteResult;

/// 3-node leader (self=1, voters=2,3) at term 1, log = [(1,1)=Noop].
fn elected_leader() -> Engine<Vec<u8>> {
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    engine.step(Event::Tick);
    engine.step(vote_response_from(2, 1, true));
    assert!(matches!(engine.role(), RoleState::Leader(_)));
    // Commit the noop so later ConfigChange proposes aren't blocked.
    engine.step(append_entries_success_from(2, 1, 1));
    engine.step(append_entries_success_from(3, 1, 1));
    engine
}

// ---------------------------------------------------------------------------
// AddLearner / PromoteLearner / RemoveLearner basic plumbing
// ---------------------------------------------------------------------------

#[test]
fn add_learner_emits_persist_config_entry() {
    let mut engine = elected_leader();
    let actions = engine.step(propose_add_learner(4));
    let persisted = collect_persist_log_entries(&actions);
    let cc = persisted
        .iter()
        .flat_map(|batch| batch.iter())
        .find_map(|e| match &e.payload {
            LogPayload::ConfigChange(c) => Some(*c),
            _ => None,
        })
        .expect("AddLearner must be persisted as a ConfigChange entry");
    assert!(matches!(cc, ConfigChange::AddLearner(id) if id == node(4)));
}

#[test]
fn add_learner_puts_node_in_learner_set_immediately_on_leader() {
    let mut engine = elected_leader();
    engine.step(propose_add_learner(4));
    // §4.3 pre-commit semantics: membership changes take effect the
    // moment the entry lands in the log.
    assert!(engine.membership().contains_learner(&node(4)));
    assert!(!engine.membership().contains_voter(&node(4)));
}

#[test]
fn promote_learner_moves_node_from_learner_to_voter() {
    let mut engine = elected_leader();
    engine.step(propose_add_learner(4));
    // §4.3: commit the AddLearner first; only one uncommitted CC at a time.
    let idx = engine.log().last_log_id().unwrap().index.get();
    engine.step(append_entries_success_from(2, 1, idx));
    engine.step(append_entries_success_from(3, 1, idx));
    assert!(engine.membership().contains_learner(&node(4)));

    engine.step(propose_promote_learner(4));
    assert!(engine.membership().contains_voter(&node(4)));
    assert!(!engine.membership().contains_learner(&node(4)));
}

#[test]
fn promote_unknown_learner_is_a_noop() {
    let mut engine = elected_leader();
    let before = engine.log().last_log_id().unwrap().index.get();
    let actions = engine.step(propose_promote_learner(4));
    // No Persist for a no-op proposal.
    let persisted = collect_persist_log_entries(&actions);
    assert!(persisted.is_empty(), "no-op promote must not persist");
    let after = engine.log().last_log_id().unwrap().index.get();
    assert_eq!(before, after);
}

#[test]
fn remove_peer_also_removes_learners() {
    let mut engine = elected_leader();
    engine.step(propose_add_learner(4));
    // Commit before proposing the next CC.
    let idx = engine.log().last_log_id().unwrap().index.get();
    engine.step(append_entries_success_from(2, 1, idx));
    engine.step(append_entries_success_from(3, 1, idx));
    assert!(engine.membership().contains_learner(&node(4)));
    engine.step(propose_remove_peer(4));
    assert!(!engine.membership().contains_learner(&node(4)));
    assert!(!engine.membership().contains_voter(&node(4)));
}

// ---------------------------------------------------------------------------
// Replication: learner receives AppendEntries; quorum excludes it
// ---------------------------------------------------------------------------

#[test]
fn broadcast_includes_learners() {
    let mut engine = elected_leader();
    engine.step(propose_add_learner(4));
    // Tick until a heartbeat fires.
    let mut saw_learner_recipient = false;
    for _ in 0..3 {
        let actions = engine.step(Event::Tick);
        let aes = collect_append_entries(&actions);
        if aes.iter().any(|(to, _)| *to == node(4)) {
            saw_learner_recipient = true;
            break;
        }
    }
    assert!(
        saw_learner_recipient,
        "leader must replicate to learners alongside voters",
    );
}

#[test]
fn learner_ack_does_not_advance_commit() {
    let mut engine = elected_leader();
    engine.step(propose_add_learner(4));
    // Append a client proposal so we have a fresh entry to commit.
    engine.step(Event::ClientProposal(b"a".to_vec()));
    let tail = engine.log().last_log_id().unwrap().index.get();
    let before_commit = engine.commit_index();
    // Only the learner acks. Commit must NOT advance (voters {2, 3}
    // haven't acked this term's entry).
    engine.step(append_entries_success_from(4, 1, tail));
    assert_eq!(
        engine.commit_index(),
        before_commit,
        "learner ack must not advance commit_index",
    );
}

// ---------------------------------------------------------------------------
// Learner never campaigns
// ---------------------------------------------------------------------------

#[test]
fn learner_does_not_start_an_election_on_timer_expiry() {
    // Bootstrap a node as follower in a 3-voter cluster, then inject
    // a committed AddLearner(self) so our own membership view marks
    // us as a learner.
    let mut engine = follower_with_env(1, &[2, 3], Box::new(StaticEnv(3)));
    // Accept a leader's AE carrying AddLearner(1) so our local
    // membership now contains self as learner. Leader is peer 2.
    let cc_entry = crate::records::log_entry::LogEntry {
        id: log_id(1, 1),
        payload: LogPayload::ConfigChange(ConfigChange::AddLearner(node(1))),
    };
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, vec![cc_entry], 1),
    ));
    assert!(engine.membership().contains_learner(&node(1)));
    let before_term = engine.current_term();
    // Now trigger the election timer well past expiry.
    for _ in 0..10 {
        engine.step(Event::Tick);
    }
    assert_eq!(
        engine.current_term(),
        before_term,
        "learner must not bump term by campaigning",
    );
    // Still a follower (or pre-candidate if pre-vote were on —
    // default test fixture has pre-vote off).
    assert!(matches!(engine.role(), RoleState::Follower(_)));
}

// ---------------------------------------------------------------------------
// Learner never grants votes
// ---------------------------------------------------------------------------

#[test]
fn learner_rejects_vote_requests() {
    let mut engine = follower_with_env(1, &[2, 3], Box::new(StaticEnv(10)));
    let cc_entry = crate::records::log_entry::LogEntry {
        id: log_id(1, 1),
        payload: LogPayload::ConfigChange(ConfigChange::AddLearner(node(1))),
    };
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, vec![cc_entry], 1),
    ));
    let actions = engine.step(vote_request_from(2, vote_request(2, 2, None)));
    let response = actions
        .iter()
        .find_map(|a| match a {
            crate::engine::action::Action::Send {
                message: crate::records::message::Message::VoteResponse(r),
                ..
            } => Some(*r),
            _ => None,
        })
        .expect("learner must still reply, just with Rejected");
    assert_eq!(response.result, VoteResult::Rejected);
}
