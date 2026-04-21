//! Tests for [`Engine::on_client_proposal`] — the application-side write
//! entry point. Leader appends + broadcasts; followers redirect when they
//! know who the leader is, otherwise drop; candidates always drop.
//!
//! Also covers `FollowerState::leader_id` bookkeeping via
//! `on_append_entries_request`, since the redirect path depends on it.

use super::fixtures::{
    append_entries_from, append_entries_request, client_proposal, collect_append_entries,
    collect_redirects, follower, follower_with_env, log_id, node, vote_response_from,
};
use crate::engine::engine::Engine;
use crate::engine::env::StaticEnv;
use crate::engine::event::Event;
use crate::engine::role_state::RoleState;
use crate::records::log_entry::LogPayload;
use crate::types::index::LogIndex;

/// 3-node leader (self=1, peers=2,3) at term 1, log = [(1,1)=Noop].
fn elected_leader_3_node() -> Engine<Vec<u8>> {
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    engine.step(Event::Tick);
    engine.step(vote_response_from(2, 1, true));
    assert!(matches!(engine.role(), RoleState::Leader(_)));
    engine
}

// ---------------------------------------------------------------------------
// Leader path — append at (last+1, current_term) and broadcast
// ---------------------------------------------------------------------------

#[test]
fn leader_appends_command_at_next_index_and_current_term() {
    let mut engine = elected_leader_3_node();
    let term_now = engine.current_term();
    let last_before = engine.log().last_log_id().unwrap().index;

    engine.step(client_proposal(b"set x = 1"));

    let last_after = engine.log().last_log_id().unwrap();
    assert_eq!(last_after.index, last_before.next());
    assert_eq!(last_after.term, term_now);
    let entry = engine.log().entry_at(last_after.index).unwrap();
    match &entry.payload {
        LogPayload::Command(c) => assert_eq!(c, b"set x = 1"),
        other => panic!("expected Command payload, got {other:?}"),
    }
}

#[test]
fn leader_broadcasts_immediately_after_appending() {
    let mut engine = elected_leader_3_node();
    let actions = engine.step(client_proposal(b"hello"));
    let appends = collect_append_entries(&actions);

    assert_eq!(
        appends.len(),
        2,
        "one AppendEntries per peer, kicked off without waiting for heartbeat",
    );
    let mut peers: Vec<u64> = appends.iter().map(|(p, _)| p.get()).collect();
    peers.sort_unstable();
    assert_eq!(peers, vec![2, 3]);
}

#[test]
fn first_proposal_on_empty_log_lands_at_index_one() {
    // Construct a leader synthetically with an empty log so we can verify
    // the LogIndex::new(1) fallback in next_index. (A naturally elected
    // leader always has the no-op at index 1 already.)
    let mut engine = follower(1);
    engine.state_mut().current_term = crate::types::term::Term::new(2);
    engine.state_mut().role = RoleState::Leader(crate::engine::role_state::LeaderState::default());

    engine.step(client_proposal(b"first"));

    let last = engine.log().last_log_id().unwrap();
    assert_eq!(last.index, LogIndex::new(1));
}

#[test]
fn leader_proposal_carries_command_to_peers() {
    let mut engine = elected_leader_3_node();
    let actions = engine.step(client_proposal(b"payload"));
    let appends = collect_append_entries(&actions);

    for (_peer, req) in &appends {
        // The new command must be in the entries vec being shipped.
        let last = req.entries.last().expect("must carry the new entry");
        match &last.payload {
            LogPayload::Command(c) => assert_eq!(c, b"payload"),
            other => panic!("expected Command in broadcast tail, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Follower path — Redirect when leader known, drop otherwise
// ---------------------------------------------------------------------------

#[test]
fn follower_with_known_leader_emits_redirect() {
    // A fresh follower learns its leader from any accepted AppendEntries.
    let mut engine = follower(1);
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, vec![], 0),
    ));
    // Now a client lands on us.
    let actions = engine.step(client_proposal(b"go away"));
    let redirects = collect_redirects(&actions);
    assert_eq!(redirects, vec![node(2)]);
    // No entries appended on a follower.
    assert!(engine.log().is_empty());
}

#[test]
fn follower_without_known_leader_drops_silently() {
    // Just booted — never heard from a leader. Nothing to redirect to.
    let mut engine = follower(1);
    let actions = engine.step(client_proposal(b"orphan"));
    assert!(actions.is_empty());
    assert!(engine.log().is_empty());
}

#[test]
fn higher_term_append_entries_resets_known_leader_and_then_records_new_one() {
    // Follower learned leader=2 at term 1. A higher-term AE from leader=3
    // demotes via become_follower (which clears leader_id), then records
    // leader=3 for the new term.
    let mut engine = follower(1);
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, vec![], 0),
    ));
    if let RoleState::Follower(f) = engine.role() {
        assert_eq!(f.leader_id, Some(node(2)));
    } else {
        panic!("expected Follower");
    }

    engine.step(append_entries_from(
        3,
        append_entries_request(5, 3, None, vec![], 0),
    ));
    if let RoleState::Follower(f) = engine.role() {
        assert_eq!(f.leader_id, Some(node(3)), "leader for new term recorded");
    } else {
        panic!("expected Follower");
    }
}

#[test]
fn stepping_down_to_follower_clears_known_leader() {
    // A higher-term VoteResponse demotes a candidate to follower. The new
    // FollowerState is fresh — leader_id starts None until the new
    // leader's first AppendEntries arrives.
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    // First, become a follower of leader 2 at term 1 so leader_id is set.
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, vec![], 0),
    ));
    // Then trigger candidacy.
    for _ in 0..crate::engine::tests::fixtures::DEFAULT_ELECTION_TIMEOUT {
        engine.step(Event::Tick);
    }
    assert!(matches!(engine.role(), RoleState::Candidate(_)));
    // A higher-term response demotes us back to follower.
    engine.step(vote_response_from(2, 99, false));
    match engine.role() {
        RoleState::Follower(f) => assert_eq!(
            f.leader_id, None,
            "new term, new election in progress: no trusted leader yet",
        ),
        other => panic!("expected Follower, got {other:?}"),
    }

    // And a proposal here drops, since there's no leader to redirect to.
    let actions = engine.step(client_proposal(b"limbo"));
    assert!(actions.is_empty());
}

// ---------------------------------------------------------------------------
// Candidate path — always drop
// ---------------------------------------------------------------------------

#[test]
fn candidate_drops_proposal_silently() {
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    engine.step(Event::Tick); // → Candidate
    assert!(matches!(engine.role(), RoleState::Candidate(_)));

    let actions = engine.step(client_proposal(b"midflight"));
    assert!(actions.is_empty());
    // Log untouched.
    assert!(engine.log().is_empty());
}

// ---------------------------------------------------------------------------
// leader_id is recorded only on accepted AE (term-valid path)
// ---------------------------------------------------------------------------

#[test]
fn stale_term_append_entries_does_not_update_known_leader() {
    let mut engine = follower(1);
    // First, set leader=2 at term 5.
    engine.step(append_entries_from(
        2,
        append_entries_request(5, 2, None, vec![], 0),
    ));
    if let RoleState::Follower(f) = engine.role() {
        assert_eq!(f.leader_id, Some(node(2)));
    } else {
        panic!("expected Follower");
    }

    // Stale AE from leader=3 at term 1 must be rejected and must not
    // overwrite leader_id.
    engine.step(append_entries_from(
        3,
        append_entries_request(1, 3, None, vec![], 0),
    ));
    if let RoleState::Follower(f) = engine.role() {
        assert_eq!(
            f.leader_id,
            Some(node(2)),
            "stale AE must not hijack leader"
        );
    } else {
        panic!("expected Follower");
    }
}

#[test]
fn prev_log_mismatch_still_records_leader_for_the_term() {
    // The peer at term 1 is the legitimate leader of the term even if its
    // first AE conflicts with our log — we should still know who it is so
    // a client redirect is possible.
    let mut engine = follower(1);
    // Seed our log so a prev_log_id mismatch is forced.
    crate::engine::tests::fixtures::seed_log(&mut engine, &[1, 1]);

    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, Some(log_id(99, 1)), vec![], 0),
    ));
    if let RoleState::Follower(f) = engine.role() {
        assert_eq!(
            f.leader_id,
            Some(node(2)),
            "leader recorded before the prev_log check fails",
        );
    } else {
        panic!("expected Follower");
    }
}

// ---------------------------------------------------------------------------
// Invariants (property tests)
// ---------------------------------------------------------------------------

use proptest::prelude::*;

proptest! {
    /// On a leader, every proposal grows the log by exactly 1, with the
    /// new entry at (last_before + 1, current_term) and a Command payload.
    #[test]
    fn leader_log_grows_by_one_per_proposal(
        commands in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..16), 0..10),
    ) {
        let mut engine = elected_leader_3_node();
        for cmd in commands {
            let term_now = engine.current_term();
            let last_before = engine
                .log()
                .last_log_id()
                .map_or(0, |l| l.index.get());

            engine.step(Event::ClientProposal(cmd.clone()));

            let last_after = engine.log().last_log_id().expect("log non-empty after proposal");
            prop_assert_eq!(last_after.index.get(), last_before + 1);
            prop_assert_eq!(last_after.term, term_now);

            let entry = engine.log().entry_at(last_after.index).expect("entry must exist");
            match &entry.payload {
                LogPayload::Command(c) => prop_assert_eq!(c, &cmd),
                other => prop_assert!(false, "expected Command, got {other:?}"),
            }
        }
    }

    /// Non-leaders never grow their own log via a proposal — replication
    /// is leader-driven. Followers may emit a Redirect; candidates always
    /// drop. Either way the log is untouched.
    #[test]
    fn non_leader_proposal_never_grows_log(
        commands in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..16), 0..10),
        as_candidate in any::<bool>(),
    ) {
        let env = StaticEnv(1);
        let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
        if as_candidate {
            engine.step(Event::Tick);
            prop_assert!(matches!(engine.role(), RoleState::Candidate(_)));
        }
        let log_len_before = engine.log().len();
        for cmd in commands {
            engine.step(Event::ClientProposal(cmd));
            prop_assert_eq!(engine.log().len(), log_len_before);
        }
    }
}

// ---------------------------------------------------------------------------
// Event::ClientProposalBatch — multiple commands land as one
// PersistLogEntries Action and one broadcast per peer.
// ---------------------------------------------------------------------------

use crate::engine::action::Action;
use crate::records::message::Message;

#[test]
fn batch_appends_all_commands_in_one_persist() {
    let mut engine = elected_leader_3_node();
    let term_now = engine.current_term();
    let last_before = engine.log().last_log_id().unwrap().index;

    let actions = engine.step(Event::ClientProposalBatch(vec![
        b"a".to_vec(),
        b"b".to_vec(),
        b"c".to_vec(),
    ]));

    // Three entries appended at consecutive indices.
    let last_after = engine.log().last_log_id().unwrap().index;
    assert_eq!(last_after.get(), last_before.get() + 3);
    for offset in 0..3 {
        let entry = engine
            .log()
            .entry_at(LogIndex::new(last_before.get() + 1 + offset))
            .expect("entry at batch index");
        assert_eq!(entry.id.term, term_now);
        assert!(matches!(entry.payload, LogPayload::Command(_)));
    }

    // Exactly one PersistLogEntries Action carrying all three entries.
    let persists: Vec<_> = actions
        .iter()
        .filter_map(|a| match a {
            Action::PersistLogEntries(v) => Some(v.len()),
            _ => None,
        })
        .collect();
    assert_eq!(persists, vec![3]);

    // One AppendEntries send per peer (2 peers, so 2 sends).
    let sends: Vec<_> = actions
        .iter()
        .filter(|a| {
            matches!(
                a,
                Action::Send {
                    message: Message::AppendEntriesRequest(_),
                    ..
                }
            )
        })
        .collect();
    assert_eq!(sends.len(), 2);
}

#[test]
fn empty_batch_is_noop() {
    let mut engine = elected_leader_3_node();
    let last_before = engine.log().last_log_id().unwrap().index;

    let actions = engine.step(Event::ClientProposalBatch(Vec::<Vec<u8>>::new()));

    assert!(actions.is_empty());
    let last_after = engine.log().last_log_id().unwrap().index;
    assert_eq!(last_before, last_after);
}

#[test]
fn follower_with_known_leader_redirects_batch() {
    let mut engine = follower(1);
    let actions = engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, vec![], 0),
    ));
    assert!(actions.iter().any(|a| matches!(a, Action::Send { .. })));

    let actions = engine.step(Event::ClientProposalBatch(vec![b"x".to_vec()]));
    // A single Redirect, not N — the host coalesces.
    assert_eq!(collect_redirects(&actions), vec![node(2)]);
}
