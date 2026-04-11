//! Tests for the `AppendEntries` receiver in [`Engine::on_append_entries_request`].
use std::collections::BTreeSet;

use super::fixtures::{
    append_entries_from, append_entries_request, collect_apply, expect_append_entries_response,
    follower, log_entries, log_id, seed_log, term,
};
use crate::records::append_entries::AppendEntriesResult;
use crate::types::index::LogIndex;
use crate::types::node::NodeId;
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Term handling — §5.1
// ---------------------------------------------------------------------------

#[test]
fn stale_term_is_rejected_without_state_change() {
    let mut engine = follower(1);
    // Advance to term 5 via a vote grant.
    engine.step(super::fixtures::vote_request_from(
        2,
        super::fixtures::vote_request(2, 5, None),
    ));
    let voted = engine.voted_for();

    let actions = engine.step(append_entries_from(
        3,
        append_entries_request(4, 3, None, vec![], 0),
    ));

    let response = expect_append_entries_response(&actions);
    assert!(matches!(
        response.result,
        AppendEntriesResult::Conflict { .. }
    ));
    assert_eq!(response.term, term(5), "response carries our term");
    assert_eq!(engine.current_term(), term(5));
    assert_eq!(
        engine.voted_for(),
        voted,
        "stale append does not clear vote"
    );
}

#[test]
fn higher_term_triggers_catch_up_before_deciding() {
    let mut engine = follower(1);

    engine.step(append_entries_from(
        2,
        append_entries_request(3, 2, None, vec![], 0),
    ));

    assert_eq!(engine.current_term(), term(3), "term caught up");
}

#[test]
fn candidate_steps_down_when_current_term_leader_appears() {
    // §5.2: "If AppendEntries RPC received from new leader: convert to follower."
    // A candidate who discovers a legitimate leader at its own term must
    // abandon the campaign and accept the leader.
    use crate::engine::role_state::RoleState;

    let mut engine = follower(1);
    // Become a candidate in term 1 (self-vote, role = Candidate).
    engine.step(super::fixtures::vote_request_from(
        2,
        super::fixtures::vote_request(2, 1, None),
    ));
    // Simulate transitioning to Candidate by bumping term via another vote
    // path is awkward; use state_mut to set the role directly, matching what
    // `become_candidate` would do at term 1.
    {
        let state = engine.state_mut();
        state.current_term = term(1);
        state.voted_for = Some(super::fixtures::node(1));
        let mut votes_granted = BTreeSet::new();
        votes_granted.insert(NodeId::new(1).unwrap());

        state.role =
            RoleState::Candidate(crate::engine::role_state::CandidateState { votes_granted });
    }

    // A legitimate leader for term 1 sends an AppendEntries.
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, vec![], 0),
    ));

    assert!(
        matches!(engine.role(), RoleState::Follower(_)),
        "candidate must convert to follower on seeing a current-term leader",
    );
    assert_eq!(engine.current_term(), term(1), "term unchanged");
}

#[test]
fn candidate_step_down_in_same_term_preserves_self_vote() {
    // §5.1 + §5.2: voted_for is per-term. A candidate who steps down to
    // follower because of a same-term AppendEntries from the legitimate
    // leader has already cast its self-vote in this term and MUST keep
    // it — otherwise a delayed RequestVote from another candidate could
    // be granted, violating "at most one vote per term".
    use crate::engine::role_state::{CandidateState, RoleState};
    use super::fixtures::{node, vote_request, vote_request_from};
    use crate::records::vote::VoteResult;

    let mut engine = follower(1);
    {
        let state = engine.state_mut();
        state.current_term = term(1);
        state.voted_for = Some(node(1));
        let mut votes_granted = BTreeSet::new();
        votes_granted.insert(node(1));
        state.role = RoleState::Candidate(CandidateState { votes_granted });
    }

    // Same-term leader appears → step down.
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, vec![], 0),
    ));
    assert!(matches!(engine.role(), RoleState::Follower(_)));
    assert_eq!(engine.current_term(), term(1));
    assert_eq!(
        engine.voted_for(),
        Some(node(1)),
        "self-vote in current term must survive same-term step-down",
    );

    // A late RequestVote from a different candidate at the same term
    // must be rejected — we already voted for ourselves.
    let actions = engine.step(vote_request_from(3, vote_request(3, 1, None)));
    let response = super::fixtures::expect_vote_response(&actions);
    assert_eq!(
        response.result,
        VoteResult::Rejected,
        "must reject RequestVote at term we already voted in",
    );
}

// ---------------------------------------------------------------------------
// Previous-log matching — §5.3 rule 2
// ---------------------------------------------------------------------------

#[test]
fn missing_prev_log_entry_is_rejected() {
    let mut engine = follower(1);
    // Log has entries 1..=2. Leader claims prev at index 5.
    seed_log(&mut engine, &[1, 1]);

    let actions = engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, Some(log_id(5, 1)), vec![], 0),
    ));

    let response = expect_append_entries_response(&actions);
    assert!(matches!(
        response.result,
        AppendEntriesResult::Conflict { .. }
    ));
}

#[test]
fn mismatched_prev_log_term_is_rejected() {
    let mut engine = follower(1);
    // Log has entry at index 2 with term 1.
    seed_log(&mut engine, &[1, 1]);

    // Leader claims prev at (index=2, term=2).
    let actions = engine.step(append_entries_from(
        2,
        append_entries_request(2, 2, Some(log_id(2, 2)), vec![], 0),
    ));

    let response = expect_append_entries_response(&actions);
    assert!(matches!(
        response.result,
        AppendEntriesResult::Conflict { .. }
    ));
}

#[test]
fn conflict_hint_for_longer_divergent_follower_does_not_exceed_prev_index() {
    // Follower has 5 entries at term 1. New leader at term 2 sends
    // prev=(index=2, term=2) — same index, divergent term, and our log
    // is *longer* than prev_index. The hint must NOT be our_last+1=6
    // (which would push the leader past its own log on retry); it must
    // be capped at prev.index=2 so the leader walks back from there.
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1, 1, 1, 1]);

    let actions = engine.step(append_entries_from(
        2,
        append_entries_request(2, 2, Some(log_id(2, 2)), vec![], 0),
    ));

    let response = expect_append_entries_response(&actions);
    match response.result {
        AppendEntriesResult::Conflict { next_index_hint } => {
            assert!(
                next_index_hint <= LogIndex::new(2),
                "hint {next_index_hint:?} must not exceed prev.index=2; \
                 a higher hint lets the leader synthesize prev_log_id \
                 from a region we don't actually agree on",
            );
        }
        AppendEntriesResult::Success { .. } => panic!("expected Conflict, got Success"),
    }
}

#[test]
fn conflict_hint_for_short_follower_uses_our_last_plus_one() {
    // Follower has 2 entries; leader's prev is at index 5. Our last+1=3,
    // which is < prev.index=5, so cap doesn't bite — hint should be 3.
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1]);

    let actions = engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, Some(log_id(5, 1)), vec![], 0),
    ));

    let response = expect_append_entries_response(&actions);
    match response.result {
        AppendEntriesResult::Conflict { next_index_hint } => {
            assert_eq!(next_index_hint, LogIndex::new(3));
        }
        AppendEntriesResult::Success { .. } => panic!("expected Conflict, got Success"),
    }
}

#[test]
fn accepts_from_empty_log_when_prev_log_is_none() {
    let mut engine = follower(1);

    let actions = engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, log_entries(&[(1, 1), (2, 1)]), 0),
    ));

    let response = expect_append_entries_response(&actions);
    assert!(matches!(
        response.result,
        AppendEntriesResult::Success {
            last_appended: Some(_)
        }
    ));
    assert_eq!(engine.log().len(), 2);
}

// ---------------------------------------------------------------------------
// Appending — §5.3 rules 3 and 4
// ---------------------------------------------------------------------------

#[test]
fn empty_entries_with_matching_prev_succeeds() {
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1, 1]);

    let actions = engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, Some(log_id(3, 1)), vec![], 0),
    ));

    let response = expect_append_entries_response(&actions);
    assert!(matches!(
        response.result,
        AppendEntriesResult::Success {
            last_appended: Some(_)
        }
    ));
    assert_eq!(engine.log().len(), 3, "heartbeat does not modify log");
}

#[test]
fn appends_new_entries_after_matching_prev() {
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1]);

    let actions = engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, Some(log_id(2, 1)), log_entries(&[(3, 1), (4, 1)]), 0),
    ));

    let response = expect_append_entries_response(&actions);
    assert!(matches!(
        response.result,
        AppendEntriesResult::Success { last_appended: Some(i) } if i == LogIndex::new(4)
    ));
    assert_eq!(engine.log().len(), 4);
    assert_eq!(
        engine.log().last_log_id().map(|l| l.index),
        Some(LogIndex::new(4))
    );
}

#[test]
fn truncates_and_replaces_on_conflicting_entry() {
    let mut engine = follower(1);
    // Log: (1,1), (2,1), (3,1). The entry at 3 will conflict with a new term.
    seed_log(&mut engine, &[1, 1, 1]);

    let actions = engine.step(append_entries_from(
        2,
        append_entries_request(2, 2, Some(log_id(2, 1)), log_entries(&[(3, 2), (4, 2)]), 0),
    ));

    let response = expect_append_entries_response(&actions);
    assert!(matches!(
        response.result,
        AppendEntriesResult::Success { last_appended: Some(i) } if i == LogIndex::new(4)
    ));
    assert_eq!(engine.log().len(), 4);
    assert_eq!(
        engine.log().term_at(LogIndex::new(3)),
        Some(term(2)),
        "conflicting entry was replaced",
    );
}

#[test]
fn idempotent_when_entries_already_present() {
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1, 1]);
    let snapshot_len = engine.log().len();

    // Resend the same entries we already have.
    let actions = engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, Some(log_id(2, 1)), log_entries(&[(3, 1)]), 0),
    ));

    let response = expect_append_entries_response(&actions);
    assert!(matches!(
        response.result,
        AppendEntriesResult::Success {
            last_appended: Some(_)
        }
    ));
    assert_eq!(
        engine.log().len(),
        snapshot_len,
        "log did not grow on replay"
    );
}

// ---------------------------------------------------------------------------
// Commit index — §5.3 rule 5
// ---------------------------------------------------------------------------

#[test]
fn leader_commit_advances_commit_index() {
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1, 1]);
    assert_eq!(engine.commit_index(), LogIndex::ZERO);

    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, Some(log_id(3, 1)), vec![], 2),
    ));

    assert_eq!(engine.commit_index(), LogIndex::new(2));
}

#[test]
fn leader_commit_is_capped_at_last_log_index() {
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1]);

    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, Some(log_id(2, 1)), vec![], 99),
    ));

    assert_eq!(
        engine.commit_index(),
        LogIndex::new(2),
        "commit capped at our last log index, never ahead of it",
    );
}

// ---------------------------------------------------------------------------
// Apply emission — followers feed newly committed entries to the host
// just like leaders do, via `Action::Apply` (Figure 2: last_applied chases
// commit_index on every server).
// ---------------------------------------------------------------------------

#[test]
fn follower_emits_apply_when_leader_commit_advances_its_commit_index() {
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1, 1]);

    let actions = engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, Some(log_id(3, 1)), vec![], 2),
    ));

    assert_eq!(engine.commit_index(), LogIndex::new(2));
    let applied = collect_apply(&actions);
    assert_eq!(applied.len(), 1, "exactly one Apply per commit advance");
    assert_eq!(applied[0].len(), 2, "entries 1 and 2 applied");
    assert_eq!(applied[0][0].id, log_id(1, 1));
    assert_eq!(applied[0][1].id, log_id(2, 1));
}

#[test]
fn follower_does_not_re_apply_already_applied_entries() {
    // Two heartbeats both carrying leader_commit=2: the second must not
    // re-emit Apply for entries the host already saw.
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1, 1]);

    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, Some(log_id(3, 1)), vec![], 2),
    ));
    let actions = engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, Some(log_id(3, 1)), vec![], 2),
    ));
    assert!(collect_apply(&actions).is_empty());
}

#[test]
fn follower_emits_no_apply_when_leader_commit_does_not_advance() {
    // leader_commit <= our commit_index → no movement → no Apply.
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1, 1]);

    let actions = engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, Some(log_id(3, 1)), vec![], 0),
    ));
    assert_eq!(engine.commit_index(), LogIndex::ZERO);
    assert!(collect_apply(&actions).is_empty());
}

// ---------------------------------------------------------------------------
// Election-timer reset — §5.2.
// ---------------------------------------------------------------------------

#[test]
fn successful_append_entries_resets_the_election_timer() {
    let mut engine = follower(1);
    engine.state_mut().election_elapsed = 5;

    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, vec![], 0),
    ));

    assert_eq!(
        engine.election_elapsed(),
        0,
        "an accepted AppendEntries must reset the election timer (§5.2)",
    );
}

#[test]
fn stale_term_append_entries_does_not_reset_the_election_timer() {
    let mut engine = follower(1);
    // Advance to term 5.
    engine.step(super::fixtures::vote_request_from(
        2,
        super::fixtures::vote_request(2, 5, None),
    ));
    engine.state_mut().election_elapsed = 7;

    // A stale leader (term 3) sends AppendEntries.
    engine.step(append_entries_from(
        3,
        append_entries_request(3, 3, None, vec![], 0),
    ));

    assert_eq!(
        engine.election_elapsed(),
        7,
        "a stale leader's AppendEntries must not reset our timer",
    );
}

#[test]
fn append_entries_with_prev_log_mismatch_still_resets_timer() {
    // §5.2: any AppendEntries from a current-term leader resets the timer,
    // even if the prev_log_id check then forces a Conflict reply. The
    // leader-discovery signal is the term match, not whether replication
    // succeeds on this particular RPC.
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1]);
    engine.state_mut().election_elapsed = 5;

    // Valid term, but prev_log refers to an index we don't have.
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, Some(log_id(5, 1)), vec![], 0),
    ));

    assert_eq!(
        engine.election_elapsed(),
        0,
        "any AppendEntries from a valid-term leader should reset the timer (§5.2)",
    );
}

// ---------------------------------------------------------------------------
// Invariants (property tests)
// ---------------------------------------------------------------------------

proptest! {
    /// Replaying an identical request must leave the log in the same state.
    /// Covers the case where a message is duplicated on the network.
    #[test]
        fn identical_request_is_idempotent(
        leader in 2u64..=3,
        term_n in 1u64..10,
        commit in 0u64..5,
    ) {
        let mut engine = follower(1);
        let request = append_entries_request(
            term_n,
            leader,
            None,
            log_entries(&[(1, term_n), (2, term_n)]),
            commit,
        );

        engine.step(append_entries_from(leader, request.clone()));
        let log_len_first = engine.log().len();
        let commit_first = engine.commit_index();

        engine.step(append_entries_from(leader, request));

        prop_assert_eq!(engine.log().len(), log_len_first);
        prop_assert_eq!(engine.commit_index(), commit_first);
    }

    /// Committed entries are durable. Under protocol-valid leader traffic —
    /// which never conflicts at or below `commit_index` (§5.4.1) — the
    /// reconciliation loop must not lose committed entries.
    #[test]
    fn committed_entries_are_never_truncated(
        seed_len in 2u64..8,
        commit_frac in 0u64..3,      // 0=none committed, 1=half, 2=all-but-one
        conflict_at_offset in 0u64..3,
    ) {
        // Seed with `seed_len` entries, all at term 1.
        let terms: Vec<u64> = vec![1; usize::try_from(seed_len).unwrap()];
        let mut engine = follower(1);
        seed_log(&mut engine, &terms);

        // Commit up to some prefix. Leader sends an empty AppendEntries to
        // advance our commit.
        let commit_to = match commit_frac {
            0 => 0,
            1 => seed_len / 2,
            _ => seed_len.saturating_sub(1),
        };
        if commit_to > 0 {
            engine.step(append_entries_from(
                2,
                append_entries_request(1, 2, engine.log().last_log_id(), vec![], commit_to),
            ));
        }
        let committed = engine.commit_index();

        // Pick a conflict index strictly above commit, and within the existing log.
        // If there's no room (commit == seed_len), this test iteration has nothing
        // interesting to check — let proptest skip it.
        let conflict_index = committed.get() + 1 + conflict_at_offset;
        prop_assume!(conflict_index <= seed_len);

        // prev_log_id points at the entry immediately before the conflict.
        // Our seed log has all term-1 entries, so the lookup is straightforward.
        let prev_index = conflict_index - 1;
        let prev_log = if prev_index == 0 {
            None
        } else {
            Some(log_id(prev_index, 1))
        };

        // Replay as a new term 2 leader, overwriting at `conflict_index`.
        engine.step(append_entries_from(
            2,
            append_entries_request(
                2,
                2,
                prev_log,
                log_entries(&[(conflict_index, 2)]),
                0,
            ),
        ));

        if committed.get() > 0 {
            prop_assert!(
                engine.log().term_at(committed).is_some(),
                "committed entry at {committed:?} was truncated",
            );
        }
    }

    /// The response's term is always the responder's current term, measured
    /// after any catch-up. Mirrors the vote-handler property.
    #[test]
        fn response_term_is_always_responders_current_term(
        leader in 2u64..=3,
        term_n in 0u64..20,
    ) {
        let mut engine = follower(1);
        let actions = engine.step(append_entries_from(
            leader,
            append_entries_request(term_n, leader, None, vec![], 0),
        ));
        let response = expect_append_entries_response(&actions);
        prop_assert_eq!(response.term, engine.current_term());
    }
}
