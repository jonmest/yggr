//! Tests for the `AppendEntries` receiver in [`Engine::on_append_entries_request`].
use super::fixtures::{
    append_entries_from, append_entries_request, expect_append_entries_response, follower,
    log_entries, log_id, seed_log, term,
};
use crate::records::append_entries::AppendEntriesResult;
use crate::types::index::LogIndex;
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
// Invariants (property tests)
// ---------------------------------------------------------------------------

proptest! {
    /// Replaying an identical request must leave the log in the same state.
    /// Covers the case where a message is duplicated on the network.
    #[test]
        fn identical_request_is_idempotent(
        leader in 2u64..10,
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
        leader in 2u64..10,
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
