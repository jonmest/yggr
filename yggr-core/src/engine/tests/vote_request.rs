//! Tests for the `RequestVote` handler in [`Engine::on_vote_request`].

use super::fixtures::{
    expect_vote_response, follower, log_id, node, seed_log, term, vote_request, vote_request_from,
};
use crate::records::vote::VoteResult;
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Term handling
// ---------------------------------------------------------------------------

#[test]
fn fresh_follower_grants_vote_for_a_valid_candidate() {
    let mut engine = follower(1);

    let actions = engine.step(vote_request_from(2, vote_request(2, 1, None)));

    let response = expect_vote_response(&actions);
    assert_eq!(response.result, VoteResult::Granted);
    assert_eq!(response.term, term(1));
    assert_eq!(engine.voted_for(), Some(node(2)));
    assert_eq!(engine.current_term(), term(1));
}

#[test]
fn follower_catches_up_to_a_higher_term_before_deciding() {
    let mut engine = follower(1);
    engine.step(vote_request_from(2, vote_request(2, 3, None))); // term jumps 0 -> 3

    assert_eq!(engine.current_term(), term(3));
    // A higher-term request from a valid candidate is granted.
    assert_eq!(engine.voted_for(), Some(node(2)));
}

#[test]
fn follower_rejects_a_stale_term_without_changing_state() {
    let mut engine = follower(1);
    // Advance to term 5 by granting one vote.
    engine.step(vote_request_from(2, vote_request(2, 5, None)));

    // A request for an older term arrives.
    let actions = engine.step(vote_request_from(3, vote_request(3, 4, None)));

    let response = expect_vote_response(&actions);
    assert_eq!(response.result, VoteResult::Rejected);
    assert_eq!(
        response.term,
        term(5),
        "response carries the responder's term"
    );
    assert_eq!(engine.current_term(), term(5));
    assert_eq!(engine.voted_for(), Some(node(2)));
}

// ---------------------------------------------------------------------------
// Vote persistence — §5.2, "each server will vote for at most one candidate
// in a given term, on a first-come-first-served basis".
// ---------------------------------------------------------------------------

#[test]
fn follower_rejects_a_second_candidate_after_voting_in_the_same_term() {
    let mut engine = follower(1);
    engine.step(vote_request_from(2, vote_request(2, 1, None))); // voted for 2

    let actions = engine.step(vote_request_from(3, vote_request(3, 1, None)));

    let response = expect_vote_response(&actions);
    assert_eq!(response.result, VoteResult::Rejected);
    assert_eq!(
        engine.voted_for(),
        Some(node(2)),
        "the original vote must not be overwritten"
    );
}

#[test]
fn follower_grants_again_when_the_same_candidate_re_requests() {
    let mut engine = follower(1);
    engine.step(vote_request_from(2, vote_request(2, 1, None))); // voted for 2

    // Same candidate, same term. Can happen if our response was lost.
    let actions = engine.step(vote_request_from(2, vote_request(2, 1, None)));

    let response = expect_vote_response(&actions);
    assert_eq!(response.result, VoteResult::Granted);
}

#[test]
fn advancing_to_a_higher_term_clears_the_prior_vote() {
    let mut engine = follower(1);
    engine.step(vote_request_from(2, vote_request(2, 1, None))); // voted for 2 in term 1
    assert_eq!(engine.voted_for(), Some(node(2)));

    // Higher-term request arrives — vote should be cleared during catch-up
    // and then granted to the new candidate.
    engine.step(vote_request_from(3, vote_request(3, 2, None)));

    assert_eq!(engine.current_term(), term(2));
    assert_eq!(engine.voted_for(), Some(node(3)));
}

// ---------------------------------------------------------------------------
// Election-timer reset — §5.2.
// ---------------------------------------------------------------------------

#[test]
fn granting_a_vote_resets_the_election_timer() {
    let mut engine = follower(1);
    // Simulate some ticks elapsing.
    engine.state_mut().election_elapsed = 5;

    engine.step(vote_request_from(2, vote_request(2, 1, None)));

    assert_eq!(
        engine.election_elapsed(),
        0,
        "granting a vote must reset the election timer (§5.2)",
    );
}

#[test]
fn rejecting_a_vote_does_not_reset_the_election_timer() {
    let mut engine = follower(1);
    // Already voted for candidate 2 in term 1.
    engine.step(vote_request_from(2, vote_request(2, 1, None)));
    // Some ticks elapse.
    engine.state_mut().election_elapsed = 7;

    // Candidate 3 is rejected.
    engine.step(vote_request_from(3, vote_request(3, 1, None)));

    assert_eq!(
        engine.election_elapsed(),
        7,
        "rejected vote requests must not reset the election timer",
    );
}

#[test]
fn granting_a_vote_draws_a_new_timeout_from_env() {
    use crate::engine::env::ScriptedEnv;
    // Construction consumes the first value; the grant reset draws the next.
    let env = ScriptedEnv::new(vec![10, 42]);
    let mut engine = super::fixtures::follower_with_env(1, &[2], Box::new(env));
    assert_eq!(engine.election_timeout_ticks(), 10);

    // Fresh follower grants a higher-term vote. Term catch-up on its own does
    // not reset (§5.2); only the grant does.
    engine.step(vote_request_from(2, vote_request(2, 5, None)));

    assert_eq!(
        engine.voted_for(),
        Some(node(2)),
        "precondition: test scenario must grant the vote",
    );
    assert_eq!(
        engine.election_timeout_ticks(),
        42,
        "granting a vote must draw a new timeout from env",
    );
}

// ---------------------------------------------------------------------------
// Log-up-to-date rule — §5.4.1.
// ---------------------------------------------------------------------------

#[test]
fn follower_with_log_rejects_candidate_whose_log_is_behind() {
    let mut engine = follower(1);
    // Seed log ending at (index=5, term=2).
    seed_log(&mut engine, &[1, 1, 2, 2, 2]);

    // Candidate in a newer election claims a strictly shorter log at the
    // same term as ours. Not up to date.
    let actions = engine.step(vote_request_from(3, vote_request(3, 4, Some(log_id(4, 2)))));

    let response = expect_vote_response(&actions);
    assert_eq!(
        response.result,
        VoteResult::Rejected,
        "candidate log with lower index at same term must be rejected",
    );
    assert_eq!(
        engine.current_term(),
        term(4),
        "term caught up to the request"
    );
    assert_eq!(
        engine.voted_for(),
        None,
        "no vote was granted, so voted_for stays unset in the new term",
    );
}

#[test]
fn follower_grants_when_candidate_log_at_same_term_is_at_least_as_long() {
    // §5.4.1: at equal last-entry terms, the longer log wins (or ties).
    let mut engine = follower(1);
    // Seed log ending at (index=3, term=2).
    seed_log(&mut engine, &[1, 2, 2]);

    // Candidate at same term 2 with equal index.
    let actions = engine.step(vote_request_from(2, vote_request(2, 3, Some(log_id(3, 2)))));
    let response = expect_vote_response(&actions);
    assert_eq!(
        response.result,
        VoteResult::Granted,
        "equal (term, index) must be considered at-least-as-up-to-date",
    );
}

#[test]
fn follower_grants_when_candidate_log_term_is_strictly_greater() {
    let mut engine = follower(1);
    // Seed log ending at (index=5, term=2).
    seed_log(&mut engine, &[1, 1, 2, 2, 2]);

    // Candidate has a shorter log but at a newer term. Per §5.4.1, higher
    // term wins regardless of index.
    let actions = engine.step(vote_request_from(3, vote_request(3, 4, Some(log_id(1, 3)))));

    let response = expect_vote_response(&actions);
    assert_eq!(response.result, VoteResult::Granted);
    assert_eq!(engine.voted_for(), Some(node(3)));
}

// ---------------------------------------------------------------------------
// Invariants (property tests)
// ---------------------------------------------------------------------------

/// Arbitrary `RequestVote` from a configured peer of `follower(1)`,
/// which has peers {2, 3}. Sending from non-members is now dropped at
/// the engine boundary and would just yield empty action vecs.
fn any_vote_request() -> impl Strategy<Value = (u64, crate::records::vote::RequestVote)> {
    let candidate = 2u64..=3;
    let term_n = 0u64..20;
    let last_log = proptest::option::of((1u64..20, 0u64..20));
    (candidate, term_n, last_log).prop_map(|(c, t, last)| {
        let last = last.map(|(i, lt)| log_id(i, lt));
        (c, vote_request(c, t, last))
    })
}

proptest! {
    /// Core safety property of §5.2: a server votes for at most one candidate
    /// in a given term.
    #[test]
    fn never_votes_for_two_different_candidates_in_the_same_term(
        candidates in proptest::collection::vec(2u64..=3, 1..20),
    ) {
        let mut engine = follower(1);
        let mut recorded: Option<u64> = None;

        for candidate in candidates {
            engine.step(vote_request_from(
                candidate,
                vote_request(candidate, 1, None),
            ));

            if let Some(voted) = engine.voted_for() {
                match recorded {
                    None => recorded = Some(voted.get()),
                    Some(prev) => prop_assert_eq!(
                        prev,
                        voted.get(),
                        "follower changed its vote within a single term",
                    ),
                }
            }
        }
    }

    /// `current_term` is monotonically non-decreasing across any sequence of
    /// handled events. Will extend naturally once Tick and AppendEntries exist.
    #[test]
    fn current_term_is_monotonically_non_decreasing(
        requests in proptest::collection::vec(any_vote_request(), 0..30),
    ) {
        let mut engine = follower(1);
        let mut last_term = engine.current_term();
        for (from, req) in requests {
            engine.step(vote_request_from(from, req));
            let now = engine.current_term();
            prop_assert!(now >= last_term, "term went backward: {last_term:?} -> {now:?}");
            last_term = now;
        }
    }

    /// A request with a term strictly less than ours must be rejected, and
    /// must not mutate `voted_for` or `current_term`.
    #[test]
    fn stale_term_is_always_rejected_without_state_change(
        current_term_n in 1u64..20,
        stale_term_n in 0u64..20,
        (from, request) in any_vote_request(),
    ) {
        prop_assume!(stale_term_n < current_term_n);
        let mut engine = follower(1);
        // Advance to `current_term_n` and grant a vote to candidate 2.
        engine.step(vote_request_from(2, vote_request(2, current_term_n, None)));
        let voted_before = engine.voted_for();
        let term_before = engine.current_term();

        let mut stale = request;
        stale.term = term(stale_term_n);
        let actions = engine.step(vote_request_from(from, stale));

        let response = expect_vote_response(&actions);
        prop_assert_eq!(response.result, VoteResult::Rejected);
        prop_assert_eq!(response.term, term_before, "response carries our term, not theirs");
        prop_assert_eq!(engine.current_term(), term_before);
        prop_assert_eq!(engine.voted_for(), voted_before);
    }

    /// When a vote is granted, `voted_for` must equal the candidate id afterward.
    /// Conversely, when rejected from an unset state, `voted_for` stays unset.
    #[test]
    fn granted_implies_voted_for_equals_candidate(
        (from, request) in any_vote_request(),
    ) {
        let mut engine = follower(1);
        let candidate = request.candidate_id;
        let actions = engine.step(vote_request_from(from, request));
        let response = expect_vote_response(&actions);

        match response.result {
            VoteResult::Granted => prop_assert_eq!(engine.voted_for(), Some(candidate)),
            VoteResult::Rejected => prop_assert_eq!(engine.voted_for(), None),
        }
    }

    /// The response's term is always the responder's current term, measured
    /// after any term catch-up. Never the request's term.
    #[test]
    fn response_term_is_always_responders_current_term(
        (from, request) in any_vote_request(),
    ) {
        let mut engine = follower(1);
        let actions = engine.step(vote_request_from(from, request));
        let response = expect_vote_response(&actions);
        prop_assert_eq!(response.term, engine.current_term());
    }

    /// Replaying an identical request must leave the engine in the same
    /// state and produce the same outgoing VoteResponse. Action vectors
    /// may differ on the second call (no PersistHardState since voted_for
    /// already matches), which is the correct shape for "we already voted
    /// for this candidate at this term".
    #[test]
    fn identical_requests_are_idempotent(
        (from, request) in any_vote_request(),
    ) {
        let mut engine = follower(1);
        let first = engine.step(vote_request_from(from, request));
        let term_after_first = engine.current_term();
        let voted_after_first = engine.voted_for();

        let second = engine.step(vote_request_from(from, request));

        prop_assert_eq!(engine.current_term(), term_after_first);
        prop_assert_eq!(engine.voted_for(), voted_after_first);
        prop_assert_eq!(
            expect_vote_response(&first),
            expect_vote_response(&second),
            "identical input must produce the same VoteResponse",
        );
    }
}
