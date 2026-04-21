//! Pre-Vote (§9.6) tests.
//!
//! A follower whose election timer fires enters the `PreCandidate` role
//! without bumping its term. It sends `RequestPreVote` to every peer.
//! Peers grant only if they themselves haven't heard from a leader
//! within their own election timeout AND the candidate's log is at
//! least as up-to-date (§5.4.1).
//!
//! A `PreCandidate` that collects a majority of grants proceeds to
//! `Candidate` in the normal way: bump term, vote for self, persist,
//! broadcast `RequestVote`. A `PreCandidate` that fails to collect a
//! majority stays in its current term — the network disruption that
//! motivated the pre-vote is avoided.

use super::fixtures::{
    append_entries_from, append_entries_request, follower, follower_with_env, node, term,
};
use crate::engine::action::Action;
use crate::engine::engine::{Engine, EngineConfig};
use crate::engine::env::StaticEnv;
use crate::engine::event::Event;
use crate::engine::incoming::Incoming;
use crate::engine::role_state::RoleState;
use crate::records::log_entry::{LogEntry, LogPayload};
use crate::records::message::Message;
use crate::records::pre_vote::{PreVoteResponse, RequestPreVote};
use crate::types::{index::LogIndex, log::LogId, node::NodeId, term::Term};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn follower_with_pre_vote(id: u64, peers: &[u64]) -> Engine<Vec<u8>> {
    // Pre-vote is on by default in `EngineConfig::new`.
    let cfg = EngineConfig::new(64 * 1024, 0);
    Engine::with_config(
        node(id),
        peers.iter().map(|&p| node(p)),
        Box::new(StaticEnv(10)),
        1,
        cfg,
    )
}

fn collect_pre_vote_requests(actions: &[Action<Vec<u8>>]) -> Vec<(NodeId, RequestPreVote)> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Send {
                to,
                message: Message::PreVoteRequest(r),
            } => Some((*to, *r)),
            _ => None,
        })
        .collect()
}

fn collect_vote_requests(actions: &[Action<Vec<u8>>]) -> Vec<NodeId> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Send {
                to,
                message: Message::VoteRequest(_),
            } => Some(*to),
            _ => None,
        })
        .collect()
}

fn pre_vote_response_from(from: u64, term_n: u64, granted: bool) -> Event<Vec<u8>> {
    Event::Incoming(Incoming {
        from: node(from),
        message: Message::PreVoteResponse(PreVoteResponse {
            term: term(term_n),
            granted,
        }),
    })
}

fn pre_vote_request_from(from: u64, term_n: u64, last_log: Option<LogId>) -> Event<Vec<u8>> {
    Event::Incoming(Incoming {
        from: node(from),
        message: Message::PreVoteRequest(RequestPreVote {
            term: term(term_n),
            candidate_id: node(from),
            last_log_id: last_log,
        }),
    })
}

// ---------------------------------------------------------------------------
// Pre-vote flow on the candidate side
// ---------------------------------------------------------------------------

#[test]
fn expired_timer_sends_pre_vote_not_vote_request_when_enabled() {
    let mut engine = follower_with_pre_vote(1, &[2, 3]);
    let original_term = engine.current_term();

    // Tick past the election timeout.
    let mut actions = Vec::new();
    for _ in 0..15 {
        actions.extend(engine.step(Event::Tick));
    }

    // We should have PreVoteRequests to both peers, and NO VoteRequests.
    let pre_votes = collect_pre_vote_requests(&actions);
    let votes = collect_vote_requests(&actions);
    assert_eq!(
        pre_votes.len(),
        2,
        "expected 2 PreVoteRequests (one per peer), got {pre_votes:?}",
    );
    assert!(
        votes.is_empty(),
        "no RequestVote should be sent before PreVote majority; got {votes:?}",
    );
    // Term must not have advanced.
    assert_eq!(engine.current_term(), original_term);
    // Role is PreCandidate, not Candidate.
    assert!(
        matches!(engine.role(), RoleState::PreCandidate(_)),
        "expected PreCandidate, got {:?}",
        engine.role(),
    );
}

#[test]
fn pre_vote_request_uses_term_plus_one_but_engine_term_stays() {
    let mut engine = follower_with_pre_vote(1, &[2, 3]);
    let mut actions = Vec::new();
    for _ in 0..15 {
        actions.extend(engine.step(Event::Tick));
    }

    let pre_votes = collect_pre_vote_requests(&actions);
    assert!(!pre_votes.is_empty(), "expected at least one pre-vote");
    for (_, r) in &pre_votes {
        assert_eq!(
            r.term,
            Term::new(1),
            "PreVote.term is current_term + 1 (0 + 1 = 1)",
        );
    }
    assert_eq!(engine.current_term(), Term::new(0));
}

#[test]
fn pre_vote_majority_promotes_to_candidate_and_sends_vote_request() {
    let mut engine = follower_with_pre_vote(1, &[2, 3]);
    // Timer expires -> PreCandidate.
    for _ in 0..15 {
        engine.step(Event::Tick);
    }
    assert!(matches!(engine.role(), RoleState::PreCandidate(_)));

    // A single peer granting is enough: self + peer 2 = 2 of 3.
    // Responder reports its own term (0); grants because it has no
    // leader and the candidate's log is trivially up-to-date (both
    // empty).
    let actions = engine.step(pre_vote_response_from(2, 0, true));

    assert!(
        matches!(engine.role(), RoleState::Candidate(_)),
        "pre-vote majority must promote to Candidate; got {:?}",
        engine.role(),
    );
    assert_eq!(
        engine.current_term(),
        Term::new(1),
        "term bumped on promotion"
    );
    // The promotion emits RequestVote to both peers.
    let vrs = collect_vote_requests(&actions);
    assert_eq!(vrs.len(), 2, "RequestVote to each peer, got {vrs:?}");
    // And hard state was persisted (term + self-vote).
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::PersistHardState { .. })),
        "promotion must persist new term + voted_for=self",
    );
}

#[test]
fn pre_vote_rejection_keeps_us_in_pre_candidate() {
    let mut engine = follower_with_pre_vote(1, &[2, 3]);
    for _ in 0..15 {
        engine.step(Event::Tick);
    }
    assert!(matches!(engine.role(), RoleState::PreCandidate(_)));

    // Peer rejects. One grant (self) is not a majority of 3.
    // Responder is at term 0 (same as us).
    engine.step(pre_vote_response_from(2, 0, false));

    // Still PreCandidate, still term 0.
    assert!(matches!(engine.role(), RoleState::PreCandidate(_)));
    assert_eq!(engine.current_term(), Term::new(0));
}

#[test]
fn pre_vote_higher_term_response_steps_down_to_follower() {
    let mut engine = follower_with_pre_vote(1, &[2, 3]);
    for _ in 0..15 {
        engine.step(Event::Tick);
    }
    // Peer's term is way ahead; that demotes us to Follower at the higher term.
    engine.step(pre_vote_response_from(2, 99, false));
    assert!(matches!(engine.role(), RoleState::Follower(_)));
    assert_eq!(engine.current_term(), Term::new(99));
}

#[test]
fn pre_vote_response_after_stepping_down_is_ignored() {
    // A PreCandidate may step down to Follower (e.g. after hearing
    // a current-term AE from a legitimate leader). If a stale
    // PreVoteResponse arrives after that, it must not revive the
    // promotion path.
    let mut engine = follower_with_pre_vote(1, &[2, 3]);
    for _ in 0..15 {
        engine.step(Event::Tick);
    }
    assert!(matches!(engine.role(), RoleState::PreCandidate(_)));

    // A leader's AE drops us back to Follower.
    engine.step(append_entries_from(
        2,
        append_entries_request(0, 2, None, vec![], 0),
    ));
    assert!(matches!(engine.role(), RoleState::Follower(_)));

    // Stale pre-vote response from the prior attempt arrives.
    engine.step(pre_vote_response_from(3, 0, true));

    // Still Follower; did not spring back to Candidate.
    assert!(
        matches!(engine.role(), RoleState::Follower(_)),
        "stale pre-vote response should not revive the pre-candidate path",
    );
}

#[test]
fn pre_vote_interrupted_by_current_term_append_entries() {
    // PreCandidate receives an AE from a legitimate leader. It must
    // drop its pre-vote attempt and return to Follower.
    let mut engine = follower_with_pre_vote(1, &[2, 3]);
    for _ in 0..15 {
        engine.step(Event::Tick);
    }
    assert!(matches!(engine.role(), RoleState::PreCandidate(_)));

    engine.step(append_entries_from(
        2,
        append_entries_request(0, 2, None, vec![], 0),
    ));

    // Back to Follower with leader_id set; term unchanged.
    match engine.role() {
        RoleState::Follower(f) => assert_eq!(f.leader_id(), Some(node(2))),
        other => panic!("expected Follower, got {other:?}"),
    }
    assert_eq!(engine.current_term(), Term::new(0));
}

#[test]
fn pre_vote_disabled_preserves_legacy_behavior() {
    // When pre_vote is off (default still on today, but test the
    // explicit-off path), the election timeout drives us straight to
    // Candidate and bumps the term.
    let cfg = EngineConfig::default().with_pre_vote(false);
    let mut engine: Engine<Vec<u8>> =
        Engine::with_config(node(1), [node(2), node(3)], Box::new(StaticEnv(10)), 1, cfg);

    let mut actions = Vec::new();
    for _ in 0..15 {
        actions.extend(engine.step(Event::Tick));
    }

    assert!(matches!(engine.role(), RoleState::Candidate(_)));
    assert_eq!(engine.current_term(), Term::new(1));
    // Classic RequestVote, no PreVoteRequest at all.
    assert_eq!(collect_pre_vote_requests(&actions).len(), 0);
    assert_eq!(collect_vote_requests(&actions).len(), 2);
}

// ---------------------------------------------------------------------------
// Pre-vote RPC on the responder side
// ---------------------------------------------------------------------------

#[test]
fn follower_grants_pre_vote_when_no_recent_leader() {
    // Fresh follower, no leader heard from. Should grant.
    let mut engine = follower(1);
    let actions = engine.step(pre_vote_request_from(2, 1, None));

    let r = expect_pre_vote_response(&actions);
    assert!(r.granted, "fresh follower with no leader should grant");
    // §5.1: pre-vote does NOT update our term or voted_for.
    assert_eq!(engine.current_term(), Term::new(0));
}

#[test]
fn follower_rejects_pre_vote_when_leader_heard_from_recently() {
    // Follower has accepted an AE from peer 2 (its leader) this term.
    let mut engine = follower(1);
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, vec![], 0),
    ));

    // Peer 3 tries to pre-vote. Follower has a live leader (2), reject.
    let actions = engine.step(pre_vote_request_from(3, 2, None));
    let r = expect_pre_vote_response(&actions);
    assert!(
        !r.granted,
        "follower with a live leader must not pre-vote for a challenger",
    );
    // §9.6 safety: our term and voted_for are unchanged.
    assert_eq!(engine.current_term(), Term::new(1));
}

#[test]
fn follower_rejects_pre_vote_for_stale_log() {
    // We have entries at term 5. A pre-voter with an older last_log
    // (term 3) must fail §5.4.1.
    let env = StaticEnv(10);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    // Catch the log up to term 5 via a higher-term AE.
    let entry = LogEntry {
        id: LogId::new(LogIndex::new(1), Term::new(5)),
        payload: LogPayload::Command(b"x".to_vec()),
    };
    engine.step(append_entries_from(
        2,
        append_entries_request(5, 2, None, vec![entry], 0),
    ));

    // Tick past the election timeout so we're no longer "recent leader".
    for _ in 0..30 {
        engine.step(Event::Tick);
    }

    let actions = engine.step(pre_vote_request_from(
        3,
        6,
        Some(LogId::new(LogIndex::new(1), Term::new(3))),
    ));
    let r = expect_pre_vote_response(&actions);
    assert!(
        !r.granted,
        "pre-vote for stale-log candidate must be rejected (§5.4.1)",
    );
}

#[test]
fn follower_pre_vote_response_does_not_update_term() {
    // Peer 2 sends PreVote at term 99 (much higher than ours). Unlike
    // a real RequestVote, a PreVote MUST NOT cause us to step-down or
    // advance our term. That's the whole point of §9.6.
    let mut engine = follower(1);
    let prior = engine.current_term();

    engine.step(pre_vote_request_from(2, 99, None));

    assert_eq!(
        engine.current_term(),
        prior,
        "PreVote receipt must not advance term (§9.6)",
    );
}

#[test]
fn pre_vote_response_carries_responder_term_not_proposed_term() {
    // Peer at term 0 replies to a pre-vote proposing term 5. Response
    // reports term 0 (responder's real term), granted based on checks.
    let mut engine = follower(1);
    let actions = engine.step(pre_vote_request_from(2, 5, None));
    let r = expect_pre_vote_response(&actions);
    assert_eq!(
        r.term,
        Term::new(0),
        "response.term reports OUR term, not the proposed one",
    );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn expect_pre_vote_response(actions: &[Action<Vec<u8>>]) -> PreVoteResponse {
    actions
        .iter()
        .find_map(|a| match a {
            Action::Send {
                message: Message::PreVoteResponse(r),
                ..
            } => Some(*r),
            _ => None,
        })
        .expect("expected a PreVoteResponse Send action")
}

// ---------------------------------------------------------------------------
// Boundary tests (kill off-by-one mutants)
// ---------------------------------------------------------------------------

#[test]
fn pre_vote_at_timeout_minus_one_is_rejected_leader_still_recent() {
    // Follower with a live leader. `election_elapsed` is strictly
    // less than `election_timeout_ticks` — the leader is still
    // considered live, so pre-vote is rejected. This pins the strict
    // `<` boundary.
    let timeout = super::fixtures::DEFAULT_ELECTION_TIMEOUT;
    let mut engine = follower(1);
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, vec![], 0),
    ));
    for _ in 0..(timeout - 1) {
        engine.step(Event::Tick);
    }
    let actions = engine.step(pre_vote_request_from(3, 2, None));
    let r = expect_pre_vote_response(&actions);
    assert!(
        !r.granted,
        "at elapsed=timeout-1 the leader is still 'recent'; pre-vote must be rejected",
    );
}

#[test]
fn pre_vote_at_exactly_timeout_is_granted_leader_no_longer_recent() {
    // Now tick the timer to exactly `timeout`. With the strict `<`
    // boundary, `elapsed (== timeout)` is NOT less than the timeout,
    // so we consider the leader no longer recent and grant. A
    // mutation to `<=` would reject here — the critical killer.
    //
    // To observe pre-vote handling rather than the follower's own
    // timer-driven election, we inject the pre-vote on the tick that
    // would otherwise start an election. The trick: the engine
    // checks `election_elapsed < election_timeout_ticks` in its
    // pre-vote predicate, so we need to reach elapsed == timeout
    // without the on_tick handler triggering a role change first.
    //
    // `on_tick` fires an election when `election_elapsed >=
    // election_timeout_ticks`. With timeout = 10, after 10 ticks
    // the 10th tick's handler sees elapsed transitioning 9 -> 10 and
    // starts an election. So we can't reach 10 without leaving the
    // Follower role — unless we circumvent the timer.
    //
    // We can't, easily. But the boundary we care about is captured
    // by a DIFFERENT angle: a follower with NO known leader always
    // grants a well-formed pre-vote regardless of elapsed. And a
    // follower whose leader_id is None but elapsed < timeout — the
    // `f.leader_id.is_some() && elapsed < timeout` predicate short-
    // circuits. So the `<` test is: leader_id=Some AND elapsed<timeout
    // => leader-recent => reject. We already have that. The `<=`
    // mutant would only differ at the exact-boundary row.
    //
    // Approach: construct a FollowerState directly with leader_id
    // set, set election_elapsed = election_timeout_ticks via the
    // test-only state_mut accessor, then pre-vote.
    use crate::engine::role_state::FollowerState;
    let mut engine = follower(1);
    engine.state_mut().election_timeout_ticks = 10;
    engine.state_mut().role = RoleState::Follower(FollowerState {
        leader_id: Some(node(2)),
    });
    engine.state_mut().election_elapsed = 10;

    let actions = engine.step(pre_vote_request_from(3, 1, None));
    let r = expect_pre_vote_response(&actions);
    assert!(
        r.granted,
        "at elapsed == timeout the leader is no longer recent; pre-vote must be granted",
    );
}

#[test]
fn pre_vote_rejected_when_proposed_term_equals_current_term() {
    // §9.6: a pre-vote should only be granted when the challenger's
    // proposed term is strictly greater than ours. A request with
    // term == our current_term is either misdirected or malicious;
    // reject.
    let mut engine = follower(1);
    // current_term is 0. Pre-vote request with term=0 (not > 0) must reject.
    let actions = engine.step(pre_vote_request_from(2, 0, None));
    let r = expect_pre_vote_response(&actions);
    assert!(
        !r.granted,
        "pre-vote at proposed_term == current_term must be rejected (> boundary)",
    );
}

#[test]
fn pre_candidate_state_proposed_term_is_readable() {
    // Accessor roundtrip — also exercises the `proposed_term()`
    // getter so a mutant replacing its body with `Default::default()`
    // fails at least one test.
    let mut engine = follower_with_pre_vote(1, &[2, 3]);
    for _ in 0..15 {
        engine.step(Event::Tick);
    }
    match engine.role() {
        RoleState::PreCandidate(p) => {
            assert_eq!(p.proposed_term(), Term::new(1));
            assert!(p.grants().contains(&node(1)));
        }
        other => panic!("expected PreCandidate, got {other:?}"),
    }
}
