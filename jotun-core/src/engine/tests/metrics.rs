//! Coverage for [`Engine::metrics`]. Each counter gets driven through
//! the narrowest event path that moves it. Gauges get asserted by
//! observing the already-covered state accessors via the same
//! snapshot.

use super::fixtures::{
    append_entries_from, append_entries_request, follower, follower_with_env, log_entries, log_id,
    node, seed_log, term, vote_request, vote_request_from,
};
use crate::engine::engine::Engine;
use crate::engine::env::StaticEnv;
use crate::engine::event::Event;
use crate::engine::incoming::Incoming;
use crate::engine::role_state::RoleState;
use crate::records::message::Message;
use crate::records::pre_vote::RequestPreVote;
use crate::records::vote::VoteResult;
use crate::types::{index::LogIndex, term::Term};

// ---------------------------------------------------------------------------
// Zero-state sanity
// ---------------------------------------------------------------------------

#[test]
fn fresh_engine_reports_zero_counters() {
    let engine = follower(1);
    let m = engine.metrics();
    assert_eq!(m.elections_started, 0);
    assert_eq!(m.pre_votes_granted, 0);
    assert_eq!(m.pre_votes_denied, 0);
    assert_eq!(m.votes_granted, 0);
    assert_eq!(m.votes_denied, 0);
    assert_eq!(m.leader_elections_won, 0);
    assert_eq!(m.higher_term_stepdowns, 0);
    assert_eq!(m.append_entries_sent, 0);
    assert_eq!(m.append_entries_received, 0);
    assert_eq!(m.append_entries_rejected, 0);
    assert_eq!(m.entries_appended, 0);
    assert_eq!(m.entries_committed, 0);
    assert_eq!(m.entries_applied, 0);
    assert_eq!(m.snapshots_sent, 0);
    assert_eq!(m.snapshots_installed, 0);
    assert_eq!(m.read_index_started, 0);
    assert_eq!(m.reads_completed, 0);
    assert_eq!(m.reads_failed, 0);
}

#[test]
fn fresh_engine_gauges_reflect_follower_at_term_zero() {
    let engine = follower(1);
    let m = engine.metrics();
    assert_eq!(m.current_term, Term::ZERO);
    assert_eq!(m.commit_index, LogIndex::ZERO);
    assert_eq!(m.last_applied, LogIndex::ZERO);
    assert_eq!(m.log_len, 0);
    assert_eq!(m.role_code, 0, "follower code");
}

// ---------------------------------------------------------------------------
// Vote counters
// ---------------------------------------------------------------------------

#[test]
fn granting_a_vote_bumps_votes_granted() {
    let mut engine = follower(1);
    engine.step(vote_request_from(2, vote_request(2, 1, None)));
    let m = engine.metrics();
    assert_eq!(m.votes_granted, 1);
    assert_eq!(m.votes_denied, 0);
}

#[test]
fn denying_a_vote_bumps_votes_denied() {
    let mut engine = follower(1);
    // First vote goes to node 2 at term 1.
    engine.step(vote_request_from(2, vote_request(2, 1, None)));
    // Now node 3 asks at the same term — rejected.
    engine.step(vote_request_from(3, vote_request(3, 1, None)));
    let m = engine.metrics();
    assert_eq!(m.votes_granted, 1);
    assert_eq!(m.votes_denied, 1);
}

// ---------------------------------------------------------------------------
// Pre-vote counters
// ---------------------------------------------------------------------------

fn follower_with_pre_vote(id: u64) -> Engine<Vec<u8>> {
    let cfg = crate::engine::engine::EngineConfig::default();
    let peers: Vec<_> = [2u64, 3]
        .into_iter()
        .filter(|&p| p != id)
        .map(node)
        .collect();
    Engine::with_config(node(id), peers, Box::new(StaticEnv(10)), 1, cfg)
}

#[test]
fn granting_a_pre_vote_bumps_pre_votes_granted() {
    let mut engine = follower_with_pre_vote(1);
    // A pre-vote at term 1 (our term is 0) from a candidate with an
    // up-to-date (empty) log — granted.
    engine.step(Event::Incoming(Incoming {
        from: node(2),
        message: Message::PreVoteRequest(RequestPreVote {
            term: term(1),
            candidate_id: node(2),
            last_log_id: None,
        }),
    }));
    let m = engine.metrics();
    assert_eq!(m.pre_votes_granted, 1);
    assert_eq!(m.pre_votes_denied, 0);
}

#[test]
fn denying_a_pre_vote_bumps_pre_votes_denied() {
    let mut engine = follower_with_pre_vote(1);
    // Pre-vote at current term (0) — denied: proposed term must be
    // strictly greater than our current term.
    engine.step(Event::Incoming(Incoming {
        from: node(2),
        message: Message::PreVoteRequest(RequestPreVote {
            term: term(0),
            candidate_id: node(2),
            last_log_id: None,
        }),
    }));
    let m = engine.metrics();
    assert_eq!(m.pre_votes_denied, 1);
    assert_eq!(m.pre_votes_granted, 0);
}

// ---------------------------------------------------------------------------
// Election / leader counters
// ---------------------------------------------------------------------------

#[test]
fn expired_election_timer_bumps_elections_started() {
    let mut engine = follower_with_env(1, &[2, 3], Box::new(StaticEnv(3)));
    // Tick past the election timeout.
    for _ in 0..4 {
        engine.step(Event::Tick);
    }
    let m = engine.metrics();
    assert_eq!(m.elections_started, 1);
}

#[test]
fn winning_election_bumps_leader_elections_won() {
    let mut engine = follower_with_env(1, &[2, 3], Box::new(StaticEnv(3)));
    // Drive into Candidate.
    for _ in 0..4 {
        engine.step(Event::Tick);
    }
    // Collect one peer vote — two-out-of-three majority.
    engine.step(Event::Incoming(Incoming {
        from: node(2),
        message: Message::VoteResponse(crate::records::vote::VoteResponse {
            term: engine.current_term(),
            result: VoteResult::Granted,
        }),
    }));
    assert!(matches!(engine.role(), RoleState::Leader(_)));
    let m = engine.metrics();
    assert_eq!(m.leader_elections_won, 1);
    assert_eq!(m.role_code, 3, "leader code");
}

#[test]
fn seeing_a_higher_term_bumps_higher_term_stepdowns() {
    // Start as a candidate at term 1, then see term 5 on an AE.
    let mut engine = follower_with_env(1, &[2, 3], Box::new(StaticEnv(3)));
    for _ in 0..4 {
        engine.step(Event::Tick);
    }
    assert_eq!(engine.current_term(), term(1));
    engine.step(append_entries_from(
        2,
        append_entries_request(5, 2, None, vec![], 0),
    ));
    let m = engine.metrics();
    assert_eq!(m.higher_term_stepdowns, 1);
}

#[test]
fn same_term_stepdown_does_not_bump_higher_term_stepdowns() {
    // Start as a candidate at term 1 (election timer expired).
    let mut engine = follower_with_env(1, &[2, 3], Box::new(StaticEnv(3)));
    for _ in 0..4 {
        engine.step(Event::Tick);
    }
    assert_eq!(engine.current_term(), term(1));
    // AE at our current term — same-term step-down back to follower.
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, vec![], 0),
    ));
    let m = engine.metrics();
    assert_eq!(
        m.higher_term_stepdowns, 0,
        "same-term step-down must not count as a higher-term stepdown",
    );
}

// ---------------------------------------------------------------------------
// Replication counters
// ---------------------------------------------------------------------------

#[test]
fn receiving_an_append_entries_bumps_append_entries_received() {
    let mut engine = follower(1);
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, vec![], 0),
    ));
    let m = engine.metrics();
    assert_eq!(m.append_entries_received, 1);
}

#[test]
fn rejected_append_entries_bumps_append_entries_rejected() {
    let mut engine = follower(1);
    // Accept an AE at term 5 first.
    engine.step(append_entries_from(
        2,
        append_entries_request(5, 2, None, vec![], 0),
    ));
    // A stale-term AE — rejected.
    engine.step(append_entries_from(
        3,
        append_entries_request(2, 3, None, vec![], 0),
    ));
    let m = engine.metrics();
    assert_eq!(m.append_entries_rejected, 1);
}

#[test]
fn accepted_ae_with_entries_bumps_entries_appended() {
    let mut engine = follower(1);
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, log_entries(&[(1, 1), (2, 1)]), 0),
    ));
    let m = engine.metrics();
    assert_eq!(m.entries_appended, 2);
}

#[test]
fn commit_advance_bumps_entries_committed_and_applied() {
    let mut engine = follower(1);
    // Append two entries at term 1.
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, log_entries(&[(1, 1), (2, 1)]), 0),
    ));
    // Leader then advances commit to 2.
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, Some(log_id(2, 1)), vec![], 2),
    ));
    let m = engine.metrics();
    assert_eq!(m.entries_committed, 2);
    assert_eq!(m.entries_applied, 2);
}

// ---------------------------------------------------------------------------
// Read counters
// ---------------------------------------------------------------------------

#[test]
fn proposing_read_as_non_leader_bumps_reads_failed() {
    let mut engine = follower(1);
    engine.step(Event::ProposeRead { id: 42 });
    let m = engine.metrics();
    assert_eq!(m.reads_failed, 1);
    assert_eq!(m.read_index_started, 0);
}

// ---------------------------------------------------------------------------
// Gauges track state
// ---------------------------------------------------------------------------

#[test]
fn gauges_track_current_term_and_commit_after_events() {
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1, 1]);
    engine.step(append_entries_from(
        2,
        append_entries_request(2, 2, Some(log_id(3, 1)), vec![], 3),
    ));
    let m = engine.metrics();
    assert_eq!(m.current_term, term(2));
    assert_eq!(m.commit_index, LogIndex::new(3));
    assert_eq!(m.log_len, 3);
}
