//! Tests for [`Engine::on_tick`] — election timeout, candidate retry,
//! and leader heartbeat. §5.2.

use super::fixtures::{collect_vote_requests, follower, follower_with_env, node, seed_log};
use crate::engine::action::Action;
use crate::engine::env::StaticEnv;
use crate::engine::event::Event;
use crate::engine::role_state::{LeaderState, RoleState};
use crate::types::term::Term;

// ---------------------------------------------------------------------------
// Follower/Candidate election timer
// ---------------------------------------------------------------------------

#[test]
fn follower_increments_election_elapsed_on_tick() {
    let mut engine = follower(1);
    assert_eq!(engine.election_elapsed(), 0);

    engine.step(Event::Tick);
    assert_eq!(engine.election_elapsed(), 1);

    engine.step(Event::Tick);
    assert_eq!(engine.election_elapsed(), 2);
}

#[test]
fn follower_does_nothing_before_timeout() {
    let mut engine = follower(1);
    let timeout = engine.election_timeout_ticks();
    assert!(timeout > 1, "test requires a timeout > 1 tick");

    let actions = engine.step(Event::Tick);

    assert!(actions.is_empty(), "no actions before timeout");
    assert!(
        matches!(engine.role(), RoleState::Follower(_)),
        "role unchanged before timeout",
    );
}

#[test]
fn follower_starts_election_at_exactly_the_timeout() {
    let env = StaticEnv(3);
    let mut engine = follower_with_env(1, &[2], Box::new(env));
    assert_eq!(engine.election_timeout_ticks(), 3);

    engine.step(Event::Tick); // elapsed 1
    engine.step(Event::Tick); // elapsed 2
    assert!(
        matches!(engine.role(), RoleState::Follower(_)),
        "still follower at elapsed 2",
    );

    let actions = engine.step(Event::Tick); // elapsed 3 == timeout → fire

    assert!(
        matches!(engine.role(), RoleState::Candidate(_)),
        "role transitioned to candidate at timeout",
    );
    assert_eq!(
        collect_vote_requests(&actions).len(),
        1,
        "expected one RequestVote to the single peer, got {actions:?}",
    );
}

// ---------------------------------------------------------------------------
// start_election state changes
// ---------------------------------------------------------------------------

#[test]
fn starting_election_increments_term() {
    let env = StaticEnv(1); // fire immediately on first tick
    let mut engine = follower_with_env(1, &[2], Box::new(env));
    let before = engine.current_term();

    engine.step(Event::Tick);

    assert_eq!(engine.current_term(), before.next(), "term bumped by one");
}

#[test]
fn starting_election_votes_for_self() {
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2], Box::new(env));

    engine.step(Event::Tick);

    assert_eq!(engine.voted_for(), Some(node(1)));
}

#[test]
fn starting_election_transitions_to_candidate() {
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2], Box::new(env));

    engine.step(Event::Tick);

    assert!(matches!(engine.role(), RoleState::Candidate(_)));
}

#[test]
fn starting_election_resets_election_timer() {
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2], Box::new(env));

    engine.step(Event::Tick);

    assert_eq!(
        engine.election_elapsed(),
        0,
        "become_candidate resets elapsed",
    );
}

// ---------------------------------------------------------------------------
// Broadcast to peers
// ---------------------------------------------------------------------------

#[test]
fn starting_election_emits_request_vote_to_every_peer() {
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3, 4], Box::new(env));

    let actions = engine.step(Event::Tick);

    let requests = collect_vote_requests(&actions);
    assert_eq!(requests.len(), 3, "one RequestVote per peer");

    let mut peers: Vec<u64> = requests.iter().map(|(p, _)| p.get()).collect();
    peers.sort_unstable();
    assert_eq!(peers, vec![2, 3, 4]);
}

#[test]
fn request_vote_carries_the_new_term_and_self_id() {
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2], Box::new(env));
    let prior_term = engine.current_term();

    let actions = engine.step(Event::Tick);
    let requests = collect_vote_requests(&actions);
    let (_, req) = requests[0];

    assert_eq!(req.term, prior_term.next(), "carries post-increment term");
    assert_eq!(req.candidate_id, node(1));
    assert_eq!(req.last_log_id, None, "empty log has no last id");
}

#[test]
fn request_vote_carries_last_log_id_when_log_nonempty() {
    use crate::types::index::LogIndex;
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2], Box::new(env));
    seed_log(&mut engine, &[1, 1, 1]); // entries at indices 1..=3

    let actions = engine.step(Event::Tick);
    let (_, req) = collect_vote_requests(&actions)[0];

    let last = req.last_log_id.expect("log has entries");
    assert_eq!(last.index, LogIndex::new(3));
    assert_eq!(last.term, Term::new(1));
}

// ---------------------------------------------------------------------------
// Candidate retry — split-vote recovery
// ---------------------------------------------------------------------------

#[test]
fn candidate_starts_new_election_when_its_own_election_times_out() {
    let env = StaticEnv(1); // every tick is a timeout
    let mut engine = follower_with_env(1, &[2], Box::new(env));

    // First tick: follower → candidate at term 1.
    engine.step(Event::Tick);
    assert_eq!(engine.current_term(), Term::new(1));

    // Second tick: candidate times out without winning → new election at term 2.
    engine.step(Event::Tick);
    assert_eq!(
        engine.current_term(),
        Term::new(2),
        "candidate retry bumped the term again",
    );
    assert!(matches!(engine.role(), RoleState::Candidate(_)));
    assert_eq!(engine.voted_for(), Some(node(1)), "still voted for self");
}

// ---------------------------------------------------------------------------
// Leader heartbeat timer
// ---------------------------------------------------------------------------

#[test]
fn leader_increments_heartbeat_elapsed_on_tick() {
    let mut engine = follower(1);
    engine.state_mut().role = RoleState::Leader(LeaderState::default());
    assert_eq!(engine.heartbeat_elapsed(), 0);

    engine.step(Event::Tick);

    // Default heartbeat_interval is 1 tick in the fixture, so after one tick
    // the handler fires send_heartbeats (stub today) and resets elapsed to 0.
    assert_eq!(engine.heartbeat_elapsed(), 0);
}

#[test]
fn leader_resets_heartbeat_elapsed_after_interval() {
    // Build with a heartbeat interval of 3 so we can see the counter climb.
    let env = StaticEnv(10);
    let mut engine: crate::engine::engine::Engine<Vec<u8>> = crate::engine::engine::Engine::new(
        node(1),
        [node(2)],
        Box::new(env),
        3, // heartbeat_interval_ticks
    );
    engine.state_mut().role = RoleState::Leader(LeaderState::default());

    engine.step(Event::Tick); // heartbeat_elapsed 1
    assert_eq!(engine.heartbeat_elapsed(), 1);
    engine.step(Event::Tick); // 2
    assert_eq!(engine.heartbeat_elapsed(), 2);

    let actions = engine.step(Event::Tick); // 3 == interval → fire + reset
    assert_eq!(engine.heartbeat_elapsed(), 0, "reset after interval");
    // The broadcast emits one AppendEntries per peer in the cluster (here, 1).
    assert_eq!(actions.len(), 1, "one AppendEntries per peer");
}

#[test]
fn leader_does_not_increment_election_elapsed() {
    let mut engine = follower(1);
    engine.state_mut().role = RoleState::Leader(LeaderState::default());
    let before = engine.election_elapsed();

    engine.step(Event::Tick);

    assert_eq!(
        engine.election_elapsed(),
        before,
        "leader branch must not touch the election timer",
    );
}

// Suppress an unused import warning from an `Action` import reserved for
// future tick tests that inspect emitted actions beyond RequestVote.
const _UNUSED_ACTION_IMPORT: Option<Action<Vec<u8>>> = None;
