//! Tests for [`Engine::broadcast_append_entries`] — the leader's outbound
//! `AppendEntries` path. Heartbeat and replication share this single code
//! path: when a peer is caught up, `entries` is empty (effective heartbeat);
//! when a peer is behind, `entries` carries what's missing.

use super::fixtures::{
    append_entries_from, append_entries_request, collect_append_entries, follower,
    follower_with_env, log_id, node, seed_log, term, vote_response_from,
};
use crate::engine::env::StaticEnv;
use crate::engine::event::Event;
use crate::engine::role_state::{LeaderState, RoleState};
use crate::records::log_entry::LogPayload;
use crate::types::index::LogIndex;

/// Drive a fresh follower → candidate → leader for a 3-node cluster.
/// Self id is 1, peers are [2, 3]. After return, the engine is Leader at term 1
/// and the test inbox already contains the initial heartbeat from `become_leader`.
fn elected_leader_3_node() -> crate::engine::engine::Engine<Vec<u8>> {
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    engine.step(Event::Tick); // → Candidate
    engine.step(vote_response_from(2, 1, true)); // → Leader (one peer grant wins in a 3-node cluster)
    assert!(matches!(engine.role(), RoleState::Leader(_)));
    engine
}

// ---------------------------------------------------------------------------
// Shape: one Send per peer
// ---------------------------------------------------------------------------

#[test]
fn fresh_leader_emits_one_append_entries_per_peer() {
    let mut engine = elected_leader_3_node();
    // Drive the heartbeat tick (default heartbeat_interval = 1).
    let actions = engine.step(Event::Tick);

    let appends = collect_append_entries(&actions);
    assert_eq!(appends.len(), 2, "one AppendEntries per peer");

    let mut peers: Vec<u64> = appends.iter().map(|(p, _)| p.get()).collect();
    peers.sort_unstable();
    assert_eq!(peers, vec![2, 3]);
}

#[test]
fn non_leader_emits_no_append_entries() {
    let mut engine = follower(1);
    // Manually call broadcast on a follower would be private; instead drive
    // a Tick which exercises the role-dispatch in on_tick. A follower's tick
    // increments election_elapsed without emitting AppendEntries.
    let actions = engine.step(Event::Tick);
    assert!(collect_append_entries(&actions).is_empty());
}

// ---------------------------------------------------------------------------
// Content: prev_log_id, entries, leader_commit, term, leader_id
// ---------------------------------------------------------------------------

#[test]
fn empty_pre_election_log_yields_prev_at_the_become_leader_noop() {
    // §5.4.2: become_leader appends a no-op at the new term. With an empty
    // pre-election log, the no-op is at (1, 1). PeerProgress::new then sets
    // nextIndex = 2, so the heartbeat carries prev = (1, 1) and no entries —
    // peers are *assumed* to already have the no-op until proven otherwise.
    let mut engine = elected_leader_3_node();
    let actions = engine.step(Event::Tick);

    for (_peer, req) in collect_append_entries(&actions) {
        assert_eq!(req.prev_log_id, Some(log_id(1, 1)), "prev = the no-op");
        assert!(req.entries.is_empty(), "peers assumed caught up");
    }
}

#[test]
fn become_leader_appends_a_noop_at_current_term() {
    let engine = elected_leader_3_node();
    // Last entry must be the no-op at the leader's current term.
    let last = engine.log().last_log_id().expect("noop must be present");
    assert_eq!(last, log_id(1, 1));
    let entry = engine
        .log()
        .entry_at(LogIndex::new(1))
        .expect("entry must exist");
    assert!(matches!(entry.payload, LogPayload::Noop));
}

#[test]
fn behind_peer_receives_unsent_entries_starting_at_next() {
    // Pre-seed: leader has 3 entries before becoming leader. become_leader
    // then appends a no-op at term 1, pushing the log to 4 entries.
    // PeerProgress::new sets nextIndex = 4 for every peer (last_pre_noop+1),
    // so the first broadcast carries just the no-op (entry 4) — peers are
    // assumed caught up to the seeded portion until proven otherwise.
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    seed_log(&mut engine, &[1, 1, 1]); // log: (1,1) (2,1) (3,1)
    engine.step(Event::Tick);
    engine.step(vote_response_from(2, 1, true));
    assert!(matches!(engine.role(), RoleState::Leader(_)));

    let actions = engine.step(Event::Tick);
    for (_peer, req) in collect_append_entries(&actions) {
        assert_eq!(
            req.prev_log_id,
            Some(log_id(4, 1)),
            "prev = the no-op the new leader appended on top of the seeded log",
        );
        assert!(req.entries.is_empty(), "peers assumed caught up");
    }
}

#[test]
fn append_entries_carries_current_term_and_leader_id() {
    let mut engine = elected_leader_3_node();
    let term_now = engine.current_term();
    let actions = engine.step(Event::Tick);

    for (_peer, req) in collect_append_entries(&actions) {
        assert_eq!(req.term, term_now);
        assert_eq!(req.leader_id, node(1));
    }
}

#[test]
fn append_entries_carries_current_commit_index() {
    // Seed log + commit, then become leader, then check broadcast carries it.
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    seed_log(&mut engine, &[1, 1, 1]);
    // Advance commit to 2 by accepting an AppendEntries from a prior leader.
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, Some(log_id(3, 1)), vec![], 2),
    ));
    assert_eq!(engine.commit_index(), LogIndex::new(2));

    // Now run an election and become leader.
    engine.step(Event::Tick);
    engine.step(vote_response_from(3, 2, true));
    assert!(matches!(engine.role(), RoleState::Leader(_)));

    // Heartbeat tick.
    let actions = engine.step(Event::Tick);
    for (_peer, req) in collect_append_entries(&actions) {
        assert_eq!(
            req.leader_commit,
            LogIndex::new(2),
            "leader_commit propagates current commit_index",
        );
    }
}

// ---------------------------------------------------------------------------
// Replication catch-up scenarios — exercise nextIndex < last_log_index.
// ---------------------------------------------------------------------------

#[test]
fn rewinding_peer_next_index_replays_missing_entries() {
    // Seed log of 3 entries, become leader, then forcibly rewind one peer's
    // nextIndex to 2 (as if a Conflict response had pushed it back). The next
    // broadcast should send entries 2 and 3 to that peer, nothing to the other.
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    seed_log(&mut engine, &[1, 1, 1]);
    engine.step(Event::Tick);
    engine.step(vote_response_from(2, 1, true));

    // Surgically rewind peer 2's nextIndex via state_mut.
    if let RoleState::Leader(ls) = &mut engine.state_mut().role {
        let next_for_2 = ls.progress.next_for(node(2)).unwrap();
        ls.progress.record_conflict(node(2), LogIndex::new(2));
        assert!(ls.progress.next_for(node(2)).unwrap() < next_for_2);
    } else {
        panic!("expected Leader");
    }

    let actions = engine.step(Event::Tick);
    let appends = collect_append_entries(&actions);
    let (_, req2) = appends
        .iter()
        .find(|(p, _)| *p == node(2))
        .expect("expected an append to peer 2");
    let (_, req3) = appends
        .iter()
        .find(|(p, _)| *p == node(3))
        .expect("expected an append to peer 3");

    // become_leader appended a no-op at (4, 1), so log = (1,1)(2,1)(3,1)(4,1).
    // PeerProgress initialized nextIndex=5 for every peer, then peer 2 was
    // rewound to 2: prev_log_index=1 → prev = (1,1), entries 2, 3, 4 sent.
    assert_eq!(req2.prev_log_id, Some(log_id(1, 1)));
    assert_eq!(req2.entries.len(), 3);
    assert_eq!(req2.entries[0].id, log_id(2, 1));
    assert_eq!(req2.entries[1].id, log_id(3, 1));
    assert_eq!(req2.entries[2].id, log_id(4, 1));
    assert!(matches!(req2.entries[2].payload, LogPayload::Noop));

    // Peer 3 still at nextIndex=5 → caught up, no entries.
    assert_eq!(req3.prev_log_id, Some(log_id(4, 1)));
    assert!(req3.entries.is_empty());
}

#[test]
fn far_behind_peer_with_next_at_one_receives_all_entries() {
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    seed_log(&mut engine, &[1, 1, 1, 1]);
    engine.step(Event::Tick);
    engine.step(vote_response_from(2, 1, true));

    // Peer 2 is way behind: rewind nextIndex all the way to 1.
    if let RoleState::Leader(ls) = &mut engine.state_mut().role {
        ls.progress.record_conflict(node(2), LogIndex::new(1));
    }

    let actions = engine.step(Event::Tick);
    let appends = collect_append_entries(&actions);
    let (_, req2) = appends.iter().find(|(p, _)| *p == node(2)).unwrap();

    // 4 seeded entries + 1 no-op from become_leader = 5 entries total.
    // nextIndex=1 → prev_log_index=0 → prev_log_id = None (before-log sentinel).
    assert_eq!(req2.prev_log_id, None);
    assert_eq!(req2.entries.len(), 5, "all entries sent");
}

// ---------------------------------------------------------------------------
// Heartbeat cadence — broadcast fires every heartbeat_interval ticks
// ---------------------------------------------------------------------------

#[test]
fn leader_broadcasts_on_each_heartbeat_interval_tick() {
    // Build with heartbeat_interval=2 so we can see the cadence.
    let env = StaticEnv(10);
    let mut engine: crate::engine::engine::Engine<Vec<u8>> = crate::engine::engine::Engine::new(
        node(1),
        [node(2)],
        Box::new(env),
        2, // heartbeat_interval_ticks
    );
    // Force into leader state directly (bypass election for this test's intent).
    engine.state_mut().current_term = term(1);
    engine.state_mut().role = RoleState::Leader(LeaderState::default());

    let a1 = engine.step(Event::Tick); // heartbeat_elapsed = 1, no broadcast
    assert!(collect_append_entries(&a1).is_empty());

    engine.step(Event::Tick); // heartbeat_elapsed = 2 → broadcast + reset
    assert_eq!(engine.heartbeat_elapsed(), 0);
}
