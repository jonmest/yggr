//! Snapshot tests: host-driven `Event::SnapshotTaken` (pass 1) plus
//! the `InstallSnapshot` RPC pair and leader-side branching (pass 2).

use super::fixtures::{
    append_entries_from, append_entries_request, append_entries_success_from,
    collect_append_entries, collect_apply_snapshots, collect_install_snapshots,
    collect_persist_snapshots, collect_snapshot_hints, follower, follower_with_env,
    install_snapshot_from, install_snapshot_response_from, log_id, node, seed_log, snapshot_taken,
    term, vote_response_from,
};
use crate::engine::action::Action;
use crate::engine::engine::{Engine, EngineConfig};
use crate::engine::env::StaticEnv;
use crate::engine::event::Event;
use crate::engine::role_state::RoleState;
use crate::records::log_entry::{LogEntry, LogPayload};
use crate::types::index::LogIndex;
use crate::types::log::LogId;
use crate::types::term::Term;

// ---------------------------------------------------------------------------
// Successful snapshot install
// ---------------------------------------------------------------------------

/// Drive a follower's `commit_index` up to `idx` by accepting an empty
/// `AppendEntries` from a fake leader after seeding.
fn commit_follower_log(engine: &mut Engine<Vec<u8>>, idx: u64) {
    let last_term = engine.log().last_log_id().unwrap().term;
    engine.step(append_entries_from(
        2,
        append_entries_request(last_term.get(), 2, engine.log().last_log_id(), vec![], idx),
    ));
}

#[test]
fn snapshot_taken_truncates_log_up_to_index_and_emits_persist_snapshot() {
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1, 1, 1, 1]);
    commit_follower_log(&mut engine, 3);
    assert_eq!(engine.commit_index(), LogIndex::new(3));

    let actions = engine.step(snapshot_taken(3, b"snap-bytes".to_vec()));

    // Log floor advanced.
    assert_eq!(engine.log().snapshot_last().index, LogIndex::new(3));
    assert_eq!(engine.log().snapshot_last().term, term(1));
    // Pre-floor entries no longer addressable.
    assert!(engine.log().entry_at(LogIndex::new(1)).is_none());
    assert!(engine.log().entry_at(LogIndex::new(2)).is_none());
    assert!(engine.log().entry_at(LogIndex::new(3)).is_none());
    // Post-floor entries survive.
    assert!(engine.log().entry_at(LogIndex::new(4)).is_some());
    assert!(engine.log().entry_at(LogIndex::new(5)).is_some());
    // last_log_id still reflects the highest index we've ever held.
    assert_eq!(engine.log().last_log_id().map(|l| l.index.get()), Some(5),);

    // PersistSnapshot emitted with the right metadata.
    let persist = actions.iter().find_map(|a| match a {
        Action::PersistSnapshot {
            last_included_index,
            last_included_term,
            bytes,
            ..
        } => Some((*last_included_index, *last_included_term, bytes.clone())),
        _ => None,
    });
    assert_eq!(
        persist,
        Some((LogIndex::new(3), term(1), b"snap-bytes".to_vec())),
    );
}

#[test]
fn snapshot_at_last_committed_index_clears_in_memory_log() {
    // Snapshot covers everything: in-memory log becomes empty but
    // last_log_id still reports the snapshot's tail.
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1, 1]);
    commit_follower_log(&mut engine, 3);

    engine.step(snapshot_taken(3, b"all".to_vec()));

    assert!(engine.log().is_empty());
    assert_eq!(engine.log().snapshot_last().index, LogIndex::new(3));
    assert_eq!(
        engine.log().last_log_id(),
        Some(log_id(3, 1)),
        "last_log_id falls through to the snapshot tail",
    );
}

#[test]
fn snapshot_advances_first_index() {
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1, 1, 1]);
    commit_follower_log(&mut engine, 2);

    assert_eq!(engine.log().first_index(), LogIndex::new(1));
    engine.step(snapshot_taken(2, b"".to_vec()));
    assert_eq!(engine.log().first_index(), LogIndex::new(3));
}

// ---------------------------------------------------------------------------
// Refusal / no-op paths
// ---------------------------------------------------------------------------

#[test]
fn snapshot_past_commit_index_is_refused() {
    // Host can only snapshot what's committed. last_committed = 2,
    // request snapshot at 5 → refused, log unchanged.
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1, 1, 1, 1]);
    commit_follower_log(&mut engine, 2);

    let actions = engine.step(snapshot_taken(5, b"too-far".to_vec()));

    assert!(
        actions.is_empty(),
        "snapshot past commit_index must yield no actions",
    );
    assert_eq!(engine.log().snapshot_last().index, LogIndex::ZERO);
    assert_eq!(engine.log().len(), 5);
}

#[test]
fn stale_snapshot_at_or_below_existing_floor_is_a_noop() {
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1, 1, 1]);
    commit_follower_log(&mut engine, 4);
    engine.step(snapshot_taken(3, b"first".to_vec()));
    assert_eq!(engine.log().snapshot_last().index, LogIndex::new(3));

    // Re-snapshot at 3 (same point): no-op.
    let actions = engine.step(snapshot_taken(3, b"second".to_vec()));
    assert!(actions.is_empty());
    assert_eq!(engine.log().snapshot_last().index, LogIndex::new(3));

    // Re-snapshot at 2 (below existing): no-op.
    let actions = engine.step(snapshot_taken(2, b"third".to_vec()));
    assert!(actions.is_empty());
    assert_eq!(engine.log().snapshot_last().index, LogIndex::new(3));
}

// ---------------------------------------------------------------------------
// Term plumbing
// ---------------------------------------------------------------------------

#[test]
fn snapshot_records_correct_term_for_floor() {
    // Mixed-term log: snapshot the boundary, term must come from the
    // entry at last_included_index, not a synthesised value.
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1, 2, 2, 3]); // entry 3 has term 2
    commit_follower_log(&mut engine, 3);

    engine.step(snapshot_taken(3, b"".to_vec()));
    assert_eq!(engine.log().snapshot_last().term, Term::new(2));
}

// ---------------------------------------------------------------------------
// Subsequent reads through entry_at and term_at
// ---------------------------------------------------------------------------

#[test]
fn term_at_floor_index_returns_snapshot_term() {
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1, 1]);
    commit_follower_log(&mut engine, 3);
    engine.step(snapshot_taken(3, b"".to_vec()));

    // Entry at index 3 is in the snapshot, but term_at(3) must still
    // return the correct term — needed for prev_log_id checks above
    // the floor.
    assert_eq!(engine.log().term_at(LogIndex::new(3)), Some(term(1)));
    // Below the floor: still no answer.
    assert_eq!(engine.log().term_at(LogIndex::new(2)), None);
}

#[test]
fn entries_from_below_floor_returns_empty_slice() {
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1, 1, 1, 1]);
    commit_follower_log(&mut engine, 3);
    engine.step(snapshot_taken(3, b"".to_vec()));

    // Asking for entries from 2 (below floor): empty — caller should
    // send InstallSnapshot instead. Asking from 4 returns the tail.
    assert!(engine.log().entries_from(LogIndex::new(2)).is_empty());
    assert_eq!(engine.log().entries_from(LogIndex::new(4)).len(), 2);
}

#[test]
fn snapshot_hint_fires_once_per_applied_threshold_band_and_rearms_after_snapshot() {
    let mut engine = Engine::with_config(
        node(1),
        [node(2), node(3)],
        Box::new(StaticEnv(10)),
        1,
        EngineConfig {
            snapshot_chunk_size_bytes: 16,
            snapshot_hint_threshold_entries: 2,
            max_log_entries: 0,
            pre_vote: false,
            lease_duration_ticks: 0,
        },
    );
    seed_log(&mut engine, &[1, 1, 1, 1]);

    let first = engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, engine.log().last_log_id(), vec![], 2),
    ));
    assert_eq!(collect_snapshot_hints(&first), vec![LogIndex::new(2)]);

    let no_repeat = engine.step(Event::Tick);
    assert!(
        collect_snapshot_hints(&no_repeat).is_empty(),
        "threshold band should only hint once until the floor advances",
    );

    engine.step(snapshot_taken(2, b"snap@2".to_vec()));

    let second = engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, engine.log().last_log_id(), vec![], 4),
    ));
    assert_eq!(collect_snapshot_hints(&second), vec![LogIndex::new(4)]);
}

// ---------------------------------------------------------------------------
// max_log_entries guardrail — forces a snapshot hint once the live log
// grows past the threshold, independent of the applied-entries band.
// ---------------------------------------------------------------------------

#[test]
fn max_log_entries_emits_snapshot_hint_when_exceeded() {
    let mut engine = Engine::with_config(
        node(1),
        [node(2), node(3)],
        Box::new(StaticEnv(10)),
        1,
        EngineConfig {
            snapshot_chunk_size_bytes: 16,
            snapshot_hint_threshold_entries: 0, // applied-band path off
            max_log_entries: 3,
            pre_vote: false,
            lease_duration_ticks: 0,
        },
    );
    // Seed 4 entries and commit them all. Live log (4) > threshold (3).
    seed_log(&mut engine, &[1, 1, 1, 1]);
    let actions = engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, engine.log().last_log_id(), vec![], 4),
    ));
    assert_eq!(collect_snapshot_hints(&actions), vec![LogIndex::new(4)]);
}

#[test]
fn max_log_entries_zero_disables_the_guardrail() {
    let mut engine = Engine::with_config(
        node(1),
        [node(2), node(3)],
        Box::new(StaticEnv(10)),
        1,
        EngineConfig {
            snapshot_chunk_size_bytes: 16,
            snapshot_hint_threshold_entries: 0,
            max_log_entries: 0,
            pre_vote: false,
            lease_duration_ticks: 0,
        },
    );
    seed_log(&mut engine, &[1, 1, 1, 1, 1, 1, 1, 1]);
    let actions = engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, engine.log().last_log_id(), vec![], 8),
    ));
    assert!(
        collect_snapshot_hints(&actions).is_empty(),
        "threshold=0 must not emit any snapshot hints",
    );
}

#[test]
fn max_log_entries_fires_once_per_crossing_and_rearms_after_snapshot() {
    let mut engine = Engine::with_config(
        node(1),
        [node(2), node(3)],
        Box::new(StaticEnv(10)),
        1,
        EngineConfig {
            snapshot_chunk_size_bytes: 16,
            snapshot_hint_threshold_entries: 0,
            max_log_entries: 2,
            pre_vote: false,
            lease_duration_ticks: 0,
        },
    );
    seed_log(&mut engine, &[1, 1, 1]);
    let first = engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, engine.log().last_log_id(), vec![], 3),
    ));
    assert_eq!(collect_snapshot_hints(&first), vec![LogIndex::new(3)]);

    // Ticking again with unchanged log/floor must not re-hint.
    let no_repeat = engine.step(Event::Tick);
    assert!(
        collect_snapshot_hints(&no_repeat).is_empty(),
        "same crossing must not re-hint",
    );

    // Take the snapshot; floor advances past the threshold, live log
    // shrinks back under. Then grow past again via AE — new hint
    // expected.
    engine.step(snapshot_taken(3, b"snap@3".to_vec()));
    // Now floor=3, live log has 0 entries. Append 3 more entries
    // (indices 4,5,6) via a leader AE so it crosses the threshold
    // again.
    let more = crate::engine::tests::fixtures::log_entries(&[(4, 1), (5, 1), (6, 1)]);
    let second = engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, engine.log().last_log_id(), more, 6),
    ));
    assert_eq!(collect_snapshot_hints(&second), vec![LogIndex::new(6)]);
}

// ---------------------------------------------------------------------------
// Pass 2: InstallSnapshot RPC (§7) — leader-side branching
// ---------------------------------------------------------------------------

/// 3-node leader (self=1, peers=2,3) at term 1, log = [(1,1)=Noop],
/// having already taken a snapshot at index 1.
fn leader_with_snapshot_config(config: EngineConfig) -> Engine<Vec<u8>> {
    let env = StaticEnv(1);
    let mut engine = Engine::with_config(node(1), [node(2), node(3)], Box::new(env), 1, config);
    engine.step(Event::Tick);
    engine.step(vote_response_from(2, 1, true));
    assert!(matches!(engine.role(), RoleState::Leader(_)));
    // Commit the noop so we can snapshot it.
    engine.step(append_entries_success_from(2, 1, 1));
    assert_eq!(engine.commit_index(), LogIndex::new(1));
    engine.step(snapshot_taken(1, b"snap@1".to_vec()));
    assert_eq!(engine.log().snapshot_last().index, LogIndex::new(1));
    engine
}

fn leader_with_snapshot() -> Engine<Vec<u8>> {
    // Classical election semantics for this test family. Pre-vote is
    // exercised separately in `tests/pre_vote.rs`.
    leader_with_snapshot_config(EngineConfig {
        snapshot_chunk_size_bytes: 64 * 1024,
        snapshot_hint_threshold_entries: 0,
        max_log_entries: 0,
        pre_vote: false,
        lease_duration_ticks: 0,
    })
}

#[test]
fn leader_sends_install_snapshot_when_peer_next_index_is_below_floor() {
    // After the snapshot at index 1, peer 3 lags. Force its nextIndex
    // to 1 (≤ floor) and ask the engine to broadcast — InstallSnapshot
    // should go to peer 3.
    let mut engine = leader_with_snapshot();
    if let RoleState::Leader(ls) = &mut engine.state_mut().role {
        ls.progress.record_conflict(node(3), LogIndex::new(1));
    }
    let actions = engine.step(Event::Tick);

    let snapshots = collect_install_snapshots(&actions);
    let to_3 = snapshots.iter().find(|(p, _)| *p == node(3));
    assert!(
        to_3.is_some(),
        "peer 3 below floor should receive InstallSnapshot, not AppendEntries: {:?}",
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Send { to, .. } => Some(to.get()),
                _ => None,
            })
            .collect::<Vec<_>>(),
    );
    let req = &to_3.unwrap().1;
    assert_eq!(req.last_included.index, LogIndex::new(1));
    assert_eq!(req.last_included.term, term(1));
    assert_eq!(req.data, b"snap@1");
    assert_eq!(req.offset, 0);
    assert!(req.done);
}

#[test]
fn leader_keeps_sending_append_entries_when_peer_is_above_floor() {
    // Peer 2's nextIndex stays at 2 (above the snapshot floor of 1) —
    // it should get AppendEntries, not InstallSnapshot.
    let mut engine = leader_with_snapshot();
    let actions = engine.step(Event::Tick);

    let appends = collect_append_entries(&actions);
    let snapshots = collect_install_snapshots(&actions);
    assert!(
        appends.iter().any(|(p, _)| *p == node(2)),
        "peer 2 above floor must receive AppendEntries",
    );
    assert!(
        !snapshots.iter().any(|(p, _)| *p == node(2)),
        "peer 2 above floor must NOT receive InstallSnapshot",
    );
}

#[test]
fn install_snapshot_response_advances_match_index_to_snapshot_floor() {
    // After leader sends InstallSnapshot and gets a Success response,
    // the peer's matchIndex must be the snapshot's tail (= our floor).
    let mut engine = leader_with_snapshot();
    if let RoleState::Leader(ls) = &mut engine.state_mut().role {
        ls.progress.record_conflict(node(3), LogIndex::new(1));
    }
    engine.step(Event::Tick); // sends InstallSnapshot

    let term_now = engine.current_term().get();
    engine.step(install_snapshot_response_from(3, term_now, 1, 1, 6, true));

    let RoleState::Leader(s) = engine.role() else {
        panic!("expected Leader");
    };
    assert_eq!(s.progress.match_for(node(3)), Some(LogIndex::new(1)));
    assert_eq!(s.progress.next_for(node(3)), Some(LogIndex::new(2)));
}

#[test]
fn higher_term_install_snapshot_response_demotes_leader() {
    let mut engine = leader_with_snapshot();
    let term_before = engine.current_term();
    engine.step(install_snapshot_response_from(
        2,
        term_before.next().get(),
        1,
        1,
        0,
        false,
    ));
    assert!(matches!(engine.role(), RoleState::Follower(_)));
    assert_eq!(engine.current_term(), term_before.next());
}

#[test]
fn leader_streams_snapshot_one_chunk_per_broadcast_and_resumes_on_ack() {
    let mut engine = leader_with_snapshot_config(EngineConfig {
        snapshot_chunk_size_bytes: 3,
        snapshot_hint_threshold_entries: 0,
        max_log_entries: 0,
        pre_vote: false,
        lease_duration_ticks: 0,
    });
    engine.state_mut().snapshot_bytes = Some(b"abcdefg".to_vec());
    if let RoleState::Leader(ls) = &mut engine.state_mut().role {
        ls.progress.record_conflict(node(3), LogIndex::new(1));
    }

    let first = engine.step(Event::Tick);
    let req = collect_install_snapshots(&first)
        .into_iter()
        .find(|(peer, _)| *peer == node(3))
        .expect("peer 3 should receive the first snapshot chunk")
        .1;
    assert_eq!(req.offset, 0);
    assert_eq!(req.data, b"abc");
    assert!(!req.done);

    engine.step(install_snapshot_response_from(3, 1, 1, 1, 3, false));
    let second = engine.step(Event::Tick);
    let req = collect_install_snapshots(&second)
        .into_iter()
        .find(|(peer, _)| *peer == node(3))
        .expect("peer 3 should receive the second snapshot chunk")
        .1;
    assert_eq!(req.offset, 3);
    assert_eq!(req.data, b"def");
    assert!(!req.done);

    engine.step(install_snapshot_response_from(3, 1, 1, 1, 6, false));
    let third = engine.step(Event::Tick);
    let req = collect_install_snapshots(&third)
        .into_iter()
        .find(|(peer, _)| *peer == node(3))
        .expect("peer 3 should receive the final snapshot chunk")
        .1;
    assert_eq!(req.offset, 6);
    assert_eq!(req.data, b"g");
    assert!(req.done);

    engine.step(install_snapshot_response_from(3, 1, 1, 1, 7, true));
    let RoleState::Leader(s) = engine.role() else {
        panic!("expected Leader");
    };
    assert_eq!(s.progress.match_for(node(3)), Some(LogIndex::new(1)));
    assert_eq!(s.progress.next_for(node(3)), Some(LogIndex::new(2)));
}

#[test]
fn leader_restarts_snapshot_transfer_when_floor_moves() {
    let mut engine = leader_with_snapshot_config(EngineConfig {
        snapshot_chunk_size_bytes: 3,
        snapshot_hint_threshold_entries: 0,
        max_log_entries: 0,
        pre_vote: false,
        lease_duration_ticks: 0,
    });
    if let RoleState::Leader(ls) = &mut engine.state_mut().role {
        ls.progress.record_conflict(node(3), LogIndex::new(1));
    }

    let first = engine.step(Event::Tick);
    let req = collect_install_snapshots(&first)
        .into_iter()
        .find(|(peer, _)| *peer == node(3))
        .expect("initial snapshot chunk must be sent")
        .1;
    assert_eq!(req.offset, 0);

    engine.step(install_snapshot_response_from(3, 1, 1, 1, 3, false));
    engine.state_mut().log.append(LogEntry {
        id: LogId::new(LogIndex::new(2), term(1)),
        payload: LogPayload::Command(Vec::new()),
    });
    engine.state_mut().commit_index = LogIndex::new(2);
    engine.state_mut().last_applied = LogIndex::new(2);
    engine.step(snapshot_taken(2, b"newer".to_vec()));

    let restarted = engine.step(Event::Tick);
    let req = collect_install_snapshots(&restarted)
        .into_iter()
        .find(|(peer, _)| *peer == node(3))
        .expect("snapshot transfer should restart from the new floor")
        .1;
    assert_eq!(req.last_included.index, LogIndex::new(2));
    assert_eq!(req.offset, 0);
    assert_eq!(req.data, b"new");
}

// ---------------------------------------------------------------------------
// Pass 2: InstallSnapshot RPC — follower-side handling
// ---------------------------------------------------------------------------

#[test]
fn follower_install_snapshot_truncates_log_and_advances_commit() {
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1]); // log entries 1,2 at term 1

    let actions = engine.step(install_snapshot_from(
        2,
        1,
        5, // last_included_index past our log
        1,
        5,
        0,
        true,
        b"the-snapshot".to_vec(),
    ));

    // Snapshot floor installed; old log wiped (5 > our last of 2,
    // and Log Matching can't hold across the wipe).
    assert_eq!(engine.log().snapshot_last().index, LogIndex::new(5));
    assert_eq!(engine.log().snapshot_last().term, term(1));
    assert!(engine.log().is_empty());

    // commit_index and last_applied jumped to the snapshot tail.
    assert_eq!(engine.commit_index(), LogIndex::new(5));

    // Actions: PersistSnapshot, ApplySnapshot, Send response.
    let persists = collect_persist_snapshots(&actions);
    assert_eq!(persists.len(), 1);
    assert_eq!(persists[0].0, LogIndex::new(5));
    assert_eq!(persists[0].2, b"the-snapshot");

    let applied = collect_apply_snapshots(&actions);
    assert_eq!(applied, vec![b"the-snapshot".to_vec()]);

    // Persist must come before ApplySnapshot must come before Send.
    let positions: Vec<usize> = actions
        .iter()
        .enumerate()
        .filter_map(|(i, a)| match a {
            Action::PersistSnapshot { .. } | Action::ApplySnapshot { .. } | Action::Send { .. } => {
                Some(i)
            }
            _ => None,
        })
        .collect();
    assert!(positions.len() >= 3);
    assert!(matches!(
        actions[positions[0]],
        Action::PersistSnapshot { .. }
    ));
    assert!(matches!(
        actions[positions[1]],
        Action::ApplySnapshot { .. }
    ));
    assert!(matches!(actions[positions[2]], Action::Send { .. }));
}

#[test]
fn follower_install_snapshot_keeps_consistent_log_tail() {
    // Seed log [(1,1) (2,1) (3,1) (4,1)]. Snapshot at (2,1) — boundary
    // entry exists with the matching term, so entries 3 and 4 survive.
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1, 1, 1]);

    engine.step(install_snapshot_from(
        2,
        1,
        2,
        1,
        2,
        0,
        true,
        b"snap".to_vec(),
    ));

    assert_eq!(engine.log().snapshot_last().index, LogIndex::new(2));
    // Entries 3, 4 still in memory.
    assert!(engine.log().entry_at(LogIndex::new(3)).is_some());
    assert!(engine.log().entry_at(LogIndex::new(4)).is_some());
    assert_eq!(engine.log().last_log_id().map(|l| l.index.get()), Some(4));
}

#[test]
fn follower_install_snapshot_below_existing_floor_is_a_noop() {
    // Already snapshotted at 5; a stale snapshot at 3 should not
    // mutate state (just ack to make the leader stop retrying).
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1, 1, 1, 1]);
    let last_term = engine.log().last_log_id().unwrap().term;
    engine.step(append_entries_from(
        2,
        append_entries_request(last_term.get(), 2, engine.log().last_log_id(), vec![], 5),
    ));
    engine.step(snapshot_taken(5, b"latest".to_vec()));
    assert_eq!(engine.log().snapshot_last().index, LogIndex::new(5));

    // Stale snapshot at 3.
    let actions = engine.step(install_snapshot_from(
        2,
        1,
        3,
        1,
        3,
        0,
        true,
        b"stale".to_vec(),
    ));
    assert_eq!(
        engine.log().snapshot_last().index,
        LogIndex::new(5),
        "stale InstallSnapshot must not move our floor backward",
    );
    let response = actions
        .iter()
        .find_map(|action| match action {
            Action::Send {
                message: crate::records::message::Message::InstallSnapshotResponse(response),
                ..
            } => Some(*response),
            _ => None,
        })
        .expect("stale snapshot must still be acknowledged");
    assert_eq!(response.last_included.index, LogIndex::new(5));
    assert!(response.done);
}

#[test]
fn install_snapshot_with_lower_term_is_rejected_with_our_term() {
    let mut engine = follower(1);
    // Bump our term past the request via a vote grant.
    engine.step(super::fixtures::vote_request_from(
        2,
        super::fixtures::vote_request(2, 5, None),
    ));
    assert_eq!(engine.current_term(), term(5));

    let actions = engine.step(install_snapshot_from(2, 1, 1, 1, 1, 0, true, b"".to_vec()));

    // Floor unchanged; reply carries our term so leader steps down.
    assert_eq!(engine.log().snapshot_last().index, LogIndex::ZERO);
    let response_term = actions
        .iter()
        .find_map(|a| match a {
            Action::Send {
                message: crate::records::message::Message::InstallSnapshotResponse(r),
                ..
            } => Some(r.term),
            _ => None,
        })
        .expect("must reply with our term");
    assert_eq!(response_term, term(5));
}

#[test]
fn install_snapshot_at_higher_term_demotes_candidate_then_accepts() {
    // We're a candidate at term 1; an InstallSnapshot at term 5 from
    // node 2 demotes us and is accepted.
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    engine.step(Event::Tick); // → Candidate at term 1
    assert!(matches!(engine.role(), RoleState::Candidate(_)));

    engine.step(install_snapshot_from(
        2,
        5,
        1,
        5,
        1,
        0,
        true,
        b"new".to_vec(),
    ));

    assert!(matches!(engine.role(), RoleState::Follower(_)));
    assert_eq!(engine.current_term(), term(5));
    assert_eq!(engine.log().snapshot_last().index, LogIndex::new(1));
}

#[test]
fn follower_buffers_chunked_snapshot_until_final_chunk() {
    let mut engine = follower(1);
    seed_log(&mut engine, &[1, 1]);

    let first = engine.step(install_snapshot_from(
        2,
        1,
        5,
        1,
        5,
        0,
        false,
        b"chunk-1".to_vec(),
    ));
    assert!(
        collect_persist_snapshots(&first).is_empty(),
        "partial chunks must not be persisted as a complete snapshot",
    );
    assert!(
        collect_apply_snapshots(&first).is_empty(),
        "partial chunks must not be applied to the state machine",
    );
    let response = first
        .iter()
        .find_map(|action| match action {
            Action::Send {
                message: crate::records::message::Message::InstallSnapshotResponse(response),
                ..
            } => Some(*response),
            _ => None,
        })
        .expect("partial snapshot chunk should be acknowledged");
    assert_eq!(response.next_offset, 7);
    assert!(!response.done);
    assert_eq!(engine.log().snapshot_last().index, LogIndex::ZERO);

    let second = engine.step(install_snapshot_from(
        2,
        1,
        5,
        1,
        5,
        7,
        true,
        b"-done".to_vec(),
    ));
    assert_eq!(engine.log().snapshot_last().index, LogIndex::new(5));
    let persists = collect_persist_snapshots(&second);
    assert_eq!(persists.len(), 1);
    assert_eq!(persists[0].2, b"chunk-1-done");
    let applied = collect_apply_snapshots(&second);
    assert_eq!(applied, vec![b"chunk-1-done".to_vec()]);
}

#[test]
fn follower_reports_resume_offset_for_out_of_order_snapshot_chunk() {
    let mut engine = follower(1);

    let actions = engine.step(install_snapshot_from(
        2,
        1,
        5,
        1,
        5,
        4,
        false,
        b"tail".to_vec(),
    ));
    let response = actions
        .iter()
        .find_map(|action| match action {
            Action::Send {
                message: crate::records::message::Message::InstallSnapshotResponse(response),
                ..
            } => Some(*response),
            _ => None,
        })
        .expect("out-of-order chunk must be acknowledged with a resume offset");
    assert_eq!(response.next_offset, 0);
    assert!(!response.done);
    assert_eq!(engine.log().snapshot_last().index, LogIndex::ZERO);
}
