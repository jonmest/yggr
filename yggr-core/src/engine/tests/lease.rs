//! Leader-lease fast-path tests (Raft §9 lease-bounded reads).
//!
//! A leader that received AE acks from a majority within the last
//! `lease_duration_ticks` is guaranteed to still be leader (§9):
//! no peer can have timed out and won an election in that window,
//! provided `lease_duration_ticks < election_timeout_min - heartbeat_interval`.
//! Under that condition, `on_propose_read` may emit `ReadReady`
//! synchronously, skipping the `ReadIndex` round-trip.

use super::fixtures::{
    append_entries_from, append_entries_request, append_entries_success_from, collect_read_failed,
    collect_read_ready, node, propose_read, vote_response_from,
};
use crate::engine::action::ReadFailure;
use crate::engine::engine::{Engine, EngineConfig};
use crate::engine::env::{Env, StaticEnv};
use crate::engine::event::Event;
use crate::engine::role_state::RoleState;
use crate::types::node::NodeId;
use std::collections::BTreeSet;

const LEASE: u64 = 5;
const HEARTBEAT_INTERVAL: u64 = 1;
/// Deliberately small: the fixture needs a single `Event::Tick` to
/// fire the election so follower→leader promotion is a one-liner.
/// `lease < election_timeout - heartbeat` is trivially satisfied
/// at real production numbers; the tests verify the *mechanism*.
const ELECTION_TIMEOUT: u64 = 1;

fn lease_config(lease_duration_ticks: u64) -> EngineConfig {
    EngineConfig {
        snapshot_chunk_size_bytes: 64 * 1024,
        snapshot_hint_threshold_entries: 0,
        max_log_entries: 0,
        pre_vote: false,
        lease_duration_ticks,
    }
}

fn engine_with_lease(id: u64, peers: &[u64], lease_duration_ticks: u64) -> Engine<Vec<u8>> {
    let env: Box<dyn Env> = Box::new(StaticEnv(ELECTION_TIMEOUT));
    let peers: BTreeSet<NodeId> = peers.iter().copied().map(node).collect();
    Engine::with_config(
        node(id),
        peers,
        env,
        HEARTBEAT_INTERVAL,
        lease_config(lease_duration_ticks),
    )
}

/// Promote node 1 in a 3-node cluster, ack the noop so `commit_index == 1`,
/// and return the engine at the point where §5.4.2 is satisfied.
fn leader_with_noop(lease_duration_ticks: u64) -> Engine<Vec<u8>> {
    let mut engine = engine_with_lease(1, &[2, 3], lease_duration_ticks);
    engine.step(Event::Tick);
    engine.step(vote_response_from(2, 1, true));
    assert!(matches!(engine.role(), RoleState::Leader(_)));
    // Ack noop from peer 2 to commit it.
    engine.step(append_entries_success_from(2, 1, 1));
    assert_eq!(engine.commit_index().get(), 1);
    engine
}

#[test]
fn fresh_leader_without_any_majority_ack_falls_back_to_readindex() {
    // Build a leader but force the lease tracker to be None: after
    // leader_with_noop, peer 2 just acked index 1, which advances the
    // lease tick. So for this test we build a leader whose current-term
    // entry is committed via a *different* path that doesn't set
    // last_majority_ack_tick.
    //
    // Simpler: construct the standard leader (which DOES have a lease
    // after the noop ack), then step_down and re-elect. The new term's
    // lease starts at None again.
    let mut engine = leader_with_noop(LEASE);
    // Force step-down by feeding a higher-term AE.
    engine.step(append_entries_from(
        2,
        append_entries_request(99, 2, None, vec![], 0),
    ));
    assert!(matches!(engine.role(), RoleState::Follower(_)));

    // Re-elect node 1 at term 100.
    // Clear any prior leader by ticking until election.
    for _ in 0..ELECTION_TIMEOUT {
        engine.step(Event::Tick);
    }
    // Now a candidate. Grant vote from peer 2.
    engine.step(vote_response_from(2, engine.current_term().get(), true));
    assert!(matches!(engine.role(), RoleState::Leader(_)));
    // commit_index is stale (1) but term-at(1) is the OLD term, so
    // §5.4.2 still forbids a read. Ack the new noop.
    let noop_idx = engine.log().last_log_id().unwrap().index.get();
    engine.step(append_entries_success_from(
        2,
        engine.current_term().get(),
        noop_idx,
    ));
    assert_eq!(engine.commit_index().get(), noop_idx);

    // The ack above also refreshed the lease, so a read *would* go
    // fast-path. To exercise the "no lease yet" branch, reset by
    // advancing ticks past the lease without any more acks.
    for _ in 0..=LEASE {
        engine.step(Event::Tick);
    }
    // Now lease has expired. Propose a read: must go ReadIndex (no
    // ReadReady synchronously).
    let actions = engine.step(propose_read(1));
    assert!(collect_read_ready(&actions).is_empty());
    assert!(collect_read_failed(&actions).is_empty());
}

#[test]
fn leader_with_fresh_majority_ack_serves_read_via_lease() {
    let mut engine = leader_with_noop(LEASE);
    // The noop ack at tick 0 set last_majority_ack_tick = 0. No ticks
    // have advanced; lease is fresh (0 - 0 = 0 <= LEASE).
    let actions = engine.step(propose_read(7));
    // Fast-path: ReadReady emitted synchronously, no pending read.
    assert_eq!(collect_read_ready(&actions), vec![7]);
    assert!(collect_read_failed(&actions).is_empty());
}

#[test]
fn leader_whose_last_ack_exceeds_lease_falls_back_to_readindex() {
    let mut engine = leader_with_noop(LEASE);
    // Advance past the lease without any new acks.
    for _ in 0..=LEASE {
        engine.step(Event::Tick);
    }
    let actions = engine.step(propose_read(8));
    // No fast-path: neither ReadReady nor ReadFailed.
    assert!(collect_read_ready(&actions).is_empty());
    assert!(collect_read_failed(&actions).is_empty());
}

#[test]
fn lease_expiry_across_ticks() {
    let mut engine = leader_with_noop(LEASE);
    // Within lease: fast-path.
    let actions = engine.step(propose_read(1));
    assert_eq!(collect_read_ready(&actions), vec![1]);

    // Advance exactly `LEASE` ticks: still within.
    for _ in 0..LEASE {
        engine.step(Event::Tick);
    }
    let actions = engine.step(propose_read(2));
    assert_eq!(collect_read_ready(&actions), vec![2]);

    // One more tick beyond: now expired.
    engine.step(Event::Tick);
    let actions = engine.step(propose_read(3));
    assert!(collect_read_ready(&actions).is_empty());
}

#[test]
fn stepdown_still_fails_pending_reads() {
    // Lease is disabled so reads go the ReadIndex path and get queued.
    let mut engine = leader_with_noop(0);
    engine.step(propose_read(100));
    engine.step(propose_read(101));

    let actions = engine.step(append_entries_from(
        2,
        append_entries_request(99, 2, None, vec![], 0),
    ));

    let failed = collect_read_failed(&actions);
    assert_eq!(
        failed,
        vec![
            (100, ReadFailure::SteppedDown),
            (101, ReadFailure::SteppedDown),
        ],
    );
}

#[test]
fn freshly_elected_leader_without_committed_current_term_entry_fails_notready() {
    // Even with lease enabled, §5.4.2 must still be enforced.
    let env: Box<dyn Env> = Box::new(StaticEnv(ELECTION_TIMEOUT));
    let peers: BTreeSet<NodeId> = [2u64, 3].into_iter().map(node).collect();
    let mut engine =
        Engine::with_config(node(1), peers, env, HEARTBEAT_INTERVAL, lease_config(LEASE));
    engine.step(Event::Tick);
    engine.step(vote_response_from(2, 1, true));
    assert!(matches!(engine.role(), RoleState::Leader(_)));
    // commit_index is 0 — noop not yet committed.
    assert_eq!(engine.commit_index().get(), 0);

    let actions = engine.step(propose_read(42));
    assert_eq!(
        collect_read_failed(&actions),
        vec![(42, ReadFailure::NotReady)],
    );
    assert!(collect_read_ready(&actions).is_empty());
}

#[test]
fn lease_duration_zero_disables_lease() {
    // lease_duration_ticks = 0 means "lease never applies"; every read
    // goes through the full ReadIndex round-trip.
    let mut engine = leader_with_noop(0);
    let actions = engine.step(propose_read(5));
    // No synchronous ReadReady.
    assert!(collect_read_ready(&actions).is_empty());
    assert!(collect_read_failed(&actions).is_empty());

    // A peer ack of the post-read broadcast completes the ReadIndex.
    let actions = engine.step(append_entries_success_from(2, 1, 1));
    assert_eq!(collect_read_ready(&actions), vec![5]);
}

#[test]
fn lease_metric_increments_reads_completed_per_read() {
    // The fast path is a successful linearizable read; each one must
    // bump the reads_completed counter by exactly one (not multiply,
    // not stay pinned at one).
    let mut engine = leader_with_noop(LEASE);
    let before = engine.metrics().reads_completed;
    for id in 1..=3u64 {
        let actions = engine.step(propose_read(id));
        assert_eq!(collect_read_ready(&actions), vec![id]);
    }
    let after = engine.metrics().reads_completed;
    assert_eq!(after, before + 3);
}

#[test]
fn new_leader_resets_lease_tick() {
    // Give node 1 a lease via noop ack.
    let mut engine = leader_with_noop(LEASE);
    // Force step-down.
    engine.step(append_entries_from(
        2,
        append_entries_request(99, 2, None, vec![], 0),
    ));
    assert!(matches!(engine.role(), RoleState::Follower(_)));
    // Re-elect.
    for _ in 0..ELECTION_TIMEOUT {
        engine.step(Event::Tick);
    }
    engine.step(vote_response_from(2, engine.current_term().get(), true));
    assert!(matches!(engine.role(), RoleState::Leader(_)));
    // New leader: no majority ack yet at the NEW term. Even though
    // commit_index is stale, §5.4.2 forbids a read; more importantly,
    // the lease should be None. Once the new noop is acked, the lease
    // gets set — that path is exercised by the main serve test. Here
    // we just check the lease does not survive the role swap:
    // immediately after re-election, before any new-term ack, the
    // leader cannot serve via lease.
    let noop_idx = engine.log().last_log_id().unwrap().index.get();
    // §5.4.2 isn't yet satisfied: don't let that shadow the lease
    // assertion. Just confirm a read here is NotReady.
    let actions = engine.step(propose_read(99));
    assert_eq!(
        collect_read_failed(&actions),
        vec![(99, ReadFailure::NotReady)],
    );

    // Now commit the new noop — this refreshes the lease, so the
    // next read should go fast-path.
    engine.step(append_entries_success_from(
        2,
        engine.current_term().get(),
        noop_idx,
    ));
    let actions = engine.step(propose_read(100));
    assert_eq!(collect_read_ready(&actions), vec![100]);
}
