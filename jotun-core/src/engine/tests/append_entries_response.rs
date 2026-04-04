//! Tests for [`Engine::on_append_entries_response`] — the leader's
//! handling of replies from peers. Covers the term checks (§5.1), the
//! Success path (matchIndex advance, majority commit, §5.4.2 same-term
//! safety, Apply emission), and the Conflict path (nextIndex rewind +
//! immediate retargeted re-send).

use super::fixtures::{
    append_entries_conflict_from, append_entries_success_from, collect_append_entries,
    collect_apply, follower, follower_with_env, log_id, node, seed_log, term,
    vote_response_from,
};
use crate::engine::engine::Engine;
use crate::engine::env::StaticEnv;
use crate::engine::event::Event;
use crate::engine::role_state::RoleState;
use crate::records::log_entry::LogPayload;
use crate::types::index::LogIndex;

/// Drive a fresh follower → leader of a 3-node cluster (self=1, peers=2,3).
/// After return: term=1, log=[(1,1)=Noop], `commit=0`, `last_applied=0`.
fn elected_leader_3_node() -> Engine<Vec<u8>> {
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    engine.step(Event::Tick); // → Candidate, term 1
    engine.step(vote_response_from(2, 1, true)); // peer 2 grants → Leader
    assert!(matches!(engine.role(), RoleState::Leader(_)));
    engine
}

// ---------------------------------------------------------------------------
// Term checks (§5.1)
// ---------------------------------------------------------------------------

#[test]
fn higher_term_response_demotes_leader_to_follower() {
    let mut engine = elected_leader_3_node();
    let term_before = engine.current_term();
    let actions = engine.step(append_entries_success_from(2, term_before.next().get(), 0));

    assert!(matches!(engine.role(), RoleState::Follower(_)));
    assert_eq!(engine.current_term(), term_before.next());
    assert!(actions.is_empty(), "step-down emits no actions");
}

#[test]
fn stale_term_response_is_ignored() {
    let mut engine = elected_leader_3_node();
    let term_before = engine.current_term();
    let actions = engine.step(append_entries_success_from(2, 0, 1));

    assert!(matches!(engine.role(), RoleState::Leader(_)));
    assert_eq!(engine.current_term(), term_before);
    assert!(actions.is_empty(), "stale response emits no actions");
}

#[test]
fn response_to_non_leader_is_ignored() {
    // A follower that somehow receives an AppendEntriesResponse (host bug,
    // crossed wires) must not blow up. Just drop the message.
    let mut engine = follower(1);
    let actions = engine.step(append_entries_success_from(2, 0, 0));
    assert!(matches!(engine.role(), RoleState::Follower(_)));
    assert!(actions.is_empty());
}

// ---------------------------------------------------------------------------
// Success path — matchIndex/nextIndex bookkeeping
// ---------------------------------------------------------------------------

#[test]
fn success_advances_match_and_next_for_the_responding_peer() {
    let mut engine = elected_leader_3_node();
    engine.step(append_entries_success_from(2, 1, 1));

    let RoleState::Leader(s) = engine.role() else {
        panic!("expected Leader");
    };
    assert_eq!(s.progress.match_for(node(2)), Some(LogIndex::new(1)));
    assert_eq!(s.progress.next_for(node(2)), Some(LogIndex::new(2)));
    // Peer 3 untouched.
    assert_eq!(s.progress.match_for(node(3)), Some(LogIndex::ZERO));
}

#[test]
fn success_with_no_entries_treats_last_appended_as_zero() {
    // Wire `None` → record_success(peer, 0). matchIndex stays at 0; no commit
    // advancement is possible.
    let mut engine = elected_leader_3_node();
    let actions = engine.step(append_entries_success_from(2, 1, 0));

    let RoleState::Leader(s) = engine.role() else {
        panic!("expected Leader");
    };
    assert_eq!(s.progress.match_for(node(2)), Some(LogIndex::ZERO));
    assert!(collect_apply(&actions).is_empty());
}

#[test]
fn stale_success_does_not_rewind_match_index() {
    // Peer ack'd index 1 first (the noop), then a stale duplicate of an
    // earlier (impossibly low) ack arrives. matchIndex must not regress.
    let mut engine = elected_leader_3_node();
    engine.step(append_entries_success_from(2, 1, 1));
    engine.step(append_entries_success_from(2, 1, 0)); // stale "I have nothing"

    let RoleState::Leader(s) = engine.role() else {
        panic!("expected Leader");
    };
    assert_eq!(
        s.progress.match_for(node(2)),
        Some(LogIndex::new(1)),
        "matchIndex must never rewind (§5.3)",
    );
}

// ---------------------------------------------------------------------------
// Success path — commit advancement and Apply emission
// ---------------------------------------------------------------------------

#[test]
fn current_term_entry_commits_on_majority_and_emits_apply() {
    // 3-node cluster, leader=1. After election the log holds a single noop
    // at (1, 1). Leader's matchIndex for itself is implicitly 1 (its own
    // log). One peer ack at index 1 makes matchIndex sorted = [0, 1, 1];
    // median = 1 → majority_index = 1. Entry 1 is from current_term, so
    // commit advances 0 → 1 and the noop is applied.
    let mut engine = elected_leader_3_node();
    let actions = engine.step(append_entries_success_from(2, 1, 1));

    assert_eq!(engine.commit_index(), LogIndex::new(1));
    let applied = collect_apply(&actions);
    assert_eq!(applied.len(), 1, "exactly one Apply per commit advance");
    assert_eq!(applied[0].len(), 1);
    assert_eq!(applied[0][0].id, log_id(1, 1));
    assert!(matches!(applied[0][0].payload, LogPayload::Noop));
}

#[test]
fn duplicate_success_at_same_index_does_not_re_apply() {
    // After the commit + apply above, a second identical Success must not
    // emit another Apply — last_applied was bumped.
    let mut engine = elected_leader_3_node();
    engine.step(append_entries_success_from(2, 1, 1));
    let actions = engine.step(append_entries_success_from(2, 1, 1));
    assert!(collect_apply(&actions).is_empty());
}

#[test]
fn no_majority_yet_no_commit_no_apply() {
    // 5-node cluster. One ack at index 1 isn't enough — need 3. matchIndex
    // sorted = [0, 0, 0, 1, 1] (peers + self), median = 0. No commit move.
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3, 4, 5], Box::new(env));
    engine.step(Event::Tick);
    engine.step(vote_response_from(2, 1, true));
    engine.step(vote_response_from(3, 1, true)); // 3 grants → leader of 5
    assert!(matches!(engine.role(), RoleState::Leader(_)));
    let term_before_commit = engine.commit_index();

    let actions = engine.step(append_entries_success_from(2, 1, 1));

    assert_eq!(engine.commit_index(), term_before_commit);
    assert!(collect_apply(&actions).is_empty());
}

#[test]
fn prior_term_entry_does_not_commit_via_majority_alone_until_current_term_entry_does() {
    // §5.4.2 / Figure 8 in microcosm.
    //
    // Setup: leader (id=1) is elected at term 5. Pre-election it already
    // had two entries (1, 2) and (2, 2) sitting around uncommitted from a
    // prior leadership. become_leader appends a noop at (3, 5).
    //
    // A peer acks all the way up to index 2 (the prior-term entry).
    // matchIndex sorted = [0, 2, 2] (peer 3, peer 2, self for index-2 view)
    // — but self's last_log is 3, so the actual sort is [0, 2, 3] →
    // median = 2. That index's term is 2, NOT the current term 5, so
    // commit must NOT advance (§5.4.2).
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    seed_log(&mut engine, &[2, 2]); // entries (1,2), (2,2)
    // Force term to 4 so the next election yields term 5.
    engine.state_mut().current_term = term(4);
    engine.step(Event::Tick); // → Candidate at term 5
    engine.step(vote_response_from(2, 5, true)); // → Leader
    assert!(matches!(engine.role(), RoleState::Leader(_)));
    assert_eq!(engine.current_term(), term(5));
    // Log is now (1,2)(2,2)(3,5=noop).

    // Peer 2 acks up to index 2 — the prior-term entry.
    let actions = engine.step(append_entries_success_from(2, 5, 2));
    assert_eq!(
        engine.commit_index(),
        LogIndex::ZERO,
        "majority replication of a prior-term entry must NOT commit alone (§5.4.2)",
    );
    assert!(collect_apply(&actions).is_empty());

    // Now peer 2 acks up to index 3 — the current-term noop. majority_index
    // = 3 (matchIndex sorted = [0, 3, 3]); entry 3 is from current term, so
    // commit jumps 0 → 3 in one shot, sweeping the prior-term entries with it.
    let actions = engine.step(append_entries_success_from(2, 5, 3));
    assert_eq!(engine.commit_index(), LogIndex::new(3));
    let applied = collect_apply(&actions);
    assert_eq!(applied.len(), 1);
    assert_eq!(
        applied[0].len(),
        3,
        "all three entries apply at once; prior-term ones piggyback on the current-term commit",
    );
    assert_eq!(applied[0][0].id, log_id(1, 2));
    assert_eq!(applied[0][1].id, log_id(2, 2));
    assert_eq!(applied[0][2].id, log_id(3, 5));
}

// ---------------------------------------------------------------------------
// Conflict path — rewind nextIndex and re-send to that peer alone
// ---------------------------------------------------------------------------

#[test]
fn conflict_rewinds_next_index_and_retargets_only_the_responding_peer() {
    // Set up a leader with a non-trivial log so a Conflict has somewhere to
    // rewind to. seed (1,1) (2,1) (3,1), elect at term 2 → noop at (4,2).
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    seed_log(&mut engine, &[1, 1, 1]);
    engine.step(Event::Tick);
    engine.step(vote_response_from(2, 1, true));
    assert!(matches!(engine.role(), RoleState::Leader(_)));
    // After election: log = (1,1)(2,1)(3,1)(4,1=noop). nextIndex = 5 for all.

    let actions = engine.step(append_entries_conflict_from(2, 1, 2));

    // Peer 2's nextIndex must be rewound to the hint.
    let RoleState::Leader(s) = engine.role() else {
        panic!("expected Leader");
    };
    assert_eq!(s.progress.next_for(node(2)), Some(LogIndex::new(2)));
    // Peer 3 untouched.
    assert_eq!(s.progress.next_for(node(3)), Some(LogIndex::new(5)));

    // Exactly one re-send, addressed to peer 2 only.
    let appends = collect_append_entries(&actions);
    assert_eq!(appends.len(), 1);
    assert_eq!(appends[0].0, node(2));

    // The retargeted request must use the *real* log to derive prev_log_id,
    // not a synthesized one. nextIndex=2 → prev_log_index=1 → prev=(1,1).
    let req = &appends[0].1;
    assert_eq!(req.prev_log_id, Some(log_id(1, 1)));
    // Entries 2..=4 should be carried.
    assert_eq!(req.entries.len(), 3);
    assert_eq!(req.entries[0].id, log_id(2, 1));
    assert_eq!(req.entries[2].id, log_id(4, 1));
}

#[test]
fn conflict_with_hint_one_yields_prev_log_id_none() {
    // Peer is empty — hint=1 means "start from the beginning". The
    // re-send must carry prev_log_id = None (the pre-log sentinel),
    // not a fabricated LogId.
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    seed_log(&mut engine, &[1, 1]);
    engine.step(Event::Tick);
    engine.step(vote_response_from(2, 1, true));

    let actions = engine.step(append_entries_conflict_from(2, 1, 1));
    let appends = collect_append_entries(&actions);
    assert_eq!(appends.len(), 1);
    assert_eq!(appends[0].0, node(2));
    assert_eq!(
        appends[0].1.prev_log_id, None,
        "hint=1 → prev_log_index=0 → prev_log_id = None sentinel",
    );
}

#[test]
fn conflict_does_not_advance_commit_or_emit_apply() {
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    engine.step(Event::Tick);
    engine.step(vote_response_from(2, 1, true));
    let commit_before = engine.commit_index();

    let actions = engine.step(append_entries_conflict_from(2, 1, 1));

    assert_eq!(engine.commit_index(), commit_before);
    assert!(collect_apply(&actions).is_empty());
}

#[test]
fn conflict_hint_past_leader_log_is_clamped() {
    // Leader has log = [(1, 1)=Noop] (just elected, empty pre-election).
    // Peer returns a hint of 99 — bogus or buggy. The leader must clamp
    // nextIndex to leader_last+1 = 2; otherwise the next AppendEntries
    // synthesizes prev_log_id from log entries that don't exist.
    let mut engine = elected_leader_3_node();
    let leader_last_next = engine
        .log()
        .last_log_id()
        .map_or(LogIndex::new(1), |l| l.index.next());

    engine.step(append_entries_conflict_from(2, 1, 99));

    let RoleState::Leader(s) = engine.role() else {
        panic!("expected Leader");
    };
    assert_eq!(
        s.progress().next_for(node(2)),
        Some(leader_last_next),
        "nextIndex must be clamped to leader_last + 1, never past our own log",
    );
}

#[test]
fn success_with_last_appended_past_leader_log_is_ignored() {
    // Peer claims it has up to index 99. Our log only goes to index 1.
    // A correct follower never acks past what we sent, so this is a
    // buggy/malicious response — must not poison matchIndex with a
    // value we can't even look up in our own log (would let
    // commit_index advance to phantom entries).
    let mut engine = elected_leader_3_node();
    let leader_last = engine.log().last_log_id().unwrap().index;

    engine.step(append_entries_success_from(2, 1, 99));

    let RoleState::Leader(s) = engine.role() else {
        panic!("expected Leader");
    };
    assert_eq!(
        s.progress().match_for(node(2)),
        Some(LogIndex::ZERO),
        "matchIndex must not be set past leader_last={}",
        leader_last.get(),
    );
}

// ---------------------------------------------------------------------------
// Invariants (property tests)
// ---------------------------------------------------------------------------

use proptest::prelude::*;

/// One step in a session: a Success or Conflict response from a peer.
#[derive(Debug, Clone)]
enum Reply {
    Success { peer: u64, last_appended: u64 },
    Conflict { peer: u64, hint: u64 },
}

fn reply_strategy() -> impl Strategy<Value = Reply> {
    prop_oneof![
        (1u64..=3, 0u64..=10).prop_map(|(peer, last)| Reply::Success {
            peer,
            last_appended: last,
        }),
        (1u64..=3, 1u64..=10).prop_map(|(peer, hint)| Reply::Conflict { peer, hint }),
    ]
}

proptest! {
    /// commit_index is monotonically non-decreasing across any sequence of
    /// responses, regardless of peer order, payload, or interleaving with
    /// stale/conflicting messages.
    #[test]
    fn commit_index_never_regresses(
        replies in proptest::collection::vec(reply_strategy(), 0..30),
    ) {
        let mut engine = elected_leader_3_node();
        let mut last_commit = engine.commit_index();
        for r in replies {
            match r {
                Reply::Success { peer, last_appended } => {
                    engine.step(append_entries_success_from(
                        peer,
                        engine.current_term().get(),
                        last_appended,
                    ));
                }
                Reply::Conflict { peer, hint } => {
                    engine.step(append_entries_conflict_from(
                        peer,
                        engine.current_term().get(),
                        hint,
                    ));
                }
            }
            let now = engine.commit_index();
            prop_assert!(
                now >= last_commit,
                "commit_index regressed: {last_commit:?} -> {now:?}",
            );
            last_commit = now;
        }
    }

    /// `last_applied <= commit_index` is the Figure 2 invariant. The
    /// debug_assert in check_invariants enforces this every step, but
    /// asserting it explicitly here makes the property visible and
    /// gives mutation testing a target.
    #[test]
    fn last_applied_never_exceeds_commit_index(
        replies in proptest::collection::vec(reply_strategy(), 0..30),
    ) {
        let mut engine = elected_leader_3_node();
        for r in replies {
            match r {
                Reply::Success { peer, last_appended } => {
                    engine.step(append_entries_success_from(
                        peer,
                        engine.current_term().get(),
                        last_appended,
                    ));
                }
                Reply::Conflict { peer, hint } => {
                    engine.step(append_entries_conflict_from(
                        peer,
                        engine.current_term().get(),
                        hint,
                    ));
                }
            }
            let commit = engine.commit_index();
            let last_applied = engine.state_mut().last_applied;
            prop_assert!(last_applied <= commit);
        }
    }

    /// Apply emits each log index at most once across a session — the
    /// host must never see the same entry twice. This is the property
    /// that proves last_applied is honored.
    #[test]
    fn no_log_index_is_applied_more_than_once(
        replies in proptest::collection::vec(reply_strategy(), 0..30),
    ) {
        let mut engine = elected_leader_3_node();
        let mut seen: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
        for r in replies {
            let actions = match r {
                Reply::Success { peer, last_appended } => engine.step(
                    append_entries_success_from(peer, engine.current_term().get(), last_appended),
                ),
                Reply::Conflict { peer, hint } => engine.step(append_entries_conflict_from(
                    peer,
                    engine.current_term().get(),
                    hint,
                )),
            };
            for batch in collect_apply(&actions) {
                for entry in batch {
                    let idx = entry.id.index.get();
                    prop_assert!(seen.insert(idx), "index {idx} applied twice");
                }
            }
        }
    }

    /// §5.4.2 in property form: every commit advance moves commit_index
    /// to an index whose entry term equals current_term. (Prior-term
    /// entries can become committed, but only as a side-effect of a
    /// current-term commit sweeping them — so the *advancement target*
    /// itself is always a current-term entry.)
    #[test]
    fn commit_advances_only_to_current_term_entries(
        replies in proptest::collection::vec(reply_strategy(), 0..30),
    ) {
        let mut engine = elected_leader_3_node();
        let mut prev_commit = engine.commit_index();
        for r in replies {
            match r {
                Reply::Success { peer, last_appended } => {
                    engine.step(append_entries_success_from(
                        peer,
                        engine.current_term().get(),
                        last_appended,
                    ));
                }
                Reply::Conflict { peer, hint } => {
                    engine.step(append_entries_conflict_from(
                        peer,
                        engine.current_term().get(),
                        hint,
                    ));
                }
            }
            let now = engine.commit_index();
            if now > prev_commit {
                let term_at_target = engine.log().term_at(now);
                prop_assert_eq!(
                    term_at_target,
                    Some(engine.current_term()),
                    "commit advanced to index {} whose term is not current_term", now.get(),
                );
            }
            prev_commit = now;
        }
    }
}

