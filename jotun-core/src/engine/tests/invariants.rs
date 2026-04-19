//! Property tests: throw adversarial but well-formed event sequences
//! at a single [`Engine`] and assert safety invariants hold.
//!
//! `Engine::check_invariants` is debug-only and runs inside every
//! `step()`; a violation panics there. These proptests drive enough
//! diverse inputs that an invariant regression would surface even on
//! paths the hand-written tests don't walk.

use proptest::prelude::*;

use super::fixtures::{
    append_entries_from, append_entries_request, append_entries_success_from,
    append_entries_conflict_from, client_proposal, follower_with_env, node,
    vote_request, vote_request_from, vote_response_from,
};
use crate::engine::env::{RandomizedEnv, StaticEnv};
use crate::engine::event::Event;
use crate::engine::role_state::RoleState;
use crate::records::log_entry::{LogEntry, LogPayload};
use crate::types::{index::LogIndex, log::LogId, node::NodeId, term::Term};

/// An event the proptest can ask the engine to process. Kept small so
/// shrinking reveals interesting minimal cases.
#[derive(Debug, Clone)]
enum Input {
    Tick,
    Propose,
    AppendEntriesFromPeer { peer: u64, term_n: u64, with_entry: bool },
    AppendEntriesSuccess { peer: u64, term_n: u64, last_appended: u64 },
    AppendEntriesConflict { peer: u64, term_n: u64, hint: u64 },
    VoteRequestFrom { peer: u64, term_n: u64 },
    VoteResponse { peer: u64, term_n: u64, granted: bool },
}

fn input_strategy() -> impl Strategy<Value = Input> {
    prop_oneof![
        1 => Just(Input::Tick),
        1 => Just(Input::Propose),
        2 => (2u64..=3, 0u64..=6, any::<bool>()).prop_map(|(peer, term_n, with_entry)| {
            Input::AppendEntriesFromPeer { peer, term_n, with_entry }
        }),
        2 => (2u64..=3, 0u64..=6, 0u64..=6).prop_map(|(peer, term_n, last_appended)| {
            Input::AppendEntriesSuccess { peer, term_n, last_appended }
        }),
        1 => (2u64..=3, 0u64..=6, 1u64..=6).prop_map(|(peer, term_n, hint)| {
            Input::AppendEntriesConflict { peer, term_n, hint }
        }),
        1 => (2u64..=3, 0u64..=6).prop_map(|(peer, term_n)| {
            Input::VoteRequestFrom { peer, term_n }
        }),
        1 => (2u64..=3, 0u64..=6, any::<bool>()).prop_map(|(peer, term_n, granted)| {
            Input::VoteResponse { peer, term_n, granted }
        }),
    ]
}

fn apply(engine: &mut crate::engine::engine::Engine<Vec<u8>>, input: &Input) {
    let event: Event<Vec<u8>> = match *input {
        Input::Tick => Event::Tick,
        Input::Propose => client_proposal(b"p"),
        Input::AppendEntriesFromPeer { peer, term_n, with_entry } => {
            let entries = if with_entry {
                vec![LogEntry {
                    id: LogId::new(LogIndex::new(1), Term::new(term_n.max(1))),
                    payload: LogPayload::Command(b"e".to_vec()),
                }]
            } else {
                Vec::new()
            };
            append_entries_from(peer, append_entries_request(term_n, peer, None, entries, 0))
        }
        Input::AppendEntriesSuccess { peer, term_n, last_appended } => {
            append_entries_success_from(peer, term_n, last_appended)
        }
        Input::AppendEntriesConflict { peer, term_n, hint } => {
            append_entries_conflict_from(peer, term_n, hint)
        }
        Input::VoteRequestFrom { peer, term_n } => {
            vote_request_from(peer, vote_request(peer, term_n, None))
        }
        Input::VoteResponse { peer, term_n, granted } => vote_response_from(peer, term_n, granted),
    };
    let _ = engine.step(event);
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        .. ProptestConfig::default()
    })]

    /// For any sequence of well-formed events, the engine's
    /// debug-only check_invariants (run after every step) must not
    /// fire, and the handful of externally-observable invariants
    /// §5.1 names must hold across the run.
    #[test]
    fn engine_invariants_hold_under_random_inputs(
        inputs in proptest::collection::vec(input_strategy(), 1..80),
    ) {
        let mut engine = follower_with_env(1, &[2, 3], Box::new(RandomizedEnv::new(42, 3, 6)));
        let mut prior_term = Term::ZERO;
        let mut prior_commit = LogIndex::ZERO;
        for input in &inputs {
            apply(&mut engine, input);

            // §5.1: current_term never decreases.
            let t = engine.current_term();
            prop_assert!(t >= prior_term, "term went backward: {prior_term:?} -> {t:?}");
            prior_term = t;

            // §5.3: commit_index never regresses.
            let c = engine.commit_index();
            prop_assert!(c >= prior_commit, "commit went backward: {prior_commit:?} -> {c:?}");
            prior_commit = c;

            // Role/term consistency: at any moment there's a single
            // role, and if we're Leader at term T we must have voted
            // for ourselves at T.
            if matches!(engine.role(), RoleState::Leader(_)) {
                prop_assert_eq!(
                    engine.voted_for(),
                    Some(node(1)),
                    "leader must have self-vote recorded for its term",
                );
            }
        }
    }

    /// A crashed + recovered engine's observable state matches what
    /// we fed into `recover_from`. Exercises the core recovery path
    /// the runtime and sim both rely on.
    #[test]
    fn recover_from_round_trips_hard_state(
        term_n in 0u64..100,
        voted_for_id in prop::option::of(1u64..=3u64),
    ) {
        use crate::engine::engine::{Engine, RecoveredHardState};

        let env = Box::new(StaticEnv(10));
        let mut engine: Engine<Vec<u8>> = Engine::new(node(1), vec![node(2), node(3)], env, 3);
        engine.recover_from(RecoveredHardState {
            current_term: Term::new(term_n),
            voted_for: voted_for_id.map(node),
            snapshot: None,
            post_snapshot_log: Vec::new(),
        });

        prop_assert_eq!(engine.current_term(), Term::new(term_n));
        prop_assert_eq!(engine.voted_for(), voted_for_id.map(node));
        prop_assert!(matches!(engine.role(), RoleState::Follower(_)));
    }
}

// ===========================================================================
// Aggressive proptests (1024 cases) — pushed at the single-engine interface
// to surface rarer invariant violations the hand-written tests might miss.
// ===========================================================================

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 1024,
        .. ProptestConfig::default()
    })]

    /// Drives a single engine with an adversarial batch of Incoming RPCs
    /// from random peers at random terms and asserts §5.1/§5.3 monotonicity:
    ///   - `current_term` never regresses,
    ///   - `commit_index` never regresses.
    /// Larger (1024-case) companion to the existing 256-case smoke proptest.
    #[test]
    fn commit_index_monotonic_across_arbitrary_incoming_rpcs(
        inputs in proptest::collection::vec(input_strategy(), 1..120),
    ) {
        let mut engine = follower_with_env(1, &[2, 3], Box::new(RandomizedEnv::new(1337, 4, 9)));
        let mut prior_term = Term::ZERO;
        let mut prior_commit = LogIndex::ZERO;
        for input in &inputs {
            apply(&mut engine, input);
            let t = engine.current_term();
            prop_assert!(t >= prior_term, "term regressed: {prior_term:?} -> {t:?}");
            prior_term = t;
            let c = engine.commit_index();
            prop_assert!(c >= prior_commit, "commit regressed: {prior_commit:?} -> {c:?}");
            prior_commit = c;
        }
    }

    /// §5.4.1: a node grants at most one vote per term. Fuzz two peers'
    /// RequestVote interleavings and check that if the engine emits a
    /// `Granted` VoteResponse, all later `Granted` responses in the same
    /// term must be for the same candidate (i.e. `voted_for` is stable
    /// within the term).
    #[test]
    fn vote_grant_is_term_unique(
        events in proptest::collection::vec(
            (2u64..=3u64, 1u64..=8u64, any::<bool>()),
            1..40,
        ),
    ) {
        use crate::records::vote::VoteResult;
        use crate::engine::action::Action;
        use crate::records::message::Message;

        let mut engine = follower_with_env(1, &[2, 3], Box::new(StaticEnv(50)));

        // Track, per term, whoever we've already granted our vote to.
        let mut granted_for_term: std::collections::BTreeMap<Term, NodeId> =
            std::collections::BTreeMap::new();

        for (candidate, term_n, with_log) in events {
            let last_log = if with_log {
                Some(LogId::new(LogIndex::new(1), Term::new(term_n)))
            } else {
                None
            };
            let req = vote_request(candidate, term_n, last_log);
            let event = vote_request_from(candidate, req);
            let actions = engine.step(event);

            // Find any VoteResponse we sent out.
            for action in &actions {
                if let Action::Send { message: Message::VoteResponse(resp), .. } = action
                    && matches!(resp.result, VoteResult::Granted)
                {
                    // Must equal the vote we recorded (voted_for is
                    // persisted hard state).
                    let vf = engine.voted_for();
                    prop_assert!(vf.is_some(), "granted without voted_for set");
                    let vf_id = vf.expect("checked above");

                    // For that response's term, either we have no prior
                    // record, or we matched it exactly.
                    let entry = granted_for_term.entry(resp.term);
                    match entry {
                        std::collections::btree_map::Entry::Vacant(v) => {
                            v.insert(vf_id);
                        }
                        std::collections::btree_map::Entry::Occupied(o) => {
                            prop_assert_eq!(
                                *o.get(), vf_id,
                                "granted two different candidates in term {:?}", resp.term,
                            );
                        }
                    }
                }
            }
        }
    }

    /// §5.3 Log Matching on a single node: the engine's own log never
    /// ends up with a term that goes backward. After any sequence of
    /// events the engine accepted, in-memory entries' terms are
    /// non-decreasing with index. (The `Log::check_invariants` debug
    /// hook enforces this per-step; this is a belt-and-suspenders
    /// property drive that forces many diverse tails.)
    #[test]
    fn log_matching_holds_on_single_engine(
        inputs in proptest::collection::vec(input_strategy(), 1..100),
    ) {
        let mut engine = follower_with_env(1, &[2, 3], Box::new(StaticEnv(20)));
        for input in &inputs {
            apply(&mut engine, input);
            // Walk in-memory entries and confirm (index, term) is
            // contiguous and term-non-decreasing.
            let log = engine.log();
            let first = log.first_index();
            let mut prev_term: Option<Term> = None;
            for offset in 0..log.len() {
                let i = LogIndex::new(first.get() + offset as u64);
                let Some(entry) = log.entry_at(i) else {
                    continue;
                };
                prop_assert_eq!(entry.id.index, i, "log index non-contiguous");
                if let Some(pt) = prev_term {
                    prop_assert!(
                        entry.id.term >= pt,
                        "term regressed across log: {pt:?} -> {:?}", entry.id.term,
                    );
                }
                prev_term = Some(entry.id.term);
            }
        }
    }

    /// `recover_from` is pure and idempotent: calling it twice with the
    /// same payload leaves the engine in the same observable state as
    /// calling it once. Pins that there's no hidden cursor/side-effect
    /// that a second call would disturb.
    #[test]
    fn recover_from_is_pure_idempotent(
        term_n in 0u64..200,
        voted_for_id in prop::option::of(1u64..=3u64),
    ) {
        use crate::engine::engine::{Engine, RecoveredHardState};

        let build = || -> RecoveredHardState<Vec<u8>> {
            RecoveredHardState {
                current_term: Term::new(term_n),
                voted_for: voted_for_id.map(node),
                snapshot: None,
                post_snapshot_log: Vec::new(),
            }
        };

        let mut once: Engine<Vec<u8>> = Engine::new(
            node(1), vec![node(2), node(3)], Box::new(StaticEnv(10)), 3,
        );
        once.recover_from(build());

        let mut twice: Engine<Vec<u8>> = Engine::new(
            node(1), vec![node(2), node(3)], Box::new(StaticEnv(10)), 3,
        );
        twice.recover_from(build());
        twice.recover_from(build());

        prop_assert_eq!(once.current_term(), twice.current_term());
        prop_assert_eq!(once.voted_for(), twice.voted_for());
        prop_assert_eq!(once.commit_index(), twice.commit_index());
        prop_assert_eq!(once.log().len(), twice.log().len());
        prop_assert_eq!(once.log().last_log_id(), twice.log().last_log_id());
        prop_assert_eq!(once.peers(), twice.peers());
    }
}
