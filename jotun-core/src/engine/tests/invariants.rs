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
use crate::types::{index::LogIndex, log::LogId, term::Term};

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
