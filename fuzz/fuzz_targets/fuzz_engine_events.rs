#![no_main]
//! Fuzz the engine itself: decode raw bytes into a structured sequence
//! of `Event<Vec<u8>>` via `arbitrary`, feed them to a fresh `Engine`,
//! and assert core §5.1/§5.3 monotonicity invariants hold.
//!
//! Only a subset of events is emitted — enough to surface crash bugs
//! without pulling in wire-format concerns (those have their own
//! fuzzer).

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;

use jotun_core::{
    engine::event::Event, Engine, Incoming, LogEntry, LogId, LogIndex, LogPayload, Message, NodeId,
    RandomizedEnv, RequestAppendEntries, RequestVote, Term, VoteResponse, VoteResult,
};
use jotun_core::records::append_entries::{AppendEntriesResponse, AppendEntriesResult};

#[derive(Debug, Arbitrary)]
enum ArbitraryEvent {
    Tick,
    Propose(Vec<u8>),
    VoteRequest { peer: u8, term: u8, with_log: bool },
    VoteResponse { peer: u8, term: u8, granted: bool },
    AppendEntries { peer: u8, term: u8, with_entry: bool, commit: u8 },
    AppendSuccess { peer: u8, term: u8, last: u8 },
    AppendConflict { peer: u8, term: u8, hint: u8 },
}

fn node(n: u64) -> NodeId {
    NodeId::new(n.max(1)).expect("n.max(1) >= 1")
}

fn build_event(ae: ArbitraryEvent) -> Event<Vec<u8>> {
    // Route peer ids into {2, 3} so they are valid cluster members.
    let peer_id = match ae {
        ArbitraryEvent::Tick | ArbitraryEvent::Propose(_) => 2,
        ArbitraryEvent::VoteRequest { peer, .. }
        | ArbitraryEvent::VoteResponse { peer, .. }
        | ArbitraryEvent::AppendEntries { peer, .. }
        | ArbitraryEvent::AppendSuccess { peer, .. }
        | ArbitraryEvent::AppendConflict { peer, .. } => 2 + (peer as u64 % 2),
    };
    let from = node(peer_id);

    match ae {
        ArbitraryEvent::Tick => Event::Tick,
        ArbitraryEvent::Propose(v) => Event::ClientProposal(v),
        ArbitraryEvent::VoteRequest { term, with_log, .. } => {
            let last_log = if with_log {
                Some(LogId::new(LogIndex::new(1), Term::new(u64::from(term).max(1))))
            } else {
                None
            };
            Event::Incoming(Incoming {
                from,
                message: Message::VoteRequest(RequestVote {
                    term: Term::new(u64::from(term)),
                    candidate_id: from,
                    last_log_id: last_log,
                }),
            })
        }
        ArbitraryEvent::VoteResponse { term, granted, .. } => Event::Incoming(Incoming {
            from,
            message: Message::VoteResponse(VoteResponse {
                term: Term::new(u64::from(term)),
                result: if granted {
                    VoteResult::Granted
                } else {
                    VoteResult::Rejected
                },
            }),
        }),
        ArbitraryEvent::AppendEntries { term, with_entry, commit, .. } => {
            let t = u64::from(term);
            let entries = if with_entry {
                vec![LogEntry {
                    id: LogId::new(LogIndex::new(1), Term::new(t.max(1))),
                    payload: LogPayload::Command(b"e".to_vec()),
                }]
            } else {
                Vec::new()
            };
            Event::Incoming(Incoming {
                from,
                message: Message::AppendEntriesRequest(RequestAppendEntries {
                    term: Term::new(t),
                    leader_id: from,
                    prev_log_id: None,
                    entries,
                    leader_commit: LogIndex::new(u64::from(commit)),
                }),
            })
        }
        ArbitraryEvent::AppendSuccess { term, last, .. } => Event::Incoming(Incoming {
            from,
            message: Message::AppendEntriesResponse(AppendEntriesResponse {
                term: Term::new(u64::from(term)),
                result: AppendEntriesResult::Success {
                    last_appended: (last > 0).then(|| LogIndex::new(u64::from(last))),
                },
            }),
        }),
        ArbitraryEvent::AppendConflict { term, hint, .. } => Event::Incoming(Incoming {
            from,
            message: Message::AppendEntriesResponse(AppendEntriesResponse {
                term: Term::new(u64::from(term)),
                result: AppendEntriesResult::Conflict {
                    next_index_hint: LogIndex::new(u64::from(hint).max(1)),
                },
            }),
        }),
    }
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let events: Vec<ArbitraryEvent> = match Vec::arbitrary(&mut u) {
        Ok(v) => v,
        Err(_) => return,
    };

    let mut engine: Engine<Vec<u8>> = Engine::new(
        node(1),
        vec![node(2), node(3)],
        Box::new(RandomizedEnv::new(42, 5, 12)),
        3,
    );

    let mut prior_term = Term::ZERO;
    let mut prior_commit = LogIndex::ZERO;
    for ae in events {
        let event = build_event(ae);
        let _ = engine.step(event);

        let t = engine.current_term();
        assert!(t >= prior_term, "term regressed: {prior_term:?} -> {t:?}");
        prior_term = t;

        let c = engine.commit_index();
        assert!(c >= prior_commit, "commit regressed: {prior_commit:?} -> {c:?}");
        prior_commit = c;
    }
});
