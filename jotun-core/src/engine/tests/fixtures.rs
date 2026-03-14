//! shared helpers for engine tests. keep them small and named after what
//! they produce so test bodies read like prose.

use crate::engine::action::Action;
use crate::engine::engine::Engine;
use crate::engine::event::Event;
use crate::engine::incoming::Incoming;
use crate::records::log_entry::LogEntry;
use crate::records::message::Message;
use crate::records::vote::{RequestVote, VoteResponse};
use crate::types::{index::LogIndex, log::LogId, node::NodeId, term::Term};

/// A fresh follower: term 0, empty log, no prior vote. Commands are `Vec<u8>`
/// so tests don't have to juggle a command type parameter.
pub fn follower(id: u64) -> Engine<Vec<u8>> {
    Engine::new(node(id))
}

pub fn node(id: u64) -> NodeId {
    NodeId::new(id).expect("test node ids must be non-zero")
}

pub fn term(n: u64) -> Term {
    Term::new(n)
}

pub fn log_id(index: u64, term_n: u64) -> LogId {
    LogId::new(LogIndex::new(index), term(term_n))
}

/// Wrap a RequestVote into the full Event the engine accepts.
pub fn vote_request_from(from: u64, request: RequestVote) -> Event<Vec<u8>> {
    Event::Incoming(Incoming {
        from: node(from),
        message: Message::VoteRequest(request),
    })
}

/// Convenience: build a plain RequestVote without the outer envelope.
pub fn vote_request(candidate: u64, term_n: u64, last_log: Option<LogId>) -> RequestVote {
    RequestVote {
        term: term(term_n),
        candidate_id: node(candidate),
        last_log_id: last_log,
    }
}

/// Populate the follower's log with dummy entries whose terms follow the
/// supplied sequence. Entry i (1-based) gets term `terms[i - 1]`. Callers
/// use this to set up a specific `(last_index, last_term)` without having
/// to write out every entry individually.
pub fn seed_log(engine: &mut Engine<Vec<u8>>, terms: &[u64]) {
    let log = &mut engine.state_mut().log;
    for (i, &t) in terms.iter().enumerate() {
        let index = LogIndex::new((i + 1) as u64);
        log.append(LogEntry {
            id: LogId::new(index, term(t)),
            command: Vec::new(),
        });
    }
}

/// Assert that the actions contain exactly one `Send` of a VoteResponse, and
/// return that response. Tests call this and then assert on the result.
pub fn expect_vote_response(actions: &[Action<Vec<u8>>]) -> VoteResponse {
    assert_eq!(actions.len(), 1, "expected exactly one action, got {actions:?}");
    match &actions[0] {
        Action::Send {
            message: Message::VoteResponse(response),
            ..
        } => *response,
        other => panic!("expected Send(VoteResponse), got {other:?}"),
    }
}
