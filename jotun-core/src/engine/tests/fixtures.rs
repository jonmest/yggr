//! shared helpers for engine tests. keep them small and named after what
//! they produce so test bodies read like prose.

use crate::engine::action::Action;
use crate::engine::engine::Engine;
use crate::engine::env::{Env, StaticEnv};
use crate::engine::event::Event;
use crate::engine::incoming::Incoming;
use crate::records::append_entries::{
    AppendEntriesResponse, AppendEntriesResult, RequestAppendEntries,
};
use crate::records::log_entry::{LogEntry, LogPayload};
use crate::records::message::Message;
use crate::records::vote::{RequestVote, VoteResponse};
use crate::types::{index::LogIndex, log::LogId, node::NodeId, term::Term};

/// Default election timeout (ticks) used by test fixtures. Chosen generous
/// enough to make off-by-one errors in timer logic obvious.
pub(super) const DEFAULT_ELECTION_TIMEOUT: u64 = 10;

/// Default heartbeat interval (ticks). Smaller than the election timeout
/// per §5.2.
pub(super) const DEFAULT_HEARTBEAT_INTERVAL: u64 = 1;

/// A fresh follower with no peers: term 0, empty log, no prior vote.
/// Suitable for receive-side tests that don't need peer broadcast.
/// Commands are `Vec<u8>` so tests don't have to juggle a command type.
pub(super) fn follower(id: u64) -> Engine<Vec<u8>> {
    Engine::new(
        node(id),
        std::iter::empty(),
        Box::new(StaticEnv(DEFAULT_ELECTION_TIMEOUT)),
        DEFAULT_HEARTBEAT_INTERVAL,
    )
}

/// A follower that uses a caller-supplied `Env`. For tests that need to
/// script a specific sequence of election timeouts.
pub(super) fn follower_with_env(id: u64, peers: &[u64], env: Box<dyn Env>) -> Engine<Vec<u8>> {
    Engine::new(
        node(id),
        peers.iter().map(|&p| node(p)),
        env,
        DEFAULT_HEARTBEAT_INTERVAL,
    )
}

pub(super) fn node(id: u64) -> NodeId {
    NodeId::new(id).expect("test node ids must be non-zero")
}

pub(super) fn term(n: u64) -> Term {
    Term::new(n)
}

pub(super) fn log_id(index: u64, term_n: u64) -> LogId {
    LogId::new(LogIndex::new(index), term(term_n))
}

/// Wrap a `RequestVote` into the full Event the engine accepts.
pub(super) fn vote_request_from(from: u64, request: RequestVote) -> Event<Vec<u8>> {
    Event::Incoming(Incoming {
        from: node(from),
        message: Message::VoteRequest(request),
    })
}

/// Convenience: build a plain `RequestVote` without the outer envelope.
pub(super) fn vote_request(candidate: u64, term_n: u64, last_log: Option<LogId>) -> RequestVote {
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
pub(super) fn seed_log(engine: &mut Engine<Vec<u8>>, terms: &[u64]) {
    let log = &mut engine.state_mut().log;
    for (i, &t) in terms.iter().enumerate() {
        let index = LogIndex::new((i + 1) as u64);
        log.append(LogEntry {
            id: LogId::new(index, term(t)),
            payload: LogPayload::Command(Vec::new()),
        });
    }
}

/// Build a log entry with an empty command payload.
pub(super) fn log_entry(index: u64, term_n: u64) -> LogEntry<Vec<u8>> {
    LogEntry {
        id: log_id(index, term_n),
        payload: LogPayload::Command(Vec::new()),
    }
}

/// Build a vector of log entries from `[(index, term)]` pairs.
pub(super) fn log_entries(spec: &[(u64, u64)]) -> Vec<LogEntry<Vec<u8>>> {
    spec.iter().map(|&(i, t)| log_entry(i, t)).collect()
}

/// Build a `RequestAppendEntries`.
pub(super) fn append_entries_request(
    term_n: u64,
    leader: u64,
    prev_log: Option<LogId>,
    entries: Vec<LogEntry<Vec<u8>>>,
    leader_commit: u64,
) -> RequestAppendEntries<Vec<u8>> {
    RequestAppendEntries {
        term: term(term_n),
        leader_id: node(leader),
        prev_log_id: prev_log,
        entries,
        leader_commit: LogIndex::new(leader_commit),
    }
}

/// Wrap a `RequestAppendEntries` into the Event envelope the engine accepts.
pub(super) fn append_entries_from(
    from: u64,
    request: RequestAppendEntries<Vec<u8>>,
) -> Event<Vec<u8>> {
    Event::Incoming(Incoming {
        from: node(from),
        message: Message::AppendEntriesRequest(request),
    })
}

/// Find the single `Send(AppendEntriesResponse)` in the actions.
/// Other actions (e.g. `Action::Apply` from a commit advance) are ignored;
/// tests that care about Apply should pull it via [`collect_apply`].
pub(super) fn expect_append_entries_response(actions: &[Action<Vec<u8>>]) -> AppendEntriesResponse {
    let mut found = actions.iter().filter_map(|a| match a {
        Action::Send {
            message: Message::AppendEntriesResponse(r),
            ..
        } => Some(*r),
        _ => None,
    });
    let response = found
        .next()
        .unwrap_or_else(|| panic!("expected one Send(AppendEntriesResponse), got {actions:?}"));
    assert!(
        found.next().is_none(),
        "expected exactly one AppendEntriesResponse, got more in {actions:?}",
    );
    response
}

/// Assert that the actions contain exactly one `Send` of a `VoteResponse`, and
/// return that response. Tests call this and then assert on the result.
pub(super) fn expect_vote_response(actions: &[Action<Vec<u8>>]) -> VoteResponse {
    assert_eq!(
        actions.len(),
        1,
        "expected exactly one action, got {actions:?}"
    );
    match &actions[0] {
        Action::Send {
            message: Message::VoteResponse(response),
            ..
        } => *response,
        other => panic!("expected Send(VoteResponse), got {other:?}"),
    }
}

/// Extract every `Send(AppendEntriesRequest)` action, paired with destination.
pub(super) fn collect_append_entries(
    actions: &[Action<Vec<u8>>],
) -> Vec<(NodeId, RequestAppendEntries<Vec<u8>>)> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Send {
                to,
                message: Message::AppendEntriesRequest(req),
            } => Some((*to, req.clone())),
            _ => None,
        })
        .collect()
}

/// Wrap a peer's `AppendEntriesResponse::Success` into an Event.
/// `last_appended_idx = 0` encodes "no entries" (the wire `None`).
pub(super) fn append_entries_success_from(
    from: u64,
    term_n: u64,
    last_appended_idx: u64,
) -> Event<Vec<u8>> {
    Event::Incoming(Incoming {
        from: node(from),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: term(term_n),
            result: AppendEntriesResult::Success {
                last_appended: (last_appended_idx > 0).then(|| LogIndex::new(last_appended_idx)),
            },
        }),
    })
}

/// Wrap a peer's `AppendEntriesResponse::Conflict` into an Event.
pub(super) fn append_entries_conflict_from(
    from: u64,
    term_n: u64,
    next_index_hint: u64,
) -> Event<Vec<u8>> {
    Event::Incoming(Incoming {
        from: node(from),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: term(term_n),
            result: AppendEntriesResult::Conflict {
                next_index_hint: LogIndex::new(next_index_hint),
            },
        }),
    })
}

/// Extract every `Action::Apply` payload from an action vector.
pub(super) fn collect_apply(actions: &[Action<Vec<u8>>]) -> Vec<Vec<LogEntry<Vec<u8>>>> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Apply(entries) => Some(entries.clone()),
            _ => None,
        })
        .collect()
}

/// Wrap a `VoteResponse` into the Event envelope the engine accepts.
pub(super) fn vote_response_from(from: u64, term_n: u64, granted: bool) -> Event<Vec<u8>> {
    use crate::records::vote::{VoteResponse, VoteResult};
    Event::Incoming(Incoming {
        from: node(from),
        message: Message::VoteResponse(VoteResponse {
            term: term(term_n),
            result: if granted {
                VoteResult::Granted
            } else {
                VoteResult::Rejected
            },
        }),
    })
}

/// Extract every `Send(RequestVote)` action, paired with its destination.
/// Order is not guaranteed; callers that care sort by peer id.
pub(super) fn collect_vote_requests(actions: &[Action<Vec<u8>>]) -> Vec<(NodeId, RequestVote)> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Send {
                to,
                message: Message::VoteRequest(req),
            } => Some((*to, *req)),
            _ => None,
        })
        .collect()
}
