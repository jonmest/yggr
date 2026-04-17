use proptest::collection::vec;
use proptest::prelude::*;

use crate::records::{
    append_entries::{AppendEntriesResponse, AppendEntriesResult, RequestAppendEntries},
    log_entry::LogEntry,
    message::Message,
    vote::{RequestVote, VoteResponse, VoteResult},
};
use crate::types::{index::LogIndex, log::LogId, node::NodeId, term::Term};

pub(super) fn term() -> impl Strategy<Value = Term> {
    any::<u64>().prop_map(Term::new)
}

pub(super) fn log_index() -> impl Strategy<Value = LogIndex> {
    any::<u64>().prop_map(LogIndex::new)
}

pub(super) fn node_id() -> impl Strategy<Value = NodeId> {
    (1u64..=u64::MAX).prop_map(|n| NodeId::new(n).unwrap())
}

pub(super) fn log_id() -> impl Strategy<Value = LogId> {
    (log_index(), term()).prop_map(|(i, t)| LogId::new(i, t))
}

pub(super) fn request_vote() -> impl Strategy<Value = RequestVote> {
    (term(), node_id(), proptest::option::of(log_id())).prop_map(
        |(term, candidate_id, last_log_id)| RequestVote {
            term,
            candidate_id,
            last_log_id,
        },
    )
}

pub(super) fn vote_result() -> impl Strategy<Value = VoteResult> {
    prop_oneof![Just(VoteResult::Granted), Just(VoteResult::Rejected)]
}

pub(super) fn vote_response() -> impl Strategy<Value = VoteResponse> {
    (term(), vote_result()).prop_map(|(term, result)| VoteResponse { term, result })
}

pub(super) fn log_entry() -> impl Strategy<Value = LogEntry<Vec<u8>>> {
    (log_id(), vec(any::<u8>(), 0..64)).prop_map(|(id, command)| LogEntry { id, command })
}

pub(super) fn request_append_entries() -> impl Strategy<Value = RequestAppendEntries<Vec<u8>>> {
    (
        term(),
        node_id(),
        proptest::option::of(log_id()),
        vec(log_entry(), 0..8),
        log_index(),
    )
        .prop_map(|(term, leader_id, prev_log_id, entries, leader_commit)| {
            RequestAppendEntries {
                term,
                leader_id,
                prev_log_id,
                entries,
                leader_commit,
            }
        })
}

pub(super) fn append_entries_result() -> impl Strategy<Value = AppendEntriesResult> {
    prop_oneof![
        proptest::option::of(log_index())
            .prop_map(|last_appended| AppendEntriesResult::Success { last_appended }),
        log_index().prop_map(|next_index_hint| AppendEntriesResult::Conflict { next_index_hint }),
    ]
}

pub(super) fn append_entries_response() -> impl Strategy<Value = AppendEntriesResponse> {
    (term(), append_entries_result())
        .prop_map(|(term, result)| AppendEntriesResponse { term, result })
}

pub(super) fn message() -> impl Strategy<Value = Message<Vec<u8>>> {
    prop_oneof![
        request_vote().prop_map(Message::VoteRequest),
        vote_response().prop_map(Message::VoteResponse),
        request_append_entries().prop_map(Message::AppendEntriesRequest),
        append_entries_response().prop_map(Message::AppendEntriesResponse),
    ]
}
