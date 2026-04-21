use proptest::collection::vec;
use proptest::prelude::*;

use crate::records::{
    append_entries::{AppendEntriesResponse, AppendEntriesResult, RequestAppendEntries},
    install_snapshot::{InstallSnapshotResponse, RequestInstallSnapshot},
    log_entry::{ConfigChange, LogEntry, LogPayload},
    message::Message,
    pre_vote::{PreVoteResponse, RequestPreVote},
    timeout_now::TimeoutNow,
    vote::{RequestVote, VoteResponse, VoteResult},
};
use crate::types::{index::LogIndex, log::LogId, node::NodeId, term::Term};

pub(super) fn term() -> impl Strategy<Value = Term> {
    any::<u64>().prop_map(Term::new)
}

/// Any [`LogIndex`] including the pre-log sentinel (0). Suitable for fields
/// that may legitimately be zero, like `leader_commit` before any commit
/// has happened.
pub(super) fn log_index() -> impl Strategy<Value = LogIndex> {
    any::<u64>().prop_map(LogIndex::new)
}

/// Strictly positive [`LogIndex`] — for fields that name a real log
/// position (entry positions, ack targets, conflict hints).
pub(super) fn nonzero_log_index() -> impl Strategy<Value = LogIndex> {
    (1u64..=u64::MAX).prop_map(LogIndex::new)
}

pub(super) fn node_id() -> impl Strategy<Value = NodeId> {
    (1u64..=u64::MAX).prop_map(|n| NodeId::new(n).unwrap())
}

pub(super) fn log_id() -> impl Strategy<Value = LogId> {
    (nonzero_log_index(), term()).prop_map(|(i, t)| LogId::new(i, t))
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

pub(super) fn config_change() -> impl Strategy<Value = ConfigChange> {
    prop_oneof![
        node_id().prop_map(ConfigChange::AddPeer),
        node_id().prop_map(ConfigChange::RemovePeer),
    ]
}

pub(super) fn log_payload() -> impl Strategy<Value = LogPayload<Vec<u8>>> {
    prop_oneof![
        Just(LogPayload::Noop),
        vec(any::<u8>(), 0..64).prop_map(LogPayload::Command),
        config_change().prop_map(LogPayload::ConfigChange),
    ]
}

pub(super) fn log_entry() -> impl Strategy<Value = LogEntry<Vec<u8>>> {
    (log_id(), log_payload()).prop_map(|(id, payload)| LogEntry { id, payload })
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
        proptest::option::of(nonzero_log_index())
            .prop_map(|last_appended| AppendEntriesResult::Success { last_appended }),
        nonzero_log_index()
            .prop_map(|next_index_hint| AppendEntriesResult::Conflict { next_index_hint }),
    ]
}

pub(super) fn append_entries_response() -> impl Strategy<Value = AppendEntriesResponse> {
    (term(), append_entries_result())
        .prop_map(|(term, result)| AppendEntriesResponse { term, result })
}

pub(super) fn request_install_snapshot() -> impl Strategy<Value = RequestInstallSnapshot> {
    (
        term(),
        node_id(),
        log_id(),
        vec(any::<u8>(), 0..256),
        any::<u64>(),
        any::<bool>(),
        log_index(),
        proptest::collection::btree_set(node_id(), 0..8),
    )
        .prop_map(
            |(term, leader_id, last_included, data, offset, done, leader_commit, peers)| {
                RequestInstallSnapshot {
                    term,
                    leader_id,
                    last_included,
                    data,
                    offset,
                    done,
                    leader_commit,
                    peers,
                }
            },
        )
}

pub(super) fn install_snapshot_response() -> impl Strategy<Value = InstallSnapshotResponse> {
    (term(), log_id(), any::<u64>(), any::<bool>()).prop_map(
        |(term, last_included, next_offset, done)| InstallSnapshotResponse {
            term,
            last_included,
            next_offset,
            done,
        },
    )
}

pub(super) fn timeout_now() -> impl Strategy<Value = TimeoutNow> {
    (term(), node_id()).prop_map(|(term, leader_id)| TimeoutNow { term, leader_id })
}

pub(super) fn request_pre_vote() -> impl Strategy<Value = RequestPreVote> {
    (term(), node_id(), proptest::option::of(log_id())).prop_map(
        |(term, candidate_id, last_log_id)| RequestPreVote {
            term,
            candidate_id,
            last_log_id,
        },
    )
}

pub(super) fn pre_vote_response() -> impl Strategy<Value = PreVoteResponse> {
    (term(), any::<bool>()).prop_map(|(term, granted)| PreVoteResponse { term, granted })
}

pub(super) fn message() -> impl Strategy<Value = Message<Vec<u8>>> {
    prop_oneof![
        request_vote().prop_map(Message::VoteRequest),
        vote_response().prop_map(Message::VoteResponse),
        request_append_entries().prop_map(Message::AppendEntriesRequest),
        append_entries_response().prop_map(Message::AppendEntriesResponse),
        request_install_snapshot().prop_map(Message::InstallSnapshotRequest),
        install_snapshot_response().prop_map(Message::InstallSnapshotResponse),
        timeout_now().prop_map(Message::TimeoutNow),
        request_pre_vote().prop_map(Message::PreVoteRequest),
        pre_vote_response().prop_map(Message::PreVoteResponse),
    ]
}
