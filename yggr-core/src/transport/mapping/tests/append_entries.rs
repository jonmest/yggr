use super::strategies;
use crate::records::{
    append_entries::{AppendEntriesResponse, RequestAppendEntries},
    log_entry::{ConfigChange, LogEntry},
};
use crate::transport::mapping::ConvertError;
use crate::transport::protobuf as proto;
use proptest::prelude::*;
use prost::Message as _;

fn valid_proto_log_entry() -> impl Strategy<Value = proto::LogEntry> {
    strategies::log_entry().prop_map(proto::LogEntry::from)
}

fn valid_proto_request_append_entries() -> impl Strategy<Value = proto::RequestAppendEntries> {
    strategies::request_append_entries().prop_map(proto::RequestAppendEntries::from)
}

fn valid_proto_append_entries_response() -> impl Strategy<Value = proto::AppendEntriesResponse> {
    strategies::append_entries_response().prop_map(proto::AppendEntriesResponse::from)
}

proptest! {
    #[test]
    fn log_entry_roundtrip(e in strategies::log_entry()) {
        let round: LogEntry<Vec<u8>> = proto::LogEntry::from(e.clone()).try_into().unwrap();
        prop_assert_eq!(e, round);
    }

    #[test]
    fn log_entry_wire_roundtrip(e in strategies::log_entry()) {
        let bytes = proto::LogEntry::from(e.clone()).encode_to_vec();
        let round: LogEntry<Vec<u8>> =
            proto::LogEntry::decode(bytes.as_slice()).unwrap().try_into().unwrap();
        prop_assert_eq!(e, round);
    }

    #[test]
    fn request_append_entries_roundtrip(r in strategies::request_append_entries()) {
        let round: RequestAppendEntries<Vec<u8>> =
            proto::RequestAppendEntries::from(r.clone()).try_into().unwrap();
        prop_assert_eq!(r, round);
    }

    #[test]
    fn request_append_entries_wire_roundtrip(r in strategies::request_append_entries()) {
        let bytes = proto::RequestAppendEntries::from(r.clone()).encode_to_vec();
        let round: RequestAppendEntries<Vec<u8>> =
            proto::RequestAppendEntries::decode(bytes.as_slice()).unwrap().try_into().unwrap();
        prop_assert_eq!(r, round);
    }

    #[test]
    fn append_entries_response_roundtrip(r in strategies::append_entries_response()) {
        let round: AppendEntriesResponse =
            proto::AppendEntriesResponse::from(r).try_into().unwrap();
        prop_assert_eq!(r, round);
    }

    #[test]
    fn append_entries_response_wire_roundtrip(r in strategies::append_entries_response()) {
        let bytes = proto::AppendEntriesResponse::from(r).encode_to_vec();
        let round: AppendEntriesResponse =
            proto::AppendEntriesResponse::decode(bytes.as_slice()).unwrap().try_into().unwrap();
        prop_assert_eq!(r, round);
    }

    // Missing LogId on a LogEntry must always reject.
    #[test]
    fn log_entry_missing_id_rejected(mut p in valid_proto_log_entry()) {
        p.id = None;
        prop_assert_eq!(
            <LogEntry<Vec<u8>>>::try_from(p),
            Err(ConvertError::MissingField("LogEntry.id"))
        );
    }

    // Missing payload oneof on a LogEntry must always reject.
    #[test]
    fn log_entry_missing_payload_rejected(mut p in valid_proto_log_entry()) {
        p.payload = None;
        prop_assert_eq!(
            <LogEntry<Vec<u8>>>::try_from(p),
            Err(ConvertError::MissingField("LogEntry.payload"))
        );
    }

    // Zero leader_id must always reject.
    #[test]
    fn request_append_entries_zero_leader_rejected(mut p in valid_proto_request_append_entries()) {
        p.leader_id = 0;
        prop_assert_eq!(
            <RequestAppendEntries<Vec<u8>>>::try_from(p),
            Err(ConvertError::ZeroNodeId)
        );
    }

    // A bad LogEntry nested in entries must propagate — boundary rejects the whole request.
    #[test]
    fn request_append_entries_bad_nested_entry_rejected(
        mut p in valid_proto_request_append_entries(),
        mut bad in valid_proto_log_entry(),
        idx in any::<prop::sample::Index>(),
    ) {
        bad.id = None;
        let i = if p.entries.is_empty() { p.entries.push(bad); 0 } else {
            let i = idx.index(p.entries.len());
            p.entries[i] = bad;
            i
        };
        let _ = i;
        prop_assert_eq!(
            <RequestAppendEntries<Vec<u8>>>::try_from(p),
            Err(ConvertError::MissingField("LogEntry.id"))
        );
    }

    // Missing oneof result must always reject.
    #[test]
    fn append_entries_response_missing_result_rejected(mut p in valid_proto_append_entries_response()) {
        p.result = None;
        prop_assert_eq!(
            AppendEntriesResponse::try_from(p),
            Err(ConvertError::MissingField("AppendEntriesResponse.result"))
        );
    }

    // A LogEntry whose id has index=0 must reject — index 0 is the
    // pre-log sentinel and never names a real entry.
    #[test]
    fn log_entry_zero_index_rejected(mut p in valid_proto_log_entry()) {
        if let Some(id) = p.id.as_mut() {
            id.index = 0;
        }
        prop_assert_eq!(
            <LogEntry<Vec<u8>>>::try_from(p),
            Err(ConvertError::ZeroLogIndex("LogId.index"))
        );
    }

    // A RequestAppendEntries whose prev_log_id has index=0 must reject.
    // None encodes "before any entry"; Some(0) is malformed.
    #[test]
    fn request_append_entries_zero_prev_log_index_rejected(
        mut p in valid_proto_request_append_entries(),
    ) {
        p.prev_log_id = Some(proto::LogId { index: 0, term: 1 });
        prop_assert_eq!(
            <RequestAppendEntries<Vec<u8>>>::try_from(p),
            Err(ConvertError::ZeroLogIndex("LogId.index"))
        );
    }
}

// AppendEntriesResponse::Success { last_appended: Some(0) } must reject
// — None already encodes "no entries", so Some(0) is malformed.
#[test]
fn append_entries_response_success_zero_last_appended_rejected() {
    let p = proto::AppendEntriesResponse {
        term: 1,
        result: Some(proto::append_entries_response::Result::Success(
            proto::AppendSuccess {
                last_appended: Some(0),
            },
        )),
    };
    let err = AppendEntriesResponse::try_from(p).unwrap_err();
    assert_eq!(
        err,
        ConvertError::ZeroLogIndex("AppendEntriesResponse.success.last_appended"),
    );
}

#[test]
fn append_entries_response_conflict_zero_hint_rejected() {
    let p = proto::AppendEntriesResponse {
        term: 1,
        result: Some(proto::append_entries_response::Result::Conflict(
            proto::AppendConflict { next_index_hint: 0 },
        )),
    };
    let err = AppendEntriesResponse::try_from(p).unwrap_err();
    assert_eq!(
        err,
        ConvertError::ZeroLogIndex("AppendEntriesResponse.conflict.next_index_hint"),
    );
}

// ---------------------------------------------------------------------------
// ConfigChange payload coverage. The strategy now includes ConfigChange,
// so log_entry_roundtrip / _wire_roundtrip transitively exercise them
// through the fuzzer — these targeted tests lock in the failure modes.
// ---------------------------------------------------------------------------

#[test]
fn config_change_missing_kind_rejected() {
    let p = proto::ConfigChange { kind: None };
    let err = ConfigChange::try_from(p).unwrap_err();
    assert_eq!(err, ConvertError::MissingField("ConfigChange.kind"));
}

#[test]
fn config_change_add_peer_zero_id_rejected() {
    let p = proto::ConfigChange {
        kind: Some(proto::config_change::Kind::AddPeer(proto::NodeIdRef {
            id: 0,
        })),
    };
    let err = ConfigChange::try_from(p).unwrap_err();
    assert_eq!(err, ConvertError::ZeroNodeId);
}

#[test]
fn config_change_remove_peer_zero_id_rejected() {
    let p = proto::ConfigChange {
        kind: Some(proto::config_change::Kind::RemovePeer(proto::NodeIdRef {
            id: 0,
        })),
    };
    let err = ConfigChange::try_from(p).unwrap_err();
    assert_eq!(err, ConvertError::ZeroNodeId);
}

#[test]
fn log_entry_config_change_payload_with_bad_inner_rejects() {
    // A LogEntry carrying a ConfigChange whose inner NodeIdRef is
    // zero must propagate the ZeroNodeId error up to the LogEntry
    // boundary.
    let p = proto::LogEntry {
        id: Some(proto::LogId { index: 1, term: 1 }),
        payload: Some(proto::log_entry::Payload::ConfigChange(
            proto::ConfigChange {
                kind: Some(proto::config_change::Kind::AddPeer(proto::NodeIdRef {
                    id: 0,
                })),
            },
        )),
    };
    let err = <LogEntry<Vec<u8>>>::try_from(p).unwrap_err();
    assert_eq!(err, ConvertError::ZeroNodeId);
}
