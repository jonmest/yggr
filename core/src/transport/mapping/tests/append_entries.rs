use super::strategies;
use crate::records::{
    append_entries::{AppendEntriesResponse, RequestAppendEntries},
    log_entry::LogEntry,
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
}
