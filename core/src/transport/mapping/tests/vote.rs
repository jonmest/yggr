use super::strategies;
use crate::records::vote::{RequestVote, VoteResponse};
use crate::transport::mapping::ConvertError;
use crate::transport::protobuf as proto;
use proptest::prelude::*;
use prost::Message as _;

fn valid_proto_request_vote() -> impl Strategy<Value = proto::RequestVote> {
    strategies::request_vote().prop_map(proto::RequestVote::from)
}

fn valid_proto_vote_response() -> impl Strategy<Value = proto::VoteResponse> {
    strategies::vote_response().prop_map(proto::VoteResponse::from)
}

proptest! {
    #[test]
    fn request_vote_roundtrip(v in strategies::request_vote()) {
        let round: RequestVote = proto::RequestVote::from(v).try_into().unwrap();
        prop_assert_eq!(v, round);
    }

    #[test]
    fn request_vote_wire_roundtrip(v in strategies::request_vote()) {
        let bytes = proto::RequestVote::from(v).encode_to_vec();
        let round: RequestVote = proto::RequestVote::decode(bytes.as_slice()).unwrap().try_into().unwrap();
        prop_assert_eq!(v, round);
    }

    #[test]
    fn vote_response_roundtrip(v in strategies::vote_response()) {
        let round: VoteResponse = proto::VoteResponse::from(v).try_into().unwrap();
        prop_assert_eq!(v, round);
    }

    #[test]
    fn vote_response_wire_roundtrip(v in strategies::vote_response()) {
        let bytes = proto::VoteResponse::from(v).encode_to_vec();
        let round: VoteResponse = proto::VoteResponse::decode(bytes.as_slice()).unwrap().try_into().unwrap();
        prop_assert_eq!(v, round);
    }

    // Zero candidate_id must always reject, regardless of other fields.
    #[test]
    fn request_vote_zero_candidate_rejected(mut p in valid_proto_request_vote()) {
        p.candidate_id = 0;
        prop_assert_eq!(RequestVote::try_from(p), Err(ConvertError::ZeroNodeId));
    }

    // Any non-Granted/Rejected discriminant must reject.
    #[test]
    fn vote_response_invalid_discriminant_rejected(
        mut p in valid_proto_vote_response(),
        bad in prop_oneof![Just(0i32), (3i32..1024), any::<i32>().prop_filter("valid", |n| !(1..=2).contains(n))],
    ) {
        p.result = bad;
        match VoteResponse::try_from(p) {
            Err(ConvertError::UnknownEnum("VoteResult", got)) => prop_assert_eq!(got, bad),
            other => prop_assert!(false, "expected UnknownEnum, got {:?}", other),
        }
    }
}
