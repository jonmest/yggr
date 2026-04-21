use super::strategies;
use crate::records::pre_vote::{PreVoteResponse, RequestPreVote};
use crate::transport::mapping::ConvertError;
use crate::transport::protobuf as proto;
use proptest::prelude::*;
use prost::Message as _;

fn valid_proto_request_pre_vote() -> impl Strategy<Value = proto::RequestPreVote> {
    strategies::request_pre_vote().prop_map(proto::RequestPreVote::from)
}

proptest! {
    #[test]
    fn request_pre_vote_roundtrip(r in strategies::request_pre_vote()) {
        let round: RequestPreVote = proto::RequestPreVote::from(r).try_into().unwrap();
        prop_assert_eq!(r, round);
    }

    #[test]
    fn request_pre_vote_wire_roundtrip(r in strategies::request_pre_vote()) {
        let bytes = proto::RequestPreVote::from(r).encode_to_vec();
        let round: RequestPreVote = proto::RequestPreVote::decode(bytes.as_slice())
            .unwrap()
            .try_into()
            .unwrap();
        prop_assert_eq!(r, round);
    }

    #[test]
    fn pre_vote_response_roundtrip(r in strategies::pre_vote_response()) {
        let round: PreVoteResponse = proto::PreVoteResponse::from(r).into();
        prop_assert_eq!(r, round);
    }

    #[test]
    fn pre_vote_response_wire_roundtrip(r in strategies::pre_vote_response()) {
        let bytes = proto::PreVoteResponse::from(r).encode_to_vec();
        let round: PreVoteResponse = proto::PreVoteResponse::decode(bytes.as_slice())
            .unwrap()
            .into();
        prop_assert_eq!(r, round);
    }

    #[test]
    fn request_pre_vote_zero_candidate_rejected(mut p in valid_proto_request_pre_vote()) {
        p.candidate_id = 0;
        prop_assert_eq!(RequestPreVote::try_from(p), Err(ConvertError::ZeroNodeId));
    }
}
