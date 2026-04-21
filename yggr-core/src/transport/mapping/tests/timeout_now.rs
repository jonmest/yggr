use super::strategies;
use crate::records::timeout_now::TimeoutNow;
use crate::transport::mapping::ConvertError;
use crate::transport::protobuf as proto;
use proptest::prelude::*;
use prost::Message as _;

fn valid_proto_timeout_now() -> impl Strategy<Value = proto::TimeoutNow> {
    strategies::timeout_now().prop_map(proto::TimeoutNow::from)
}

proptest! {
    #[test]
    fn timeout_now_roundtrip(t in strategies::timeout_now()) {
        let round: TimeoutNow = proto::TimeoutNow::from(t).try_into().unwrap();
        prop_assert_eq!(t, round);
    }

    #[test]
    fn timeout_now_wire_roundtrip(t in strategies::timeout_now()) {
        let bytes = proto::TimeoutNow::from(t).encode_to_vec();
        let round: TimeoutNow = proto::TimeoutNow::decode(bytes.as_slice()).unwrap().try_into().unwrap();
        prop_assert_eq!(t, round);
    }

    #[test]
    fn timeout_now_zero_leader_rejected(mut p in valid_proto_timeout_now()) {
        p.leader_id = 0;
        prop_assert_eq!(TimeoutNow::try_from(p), Err(ConvertError::ZeroNodeId));
    }
}
