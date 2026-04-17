use super::strategies;
use crate::records::message::Message;
use crate::transport::mapping::ConvertError;
use crate::transport::protobuf as proto;
use crate::types::log::LogId;
use proptest::collection::vec;
use proptest::prelude::*;
use prost::Message as _;

proptest! {
    #[test]
    fn log_id_roundtrip(id in strategies::log_id()) {
        let round: LogId = proto::LogId::from(id).into();
        prop_assert_eq!(id, round);
    }

    #[test]
    fn message_roundtrip(m in strategies::message()) {
        let round: Message<Vec<u8>> = proto::Message::from(m.clone()).try_into().unwrap();
        prop_assert_eq!(m, round);
    }

    #[test]
    fn message_wire_roundtrip(m in strategies::message()) {
        let bytes = proto::Message::from(m.clone()).encode_to_vec();
        let decoded = proto::Message::decode(bytes.as_slice()).unwrap();
        let round: Message<Vec<u8>> = decoded.try_into().unwrap();
        prop_assert_eq!(m, round);
    }
}

#[test]
fn message_missing_kind_rejected() {
    let empty = proto::Message { kind: None };
    let err = <Message<Vec<u8>>>::try_from(empty).unwrap_err();
    assert_eq!(err, ConvertError::MissingField("Message.kind"));
}

// The mapper sits at the network boundary: untrusted bytes go in. It must
// never panic, only succeed or return a ConvertError.
proptest! {
    #[test]
    fn arbitrary_bytes_never_panic(bytes in vec(any::<u8>(), 0..512)) {
        if let Ok(decoded) = proto::Message::decode(bytes.as_slice()) {
            let _ = <Message<Vec<u8>>>::try_from(decoded);
        }
    }
}
