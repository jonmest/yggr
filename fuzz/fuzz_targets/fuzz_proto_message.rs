#![no_main]
//! Fuzz the wire → domain boundary: arbitrary bytes → `proto::Message`
//! → `Message::<Vec<u8>>::try_from`. Must never panic.
//! Any panic here is a network-boundary crash bug.

use libfuzzer_sys::fuzz_target;

use jotun_core::transport::protobuf as proto;
use jotun_core::Message;
use prost::Message as _;

fuzz_target!(|data: &[u8]| {
    if let Ok(decoded) = proto::Message::decode(data) {
        let _ = <Message<Vec<u8>>>::try_from(decoded);
    }
});
