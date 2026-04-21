use super::strategies;
use crate::records::install_snapshot::{InstallSnapshotResponse, RequestInstallSnapshot};
use crate::transport::mapping::ConvertError;
use crate::transport::protobuf as proto;
use proptest::prelude::*;
use prost::Message as _;

fn valid_proto_request_install_snapshot() -> impl Strategy<Value = proto::RequestInstallSnapshot> {
    strategies::request_install_snapshot().prop_map(proto::RequestInstallSnapshot::from)
}

fn valid_proto_install_snapshot_response() -> impl Strategy<Value = proto::InstallSnapshotResponse>
{
    strategies::install_snapshot_response().prop_map(proto::InstallSnapshotResponse::from)
}

proptest! {
    #[test]
    fn request_install_snapshot_roundtrip(r in strategies::request_install_snapshot()) {
        let round: RequestInstallSnapshot =
            proto::RequestInstallSnapshot::from(r.clone()).try_into().unwrap();
        prop_assert_eq!(r, round);
    }

    #[test]
    fn request_install_snapshot_wire_roundtrip(r in strategies::request_install_snapshot()) {
        let bytes = proto::RequestInstallSnapshot::from(r.clone()).encode_to_vec();
        let round: RequestInstallSnapshot =
            proto::RequestInstallSnapshot::decode(bytes.as_slice()).unwrap().try_into().unwrap();
        prop_assert_eq!(r, round);
    }

    #[test]
    fn install_snapshot_response_roundtrip(r in strategies::install_snapshot_response()) {
        let round: InstallSnapshotResponse =
            proto::InstallSnapshotResponse::from(r).try_into().unwrap();
        prop_assert_eq!(r, round);
    }

    #[test]
    fn install_snapshot_response_wire_roundtrip(r in strategies::install_snapshot_response()) {
        let bytes = proto::InstallSnapshotResponse::from(r).encode_to_vec();
        let round: InstallSnapshotResponse =
            proto::InstallSnapshotResponse::decode(bytes.as_slice()).unwrap().try_into().unwrap();
        prop_assert_eq!(r, round);
    }

    #[test]
    fn request_install_snapshot_zero_leader_rejected(
        mut p in valid_proto_request_install_snapshot(),
    ) {
        p.leader_id = 0;
        prop_assert_eq!(RequestInstallSnapshot::try_from(p), Err(ConvertError::ZeroNodeId));
    }

    #[test]
    fn request_install_snapshot_missing_last_included_rejected(
        mut p in valid_proto_request_install_snapshot(),
    ) {
        p.last_included = None;
        prop_assert_eq!(
            RequestInstallSnapshot::try_from(p),
            Err(ConvertError::MissingField("RequestInstallSnapshot.last_included"))
        );
    }

    #[test]
    fn install_snapshot_response_missing_last_included_rejected(
        mut p in valid_proto_install_snapshot_response(),
    ) {
        p.last_included = None;
        prop_assert_eq!(
            InstallSnapshotResponse::try_from(p),
            Err(ConvertError::MissingField("InstallSnapshotResponse.last_included"))
        );
    }

    // A bad nested last_included (index = 0) propagates out of the
    // request boundary — pins the `try_into()?` on last_included.
    #[test]
    fn request_install_snapshot_zero_last_included_index_rejected(
        mut p in valid_proto_request_install_snapshot(),
    ) {
        if let Some(li) = p.last_included.as_mut() {
            li.index = 0;
        }
        prop_assert_eq!(
            RequestInstallSnapshot::try_from(p),
            Err(ConvertError::ZeroLogIndex("LogId.index"))
        );
    }

    #[test]
    fn install_snapshot_response_zero_last_included_index_rejected(
        mut p in valid_proto_install_snapshot_response(),
    ) {
        if let Some(li) = p.last_included.as_mut() {
            li.index = 0;
        }
        prop_assert_eq!(
            InstallSnapshotResponse::try_from(p),
            Err(ConvertError::ZeroLogIndex("LogId.index"))
        );
    }
}

// A RequestInstallSnapshot whose peers list contains NodeIdRef { id: 0 }
// must reject with ZeroNodeId — pins the peers-parsing loop that was
// otherwise only exercised with empty peer sets.
#[test]
fn request_install_snapshot_zero_peer_id_rejected() {
    let p = proto::RequestInstallSnapshot {
        term: 1,
        leader_id: 2,
        last_included: Some(proto::LogId { index: 1, term: 1 }),
        data: vec![],
        offset: 0,
        done: true,
        leader_commit: 1,
        peers: vec![proto::NodeIdRef { id: 3 }, proto::NodeIdRef { id: 0 }],
    };
    let err = RequestInstallSnapshot::try_from(p).unwrap_err();
    assert_eq!(err, ConvertError::ZeroNodeId);
}

#[test]
fn request_install_snapshot_with_populated_peers_roundtrips() {
    // Proptest covers this fuzzily, but a deterministic case nails
    // the peers-copy path on recover.
    let mut peers = std::collections::BTreeSet::new();
    peers.insert(crate::types::node::NodeId::new(2).unwrap());
    peers.insert(crate::types::node::NodeId::new(3).unwrap());
    let r = RequestInstallSnapshot {
        term: crate::types::term::Term::new(5),
        leader_id: crate::types::node::NodeId::new(1).unwrap(),
        last_included: crate::types::log::LogId::new(
            crate::types::index::LogIndex::new(10),
            crate::types::term::Term::new(5),
        ),
        data: vec![1, 2, 3, 4],
        offset: 0,
        done: true,
        leader_commit: crate::types::index::LogIndex::new(10),
        peers: peers.clone(),
    };
    let round: RequestInstallSnapshot = proto::RequestInstallSnapshot::from(r.clone())
        .try_into()
        .unwrap();
    assert_eq!(r, round);
    assert_eq!(round.peers, peers);
}
