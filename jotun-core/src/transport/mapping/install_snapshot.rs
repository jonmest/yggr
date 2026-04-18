use crate::records::install_snapshot::{InstallSnapshotResponse, RequestInstallSnapshot};
use crate::types::{index::LogIndex, node::NodeId, term::Term};

use super::super::protobuf as proto;
use super::ConvertError;

impl From<RequestInstallSnapshot> for proto::RequestInstallSnapshot {
    fn from(v: RequestInstallSnapshot) -> Self {
        Self {
            term: v.term.get(),
            leader_id: v.leader_id.get(),
            last_included: Some(v.last_included.into()),
            data: v.data,
            leader_commit: v.leader_commit.get(),
            peers: v
                .peers
                .into_iter()
                .map(|id| proto::NodeIdRef { id: id.get() })
                .collect(),
        }
    }
}

impl TryFrom<proto::RequestInstallSnapshot> for RequestInstallSnapshot {
    type Error = ConvertError;

    fn try_from(v: proto::RequestInstallSnapshot) -> Result<Self, Self::Error> {
        let mut peers = std::collections::BTreeSet::new();
        for p in v.peers {
            peers.insert(NodeId::new(p.id).ok_or(ConvertError::ZeroNodeId)?);
        }
        Ok(Self {
            term: Term::new(v.term),
            leader_id: NodeId::new(v.leader_id).ok_or(ConvertError::ZeroNodeId)?,
            last_included: v
                .last_included
                .ok_or(ConvertError::MissingField(
                    "RequestInstallSnapshot.last_included",
                ))?
                .try_into()?,
            data: v.data,
            leader_commit: LogIndex::new(v.leader_commit),
            peers,
        })
    }
}

impl From<InstallSnapshotResponse> for proto::InstallSnapshotResponse {
    fn from(v: InstallSnapshotResponse) -> Self {
        Self { term: v.term.get() }
    }
}

impl TryFrom<proto::InstallSnapshotResponse> for InstallSnapshotResponse {
    type Error = ConvertError;

    fn try_from(v: proto::InstallSnapshotResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            term: Term::new(v.term),
        })
    }
}
