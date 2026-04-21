use crate::records::timeout_now::TimeoutNow;
use crate::types::{node::NodeId, term::Term};

use super::super::protobuf as proto;
use super::ConvertError;

impl From<TimeoutNow> for proto::TimeoutNow {
    fn from(v: TimeoutNow) -> Self {
        Self {
            term: v.term.get(),
            leader_id: v.leader_id.get(),
        }
    }
}

impl TryFrom<proto::TimeoutNow> for TimeoutNow {
    type Error = ConvertError;

    fn try_from(v: proto::TimeoutNow) -> Result<Self, Self::Error> {
        Ok(Self {
            term: Term::new(v.term),
            leader_id: NodeId::new(v.leader_id).ok_or(ConvertError::ZeroNodeId)?,
        })
    }
}
