use crate::records::vote::{RequestVote, VoteResponse, VoteResult};
use crate::types::{node::NodeId, term::Term};

use super::super::protobuf as proto;
use super::ConvertError;

impl From<RequestVote> for proto::RequestVote {
    fn from(v: RequestVote) -> Self {
        Self {
            term: v.term.get(),
            candidate_id: v.candidate_id.get(),
            last_log_id: v.last_log_id.map(Into::into),
        }
    }
}

impl TryFrom<proto::RequestVote> for RequestVote {
    type Error = ConvertError;

    fn try_from(v: proto::RequestVote) -> Result<Self, Self::Error> {
        Ok(Self {
            term: Term::new(v.term),
            candidate_id: NodeId::new(v.candidate_id).ok_or(ConvertError::ZeroNodeId)?,
            last_log_id: v.last_log_id.map(Into::into),
        })
    }
}

impl From<VoteResult> for proto::VoteResult {
    fn from(v: VoteResult) -> Self {
        match v {
            VoteResult::Granted => proto::VoteResult::Granted,
            VoteResult::Rejected => proto::VoteResult::Rejected,
        }
    }
}

impl TryFrom<proto::VoteResult> for VoteResult {
    type Error = ConvertError;

    fn try_from(v: proto::VoteResult) -> Result<Self, Self::Error> {
        match v {
            proto::VoteResult::Granted => Ok(VoteResult::Granted),
            proto::VoteResult::Rejected => Ok(VoteResult::Rejected),
            proto::VoteResult::Unspecified => {
                Err(ConvertError::UnknownEnum("VoteResult", v as i32))
            }
        }
    }
}

impl From<VoteResponse> for proto::VoteResponse {
    fn from(v: VoteResponse) -> Self {
        Self {
            term: v.term.get(),
            result: proto::VoteResult::from(v.result) as i32,
        }
    }
}

impl TryFrom<proto::VoteResponse> for VoteResponse {
    type Error = ConvertError;

    fn try_from(v: proto::VoteResponse) -> Result<Self, Self::Error> {
        let result = proto::VoteResult::try_from(v.result)
            .map_err(|_| ConvertError::UnknownEnum("VoteResult", v.result))?;
        Ok(Self {
            term: Term::new(v.term),
            result: VoteResult::try_from(result)?,
        })
    }
}
