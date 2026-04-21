use crate::records::pre_vote::{PreVoteResponse, RequestPreVote};
use crate::types::{node::NodeId, term::Term};

use super::super::protobuf as proto;
use super::ConvertError;

impl From<RequestPreVote> for proto::RequestPreVote {
    fn from(v: RequestPreVote) -> Self {
        Self {
            term: v.term.get(),
            candidate_id: v.candidate_id.get(),
            last_log_id: v.last_log_id.map(Into::into),
        }
    }
}

impl TryFrom<proto::RequestPreVote> for RequestPreVote {
    type Error = ConvertError;

    fn try_from(v: proto::RequestPreVote) -> Result<Self, Self::Error> {
        Ok(Self {
            term: Term::new(v.term),
            candidate_id: NodeId::new(v.candidate_id).ok_or(ConvertError::ZeroNodeId)?,
            last_log_id: v.last_log_id.map(TryInto::try_into).transpose()?,
        })
    }
}

impl From<PreVoteResponse> for proto::PreVoteResponse {
    fn from(v: PreVoteResponse) -> Self {
        Self {
            term: v.term.get(),
            granted: v.granted,
        }
    }
}

impl From<proto::PreVoteResponse> for PreVoteResponse {
    fn from(v: proto::PreVoteResponse) -> Self {
        Self {
            term: Term::new(v.term),
            granted: v.granted,
        }
    }
}
