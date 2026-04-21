pub mod append_entries;
pub mod install_snapshot;
pub mod pre_vote;
pub mod timeout_now;
pub mod vote;

#[cfg(test)]
mod tests;

use crate::records::message::Message;
use crate::types::{index::LogIndex, log::LogId, term::Term};

use super::protobuf as proto;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConvertError {
    ZeroNodeId,
    /// A field that should carry a real log position arrived as 0.
    /// `LogIndex::ZERO` is a pre-log sentinel and must never appear on
    /// the wire as the index of an actual entry or response position.
    ZeroLogIndex(&'static str),
    MissingField(&'static str),
    UnknownEnum(&'static str, i32),
}

impl std::fmt::Display for ConvertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroNodeId => write!(f, "node id must be non-zero"),
            Self::ZeroLogIndex(field) => {
                write!(f, "log index must be non-zero in field: {field}")
            }
            Self::MissingField(name) => write!(f, "missing required field: {name}"),
            Self::UnknownEnum(name, v) => write!(f, "unknown enum value for {name}: {v}"),
        }
    }
}

impl std::error::Error for ConvertError {}

impl From<LogId> for proto::LogId {
    fn from(v: LogId) -> Self {
        Self {
            index: v.index.get(),
            term: v.term.get(),
        }
    }
}

impl TryFrom<proto::LogId> for LogId {
    type Error = ConvertError;

    fn try_from(v: proto::LogId) -> Result<Self, Self::Error> {
        if v.index == 0 {
            return Err(ConvertError::ZeroLogIndex("LogId.index"));
        }
        Ok(LogId::new(LogIndex::new(v.index), Term::new(v.term)))
    }
}

impl<C: Into<Vec<u8>>> From<Message<C>> for proto::Message {
    fn from(v: Message<C>) -> Self {
        use proto::message::Kind;
        let kind = match v {
            Message::VoteRequest(m) => Kind::VoteRequest(m.into()),
            Message::VoteResponse(m) => Kind::VoteResponse(m.into()),
            Message::AppendEntriesRequest(m) => Kind::AppendEntriesRequest(m.into()),
            Message::AppendEntriesResponse(m) => Kind::AppendEntriesResponse(m.into()),
            Message::InstallSnapshotRequest(m) => Kind::InstallSnapshotRequest(m.into()),
            Message::InstallSnapshotResponse(m) => Kind::InstallSnapshotResponse(m.into()),
            Message::TimeoutNow(m) => Kind::TimeoutNow(m.into()),
            Message::PreVoteRequest(m) => Kind::PreVoteRequest(m.into()),
            Message::PreVoteResponse(m) => Kind::PreVoteResponse(m.into()),
        };
        Self { kind: Some(kind) }
    }
}

impl<C: From<Vec<u8>>> TryFrom<proto::Message> for Message<C> {
    type Error = ConvertError;

    fn try_from(v: proto::Message) -> Result<Self, Self::Error> {
        use proto::message::Kind;
        match v.kind.ok_or(ConvertError::MissingField("Message.kind"))? {
            Kind::VoteRequest(m) => Ok(Message::VoteRequest(m.try_into()?)),
            Kind::VoteResponse(m) => Ok(Message::VoteResponse(m.try_into()?)),
            Kind::AppendEntriesRequest(m) => Ok(Message::AppendEntriesRequest(m.try_into()?)),
            Kind::AppendEntriesResponse(m) => Ok(Message::AppendEntriesResponse(m.try_into()?)),
            Kind::InstallSnapshotRequest(m) => Ok(Message::InstallSnapshotRequest(m.try_into()?)),
            Kind::InstallSnapshotResponse(m) => Ok(Message::InstallSnapshotResponse(m.try_into()?)),
            Kind::TimeoutNow(m) => Ok(Message::TimeoutNow(m.try_into()?)),
            Kind::PreVoteRequest(m) => Ok(Message::PreVoteRequest(m.try_into()?)),
            Kind::PreVoteResponse(m) => Ok(Message::PreVoteResponse(m.into())),
        }
    }
}
