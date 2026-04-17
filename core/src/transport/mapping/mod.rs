pub mod append_entries;
pub mod vote;

#[cfg(test)]
mod tests;

use crate::records::message::Message;
use crate::types::{index::LogIndex, log::LogId, term::Term};

use super::protobuf as proto;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConvertError {
    ZeroNodeId,
    MissingField(&'static str),
    UnknownEnum(&'static str, i32),
}

impl std::fmt::Display for ConvertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroNodeId => write!(f, "node id must be non-zero"),
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

impl From<proto::LogId> for LogId {
    fn from(v: proto::LogId) -> Self {
        LogId::new(LogIndex::new(v.index), Term::new(v.term))
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
        }
    }
}
