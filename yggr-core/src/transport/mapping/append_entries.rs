use crate::records::{
    append_entries::{AppendEntriesResponse, AppendEntriesResult, RequestAppendEntries},
    log_entry::{ConfigChange, LogEntry, LogPayload},
};
use crate::types::{index::LogIndex, node::NodeId, term::Term};

use super::super::protobuf as proto;
use super::ConvertError;

impl From<ConfigChange> for proto::ConfigChange {
    fn from(v: ConfigChange) -> Self {
        use proto::config_change::Kind;
        let kind = match v {
            ConfigChange::AddPeer(id) => Kind::AddPeer(proto::NodeIdRef { id: id.get() }),
            ConfigChange::RemovePeer(id) => Kind::RemovePeer(proto::NodeIdRef { id: id.get() }),
        };
        Self { kind: Some(kind) }
    }
}

impl TryFrom<proto::ConfigChange> for ConfigChange {
    type Error = ConvertError;

    fn try_from(v: proto::ConfigChange) -> Result<Self, Self::Error> {
        use proto::config_change::Kind;
        match v
            .kind
            .ok_or(ConvertError::MissingField("ConfigChange.kind"))?
        {
            Kind::AddPeer(r) => Ok(ConfigChange::AddPeer(
                NodeId::new(r.id).ok_or(ConvertError::ZeroNodeId)?,
            )),
            Kind::RemovePeer(r) => Ok(ConfigChange::RemovePeer(
                NodeId::new(r.id).ok_or(ConvertError::ZeroNodeId)?,
            )),
        }
    }
}

impl<C: Into<Vec<u8>>> From<LogEntry<C>> for proto::LogEntry {
    fn from(v: LogEntry<C>) -> Self {
        let payload = match v.payload {
            LogPayload::Noop => proto::log_entry::Payload::Noop(proto::Noop {}),
            LogPayload::Command(c) => proto::log_entry::Payload::Command(c.into()),
            LogPayload::ConfigChange(cc) => proto::log_entry::Payload::ConfigChange(cc.into()),
        };
        Self {
            id: Some(v.id.into()),
            payload: Some(payload),
        }
    }
}

impl<C: From<Vec<u8>>> TryFrom<proto::LogEntry> for LogEntry<C> {
    type Error = ConvertError;

    fn try_from(v: proto::LogEntry) -> Result<Self, Self::Error> {
        let payload = match v
            .payload
            .ok_or(ConvertError::MissingField("LogEntry.payload"))?
        {
            proto::log_entry::Payload::Noop(_) => LogPayload::Noop,
            proto::log_entry::Payload::Command(b) => LogPayload::Command(C::from(b)),
            proto::log_entry::Payload::ConfigChange(cc) => LogPayload::ConfigChange(cc.try_into()?),
        };
        Ok(Self {
            id: v
                .id
                .ok_or(ConvertError::MissingField("LogEntry.id"))?
                .try_into()?,
            payload,
        })
    }
}

impl<C: Into<Vec<u8>>> From<RequestAppendEntries<C>> for proto::RequestAppendEntries {
    fn from(v: RequestAppendEntries<C>) -> Self {
        Self {
            term: v.term.get(),
            leader_id: v.leader_id.get(),
            prev_log_id: v.prev_log_id.map(Into::into),
            entries: v.entries.into_iter().map(Into::into).collect(),
            leader_commit: v.leader_commit.get(),
        }
    }
}

impl<C: From<Vec<u8>>> TryFrom<proto::RequestAppendEntries> for RequestAppendEntries<C> {
    type Error = ConvertError;

    fn try_from(v: proto::RequestAppendEntries) -> Result<Self, Self::Error> {
        Ok(Self {
            term: Term::new(v.term),
            leader_id: NodeId::new(v.leader_id).ok_or(ConvertError::ZeroNodeId)?,
            prev_log_id: v.prev_log_id.map(TryInto::try_into).transpose()?,
            entries: v
                .entries
                .into_iter()
                .map(LogEntry::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            leader_commit: LogIndex::new(v.leader_commit),
        })
    }
}

impl From<AppendEntriesResponse> for proto::AppendEntriesResponse {
    fn from(v: AppendEntriesResponse) -> Self {
        use proto::append_entries_response::Result as R;
        let result = match v.result {
            AppendEntriesResult::Success { last_appended } => R::Success(proto::AppendSuccess {
                last_appended: last_appended.map(LogIndex::get),
            }),
            AppendEntriesResult::Conflict { next_index_hint } => {
                R::Conflict(proto::AppendConflict {
                    next_index_hint: next_index_hint.get(),
                })
            }
        };
        Self {
            term: v.term.get(),
            result: Some(result),
        }
    }
}

impl TryFrom<proto::AppendEntriesResponse> for AppendEntriesResponse {
    type Error = ConvertError;

    fn try_from(v: proto::AppendEntriesResponse) -> Result<Self, Self::Error> {
        use proto::append_entries_response::Result as R;
        let result = match v
            .result
            .ok_or(ConvertError::MissingField("AppendEntriesResponse.result"))?
        {
            R::Success(s) => {
                // None = "no entries"; Some(0) is the pre-log sentinel
                // and must not appear on the wire as an ack target.
                if let Some(0) = s.last_appended {
                    return Err(ConvertError::ZeroLogIndex(
                        "AppendEntriesResponse.success.last_appended",
                    ));
                }
                AppendEntriesResult::Success {
                    last_appended: s.last_appended.map(LogIndex::new),
                }
            }
            R::Conflict(c) => {
                if c.next_index_hint == 0 {
                    return Err(ConvertError::ZeroLogIndex(
                        "AppendEntriesResponse.conflict.next_index_hint",
                    ));
                }
                AppendEntriesResult::Conflict {
                    next_index_hint: LogIndex::new(c.next_index_hint),
                }
            }
        };
        Ok(Self {
            term: Term::new(v.term),
            result,
        })
    }
}
