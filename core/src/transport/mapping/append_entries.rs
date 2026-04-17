use crate::records::{
    append_entries::{AppendEntriesResponse, AppendEntriesResult, RequestAppendEntries},
    log_entry::LogEntry,
};
use crate::types::{index::LogIndex, node::NodeId, term::Term};

use super::super::protobuf as proto;
use super::ConvertError;

impl<C: Into<Vec<u8>>> From<LogEntry<C>> for proto::LogEntry {
    fn from(v: LogEntry<C>) -> Self {
        Self {
            id: Some(v.id.into()),
            command: v.command.into(),
        }
    }
}

impl<C: From<Vec<u8>>> TryFrom<proto::LogEntry> for LogEntry<C> {
    type Error = ConvertError;

    fn try_from(v: proto::LogEntry) -> Result<Self, Self::Error> {
        Ok(Self {
            id: v
                .id
                .ok_or(ConvertError::MissingField("LogEntry.id"))?
                .into(),
            command: C::from(v.command),
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
            prev_log_id: v.prev_log_id.map(Into::into),
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
            R::Success(s) => AppendEntriesResult::Success {
                last_appended: s.last_appended.map(LogIndex::new),
            },
            R::Conflict(c) => AppendEntriesResult::Conflict {
                next_index_hint: LogIndex::new(c.next_index_hint),
            },
        };
        Ok(Self {
            term: Term::new(v.term),
            result,
        })
    }
}
