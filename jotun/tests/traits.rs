//! Smoke tests for the trait surface. No real Node yet; just prove
//! that the traits can be implemented for a toy in-memory KV store
//! and that the basic types compose.

#![allow(
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    clippy::missing_const_for_fn,
    clippy::unwrap_used,
    clippy::expect_used
)]

use std::collections::BTreeMap;
use std::convert::Infallible;

use jotun::{DecodeError, NodeId, StateMachine, Storage, StoredHardState, StoredSnapshot};
use jotun_core::{Incoming, LogEntry, LogIndex, Message, Term};

// ---------------------------------------------------------------------------
// Toy state machine
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct Kv {
    map: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum KvCmd {
    Set { key: String, value: String },
    Delete { key: String },
}

impl StateMachine for Kv {
    type Command = KvCmd;
    type Response = Option<String>;

    fn encode_command(c: &KvCmd) -> Vec<u8> {
        // Trivial line-based encoding; real users would pick serde
        // or prost. Format: `S\nkey\nvalue` or `D\nkey`.
        match c {
            KvCmd::Set { key, value } => format!("S\n{key}\n{value}").into_bytes(),
            KvCmd::Delete { key } => format!("D\n{key}").into_bytes(),
        }
    }

    fn decode_command(bytes: &[u8]) -> Result<KvCmd, DecodeError> {
        let s = std::str::from_utf8(bytes).map_err(|e| DecodeError::new(e.to_string()))?;
        let mut parts = s.splitn(3, '\n');
        let tag = parts.next().ok_or_else(|| DecodeError::new("empty"))?;
        match tag {
            "S" => {
                let key = parts.next().ok_or_else(|| DecodeError::new("missing key"))?;
                let value = parts.next().ok_or_else(|| DecodeError::new("missing value"))?;
                Ok(KvCmd::Set {
                    key: key.into(),
                    value: value.into(),
                })
            }
            "D" => {
                let key = parts.next().ok_or_else(|| DecodeError::new("missing key"))?;
                Ok(KvCmd::Delete { key: key.into() })
            }
            other => Err(DecodeError::new(format!("unknown tag: {other}"))),
        }
    }

    fn apply(&mut self, cmd: KvCmd) -> Option<String> {
        match cmd {
            KvCmd::Set { key, value } => self.map.insert(key, value),
            KvCmd::Delete { key } => self.map.remove(&key),
        }
    }
}

#[test]
fn state_machine_command_round_trips_through_encode_decode() {
    let cmd = KvCmd::Set {
        key: "hello".into(),
        value: "world".into(),
    };
    let bytes = Kv::encode_command(&cmd);
    let round = Kv::decode_command(&bytes).unwrap();
    assert_eq!(cmd, round);
}

#[test]
fn state_machine_decode_error_is_propagated() {
    let err = Kv::decode_command(b"X\nkey").unwrap_err();
    assert!(err.reason.contains("unknown tag"));
}

#[test]
fn state_machine_apply_returns_previous_value_for_set() {
    let mut kv = Kv::default();
    assert_eq!(kv.apply(KvCmd::Set { key: "k".into(), value: "v1".into() }), None);
    assert_eq!(
        kv.apply(KvCmd::Set { key: "k".into(), value: "v2".into() }),
        Some("v1".into()),
    );
    assert_eq!(kv.apply(KvCmd::Delete { key: "k".into() }), Some("v2".into()));
    assert_eq!(kv.apply(KvCmd::Delete { key: "k".into() }), None);
}

#[test]
fn default_snapshot_returns_empty_bytes() {
    let kv = Kv::default();
    assert!(kv.snapshot().is_empty());
}

// ---------------------------------------------------------------------------
// Toy storage
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct MemoryStorage<C> {
    hard_state: Option<StoredHardState>,
    snapshot: Option<StoredSnapshot>,
    log: Vec<LogEntry<C>>,
}

impl<C: Send + Clone + 'static> Storage<C> for MemoryStorage<C> {
    type Error = Infallible;

    async fn recover(&mut self) -> Result<jotun::storage::RecoveredState<C>, Self::Error> {
        Ok(jotun::storage::RecoveredState {
            hard_state: self.hard_state.clone(),
            snapshot: self.snapshot.clone(),
            log: self.log.clone(),
        })
    }

    async fn persist_hard_state(&mut self, state: StoredHardState) -> Result<(), Self::Error> {
        self.hard_state = Some(state);
        Ok(())
    }

    async fn append_log(&mut self, entries: Vec<LogEntry<C>>) -> Result<(), Self::Error> {
        for entry in entries {
            let i = entry.id.index.get();
            let snap_floor = self.snapshot.as_ref().map_or(0, |s| s.last_included_index.get());
            if i <= snap_floor {
                continue;
            }
            let local_idx = (i - snap_floor - 1) as usize;
            if local_idx < self.log.len() {
                self.log[local_idx] = entry;
            } else {
                self.log.push(entry);
            }
        }
        Ok(())
    }

    async fn truncate_log(&mut self, from: LogIndex) -> Result<(), Self::Error> {
        let snap_floor = self.snapshot.as_ref().map_or(0, |s| s.last_included_index.get());
        let local = from.get().saturating_sub(snap_floor + 1) as usize;
        if local < self.log.len() {
            self.log.truncate(local);
        }
        Ok(())
    }

    async fn persist_snapshot(&mut self, snap: StoredSnapshot) -> Result<(), Self::Error> {
        let drop_through = snap.last_included_index.get() as usize;
        let keep_from = drop_through.min(self.log.len());
        self.log.drain(..keep_from);
        self.snapshot = Some(snap);
        Ok(())
    }
}

fn nid(n: u64) -> NodeId {
    NodeId::new(n).unwrap()
}

#[tokio::test]
async fn memory_storage_round_trips_hard_state() {
    let mut s: MemoryStorage<Vec<u8>> = MemoryStorage::default();
    s.persist_hard_state(StoredHardState {
        current_term: Term::new(7),
        voted_for: Some(nid(2)),
    })
    .await
    .unwrap();
    let r = s.recover().await.unwrap();
    assert_eq!(
        r.hard_state,
        Some(StoredHardState {
            current_term: Term::new(7),
            voted_for: Some(nid(2)),
        })
    );
}

#[tokio::test]
async fn memory_storage_round_trips_log_with_snapshot_floor() {
    use jotun_core::types::log::LogId;
    use jotun_core::{LogEntry, LogPayload};
    let mut s: MemoryStorage<Vec<u8>> = MemoryStorage::default();
    let entries: Vec<LogEntry<Vec<u8>>> = (1..=5)
        .map(|i| LogEntry {
            id: LogId::new(LogIndex::new(i), Term::new(1)),
            payload: LogPayload::Command(vec![i as u8]),
        })
        .collect();
    s.append_log(entries.clone()).await.unwrap();

    s.persist_snapshot(StoredSnapshot {
        last_included_index: LogIndex::new(3),
        last_included_term: Term::new(1),
        bytes: b"snap".to_vec(),
    })
    .await
    .unwrap();

    let r = s.recover().await.unwrap();
    assert_eq!(r.snapshot.as_ref().unwrap().last_included_index, LogIndex::new(3));
    assert_eq!(r.log.len(), 2);
    assert_eq!(r.log[0].id.index, LogIndex::new(4));
    assert_eq!(r.log[1].id.index, LogIndex::new(5));
}

// ---------------------------------------------------------------------------
// Trait bounds: confirm impls satisfy Send + 'static.
// ---------------------------------------------------------------------------

#[allow(dead_code)]
fn assert_send_static<T: Send + 'static>() {}

#[test]
fn impls_are_send_static() {
    assert_send_static::<Kv>();
    assert_send_static::<MemoryStorage<Vec<u8>>>();
    // Engine carries Incoming/Message between threads via Action::Send.
    let _: Option<Incoming<Vec<u8>>> = None;
    let _: Option<Message<Vec<u8>>> = None;
}
