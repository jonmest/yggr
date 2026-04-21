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

use std::error::Error as _;
use yggr::{
    DecodeError, NodeId, StateMachine, Storage, StoredHardState, StoredSnapshot, Transport,
};
use yggr_core::{Incoming, LogEntry, LogIndex, Message, Term};

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
                let key = parts
                    .next()
                    .ok_or_else(|| DecodeError::new("missing key"))?;
                let value = parts
                    .next()
                    .ok_or_else(|| DecodeError::new("missing value"))?;
                Ok(KvCmd::Set {
                    key: key.into(),
                    value: value.into(),
                })
            }
            "D" => {
                let key = parts
                    .next()
                    .ok_or_else(|| DecodeError::new("missing key"))?;
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
    assert_eq!(
        kv.apply(KvCmd::Set {
            key: "k".into(),
            value: "v1".into()
        }),
        None
    );
    assert_eq!(
        kv.apply(KvCmd::Set {
            key: "k".into(),
            value: "v2".into()
        }),
        Some("v1".into()),
    );
    assert_eq!(
        kv.apply(KvCmd::Delete { key: "k".into() }),
        Some("v2".into())
    );
    assert_eq!(kv.apply(KvCmd::Delete { key: "k".into() }), None);
}

#[test]
fn default_snapshot_returns_empty_bytes() {
    let kv = Kv::default();
    assert!(kv.snapshot().unwrap().is_empty());
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

    async fn recover(&mut self) -> Result<yggr::storage::RecoveredState<C>, Self::Error> {
        Ok(yggr::storage::RecoveredState {
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
        let mut truncated = false;
        for entry in entries {
            let i = entry.id.index.get();
            let snap_floor = self
                .snapshot
                .as_ref()
                .map_or(0, |s| s.last_included_index.get());
            if i <= snap_floor {
                continue;
            }
            let local_idx = (i - snap_floor - 1) as usize;
            if !truncated {
                let keep = local_idx.min(self.log.len());
                self.log.truncate(keep);
                truncated = true;
            }
            self.log.push(entry);
        }
        Ok(())
    }

    async fn truncate_log(&mut self, from: LogIndex) -> Result<(), Self::Error> {
        let snap_floor = self
            .snapshot
            .as_ref()
            .map_or(0, |s| s.last_included_index.get());
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
    use yggr_core::types::log::LogId;
    use yggr_core::{LogEntry, LogPayload};
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
        peers: std::collections::BTreeSet::new(),
        bytes: b"snap".to_vec(),
    })
    .await
    .unwrap();

    let r = s.recover().await.unwrap();
    assert_eq!(
        r.snapshot.as_ref().unwrap().last_included_index,
        LogIndex::new(3)
    );
    assert_eq!(r.log.len(), 2);
    assert_eq!(r.log[0].id.index, LogIndex::new(4));
    assert_eq!(r.log[1].id.index, LogIndex::new(5));
}

#[tokio::test]
async fn memory_storage_append_log_overwrite_drops_stale_tail() {
    use yggr_core::types::log::LogId;
    use yggr_core::{LogEntry, LogPayload};

    let mut s: MemoryStorage<Vec<u8>> = MemoryStorage::default();
    s.append_log(vec![
        LogEntry {
            id: LogId::new(LogIndex::new(1), Term::new(1)),
            payload: LogPayload::Command(b"a".to_vec()),
        },
        LogEntry {
            id: LogId::new(LogIndex::new(2), Term::new(1)),
            payload: LogPayload::Command(b"b".to_vec()),
        },
        LogEntry {
            id: LogId::new(LogIndex::new(3), Term::new(2)),
            payload: LogPayload::Command(b"c".to_vec()),
        },
        LogEntry {
            id: LogId::new(LogIndex::new(4), Term::new(2)),
            payload: LogPayload::Command(b"d".to_vec()),
        },
    ])
    .await
    .unwrap();

    s.append_log(vec![LogEntry {
        id: LogId::new(LogIndex::new(3), Term::new(3)),
        payload: LogPayload::Command(b"x".to_vec()),
    }])
    .await
    .unwrap();

    let recovered = s.recover().await.unwrap();
    assert_eq!(recovered.log.len(), 3);
    assert_eq!(recovered.log[0].id.index, LogIndex::new(1));
    assert_eq!(recovered.log[1].id.index, LogIndex::new(2));
    assert_eq!(recovered.log[2].id.index, LogIndex::new(3));
    assert_eq!(recovered.log[2].payload, LogPayload::Command(b"x".to_vec()));
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

// ---------------------------------------------------------------------------
// state_machine.rs direct coverage
// ---------------------------------------------------------------------------

#[test]
fn decode_error_new_and_display() {
    let e = DecodeError::new("bad input");
    assert_eq!(e.reason, "bad input");
    let s = format!("{e}");
    assert!(s.contains("command decode error"));
    assert!(s.contains("bad input"));
    // Error::source is the default (None).
    assert!(e.source().is_none());
    // Clone + Eq round-trip.
    let e2 = e.clone();
    assert_eq!(e, e2);
}

/// State machine that opts out of `snapshot()` and relies on the default
/// `restore()` panic.
struct NoSnapshotKv;

impl StateMachine for NoSnapshotKv {
    type Command = Vec<u8>;
    type Response = ();
    fn encode_command(c: &Self::Command) -> Vec<u8> {
        c.clone()
    }
    fn decode_command(bytes: &[u8]) -> Result<Self::Command, DecodeError> {
        Ok(bytes.to_vec())
    }
    fn apply(&mut self, _: Self::Command) -> Self::Response {}
}

#[test]
#[should_panic(expected = "StateMachine::restore not implemented")]
fn default_restore_panics_with_documented_message() {
    let mut sm = NoSnapshotKv;
    sm.restore(b"anything".to_vec());
}

#[test]
fn default_snapshot_for_custom_impl_is_empty() {
    let sm = NoSnapshotKv;
    assert!(sm.snapshot().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// transport::Transport default shutdown() — covers the 3-line default body.
// ---------------------------------------------------------------------------

struct NopTransport;

impl Transport<Vec<u8>> for NopTransport {
    type Error = Infallible;
    async fn send(&self, _to: NodeId, _message: Message<Vec<u8>>) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn recv(&mut self) -> Option<Incoming<Vec<u8>>> {
        None
    }
    // Intentionally does NOT override `shutdown` — exercises the default.
}

#[tokio::test]
async fn transport_default_shutdown_is_a_noop() {
    let mut t = NopTransport;
    // Default impl returns () immediately.
    t.shutdown().await;
    // And recv still returns None.
    assert!(t.recv().await.is_none());
    t.send(
        nid(1),
        Message::VoteRequest(yggr_core::RequestVote {
            term: Term::new(1),
            candidate_id: nid(1),
            last_log_id: None,
        }),
    )
    .await
    .unwrap();
}
