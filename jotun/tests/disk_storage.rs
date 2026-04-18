//! End-to-end tests for [`jotun::DiskStorage`]: round-trip every kind
//! of write through an actual filesystem and confirm `recover` returns
//! exactly what we wrote.

#![allow(
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    clippy::missing_const_for_fn,
    clippy::unwrap_used,
    clippy::expect_used
)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use jotun::{DiskStorage, NodeId, Storage, StoredHardState, StoredSnapshot};
use jotun_core::types::log::LogId;
use jotun_core::{LogEntry, LogIndex, LogPayload, Term};

/// Per-test temp dir, cleaned up on drop. No external dep.
struct TmpDir(PathBuf);

impl TmpDir {
    fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("jotun-disk-test-{pid}-{n}"));
        let _ = std::fs::remove_dir_all(&path);
        Self(path)
    }
}

impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn nid(n: u64) -> NodeId {
    NodeId::new(n).unwrap()
}

fn entry(index: u64, term: u64, cmd: &[u8]) -> LogEntry<Vec<u8>> {
    LogEntry {
        id: LogId::new(LogIndex::new(index), Term::new(term)),
        payload: LogPayload::Command(cmd.to_vec()),
    }
}

#[tokio::test]
async fn fresh_recovery_returns_default_empty() {
    let tmp = TmpDir::new();
    let mut s: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    let r = <DiskStorage as Storage<Vec<u8>>>::recover(&mut s)
        .await
        .unwrap();
    assert!(r.hard_state.is_none());
    assert!(r.snapshot.is_none());
    assert!(r.log.is_empty());
}

#[tokio::test]
async fn hard_state_round_trips() {
    let tmp = TmpDir::new();
    let mut s: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    <DiskStorage as Storage<Vec<u8>>>::persist_hard_state(
        &mut s,
        StoredHardState {
            current_term: Term::new(42),
            voted_for: Some(nid(7)),
        },
    )
    .await
    .unwrap();

    // Re-open from disk to prove durability.
    drop(s);
    let mut s2: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    let r = <DiskStorage as Storage<Vec<u8>>>::recover(&mut s2)
        .await
        .unwrap();
    assert_eq!(
        r.hard_state,
        Some(StoredHardState {
            current_term: Term::new(42),
            voted_for: Some(nid(7)),
        })
    );
}

#[tokio::test]
async fn hard_state_with_no_vote_round_trips() {
    let tmp = TmpDir::new();
    let mut s: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    <DiskStorage as Storage<Vec<u8>>>::persist_hard_state(
        &mut s,
        StoredHardState {
            current_term: Term::new(3),
            voted_for: None,
        },
    )
    .await
    .unwrap();
    let r = <DiskStorage as Storage<Vec<u8>>>::recover(&mut s)
        .await
        .unwrap();
    assert_eq!(
        r.hard_state,
        Some(StoredHardState {
            current_term: Term::new(3),
            voted_for: None,
        })
    );
}

#[tokio::test]
async fn log_append_and_recover() {
    let tmp = TmpDir::new();
    let mut s: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    let entries = vec![entry(1, 1, b"a"), entry(2, 1, b"bb"), entry(3, 1, b"ccc")];
    <DiskStorage as Storage<Vec<u8>>>::append_log(&mut s, entries.clone())
        .await
        .unwrap();

    drop(s);
    let mut s2: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    let r: jotun::storage::RecoveredState<Vec<u8>> =
        <DiskStorage as Storage<Vec<u8>>>::recover(&mut s2)
            .await
            .unwrap();
    assert_eq!(r.log, entries);
}

#[tokio::test]
async fn log_truncate_drops_tail() {
    let tmp = TmpDir::new();
    let mut s: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    let entries = vec![
        entry(1, 1, b"a"),
        entry(2, 1, b"b"),
        entry(3, 1, b"c"),
        entry(4, 1, b"d"),
    ];
    <DiskStorage as Storage<Vec<u8>>>::append_log(&mut s, entries.clone())
        .await
        .unwrap();
    <DiskStorage as Storage<Vec<u8>>>::truncate_log(&mut s, LogIndex::new(3))
        .await
        .unwrap();

    let r: jotun::storage::RecoveredState<Vec<u8>> =
        <DiskStorage as Storage<Vec<u8>>>::recover(&mut s)
            .await
            .unwrap();
    assert_eq!(r.log, entries[..2].to_vec());
}

#[tokio::test]
async fn snapshot_replaces_log_prefix() {
    let tmp = TmpDir::new();
    let mut s: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    let entries = vec![
        entry(1, 1, b"a"),
        entry(2, 1, b"b"),
        entry(3, 1, b"c"),
        entry(4, 1, b"d"),
    ];
    <DiskStorage as Storage<Vec<u8>>>::append_log(&mut s, entries.clone())
        .await
        .unwrap();
    <DiskStorage as Storage<Vec<u8>>>::persist_snapshot(
        &mut s,
        StoredSnapshot {
            last_included_index: LogIndex::new(2),
            last_included_term: Term::new(1),
            bytes: b"snapshot-bytes".to_vec(),
        },
    )
    .await
    .unwrap();

    drop(s);
    let mut s2: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    let r: jotun::storage::RecoveredState<Vec<u8>> =
        <DiskStorage as Storage<Vec<u8>>>::recover(&mut s2)
            .await
            .unwrap();

    let snap = r.snapshot.expect("snapshot must be recovered");
    assert_eq!(snap.last_included_index, LogIndex::new(2));
    assert_eq!(snap.last_included_term, Term::new(1));
    assert_eq!(snap.bytes, b"snapshot-bytes");
    // Only entries 3 and 4 survive.
    assert_eq!(r.log, vec![entry(3, 1, b"c"), entry(4, 1, b"d")]);
}

#[tokio::test]
async fn appends_after_snapshot_use_correct_indices() {
    let tmp = TmpDir::new();
    let mut s: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    <DiskStorage as Storage<Vec<u8>>>::append_log(
        &mut s,
        vec![entry(1, 1, b"a"), entry(2, 1, b"b")],
    )
    .await
    .unwrap();
    <DiskStorage as Storage<Vec<u8>>>::persist_snapshot(
        &mut s,
        StoredSnapshot {
            last_included_index: LogIndex::new(2),
            last_included_term: Term::new(1),
            bytes: b"snap".to_vec(),
        },
    )
    .await
    .unwrap();
    // New entry at index 3 (above the floor).
    <DiskStorage as Storage<Vec<u8>>>::append_log(&mut s, vec![entry(3, 1, b"c")])
        .await
        .unwrap();

    drop(s);
    let mut s2: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    let r: jotun::storage::RecoveredState<Vec<u8>> =
        <DiskStorage as Storage<Vec<u8>>>::recover(&mut s2)
            .await
            .unwrap();
    assert_eq!(r.log, vec![entry(3, 1, b"c")]);
}
