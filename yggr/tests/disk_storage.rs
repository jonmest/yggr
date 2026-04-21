//! End-to-end tests for [`yggr::DiskStorage`]: round-trip every kind
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

use yggr::{DiskStorage, NodeId, Storage, StoredHardState, StoredSnapshot};
use yggr_core::types::log::LogId;
use yggr_core::{LogEntry, LogIndex, LogPayload, Term};

/// Per-test temp dir, cleaned up on drop. No external dep.
struct TmpDir(PathBuf);

impl TmpDir {
    fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("yggr-disk-test-{pid}-{n}"));
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

async fn open_segmented(tmp: &TmpDir, segment_target_bytes: u64) -> DiskStorage {
    DiskStorage::open_with_segment_target(&tmp.0, segment_target_bytes)
        .await
        .unwrap()
}

fn segment_names(tmp: &TmpDir) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(tmp.0.join("log"))
        .unwrap()
        .map(|entry| {
            entry
                .unwrap()
                .file_name()
                .into_string()
                .expect("segment filename is valid utf-8")
        })
        .collect();
    names.sort();
    names
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
    let r: yggr::storage::RecoveredState<Vec<u8>> =
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

    let r: yggr::storage::RecoveredState<Vec<u8>> =
        <DiskStorage as Storage<Vec<u8>>>::recover(&mut s)
            .await
            .unwrap();
    assert_eq!(r.log, entries[..2].to_vec());
}

#[tokio::test]
async fn append_log_overwrite_drops_stale_tail() {
    let tmp = TmpDir::new();
    let mut s: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    <DiskStorage as Storage<Vec<u8>>>::append_log(
        &mut s,
        vec![
            entry(1, 1, b"a"),
            entry(2, 1, b"b"),
            entry(3, 2, b"c"),
            entry(4, 2, b"d"),
        ],
    )
    .await
    .unwrap();

    <DiskStorage as Storage<Vec<u8>>>::append_log(&mut s, vec![entry(3, 3, b"x")])
        .await
        .unwrap();

    drop(s);
    let mut reopened: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    let recovered = <DiskStorage as Storage<Vec<u8>>>::recover(&mut reopened)
        .await
        .unwrap();
    assert_eq!(
        recovered.log,
        vec![entry(1, 1, b"a"), entry(2, 1, b"b"), entry(3, 3, b"x")]
    );
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
            peers: std::collections::BTreeSet::new(),
            bytes: b"snapshot-bytes".to_vec(),
        },
    )
    .await
    .unwrap();

    drop(s);
    let mut s2: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    let r: yggr::storage::RecoveredState<Vec<u8>> =
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
            peers: std::collections::BTreeSet::new(),
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
    let r: yggr::storage::RecoveredState<Vec<u8>> =
        <DiskStorage as Storage<Vec<u8>>>::recover(&mut s2)
            .await
            .unwrap();
    assert_eq!(r.log, vec![entry(3, 1, b"c")]);
}

#[tokio::test]
async fn segmented_log_rolls_over_to_new_files() {
    let tmp = TmpDir::new();
    let mut s = open_segmented(&tmp, 1).await;

    <DiskStorage as Storage<Vec<u8>>>::append_log(
        &mut s,
        vec![entry(1, 1, b"a"), entry(2, 1, b"b"), entry(3, 1, b"c")],
    )
    .await
    .unwrap();

    assert_eq!(
        segment_names(&tmp),
        vec![
            "000000000000001.log".to_string(),
            "000000000000002.log".to_string(),
            "000000000000003.log".to_string()
        ]
    );

    drop(s);
    let mut reopened = open_segmented(&tmp, 1).await;
    let recovered = <DiskStorage as Storage<Vec<u8>>>::recover(&mut reopened)
        .await
        .unwrap();
    assert_eq!(
        recovered.log,
        vec![entry(1, 1, b"a"), entry(2, 1, b"b"), entry(3, 1, b"c")]
    );
}

#[tokio::test]
async fn truncate_drops_whole_segment_files() {
    let tmp = TmpDir::new();
    let mut s = open_segmented(&tmp, 1).await;
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

    assert_eq!(
        segment_names(&tmp),
        vec![
            "000000000000001.log".to_string(),
            "000000000000002.log".to_string()
        ]
    );

    drop(s);
    let mut reopened = open_segmented(&tmp, 1).await;
    let recovered = <DiskStorage as Storage<Vec<u8>>>::recover(&mut reopened)
        .await
        .unwrap();
    assert_eq!(recovered.log, entries[..2].to_vec());
}

#[tokio::test]
async fn snapshot_drops_whole_segment_files() {
    let tmp = TmpDir::new();
    let mut s = open_segmented(&tmp, 1).await;
    let entries = vec![
        entry(1, 1, b"a"),
        entry(2, 1, b"b"),
        entry(3, 1, b"c"),
        entry(4, 1, b"d"),
    ];
    <DiskStorage as Storage<Vec<u8>>>::append_log(&mut s, entries)
        .await
        .unwrap();

    <DiskStorage as Storage<Vec<u8>>>::persist_snapshot(
        &mut s,
        StoredSnapshot {
            last_included_index: LogIndex::new(3),
            last_included_term: Term::new(1),
            peers: std::collections::BTreeSet::new(),
            bytes: b"snap".to_vec(),
        },
    )
    .await
    .unwrap();

    assert_eq!(segment_names(&tmp), vec!["000000000000004.log".to_string()]);

    drop(s);
    let mut reopened = open_segmented(&tmp, 1).await;
    let recovered = <DiskStorage as Storage<Vec<u8>>>::recover(&mut reopened)
        .await
        .unwrap();
    assert_eq!(recovered.log, vec![entry(4, 1, b"d")]);
}

#[tokio::test]
async fn truncate_on_empty_storage_is_a_noop() {
    // Exercises the `self.segments.is_empty()` early-return in
    // truncate_log — the NotFound guard is never hit because we never
    // try to remove anything.
    let tmp = TmpDir::new();
    let mut s: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    <DiskStorage as Storage<Vec<u8>>>::truncate_log(&mut s, LogIndex::new(1))
        .await
        .unwrap();
    let r = <DiskStorage as Storage<Vec<u8>>>::recover(&mut s)
        .await
        .unwrap();
    assert!(r.log.is_empty());
}

#[tokio::test]
async fn truncate_tolerates_externally_deleted_segments() {
    // Simulate a racing external actor that deletes a segment file
    // between append and truncate — truncate must treat NotFound as
    // "already gone" and succeed rather than erroring out. This
    // exercises the `Err(e) if e.kind() == NotFound => {}` guard in
    // truncate_log's segment-removal loop.
    let tmp = TmpDir::new();
    let mut s = open_segmented(&tmp, 1).await;
    <DiskStorage as Storage<Vec<u8>>>::append_log(
        &mut s,
        vec![
            entry(1, 1, b"a"),
            entry(2, 1, b"b"),
            entry(3, 1, b"c"),
            entry(4, 1, b"d"),
        ],
    )
    .await
    .unwrap();

    // Nuke segments 3 and 4 on disk before truncate can see them. The
    // storage still has them in its in-memory segment index.
    std::fs::remove_file(tmp.0.join("log/000000000000003.log")).unwrap();
    std::fs::remove_file(tmp.0.join("log/000000000000004.log")).unwrap();

    // Truncating from 3 would normally remove those two files; both
    // are already gone, so the NotFound arm should swallow the error.
    <DiskStorage as Storage<Vec<u8>>>::truncate_log(&mut s, LogIndex::new(3))
        .await
        .unwrap();
}

#[tokio::test]
async fn snapshot_tolerates_externally_deleted_segment_files() {
    // persist_snapshot removes every segment fully below the floor;
    // if a segment was externally deleted its removal must not error.
    // Exercises the NotFound guard in the persist_snapshot remove loop.
    let tmp = TmpDir::new();
    let mut s = open_segmented(&tmp, 1).await;
    <DiskStorage as Storage<Vec<u8>>>::append_log(
        &mut s,
        vec![
            entry(1, 1, b"a"),
            entry(2, 1, b"b"),
            entry(3, 1, b"c"),
            entry(4, 1, b"d"),
        ],
    )
    .await
    .unwrap();

    std::fs::remove_file(tmp.0.join("log/000000000000001.log")).unwrap();
    std::fs::remove_file(tmp.0.join("log/000000000000002.log")).unwrap();

    <DiskStorage as Storage<Vec<u8>>>::persist_snapshot(
        &mut s,
        StoredSnapshot {
            last_included_index: LogIndex::new(3),
            last_included_term: Term::new(1),
            peers: std::collections::BTreeSet::new(),
            bytes: b"snap".to_vec(),
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn snapshot_straddle_rewrites_to_new_start_index() {
    // The straddle-rewrite path renames a segment when the surviving
    // entries start at a higher index than the file's start_index.
    // With a single huge segment containing [1,2,3] and floor=2, the
    // rewrite writes a new ..003.log and removes ..001.log.
    let tmp = TmpDir::new();
    let mut s = open_segmented(&tmp, u64::MAX).await;
    <DiskStorage as Storage<Vec<u8>>>::append_log(
        &mut s,
        vec![entry(1, 1, b"a"), entry(2, 1, b"b"), entry(3, 1, b"c")],
    )
    .await
    .unwrap();

    <DiskStorage as Storage<Vec<u8>>>::persist_snapshot(
        &mut s,
        StoredSnapshot {
            last_included_index: LogIndex::new(2),
            last_included_term: Term::new(1),
            peers: std::collections::BTreeSet::new(),
            bytes: b"snap".to_vec(),
        },
    )
    .await
    .unwrap();

    // File was renamed from 000...001.log to 000...003.log.
    assert_eq!(segment_names(&tmp), vec!["000000000000003.log".to_string()]);
    drop(s);
    let mut reopened = open_segmented(&tmp, u64::MAX).await;
    let recovered = <DiskStorage as Storage<Vec<u8>>>::recover(&mut reopened)
        .await
        .unwrap();
    assert_eq!(recovered.log, vec![entry(3, 1, b"c")]);
}

#[tokio::test]
async fn snapshot_with_peers_round_trips() {
    // Exercises the peer-encoding loop (>0 peers) in persist_snapshot
    // meta and the matching decode loop in read_snapshot.
    let tmp = TmpDir::new();
    let mut s: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    let mut peers = std::collections::BTreeSet::new();
    peers.insert(nid(1));
    peers.insert(nid(3));
    peers.insert(nid(7));
    <DiskStorage as Storage<Vec<u8>>>::persist_snapshot(
        &mut s,
        StoredSnapshot {
            last_included_index: LogIndex::new(5),
            last_included_term: Term::new(2),
            peers: peers.clone(),
            bytes: b"snapped".to_vec(),
        },
    )
    .await
    .unwrap();

    drop(s);
    let mut s2: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    let r = <DiskStorage as Storage<Vec<u8>>>::recover(&mut s2)
        .await
        .unwrap();
    let snap = r.snapshot.expect("snapshot present");
    assert_eq!(snap.peers, peers);
    assert_eq!(snap.bytes, b"snapped");
}

#[tokio::test]
async fn snapshot_covers_only_segment_leaves_empty_tail() {
    // last_included_index covers every entry in the single existing
    // segment — triggers the `is_only` branch of the "None new_start"
    // arm in the straddle rewrite.
    let tmp = TmpDir::new();
    let mut s = open_segmented(&tmp, u64::MAX).await;
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
            peers: std::collections::BTreeSet::new(),
            bytes: b"snap".to_vec(),
        },
    )
    .await
    .unwrap();

    // Appends resume with the correct index on the emptied tail.
    <DiskStorage as Storage<Vec<u8>>>::append_log(&mut s, vec![entry(3, 2, b"c")])
        .await
        .unwrap();

    drop(s);
    let mut reopened = open_segmented(&tmp, u64::MAX).await;
    let recovered = <DiskStorage as Storage<Vec<u8>>>::recover(&mut reopened)
        .await
        .unwrap();
    assert_eq!(recovered.log, vec![entry(3, 2, b"c")]);
}

#[tokio::test]
async fn rescan_creates_missing_log_dir() {
    // Wipe the log/ dir between open and recover so rescan_segments
    // hits its NotFound + create_dir_all branch.
    let tmp = TmpDir::new();
    let mut s: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    std::fs::remove_dir_all(tmp.0.join("log")).unwrap();
    let r = <DiskStorage as Storage<Vec<u8>>>::recover(&mut s)
        .await
        .unwrap();
    assert!(r.log.is_empty());
    assert!(tmp.0.join("log").exists());
}

#[tokio::test]
async fn rescan_ignores_and_cleans_stray_tmp_files() {
    // Drop a .tmp file into log/ to exercise the `tmp -> remove_file`
    // cleanup path in rescan_segments. Also drop a file whose name
    // doesn't parse as a segment, to exercise the parse-None continue.
    let tmp = TmpDir::new();
    let mut s: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    std::fs::write(tmp.0.join("log").join("stray.tmp"), b"junk").unwrap();
    std::fs::write(tmp.0.join("log").join("not-a-segment.txt"), b"junk").unwrap();
    let r = <DiskStorage as Storage<Vec<u8>>>::recover(&mut s)
        .await
        .unwrap();
    assert!(r.log.is_empty());
    assert!(!tmp.0.join("log").join("stray.tmp").exists());
    // The non-matching file is ignored but left in place.
    assert!(tmp.0.join("log").join("not-a-segment.txt").exists());
}

#[tokio::test]
async fn disk_storage_error_display_and_source() {
    use std::error::Error as _;
    // Cover the Malformed variant explicitly — we can construct it by
    // corrupting hard_state.bin to a bad tag and trying to recover.
    let tmp = TmpDir::new();
    let mut s: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    <DiskStorage as Storage<Vec<u8>>>::persist_hard_state(
        &mut s,
        StoredHardState {
            current_term: Term::new(1),
            voted_for: None,
        },
    )
    .await
    .unwrap();
    let hs = tmp.0.join("hard_state.bin");
    let mut bytes = std::fs::read(&hs).unwrap();
    bytes[8] = 9; // bad tag
    std::fs::write(&hs, &bytes).unwrap();
    let err = <DiskStorage as Storage<Vec<u8>>>::recover(&mut s)
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("malformed"), "got {msg}");
    // Malformed has no source.
    assert!(err.source().is_none());
}

#[tokio::test]
async fn malformed_hard_state_wrong_size_rejected() {
    let tmp = TmpDir::new();
    std::fs::create_dir_all(&tmp.0).unwrap();
    std::fs::write(tmp.0.join("hard_state.bin"), b"too short").unwrap();
    let mut s: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    let err = <DiskStorage as Storage<Vec<u8>>>::recover(&mut s)
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("wrong size"));
}

#[tokio::test]
async fn malformed_snapshot_meta_too_short_rejected() {
    let tmp = TmpDir::new();
    std::fs::create_dir_all(&tmp.0).unwrap();
    std::fs::write(tmp.0.join("snapshot_meta.bin"), b"short").unwrap();
    std::fs::write(tmp.0.join("snapshot.bin"), b"").unwrap();
    let mut s: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    let err = <DiskStorage as Storage<Vec<u8>>>::recover(&mut s)
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("too short"));
}

#[tokio::test]
async fn malformed_snapshot_meta_size_mismatch_rejected() {
    // 20 bytes of header claiming peer_count=2 but no peer bytes.
    let tmp = TmpDir::new();
    std::fs::create_dir_all(&tmp.0).unwrap();
    let mut meta = Vec::new();
    meta.extend_from_slice(&1u64.to_le_bytes()); // index
    meta.extend_from_slice(&1u64.to_le_bytes()); // term
    meta.extend_from_slice(&2u32.to_le_bytes()); // peer_count
    std::fs::write(tmp.0.join("snapshot_meta.bin"), &meta).unwrap();
    std::fs::write(tmp.0.join("snapshot.bin"), b"").unwrap();
    let mut s: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    let err = <DiskStorage as Storage<Vec<u8>>>::recover(&mut s)
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("size mismatch"));
}

#[tokio::test]
async fn append_log_empty_is_noop() {
    // Exercises the fast-path early return in append_log.
    let tmp = TmpDir::new();
    let mut s: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();
    <DiskStorage as Storage<Vec<u8>>>::append_log(&mut s, vec![])
        .await
        .unwrap();
    let r = <DiskStorage as Storage<Vec<u8>>>::recover(&mut s)
        .await
        .unwrap();
    assert!(r.log.is_empty());
    // No segment file was created.
    assert!(segment_names(&tmp).is_empty());
}

#[tokio::test]
async fn recover_ignores_torn_tail_frame() {
    let tmp = TmpDir::new();
    let mut s = open_segmented(&tmp, u64::MAX).await;

    <DiskStorage as Storage<Vec<u8>>>::append_log(
        &mut s,
        vec![entry(1, 1, b"a"), entry(2, 1, b"b"), entry(3, 1, b"c")],
    )
    .await
    .unwrap();
    drop(s);

    let segment_path = tmp.0.join("log/000000000000001.log");
    let bytes = std::fs::read(&segment_path).unwrap();
    assert!(bytes.len() > 1, "segment must contain at least one frame");
    std::fs::write(&segment_path, &bytes[..bytes.len() - 1]).unwrap();

    let mut reopened = open_segmented(&tmp, u64::MAX).await;
    let recovered = <DiskStorage as Storage<Vec<u8>>>::recover(&mut reopened)
        .await
        .unwrap();
    assert_eq!(recovered.log, vec![entry(1, 1, b"a"), entry(2, 1, b"b")]);
}

// ---------------------------------------------------------------------------
// Orphaned-file sweep on open(): stale `.tmp` files in the data dir
// root (from a crash mid-atomic-rename) get removed so they don't
// accumulate on disk.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn open_removes_root_tmp_files_from_prior_crash() {
    let tmp = TmpDir::new();
    // Pre-create the data dir so we can drop a stray tmp before open.
    std::fs::create_dir_all(&tmp.0).unwrap();
    let stale_snapshot_tmp = tmp.0.join("snapshot.bin.tmp");
    let stale_meta_tmp = tmp.0.join("snapshot_meta.bin.tmp");
    std::fs::write(&stale_snapshot_tmp, b"partial snapshot bytes").unwrap();
    std::fs::write(&stale_meta_tmp, b"partial meta").unwrap();
    assert!(stale_snapshot_tmp.exists());
    assert!(stale_meta_tmp.exists());

    let _s: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();

    assert!(
        !stale_snapshot_tmp.exists(),
        "open() must sweep orphaned snapshot.bin.tmp",
    );
    assert!(
        !stale_meta_tmp.exists(),
        "open() must sweep orphaned snapshot_meta.bin.tmp",
    );
}

#[tokio::test]
async fn open_leaves_real_files_untouched() {
    let tmp = TmpDir::new();
    std::fs::create_dir_all(&tmp.0).unwrap();
    let real_snapshot = tmp.0.join("snapshot.bin");
    std::fs::write(&real_snapshot, b"real snapshot").unwrap();

    let _s: DiskStorage = DiskStorage::open(&tmp.0).await.unwrap();

    assert!(real_snapshot.exists(), "real files must survive open()");
}
