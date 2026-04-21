#![allow(
    clippy::indexing_slicing, // we slice fixed-size byte buffers we just sized
    clippy::expect_used,      // ditto — bounds proven by the surrounding length checks
    clippy::single_match_else,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
//! Default on-disk [`crate::Storage`] implementation.
//!
//! Layout in the data directory:
//!
//! ```text
//! data_dir/
//!   hard_state.bin          // current_term + voted_for, ~17 bytes
//!   snapshot.bin            // raw snapshot bytes
//!   snapshot_meta.bin       // last_included_index + last_included_term
//!   log/
//!     000000000000001.log   // segment starting at log index 1
//!     000000001000001.log   // segment starting at log index 1000001
//!     ...
//! ```
//!
//! The log is sharded into segments. Each segment's filename is the
//! 1-based log index of its first entry, zero-padded to 15 digits so
//! lexicographic sort equals numeric sort. The on-disk frame format
//! inside a segment is `[u32 LE length][prost-encoded LogEntry]`,
//! the same stream we used in the pre-segmented single-file layout.
//!
//! Segmentation buys us cheap compaction: `truncate_log` and
//! `persist_snapshot` can drop whole segment files instead of
//! rewriting the entire log. Only the segment that straddles the cut
//! gets rewritten.
//!
//! All atomic-replace writes (hard state, snapshot, snapshot meta, and
//! segment rewrites/creations) go via a `*.tmp` sibling + rename, with
//! an fsync on the file then an fsync on the directory. The active tail
//! segment is fsynced after every `append_log` call — same durability
//! story as the pre-segmented impl.

use std::path::{Path, PathBuf};

use jotun_core::transport::protobuf as proto;
use jotun_core::{LogEntry, LogIndex, NodeId, Term};
use prost::Message as _;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

use crate::storage::{RecoveredState, Storage, StoredHardState, StoredSnapshot};

const HARD_STATE_FILE: &str = "hard_state.bin";
const SNAPSHOT_FILE: &str = "snapshot.bin";
const SNAPSHOT_META_FILE: &str = "snapshot_meta.bin";
const LOG_DIR: &str = "log";
const SEGMENT_EXT: &str = "log";
/// Width of the zero-padded starting-index in segment filenames.
/// 15 digits covers `u64::MAX / 1000` starting indices, which is more
/// than enough headroom for any realistic deployment.
const SEGMENT_INDEX_WIDTH: usize = 15;
/// Default rollover target. Segments grow past this by at most one
/// entry — we only check after appending a batch.
const DEFAULT_SEGMENT_TARGET_BYTES: u64 = 64 * 1024 * 1024;

/// A single log segment on disk.
#[derive(Debug, Clone)]
struct Segment {
    /// The 1-based index of the first entry in this segment.
    start_index: LogIndex,
    /// Absolute path to the segment file.
    path: PathBuf,
    /// Current size of the segment file in bytes. Updated as we append.
    size_bytes: u64,
}

/// Filesystem-backed [`Storage`].
///
/// `C` is the application command type — we round-trip commands
/// through `Vec<u8>` (the wire format), so `C` must support
/// `Into<Vec<u8>>` and `From<Vec<u8>>`. The vast majority of users
/// just use `Vec<u8>` directly and let their `StateMachine`'s
/// `encode_command` / `decode_command` do the conversion.
#[derive(Debug)]
pub struct DiskStorage {
    data_dir: PathBuf,
    /// In-memory segment index, sorted by `start_index` ascending. The
    /// last element (if any) is the open tail segment.
    segments: Vec<Segment>,
    /// Cached handle to the tail segment, opened lazily for append.
    tail_file: Option<File>,
    /// Cached snapshot floor so recovery / append can filter stale
    /// entries without re-reading the meta file.
    snapshot_floor: LogIndex,
    /// Target segment size; once a tail segment exceeds this after a
    /// write it gets rolled over.
    segment_target_bytes: u64,
}

impl DiskStorage {
    /// Open or create a `DiskStorage` in `data_dir`. Creates the data
    /// directory (and its `log/` subdirectory) if missing. Segment
    /// files are opened lazily on first write or by [`Self::recover`].
    pub async fn open(data_dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        Self::open_with_segment_target(data_dir, DEFAULT_SEGMENT_TARGET_BYTES).await
    }

    /// Open or create a `DiskStorage` in `data_dir` with a custom
    /// segment rollover target. Intended for tests that want to
    /// exercise segmentation without writing 64 MiB of log.
    pub async fn open_with_segment_target(
        data_dir: impl Into<PathBuf>,
        segment_target_bytes: u64,
    ) -> std::io::Result<Self> {
        let data_dir = data_dir.into();
        tokio::fs::create_dir_all(&data_dir).await?;
        tokio::fs::create_dir_all(data_dir.join(LOG_DIR)).await?;
        Ok(Self {
            data_dir,
            segments: Vec::new(),
            tail_file: None,
            snapshot_floor: LogIndex::ZERO,
            segment_target_bytes: segment_target_bytes.max(1),
        })
    }

    fn path(&self, name: &str) -> PathBuf {
        self.data_dir.join(name)
    }

    fn log_dir(&self) -> PathBuf {
        self.data_dir.join(LOG_DIR)
    }

    fn segment_path(&self, start_index: LogIndex) -> PathBuf {
        self.log_dir().join(segment_filename(start_index))
    }

    /// fsync the data dir so an atomic-rename inside it is durable.
    async fn fsync_dir(path: &Path) -> std::io::Result<()> {
        let dir = File::open(path).await?;
        dir.sync_all().await?;
        Ok(())
    }

    /// Atomic-replace `path` with `bytes`: write to `path.tmp`, fsync,
    /// rename, fsync the parent dir.
    async fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
        let tmp = tmp_sibling(path);
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .await?;
            f.write_all(bytes).await?;
            f.sync_all().await?;
        }
        tokio::fs::rename(&tmp, path).await?;
        if let Some(parent) = path.parent() {
            Self::fsync_dir(parent).await?;
        }
        Ok(())
    }

    /// Rescan `log/`, rebuild the in-memory segment index. Does not
    /// touch `tail_file`; caller is responsible for dropping any cached
    /// handle before this is called.
    async fn rescan_segments(&mut self) -> std::io::Result<()> {
        self.segments.clear();
        let dir = self.log_dir();
        let mut read = match tokio::fs::read_dir(&dir).await {
            Ok(r) => r,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tokio::fs::create_dir_all(&dir).await?;
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        let mut found: Vec<Segment> = Vec::new();
        while let Some(entry) = read.next_entry().await? {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            // Ignore stray temp files from a mid-rename crash.
            if Path::new(name).extension().is_some_and(|e| e == "tmp") {
                let _ = tokio::fs::remove_file(&path).await;
                continue;
            }
            let Some(start_index) = parse_segment_filename(name) else {
                continue;
            };
            let size_bytes = entry.metadata().await?.len();
            found.push(Segment {
                start_index,
                path,
                size_bytes,
            });
        }
        found.sort_by_key(|s| s.start_index);
        self.segments = found;
        Ok(())
    }

    /// Open (or create) the tail segment for append, caching the handle.
    /// Caller must ensure `self.segments` is non-empty.
    async fn open_tail_for_append(&mut self) -> std::io::Result<()> {
        if self.tail_file.is_some() {
            return Ok(());
        }
        let tail = self
            .segments
            .last()
            .expect("open_tail_for_append requires a tail segment");
        let f = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&tail.path)
            .await?;
        self.tail_file = Some(f);
        Ok(())
    }

    /// Ensure there is a tail segment starting at `start_index`. If the
    /// segment list is empty, creates the segment file on disk.
    async fn ensure_tail_segment(&mut self, start_index: LogIndex) -> std::io::Result<()> {
        if !self.segments.is_empty() {
            return Ok(());
        }
        let path = self.segment_path(start_index);
        // Touch the segment file so it shows up in `log/` even if the
        // first append is empty.
        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .await?;
        f.sync_all().await?;
        drop(f);
        Self::fsync_dir(&self.log_dir()).await?;
        self.segments.push(Segment {
            start_index,
            path,
            size_bytes: 0,
        });
        Ok(())
    }

    /// Roll over to a fresh tail segment starting at `start_index`.
    /// fsyncs and closes the previous tail, creates the new segment
    /// file, fsyncs the log dir so the new file is durable.
    async fn roll_new_segment(&mut self, start_index: LogIndex) -> std::io::Result<()> {
        if let Some(f) = self.tail_file.take() {
            f.sync_all().await?;
        }
        let path = self.segment_path(start_index);
        let f = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .await?;
        f.sync_all().await?;
        drop(f);
        Self::fsync_dir(&self.log_dir()).await?;
        self.segments.push(Segment {
            start_index,
            path,
            size_bytes: 0,
        });
        Ok(())
    }
}

/// Format a segment's filename from its starting index: zero-padded
/// so lexicographic sort matches numeric sort.
fn segment_filename(start_index: LogIndex) -> String {
    format!(
        "{:0width$}.{ext}",
        start_index.get(),
        width = SEGMENT_INDEX_WIDTH,
        ext = SEGMENT_EXT,
    )
}

/// Inverse of [`segment_filename`]. Returns `None` for names that do
/// not match the expected pattern.
fn parse_segment_filename(name: &str) -> Option<LogIndex> {
    let stem = name.strip_suffix(&format!(".{SEGMENT_EXT}"))?;
    if stem.len() != SEGMENT_INDEX_WIDTH {
        return None;
    }
    let n: u64 = stem.parse().ok()?;
    Some(LogIndex::new(n))
}

fn tmp_sibling(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    PathBuf::from(s)
}

/// Errors returned by [`DiskStorage`].
#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub enum DiskStorageError {
    Io(std::io::Error),
    Decode(prost::DecodeError),
    Malformed(&'static str),
}

impl std::fmt::Display for DiskStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io error: {e}"),
            Self::Decode(e) => write!(f, "protobuf decode error: {e}"),
            Self::Malformed(s) => write!(f, "malformed on-disk state: {s}"),
        }
    }
}

impl std::error::Error for DiskStorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Decode(e) => Some(e),
            Self::Malformed(_) => None,
        }
    }
}

impl From<std::io::Error> for DiskStorageError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<prost::DecodeError> for DiskStorageError {
    fn from(e: prost::DecodeError) -> Self {
        Self::Decode(e)
    }
}

impl<C> Storage<C> for DiskStorage
where
    C: Send + Clone + Into<Vec<u8>> + From<Vec<u8>> + 'static,
{
    type Error = DiskStorageError;

    async fn recover(&mut self) -> Result<RecoveredState<C>, Self::Error> {
        let hard_state = read_hard_state(&self.path(HARD_STATE_FILE)).await?;
        let snapshot =
            read_snapshot(&self.path(SNAPSHOT_FILE), &self.path(SNAPSHOT_META_FILE)).await?;
        if let Some(snap) = &snapshot {
            self.snapshot_floor = snap.last_included_index;
        }

        // Rebuild the in-memory segment index from whatever's on disk.
        self.tail_file = None;
        self.rescan_segments().await?;

        // Collect entries from every segment. `read_segment` tolerates
        // a torn tail-frame (from a mid-write crash) by stopping at the
        // last fully-decodable frame.
        let mut log = Vec::new();
        for seg in &self.segments {
            let entries = read_segment::<C>(&seg.path).await?;
            for e in entries {
                if e.id.index > self.snapshot_floor {
                    log.push(e);
                }
            }
        }

        Ok(RecoveredState {
            hard_state,
            snapshot,
            log,
        })
    }

    async fn persist_hard_state(&mut self, state: StoredHardState) -> Result<(), Self::Error> {
        // Layout: [u64 LE term][u8 has_voted_for][u64 LE voted_for_id]
        let mut buf = Vec::with_capacity(17);
        buf.extend_from_slice(&state.current_term.get().to_le_bytes());
        match state.voted_for {
            Some(id) => {
                buf.push(1);
                buf.extend_from_slice(&id.get().to_le_bytes());
            }
            None => {
                buf.push(0);
                buf.extend_from_slice(&0u64.to_le_bytes());
            }
        }
        Self::atomic_write(&self.path(HARD_STATE_FILE), &buf).await?;
        Ok(())
    }

    async fn append_log(&mut self, entries: Vec<LogEntry<C>>) -> Result<(), Self::Error> {
        if entries.is_empty() {
            return Ok(());
        }

        // `append_log` replaces the durable suffix starting at the
        // first incoming index. This keeps crash-recovery aligned with
        // the engine's in-memory follower truncation.
        <DiskStorage as Storage<C>>::truncate_log(self, entries[0].id.index).await?;

        // If we've never written before, seed the first segment with
        // the index of the first entry being appended.
        if self.segments.is_empty() {
            self.ensure_tail_segment(entries[0].id.index).await?;
        }
        self.open_tail_for_append().await?;

        // Pre-encode so we know each frame's size up-front; that makes
        // the rollover bookkeeping cleaner.
        let mut encoded: Vec<(LogIndex, Vec<u8>)> = Vec::with_capacity(entries.len());
        for e in entries {
            let idx = e.id.index;
            let proto_entry: proto::LogEntry = e.into();
            encoded.push((idx, proto_entry.encode_to_vec()));
        }

        for (idx, frame) in encoded {
            let len = u32::try_from(frame.len())
                .map_err(|_| DiskStorageError::Malformed("log entry exceeds 4 GiB"))?;
            let frame_size = u64::from(len) + 4;

            // Roll over if the current tail would cross the target
            // *and* it already has at least one entry. We never create
            // an empty segment just to hold one oversized frame.
            let needs_roll = {
                let tail = self
                    .segments
                    .last()
                    .expect("tail seeded by ensure_tail_segment above");
                tail.size_bytes > 0 && tail.size_bytes >= self.segment_target_bytes
            };
            if needs_roll {
                self.roll_new_segment(idx).await?;
                self.open_tail_for_append().await?;
            }

            let f = self.tail_file.as_mut().expect("opened above");
            f.write_all(&len.to_le_bytes()).await?;
            f.write_all(&frame).await?;
            let tail = self
                .segments
                .last_mut()
                .expect("tail present after rollover check");
            tail.size_bytes += frame_size;
        }

        // One fsync per append_log call, same cadence as the pre-
        // segmented impl.
        let f = self.tail_file.as_mut().expect("tail open");
        f.sync_all().await?;
        Ok(())
    }

    async fn truncate_log(&mut self, from: LogIndex) -> Result<(), Self::Error> {
        // Fast path: drop whole segments whose start_index >= from.
        // If `from` lands inside the segment that survives, rewrite
        // that one segment with the entries strictly less than `from`.
        if self.segments.is_empty() {
            return Ok(());
        }

        // Find the segment that would contain `from`. `straddler_pos`
        // points at the last segment with `start_index < from` (i.e.,
        // the one that might need a rewrite). Every segment after it
        // is dropped wholesale.
        let drop_from_idx = self
            .segments
            .iter()
            .position(|s| s.start_index >= from)
            .unwrap_or(self.segments.len());

        // Drop the whole-segment tail.
        self.tail_file = None;
        let to_remove: Vec<Segment> = self.segments.drain(drop_from_idx..).collect();
        for s in to_remove {
            match tokio::fs::remove_file(&s.path).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }

        // Rewrite the straddler (now the new tail, if any) keeping only
        // entries < `from`. Skip this if the straddler is empty or every
        // entry in it already survives.
        if let Some(last) = self.segments.last().cloned() {
            let kept = read_segment::<C>(&last.path)
                .await?
                .into_iter()
                .filter(|e| e.id.index < from)
                .collect::<Vec<_>>();
            let bytes = encode_frames(&kept)?;
            let new_size = bytes.len() as u64;
            Self::atomic_write(&last.path, &bytes).await?;
            if let Some(tail) = self.segments.last_mut() {
                tail.size_bytes = new_size;
            }
        }

        Self::fsync_dir(&self.log_dir()).await?;
        Ok(())
    }

    async fn persist_snapshot(&mut self, snap: StoredSnapshot) -> Result<(), Self::Error> {
        // Snapshot meta:
        //   [u64 LE index][u64 LE term][u32 LE peer_count][u64 LE peer_id × peer_count]
        // Peers are the committed cluster membership at
        // `last_included_index`; recovery reconstructs them from here.
        let peer_count = u32::try_from(snap.peers.len())
            .map_err(|_| DiskStorageError::Malformed("snapshot peer count exceeds 4B"))?;
        let mut meta = Vec::with_capacity(16 + 4 + (snap.peers.len() * 8));
        meta.extend_from_slice(&snap.last_included_index.get().to_le_bytes());
        meta.extend_from_slice(&snap.last_included_term.get().to_le_bytes());
        meta.extend_from_slice(&peer_count.to_le_bytes());
        for id in &snap.peers {
            meta.extend_from_slice(&id.get().to_le_bytes());
        }

        // Order: bytes first (large), meta second (small). Meta is the
        // commit-pointer. A crash between the two leaves orphan bytes
        // that recovery ignores.
        Self::atomic_write(&self.path(SNAPSHOT_FILE), &snap.bytes).await?;
        Self::atomic_write(&self.path(SNAPSHOT_META_FILE), &meta).await?;
        self.snapshot_floor = snap.last_included_index;

        // Drop every segment whose entire span is <= last_included_index.
        // A segment spans [start_index, next_start_index - 1]; for the
        // tail, we don't know the upper bound, so we only drop it if
        // start_index is fully below the floor AND there's a newer
        // segment above it (handled implicitly: only non-tail segments
        // have a known upper bound).
        //
        // Concrete rule used here:
        //   - If segment[i+1].start_index - 1 <= floor, segment[i] is
        //     fully covered -> drop it.
        //   - If segment[i] is the tail, we rewrite it in the straddle
        //     step below (cheap when tail is small; the tail IS the
        //     active write path).
        let floor = snap.last_included_index;
        let mut keep_from = 0usize;
        for i in 0..self.segments.len() {
            let next_start = self.segments.get(i + 1).map(|s| s.start_index);
            match next_start {
                Some(ns) => {
                    // segment i covers [start_index, ns.get() - 1]
                    if ns.get() > 0 && LogIndex::new(ns.get() - 1) <= floor {
                        keep_from = i + 1;
                    }
                }
                None => {
                    // tail — never dropped as a whole here; handled by
                    // straddle rewrite below. Break either way.
                    break;
                }
            }
        }
        if keep_from > 0 {
            self.tail_file = None;
            let to_remove: Vec<Segment> = self.segments.drain(..keep_from).collect();
            for s in to_remove {
                match tokio::fs::remove_file(&s.path).await {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }

        // Straddle rewrite: the first surviving segment may contain
        // entries at or below the floor that need to go. We also need
        // to rename the file since its start_index changes.
        if let Some(first) = self.segments.first().cloned() {
            let kept: Vec<LogEntry<C>> = read_segment::<C>(&first.path)
                .await?
                .into_iter()
                .filter(|e| e.id.index > floor)
                .collect();

            let new_start = kept.first().map(|e| e.id.index);
            match new_start {
                Some(new_start_idx) if new_start_idx == first.start_index => {
                    // Nothing below the floor in this segment and
                    // start_index is unchanged — no rewrite needed.
                }
                Some(new_start_idx) => {
                    // Some prefix was dropped; write a new segment file
                    // with the new starting index and remove the old.
                    let bytes = encode_frames(&kept)?;
                    let new_path = self.segment_path(new_start_idx);
                    Self::atomic_write(&new_path, &bytes).await?;
                    // Remove the old file (different name).
                    if new_path != first.path {
                        match tokio::fs::remove_file(&first.path).await {
                            Ok(()) => {}
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                            Err(e) => return Err(e.into()),
                        }
                    }
                    // Drop any cached tail handle — if this was the
                    // tail, its path just changed.
                    self.tail_file = None;
                    if let Some(s) = self.segments.first_mut() {
                        s.start_index = new_start_idx;
                        s.path = new_path;
                        s.size_bytes = bytes.len() as u64;
                    }
                }
                None => {
                    // Every entry in the segment is covered by the
                    // snapshot. If it's a non-tail segment, remove it
                    // outright — we don't need empty placeholders.
                    // If it's the tail AND the only segment, leave an
                    // empty file in place so appends can resume.
                    let is_only = self.segments.len() == 1;
                    if is_only {
                        // Rewrite empty. Keep the filename as-is; the
                        // next append will treat this as the tail.
                        Self::atomic_write(&first.path, &[]).await?;
                        self.tail_file = None;
                        if let Some(s) = self.segments.first_mut() {
                            s.size_bytes = 0;
                        }
                    } else {
                        match tokio::fs::remove_file(&first.path).await {
                            Ok(()) => {}
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                            Err(e) => return Err(e.into()),
                        }
                        self.segments.remove(0);
                        self.tail_file = None;
                    }
                }
            }
        }

        Self::fsync_dir(&self.log_dir()).await?;
        Ok(())
    }
}

/// Encode a slice of log entries into a length-prefixed byte stream.
fn encode_frames<C>(entries: &[LogEntry<C>]) -> Result<Vec<u8>, DiskStorageError>
where
    C: Clone + Into<Vec<u8>>,
{
    let mut out = Vec::new();
    for e in entries {
        let proto_entry: proto::LogEntry = e.clone().into();
        let frame = proto_entry.encode_to_vec();
        let len = u32::try_from(frame.len())
            .map_err(|_| DiskStorageError::Malformed("log entry exceeds 4 GiB"))?;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&frame);
    }
    Ok(out)
}

async fn read_hard_state(path: &Path) -> Result<Option<StoredHardState>, DiskStorageError> {
    let bytes = match tokio::fs::read(path).await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if bytes.len() != 17 {
        return Err(DiskStorageError::Malformed("hard_state.bin wrong size"));
    }
    let term_bytes: [u8; 8] = bytes[0..8].try_into().expect("8 bytes");
    let has = bytes[8];
    let id_bytes: [u8; 8] = bytes[9..17].try_into().expect("8 bytes");
    let term = Term::new(u64::from_le_bytes(term_bytes));
    let voted_for = match has {
        0 => None,
        1 => {
            let id = u64::from_le_bytes(id_bytes);
            Some(NodeId::new(id).ok_or(DiskStorageError::Malformed(
                "hard_state.bin voted_for is zero with has_voted_for=1",
            ))?)
        }
        _ => return Err(DiskStorageError::Malformed("hard_state.bin bad tag")),
    };
    Ok(Some(StoredHardState {
        current_term: term,
        voted_for,
    }))
}

async fn read_snapshot(
    bytes_path: &Path,
    meta_path: &Path,
) -> Result<Option<StoredSnapshot>, DiskStorageError> {
    let meta = match tokio::fs::read(meta_path).await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if meta.len() < 20 {
        return Err(DiskStorageError::Malformed("snapshot_meta.bin too short"));
    }
    let idx_bytes: [u8; 8] = meta[0..8].try_into().expect("8 bytes");
    let term_bytes: [u8; 8] = meta[8..16].try_into().expect("8 bytes");
    let count_bytes: [u8; 4] = meta[16..20].try_into().expect("4 bytes");
    let last_included_index = LogIndex::new(u64::from_le_bytes(idx_bytes));
    let last_included_term = Term::new(u64::from_le_bytes(term_bytes));
    let peer_count = u32::from_le_bytes(count_bytes) as usize;
    let expected_len = 20 + peer_count * 8;
    if meta.len() != expected_len {
        return Err(DiskStorageError::Malformed(
            "snapshot_meta.bin size mismatch",
        ));
    }
    let mut peers = std::collections::BTreeSet::new();
    for i in 0..peer_count {
        let off = 20 + i * 8;
        let raw: [u8; 8] = meta[off..off + 8].try_into().expect("8 bytes");
        let peer = NodeId::new(u64::from_le_bytes(raw)).ok_or(DiskStorageError::Malformed(
            "snapshot_meta.bin peer id is zero",
        ))?;
        peers.insert(peer);
    }
    let bytes = tokio::fs::read(bytes_path).await?;
    Ok(Some(StoredSnapshot {
        last_included_index,
        last_included_term,
        peers,
        bytes,
    }))
}

/// Read every fully-decodable frame from one segment file. A torn
/// trailing frame (either an incomplete length prefix or an incomplete
/// body) is treated as EOF — recovery then returns the frames up to
/// that point. Matches the implicit behavior of the single-file impl.
async fn read_segment<C>(path: &Path) -> Result<Vec<LogEntry<C>>, DiskStorageError>
where
    C: From<Vec<u8>>,
{
    let f = match File::open(path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    let mut r = BufReader::new(f);
    let mut entries = Vec::new();
    loop {
        let mut len_buf = [0u8; 4];
        match r.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut frame = vec![0u8; len];
        match r.read_exact(&mut frame).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Torn frame: keep everything we already decoded.
                break;
            }
            Err(e) => return Err(e.into()),
        }
        let proto_entry = proto::LogEntry::decode(frame.as_slice())?;
        let entry: LogEntry<C> = proto_entry
            .try_into()
            .map_err(|_| DiskStorageError::Malformed("log entry failed validation"))?;
        entries.push(entry);
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_filename_round_trip() {
        let name = segment_filename(LogIndex::new(1));
        assert_eq!(name, "000000000000001.log");
        assert_eq!(parse_segment_filename(&name), Some(LogIndex::new(1)));
    }

    #[test]
    fn parse_rejects_non_segment_names() {
        assert!(parse_segment_filename("log.bin").is_none());
        assert!(parse_segment_filename("000000000000001.tmp").is_none());
        assert!(parse_segment_filename("not-a-number.log").is_none());
        // Wrong width.
        assert!(parse_segment_filename("1.log").is_none());
    }
}
