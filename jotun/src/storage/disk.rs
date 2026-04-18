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
//!   hard_state.bin      // current_term + voted_for, ~16 bytes
//!   log.bin             // append-only stream of length-prefixed
//!                       // protobuf-encoded LogEntry frames
//!   snapshot.bin        // raw snapshot bytes (the host's
//!                       // state-machine output)
//!   snapshot_meta.bin   // last_included_index + last_included_term
//! ```
//!
//! All atomic-replace writes go via a `*.tmp` sibling + rename, with
//! an fsync on the file then an fsync on the directory. Append-only
//! writes to `log.bin` flush + fsync per call.
//!
//! v1 is intentionally simple: one log file, no segments, no
//! compaction beyond what snapshots provide. The trait shape lets
//! users drop in something fancier (sled, rocksdb) without engine
//! changes.

use std::path::{Path, PathBuf};

use jotun_core::transport::protobuf as proto;
use jotun_core::{LogEntry, LogIndex, NodeId, Term};
use prost::Message as _;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

use crate::storage::{RecoveredState, Storage, StoredHardState, StoredSnapshot};

const HARD_STATE_FILE: &str = "hard_state.bin";
const LOG_FILE: &str = "log.bin";
const SNAPSHOT_FILE: &str = "snapshot.bin";
const SNAPSHOT_META_FILE: &str = "snapshot_meta.bin";

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
    log_file: Option<File>,
    /// Cached snapshot floor so append/truncate can compute physical
    /// log positions without re-reading the meta file.
    snapshot_floor: LogIndex,
}

impl DiskStorage {
    /// Open or create a `DiskStorage` in `data_dir`. Creates the
    /// directory if missing. The actual log file is opened lazily
    /// on first write or by [`Self::recover`].
    pub async fn open(data_dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let data_dir = data_dir.into();
        tokio::fs::create_dir_all(&data_dir).await?;
        Ok(Self {
            data_dir,
            log_file: None,
            snapshot_floor: LogIndex::ZERO,
        })
    }

    fn path(&self, name: &str) -> PathBuf {
        self.data_dir.join(name)
    }

    async fn open_log_for_append(&mut self) -> std::io::Result<()> {
        if self.log_file.is_some() {
            return Ok(());
        }
        let f = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(self.path(LOG_FILE))
            .await?;
        self.log_file = Some(f);
        Ok(())
    }

    /// fsync the data dir so an atomic-rename inside it is durable.
    async fn fsync_data_dir(&self) -> std::io::Result<()> {
        let dir = File::open(&self.data_dir).await?;
        dir.sync_all().await?;
        Ok(())
    }

    /// Atomic-replace `name` with `bytes`: write to `name.tmp`,
    /// fsync, rename, fsync the dir.
    async fn atomic_write(&self, name: &str, bytes: &[u8]) -> std::io::Result<()> {
        let final_path = self.path(name);
        let tmp_path = self.path(&format!("{name}.tmp"));
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)
                .await?;
            f.write_all(bytes).await?;
            f.sync_all().await?;
        }
        tokio::fs::rename(&tmp_path, &final_path).await?;
        self.fsync_data_dir().await?;
        Ok(())
    }
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
        let log = read_log::<C>(&self.path(LOG_FILE), self.snapshot_floor).await?;
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
        self.atomic_write(HARD_STATE_FILE, &buf).await?;
        Ok(())
    }

    async fn append_log(&mut self, entries: Vec<LogEntry<C>>) -> Result<(), Self::Error> {
        if entries.is_empty() {
            return Ok(());
        }
        self.open_log_for_append().await?;
        let f = self.log_file.as_mut().expect("opened above");
        for entry in entries {
            let proto_entry: proto::LogEntry = entry.into();
            let bytes = proto_entry.encode_to_vec();
            let len = u32::try_from(bytes.len())
                .map_err(|_| DiskStorageError::Malformed("log entry exceeds 4 GiB"))?;
            f.write_all(&len.to_le_bytes()).await?;
            f.write_all(&bytes).await?;
        }
        f.sync_all().await?;
        Ok(())
    }

    async fn truncate_log(&mut self, from: LogIndex) -> Result<(), Self::Error> {
        // Easiest correct path: re-read the log, drop entries with
        // index >= from, rewrite atomically. Slow for big logs;
        // segments would fix it, but those come post-0.1.
        let entries = read_log::<C>(&self.path(LOG_FILE), self.snapshot_floor).await?;
        let kept: Vec<LogEntry<C>> = entries.into_iter().filter(|e| e.id.index < from).collect();
        let mut bytes = Vec::new();
        for entry in kept {
            let proto_entry: proto::LogEntry = entry.into();
            let frame = proto_entry.encode_to_vec();
            let len = u32::try_from(frame.len())
                .map_err(|_| DiskStorageError::Malformed("log entry exceeds 4 GiB"))?;
            bytes.extend_from_slice(&len.to_le_bytes());
            bytes.extend_from_slice(&frame);
        }
        // Drop the cached file handle so the rename below replaces it.
        self.log_file = None;
        self.atomic_write(LOG_FILE, &bytes).await?;
        Ok(())
    }

    async fn persist_snapshot(&mut self, snap: StoredSnapshot) -> Result<(), Self::Error> {
        // Snapshot meta: [u64 LE index][u64 LE term]
        let mut meta = Vec::with_capacity(16);
        meta.extend_from_slice(&snap.last_included_index.get().to_le_bytes());
        meta.extend_from_slice(&snap.last_included_term.get().to_le_bytes());

        // Order matters: write the bytes first (large), then the
        // meta file (small) — recovery treats meta as the
        // commit-pointer. If a crash happens between the two writes
        // we'll see snapshot bytes with no meta and ignore them.
        self.atomic_write(SNAPSHOT_FILE, &snap.bytes).await?;
        self.atomic_write(SNAPSHOT_META_FILE, &meta).await?;

        // Drop log entries up to and including the snapshot's tail.
        // Easiest: re-read, filter, rewrite. Same approach as truncate.
        self.snapshot_floor = snap.last_included_index;
        let entries = read_log::<C>(&self.path(LOG_FILE), LogIndex::ZERO).await?;
        let kept: Vec<LogEntry<C>> = entries
            .into_iter()
            .filter(|e| e.id.index > snap.last_included_index)
            .collect();
        let mut bytes = Vec::new();
        for entry in kept {
            let proto_entry: proto::LogEntry = entry.into();
            let frame = proto_entry.encode_to_vec();
            let len = u32::try_from(frame.len())
                .map_err(|_| DiskStorageError::Malformed("log entry exceeds 4 GiB"))?;
            bytes.extend_from_slice(&len.to_le_bytes());
            bytes.extend_from_slice(&frame);
        }
        self.log_file = None;
        self.atomic_write(LOG_FILE, &bytes).await?;
        Ok(())
    }
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
    if meta.len() != 16 {
        return Err(DiskStorageError::Malformed("snapshot_meta.bin wrong size"));
    }
    let idx_bytes: [u8; 8] = meta[0..8].try_into().expect("8 bytes");
    let term_bytes: [u8; 8] = meta[8..16].try_into().expect("8 bytes");
    let last_included_index = LogIndex::new(u64::from_le_bytes(idx_bytes));
    let last_included_term = Term::new(u64::from_le_bytes(term_bytes));
    let bytes = tokio::fs::read(bytes_path).await?;
    Ok(Some(StoredSnapshot {
        last_included_index,
        last_included_term,
        bytes,
    }))
}

async fn read_log<C>(
    path: &Path,
    snapshot_floor: LogIndex,
) -> Result<Vec<LogEntry<C>>, DiskStorageError>
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
        r.read_exact(&mut frame).await?;
        let proto_entry = proto::LogEntry::decode(frame.as_slice())?;
        let entry: LogEntry<C> = proto_entry
            .try_into()
            .map_err(|_| DiskStorageError::Malformed("log entry failed validation"))?;
        if entry.id.index > snapshot_floor {
            entries.push(entry);
        }
    }
    Ok(entries)
}
