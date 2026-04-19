#![no_main]
//! Fuzz `DiskStorage::open` + `Storage::recover` against adversarial
//! on-disk state. Writes arbitrary bytes into the three durable file
//! locations (hard state, snapshot, snapshot meta) plus a random log
//! segment file, then opens and recovers. Must never panic — may
//! return `Err` on corrupt input.

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;

use jotun::storage::{DiskStorage, Storage};

#[derive(Debug, Arbitrary)]
struct Layout {
    hard_state: Vec<u8>,
    snapshot: Vec<u8>,
    snapshot_meta: Vec<u8>,
    segment_start: u64,
    segment: Vec<u8>,
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let Ok(layout) = Layout::arbitrary(&mut u) else {
        return;
    };

    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(_) => return,
    };

    rt.block_on(async move {
        let Ok(tmp) = tempfile::tempdir() else {
            return;
        };
        let dir = tmp.path();

        // Seed the three well-known files with raw bytes.
        let _ = tokio::fs::write(dir.join("hard_state.bin"), &layout.hard_state).await;
        let _ = tokio::fs::write(dir.join("snapshot.bin"), &layout.snapshot).await;
        let _ = tokio::fs::write(dir.join("snapshot_meta.bin"), &layout.snapshot_meta).await;

        // Seed one log segment file.
        let log_dir = dir.join("log");
        let _ = tokio::fs::create_dir_all(&log_dir).await;
        let seg_name = format!("{:020}.seg", layout.segment_start);
        let _ = tokio::fs::write(log_dir.join(seg_name), &layout.segment).await;

        if let Ok(mut storage) = DiskStorage::open(dir).await {
            let _res: Result<_, _> = <DiskStorage as Storage<Vec<u8>>>::recover(&mut storage).await;
        }
    });
});
