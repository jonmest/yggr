# Deterministic simulation and storage-fault modelling

**Status:** Draft
**Target releases:** `yggr-core 0.2.0`, `yggr 0.2.0`, `yggr-sim` internal
**Scope:** Chunks 1-3 described below. ~6-9 weeks of focused work.

## Motivation

yggr today catches **Raft protocol bugs** via its deterministic chaos simulator: partial fsync, partitions, crashes, reorderings. It does not catch **storage-hardware failure modes** — bitrot, torn writes, misdirected I/O, latent sector errors. These are real-world failures of every disk and filesystem, and any Raft implementation that silently consumes corrupted on-disk state can diverge from its replicas without anyone noticing until months later.

TigerBeetle and FoundationDB have established that the right approach is:

1. Make every source of non-determinism injectable through an abstraction.
2. Simulate storage at a physical level (pages, not logical entries).
3. Add integrity checks (checksums) to every persisted byte.
4. Drive the whole thing from a seeded scheduler that picks faults randomly.

This spec defines how we bring yggr up to that bar. The design is deliberately **staged** so each chunk delivers standalone value even if the next isn't done.

## Non-goals

- **Byte-level network fault injection.** yggr runs length-prefixed frames over TCP; the OS doesn't surface byte corruption. Message-level drop/reorder/partition is sufficient.
- **Clock-skew modelling.** yggr's lease and timing use tick counts, not wall clock. Adding `Clock` skew would be theatre.
- **Full deterministic tokio runtime** (à la Madsim, Turmoil). Several-month project; `runtime_chaos.rs` already stresses the integrated stack adequately.
- **A separate VOPR binary.** Keep fault injection inside the existing `yggr-sim` crate.
- **TigerBeetle-specific accounting properties.** yggr is a generic Raft library; we don't need double-entry bookkeeping invariants.

## Success criteria

On completion of all three chunks:

- Every line of production code (`yggr-core`, `yggr`) is deterministic under a single seed + sim-controlled inputs. Enforced by a lint (no `std::time::Instant`, no `rand::thread_rng` outside `yggr-sim`).
- The on-disk format has checksums on every record (log entry, hard state, snapshot chunk). Corrupt records are detected on recovery and handled via log truncation, not silently consumed.
- The sim can inject realistic hardware faults: bitrot, torn writes, misdirected reads/writes, latent sector errors.
- A new public invariant is asserted by `yggr-sim`: *under any schedule in which at most `⌊(n-1)/2⌋` nodes experience arbitrary storage corruption at any one time, the cluster remains safe (Election Safety, Log Matching, Leader Completeness, State Machine Safety all hold).*
- The README makes a concrete, defensible claim: "Tested against a deterministic simulator that injects hardware-level storage faults; survives bitrot, torn writes, and misdirected I/O on a minority of nodes."

## Architecture at a glance

Three abstractions straddle the boundary between production code and simulation:

```
┌──────────────────────────────────────────────────────────┐
│                    yggr-core (engine)                    │
│                     pure, no I/O                         │
└──────────────────────────────────────────────────────────┘
                            │
                            │  Storage trait (logical: entries, hard state, snapshots)
                            │
┌──────────────────────────────────────────────────────────┐
│                        yggr (runtime)                    │
│  DiskStorage<B: BlockStore> — logical layer              │
└──────────────────────────────────────────────────────────┘
                            │
                            │  BlockStore trait (physical: 4 KiB page I/O, fsync, rename)
                            │
┌──────────────────────────────────────────────────────────┐
│   production:  FileBlockStore  (real filesystem)         │
│   sim:         FaultyBlockStore (in-memory + faults)     │
└──────────────────────────────────────────────────────────┘
```

`DiskStorage` becomes generic over a `BlockStore`. The production path uses `FileBlockStore` (thin wrapper around `tokio::fs`). The sim path uses `FaultyBlockStore`, which holds an in-memory block array and exposes fault-injection hooks.

Above `BlockStore`, `DiskStorage` does format work: serialise a log entry into one or more blocks with checksums, read blocks back and validate, truncate at the first corrupt block.

## Chunk 1: Determinism audit (1-2 weeks)

### Intent

Prove, and then enforce, that production code has zero non-sim-controlled non-determinism.

### Concrete work

1. **Inventory.** Grep `yggr-core` and `yggr` for:
   - `rand::thread_rng`, `rand::random`
   - `std::time::Instant::now`, `SystemTime::now`
   - `std::thread::spawn`, `tokio::spawn` without a tracked handle
   - `HashMap`/`HashSet` without a seeded hasher (iteration order non-determinism)
   - Any `tokio::time::sleep` that isn't driven by a logical tick

   Produce a table: file:line → source of non-determinism → proposed abstraction.

2. **Abstract each source.**
   - `Env` trait already handles election-timeout randomness. Keep.
   - New `Clock` trait: `now_ticks() -> u64`. Production impl counts from `Instant::now`; sim impl is driven by the scheduler.
   - `HashMap` → `BTreeMap` in any code path whose iteration order affects externally-visible behaviour. (Most of yggr-core is already `BTreeMap`.)
   - `tokio::spawn` calls in `yggr` that aren't sim-injectable: flag them. Two paths — either (a) accept that the runtime isn't fully deterministic (already the case; `runtime_chaos.rs` uses real tokio), or (b) plan a future `Scheduler` abstraction. This spec picks (a); the engine remains fully deterministic, the runtime is "well-tested chaotic" but not formally deterministic.

3. **Lint enforcement.** Add to `yggr-core/src/lib.rs` and `yggr/src/lib.rs`:

   ```rust
   #![cfg_attr(not(any(test, feature = "sim")), forbid(
       // custom clippy restriction lints
   ))]
   ```

   Plus a `deny.toml` rule (or a small `build.rs` check) forbidding `rand::thread_rng` and `Instant::now` imports in the non-test, non-sim code paths.

4. **Documentation.** A `CONTRIBUTING.md` section named "Determinism" listing the banned functions and the approved abstractions.

### Deliverables

- PR that introduces `Clock` trait + sim impl + production impl.
- PR that enforces the lint.
- `CONTRIBUTING.md` section.

### Acceptance

- `cargo test --workspace` green.
- `cargo build -p yggr-core -p yggr` green with the restriction lint.
- Manual review of the inventory table shows no unaddressed sources.

### Risks

- The runtime uses `tokio::time::sleep` deliberately. Fully abstracting that is out of scope; this chunk acknowledges a remaining non-determinism window at the runtime layer.

---

## Chunk 2: `BlockStore` abstraction + checksums (2-3 weeks)

### Intent

Introduce a physical-storage abstraction that production and sim both implement. Add checksums to the on-disk format. The engine sees no change; `DiskStorage` does the heavy lifting.

### Concrete work

1. **Define `BlockStore` trait.**

   ```rust
   #[async_trait]
   pub trait BlockStore: Send + Sync {
       /// Read a single 4 KiB page.
       async fn read_page(&self, page_id: PageId) -> Result<Page, BlockStoreError>;
       /// Write a single 4 KiB page. Not durable until `fsync`.
       async fn write_page(&mut self, page_id: PageId, page: Page) -> Result<(), BlockStoreError>;
       /// Flush all pending writes to durable storage.
       async fn fsync(&mut self) -> Result<(), BlockStoreError>;
       /// Atomically rename the entire store (for snapshot installs).
       async fn rename(&mut self, new_path: &Path) -> Result<(), BlockStoreError>;
       /// Truncate beyond a page. Pages at or below survive.
       async fn truncate(&mut self, last_page: PageId) -> Result<(), BlockStoreError>;
       /// Iterate pages in sorted order (for recovery).
       fn iter_pages(&self) -> impl Iterator<Item = (PageId, Page)>;
   }
   ```

   `Page` is `[u8; 4096]`. `PageId` is a `u64`.

2. **Implement `FileBlockStore` (production).** Thin wrapper over `tokio::fs`. One file per store, pages are offset-addressable. `fsync` calls `File::sync_all`.

3. **Implement `FaultyBlockStore` (sim).** In-memory `BTreeMap<PageId, Page>`. Fault-injection methods:
   - `bitrot(page_id, bit_index)` — flip one bit in a stored page.
   - `tear_write(page_id, truncate_to)` — previous `write_page` is partially applied.
   - `misdirect_write(from, to)` — next `write_page` lands at wrong page.
   - `latent_read_error(page_id)` — next `read_page` returns zeros.

4. **Refactor `DiskStorage` to be generic over `BlockStore`.**

   ```rust
   pub struct DiskStorage<B: BlockStore = FileBlockStore> {
       block_store: B,
       // metadata
   }
   ```

   Type alias: `pub type DiskStorage = DiskStorage<FileBlockStore>` for the default case. Users who want to wire their own block store (e.g. for in-memory testing) can instantiate directly.

5. **Define the on-disk record format.** Every record (log entry, hard state, snapshot chunk) starts with a fixed header:

   ```
   +----+----+--------+----+------------------+
   | magic    | length | crc32c | payload     |
   | 2 bytes  | 4 B    | 4 B    | length bytes|
   +----+----+--------+----+------------------+
   ```

   `magic` disambiguates record types. `length` excludes the header. `crc32c` is over `length || payload`. Readers that see a bad checksum treat the record as missing and truncate the log at that point.

   Migration note: current on-disk format has no header. **We rewrite it; no backward compatibility since there are no external users at 0.1.0.** Add a top-level format-version byte to make the next change cleaner.

6. **Recovery.** On `DiskStorage::recover`:
   - Walk pages in order.
   - For each record, validate checksum.
   - On first bad checksum, truncate: that record and everything after is dropped.
   - Log a warning with the offset + bytes lost.
   - Return the surviving prefix.

   Raft's existing log-matching property then re-replicates the dropped tail from the leader.

7. **Tests.**
   - Unit tests for `FileBlockStore`: round-trip, crash-mid-write (simulated via truncation of the file), fsync barrier.
   - Unit tests for `FaultyBlockStore`: each fault method produces the expected state.
   - Integration test: create a `DiskStorage` over `FaultyBlockStore`, bitrot a committed log entry, re-open, assert truncation at the right point.

### Deliverables

- New module `yggr/src/storage/block_store.rs`.
- `FileBlockStore` production impl.
- `FaultyBlockStore` sim impl (lives in `yggr-sim` or a new `yggr-testing` crate — TBD in implementation).
- Refactored `DiskStorage` with checksummed format.
- Migration note in `CHANGELOG.md` → 0.2.0.

### Acceptance

- All existing `DiskStorage` tests pass after rewrite.
- New "recovery from a bit-flipped log" test passes.
- Benchmarks: page-level I/O adds at most ~5% overhead vs the current logical format on 1 M-entry recovery (measured via criterion).

### Risks

- The 4 KiB page granularity wastes space for small log entries. Mitigate by packing multiple records into one page when possible, or by accepting the waste for v1. Decision during implementation.
- Existing fuzzing targets (`fuzz/fuzz_disk_storage_recover.rs`) will need rewriting against the new format.

---

## Chunk 3: Fault-injection scheduler (1-2 weeks)

### Intent

Drive `FaultyBlockStore` from the sim's scheduler. Assert the new safety property under corruption.

### Concrete work

1. **New `Event` variants in `yggr-sim`:**

   ```rust
   Bitrot { node: NodeId, page_id: PageId, bit_index: u16 },
   TearWrite { node: NodeId, page_id: PageId, truncate_to: usize },
   MisdirectWrite { node: NodeId, from: PageId, to: PageId },
   LatentReadError { node: NodeId, page_id: PageId },
   ```

2. **`Policy` gains knobs:**

   ```rust
   pub struct Policy {
       // existing knobs...
       pub bitrot_probability: f64,
       pub torn_write_probability: f64,
       pub misdirected_write_probability: f64,
       pub latent_read_error_probability: f64,
       /// Bound on simultaneously-corrupted nodes. Violating this in
       /// the schedule would let the sim trivially violate safety —
       /// Raft is not a Byzantine-fault-tolerant protocol.
       pub max_corrupt_nodes: usize,
   }
   ```

   `Policy::chaos` defaults sets each probability to ~0.01 and `max_corrupt_nodes = (n - 1) / 2`.

3. **New invariant:**

   > For any schedule produced by `Policy::chaos` where `max_corrupt_nodes ≤ ⌊(n-1)/2⌋`, the four standard safety properties hold after every step.

   This is already asserted by the existing invariant checker; we add proptest coverage that specifically exercises corruption.

4. **Proptest case.**

   ```rust
   proptest! {
       #[test]
       fn chaos_with_storage_corruption_preserves_safety(seed in any::<u64>()) {
           let mut cluster = Cluster::<u64>::new(seed, 5);
           let mut policy = Policy::chaos(Some(1));
           policy.bitrot_probability = 0.02;
           policy.torn_write_probability = 0.01;
           policy.max_corrupt_nodes = 2;  // (5-1)/2
           cluster.set_policy(policy);
           for _ in 0..2000 { cluster.step(); }
       }
   }
   ```

5. **Liveness isn't asserted.** Sustained corruption of `⌊(n-1)/2⌋` nodes may prevent progress; that's acceptable. Safety is the claim.

### Deliverables

- New sim `Event` variants.
- Extended `Policy` with corruption knobs.
- New proptest covering storage-corruption chaos.
- Regression test for any seed that exposes a bug during development.

### Acceptance

- `PROPTEST_CASES=2000 cargo test -p yggr-sim chaos_with_storage_corruption_preserves_safety` green on CI.
- New README paragraph truthfully summarising the capability.

### Risks

- We may find bugs. That's the point. Budget one week of follow-up on whatever shakes out.

---

## Phased delivery plan

| Phase | Weeks | Deliverable |
|---|---|---|
| 1.1: Determinism inventory | 0.5 | Table; no code |
| 1.2: `Clock` + `Env` cleanup | 0.5-1 | PR, lint, CONTRIBUTING |
| 2.1: `BlockStore` trait + `FileBlockStore` | 1 | Refactor PR |
| 2.2: Checksums + new record format | 1-2 | PR, fuzzer rewrite |
| 2.3: `FaultyBlockStore` | 0.5 | PR |
| 3.1: Sim event variants + policy knobs | 1 | PR |
| 3.2: Corruption proptests, bug-fix follow-up | 1 | PR(s) |

Total: ~6-9 weeks calendar, depending on how much breaks under corruption fuzzing.

**Release line-up:**
- **0.2.0**: Chunks 1 + 2 (determinism + checksums). Real user-visible hardening.
- **0.2.1 or 0.3.0**: Chunk 3. Internal correctness upgrade; README claim grows.

## Open questions

1. **Page size.** 4 KiB matches real disks. But if yggr typically writes small records (< 1 KiB), per-page overhead is high. Should the format pack records, or accept waste? (Default: pack during implementation, revisit if benchmarks object.)

2. **Snapshot chunks vs pages.** Snapshots are opaque user bytes, potentially megabytes. They already chunk at `snapshot_chunk_size_bytes`. Page-level checksums for snapshots add modest overhead but real robustness. Do it.

3. **Concurrent writes.** Current `DiskStorage` has a single writer. `BlockStore` should enforce that too — no shared-mutable-state concurrency.

4. **Crate split.** Does `FaultyBlockStore` go in `yggr-sim` (currently unpublished) or a new `yggr-testing` crate (published so downstream users can test against yggr)? Defer decision to Chunk 2 implementation.

5. **Does Chunk 3 eventually motivate per-node `BlockStore` configuration in the runtime?** Probably yes — users running yggr on NVMe vs SATA vs EBS may want different read/write pathways. Out of scope for 0.2.0, but worth preserving the option.

## What this buys yggr

After Chunk 1: yggr-core provably deterministic. Reliable bug reproduction. Worthwhile on its own.

After Chunk 2: yggr detects storage corruption in production. Standard among serious databases; now yggr has it too.

After Chunk 3: yggr makes a quantitative corruption-safety claim backed by millions of simulated minutes. Places yggr in a tier shared by TigerBeetle, FoundationDB, CockroachDB — not etcd, not hashicorp/raft.

## Decision

This spec is a draft. Before implementation starts, decide:

- [ ] Commit to all three chunks, or only 1+2?
- [ ] Accept 6-9 weeks of focused work, or spread over multiple months in parallel with `yggr-streams`?
- [ ] Crate split for `FaultyBlockStore`: sim-internal, new testing crate, or runtime feature flag?

Once decided, the first PR (Chunk 1.1, determinism inventory) is essentially zero risk and clarifies the real scope of Chunk 1.2.
