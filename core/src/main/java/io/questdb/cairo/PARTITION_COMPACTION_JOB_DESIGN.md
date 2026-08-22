# Partition compaction job — design

Design only, nothing here is built. Builds on `COMPOSITE_PARTITION_STATE.md` in this same
directory (the "lazy geometry" composite-partition feature).

## Goal

`TableWriter` can leave a partition uncompacted for a long time:
- a native composite partition accumulates pieces (row-group-like slices sharing one set of
  column files),
- a Parquet-format partition accumulates dead row groups from in-place O3 updates.

When a partition is not "hot" (no writes for a configured idle timeout, default 15 min), a
background job should compact it and swap the result into the live `TableWriter`, the way
Enterprise's storage-policy job swaps in a recompacted partition.

## 1. Trigger: periodic scan

- `SynchronizedJob` styled on `WalPurgeJob`'s interval gate (`checkInterval` / `runSerially()`,
  `WalPurgeJob.java:65,89,587-593`). New config `cairo.partition.compaction.check.interval`,
  default 2 min, mirroring `cairo.wal.purge.interval` (`PropertyKey.java:563`). The poll cadence
  is separate from the per-partition idle-eligibility threshold
  (`cairo.partition.compaction.idle.timeout`, default 15 min).

Per tick:

1. **Enumerate tables** — `CairoEngine.getTableTokens(bucket, false)` (`CairoEngine.java:1318`).
   `CairoEngine.hydrateRecentWriteTracker()` (`:1496-1524`) already does a near-identical "for
   every table, peek at `_txn`" sweep at startup (to seed `RecentWriteTracker`) and is the direct
   template to copy.
2. **Per table, open `_txn` standalone** — `new TxReader(ff).ofRO(path, timestampType,
   partitionBy)`, the same lightweight pattern used at `TableReader.java:161/214`,
   `RebuildColumnBase.java:224`, `CopyImportTask.java:624` — then
   `TableUtils.safeReadTxn(txReader, clock, spinLockTimeout)` for a torn-read-safe snapshot. No
   writer or reader lock; a short-lived mmap closed right after. `timestampType`/`partitionBy`
   come from `getTableMetadata(tableToken)`, same as the hydrate path.
3. **Classify first, cheapest gate** — for each attached partition, read the `_txn` bits already
   in hand: `isPartitionCompositeByRawIndex` (`TxReader.java:520-525`) or a nonzero
   `getPartitionParquetFileSizeByRawIndex` (`TxReader.java:636`). **If neither, skip
   immediately** — a plain native single-piece partition has no dead space to reclaim; the job
   never opens `_geometry`/`_pm` for it and it never reaches step 5.
4. **Recency filter, no extra I/O** — for composite/Parquet survivors only: skip any partition
   whose upper timestamp bound falls inside `[now - idleTimeout, now]`. For time-partitioned
   data, only the newest partition(s) can plausibly still be taking O3 writes, so this alone
   eliminates most remaining candidates using fields already in the `_txn` record.
5. **Confirm before acting** — only for the (small) survivor set, one more targeted read:
   - composite candidates: `_geometry`'s `lastWriteMicros` (see §2),
   - Parquet candidates: `_pm`'s footer `unusedBytes` / `getParquetFileSize()` (see §4).
6. Only partitions passing both checks get queued for compact-and-swap (§5) via
   `getWriterOrPublishCommand`.

Net effect: the 2-minute tick is O(tables), one small `_txn` mmap each, zero writer contention;
the more expensive per-partition `_geometry`/`_pm` reads only happen for candidates that already
look idle and dead-space-bearing from `_txn` alone.

## 2. Idle detection — native composite partitions

- `PartitionGeometry.getLastWriteMicros(int)` (`PartitionGeometry.java:221-223`) already tracks
  per-directory last-write time, populated at commit (`:460,480,601`) and persisted via
  `PartitionGeometryFile.getLastWriteMicros()` (`PartitionGeometryFile.java:180`).
- **It is write-only today** — no reader exists anywhere in the codebase. This job becomes its
  first reader.

## 3. Compaction — native composite partitions (merge pieces)

- Nothing does this yet. `PartitionGeometry.compactPieces()` (`:497-514`) only repacks the
  in-memory `LongList`, it is not a data merge.
- Closest existing template: `TableWriter.squashSplitPartitions(int,int,int,boolean)`
  (`TableWriter.java:13725`), which merges via `FrameAlgebra.append` — but that is the unrelated
  O3 partition-**split** feature (separate directories per logical partition), not intra-directory
  pieces. Reuse the append-by-frame idea, not the code path.
- Do the merge off the writer thread, against a `TableReader` snapshot — same as Enterprise's
  `convertPartitionToParquet` (`StoragePolicyJob.java:205`), which needs no writer lock for the
  build step.
- Write output into a new partition-version directory named by a txn-based generation number,
  same convention `switchNativePartitionWithParquet` uses (`TableWriter.java:3839-3841`).

## 4. Compaction — Parquet-format partitions (dead row groups)

Mostly **already exists** — asymmetric with §3.

- O3 commits into a Parquet partition update in place by default (`isRewrite == false`):
  `PartitionUpdater.updateRowGroup()` (`O3PartitionJob.java:3440`) appends the merged row group
  at the file tail and repoints the footer entry — the superseded row group's bytes stay
  physically in the `.parquet` file.
- `_pm` holds an MVCC footer chain (`ParquetMetaFileReader.java:82-93`): each update appends a
  new footer generation with a backlink (`prev_parquet_meta_file_size`) and an `UNUSED_BYTES`
  count. A rolled-back update can leave one orphaned dead footer at the tail — harmless, readers
  resolve the footer by walking backward to the one matching the committed size
  (`resolveFooter`, `:688-718`), never by raw file size.
- `O3PartitionJob` already decides update-vs-rewrite per commit:
  `isRewrite = schemaChanged || rowGroupCount==1 || coalescableTie ||
  unusedBytes/parquetSize > ratio || unusedBytes > maxBytes`
  (`O3PartitionJob.java:1088-1093`), config
  `cairo.partition.encoder.parquet.o3.rewrite.unused.{ratio,max.bytes}`
  (`PropertyKey.java:654-655`).
- On rewrite: new txn-named dir + fresh `.parquet`/`_pm` (`:4458-4478`), only live row groups
  get copied in (`COPY_ROW_GROUP_SLICE` plan from `O3ParquetMergeStrategy.computeMergeActions`,
  `:84/:147`), via Rust `PartitionUpdater.copy_row_group` / `copy_row_group_with_null_columns`
  (`update.rs:514/702`, JNI in `jni.rs`).
- **The dead-space ratio is already durable** — `ParquetMetaFileReader.getUnusedBytes()` /
  `getParquetFileSize()` (`:578-583`, `:451-460`) — unlike §2/§3, no new tracking is needed; the
  idle job just reads it.
- Gap: this rewrite is currently **opportunistic-only, fired mid-O3-commit**. There is no
  standalone/idle-triggered entry point (`VACUUM TABLE` only purges old column-version files,
  unrelated — `SqlCompilerImpl.java:4003-4048`).
- Design: same idle scan as §1, then call the existing rewrite decision+path proactively instead
  of waiting for the next O3 commit to trip the ratio — reuse `O3PartitionJob`'s rewrite branch
  and the Rust copy-row-group ops as-is, then swap in via the same protocol as §5. Undecided:
  reuse the existing `rewrite.unused.ratio` config or add a separate idle-triggered threshold
  (template: `cairo.table.registry.compaction.threshold`'s int + `-1`-disables shape,
  `PropertyKey.java:604`, `CairoConfiguration.java:885`, `TableNameRegistryStore.java:580-583`).

## 5. Swap into TableWriter

Mirror Enterprise's storage-policy swap mechanism directly:

- `CairoEngine.getWriterOrPublishCommand()` (`CairoEngine.java:1476` → `WriterPool.java:181`):
  idle writer → apply directly; busy writer → serialize an `AsyncWriterCommand` onto the writer's
  own `TableWriterTask` queue, applied later on the writer's own thread via `TableWriter.tick()`.
  Same channel Enterprise uses for `CMD_STORAGE_POLICY`.
- On the writer thread: link the merged/rewritten files into the new version dir, then flip the
  tx-file entry in one commit — for the native path, clear `PARTITION_COMPOSITE_FLAG` and drop
  `geometryRef` (`TxWriter.setPartitionGeometryRef`, `:456-460`); either path then calls
  `bumpPartitionTableVersion()`, `commitTxWriter()`. Same shape as
  `switchNativePartitionWithParquet` (`TableWriter.java:3880-3882`).
- Delete the old directory only **after** that commit, best-effort, non-rolling-back
  (`:3903`, comment `:3893-3896`).
- Pre-commit failure: `rmdir` the new version dir and rethrow — writer state untouched
  (`:3883-3887`).

## 6. Staleness

Ingestion may write a new piece (or advance `nameTxn`) between snapshot and swap. Mirror
`StoragePolicyWriterCommand.applyParquetCommit`'s generation check (`resolveSquashTracker`,
`StoragePolicyJob.java:705`; check at `:180-217`): compare a `nameTxn`/piece-count snapshot taken
at merge-start against live values at swap-time; on mismatch, return stale and re-enqueue
(`reEnqueueConversionOrDrop`, `:582`) up to a max retry count.

## Config surface (new keys)

- `cairo.partition.compaction.check.interval` — job poll cadence, default 2 min.
- `cairo.partition.compaction.idle.timeout` — per-partition eligibility threshold, default 15
  min.
- Parquet dead-space threshold: reuse `cairo.partition.encoder.parquet.o3.rewrite.unused.ratio`
  or add a dedicated key — undecided, see §4.

## Open items

- Interaction with the existing O3 split-squash in `housekeep()` (`TableWriter.java:7291`) — the
  two merge mechanisms (split-dir squash vs. piece/row-group compaction) should not fire in the
  same commit.
- WAL vs. non-WAL tables — confirm this targets WAL tables the same way Enterprise does (via
  `TableReader` over materialized state).
- Native composite output format: native only, or also offer Parquet output by reusing
  `produceParquetFromNative`?
