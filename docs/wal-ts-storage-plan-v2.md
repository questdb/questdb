# WAL ts-storage plan

Status: design, not yet implemented.
Scope: WAL apply path only, gated behind a config flag.

## 1. Motivation

Today, when out-of-order (O3) timestamps arrive, QuestDB sorts rows
and rewrites entire partition column files via the prefix/merge/suffix
path in `O3PartitionJob` / `O3CopyJob` / `O3OpenColumnJob`. A single
late row can trigger a rewrite of millions. Customers feel this as a
non-linear spike in disk I/O that can stall the database.

The new design changes the rules:

- Rows are appended to column files in **per-commit sorted order**.
  Each WAL transaction is sorted in memory once (cheap), then
  appended as one contiguous sorted run at the partition's column
  tails. No global re-sort. No O3 column rewrite.
- Per-commit metadata (`CommitStat`) records each run's
  `(physRowStart, rowCount, minTs, maxTs, sortedFlag)`.
- Ordered reads do a K-way merge over the partition's sorted runs.
  Order-insensitive reads scan runs directly with full SIMD.
- Background compaction folds adjacent runs (free) or merges
  out-of-order runs (with column rewrite, off the critical path).

## 2. Scope and gating

- New config flag `cairo.wal.ts.runs.enabled`, default `false`.
- When flag is on: WAL apply for **all** WAL tables uses the new path.
- When flag is off: behaviour is unchanged from today.
- Non-WAL tables: untouched in either case.

## 3. Storage model

### 3.1 Tiers

| Tier | Storage location                    | Per-partition state                                                        |
|------|-------------------------------------|----------------------------------------------------------------------------|
| 1    | Local active                        | Sorted runs (column files) + CommitStat list                               |
| 2    | Parquet on object store             | Parquet's own row-group statistics; _pm sidecar locally                    |
| 3    | Parquet base + local active overlay | Parquet (read-only) + local sorted runs + CommitStat list for overlay only |

A partition is in exactly one tier at any time.

### 3.2 Transitions

- **1 -> 2 (seal)**: existing parquet-conversion machinery. Writes
  the partition's data to a parquet file, uploads (or moves locally
  for non-object-store deployments), drops local column files.
- **2 -> 3 (cold write)**: a WAL commit lands on a sealed partition.
  Local column files are initialised empty; the commit's rows are
  appended as a sorted run; CommitStat list begins.
- **3 -> 2 (promotion)**: overlay exceeds a threshold (row count,
  byte size, age, or user request). Streams cold base + overlay
  through the K-way merge cursor with dedup applied, writes a new
  parquet, swaps it in, clears local state.

PR-1 implements **Tier 1 only**. Tier 3 read-side and the promotion
path are explicit follow-ups (see §13).

### 3.3 In-partition layout

For Tier 1 and Tier 3 (overlay portion):

- Column files (`<col>.d`, `<col>.i`, `<col>.v`, etc.) are written
  append-only. Each WAL commit appends one sorted run; within a run,
  rows are in ascending ts order. Across runs, ts ranges may overlap
  (O3 = a later commit has an earlier ts range than an earlier
  commit).
- New sidecar file `_ts.commits` per partition. Append-only array of
  CommitStat records (§4).
- Valid size of `_ts.commits` is stored in `_txn` slot [3]
  (`PARTITION_PARQUET_FILE_SIZE_OFFSET`), reusing the slot since
  Tier-1 partitions are mutually exclusive with parquet partitions.
  A new flag bit `PARTITION_MASK_TS_COMMITS_BIT_OFFSET = 59`
  disambiguates the slot's meaning.

For Tier 3, the cold parquet is referenced from the existing parquet
slot and flag bits. Local overlay state is layered on top with its
own `_ts.commits` and `PARTITION_MASK_TS_COMMITS_BIT_OFFSET` set
**alongside** the parquet flag bit. Slot [3] in that combined state
needs to express both the parquet file size and the `_ts.commits`
size. PR-1 does not touch Tier 3, so this is deferred; the natural
layout when Tier 3 lands is a small extension to the per-partition
record (a new dedicated slot via a `_txn` format version bump, or a
new pair of slots reusing reserved bits).

## 4. CommitStat format

```
CommitStat {
  physRowStart  : i64   // first physRowId in this slice
  rowCount      : u32   // rows in this slice
  flags         : u16
  reserved      : u16
  minTs         : i64   // from WalEventCursor.getMinTimestamp()
  maxTs         : i64   // from WalEventCursor.getMaxTimestamp()
  extLength     : u32   // bytes of extension TLV records (0 if none)
  extensions    : extLength bytes
}
```

Record size: 36 bytes header + `extLength`. For a partition with
one million commits and no extensions: ~36 MB total. Memory-mapped,
fully in memory at partition open.

Flags (low bits):
- bit 0 `SORTED_BY_TS`: rows in this slice are non-decreasing by ts.
  Always set by PR-1 (every commit is sorted at WAL apply).
- bit 1 `HAS_EXTENSIONS`: `extLength > 0`.
- bit 2 `MERGED`: this CommitStat is the output of a compaction, not
  a raw WAL commit. Useful for observability and for skipping
  compaction-of-compactions in selection heuristics.
- bits 3-15: reserved.

### Extension TLV format

Length-prefixed type-length-value records. Unknown types skipped via
length. Old readers ignore unknown extensions.

```
ExtRecord { type : u16; length : u16; payload : length bytes }
```

Reserved types for follow-up PRs:
- `EXT_SYMBOL_PRESENCE = 1` -- per-column symbol-value list for a
  slice. Enables bitmap-style skipping for equality filters.
- `EXT_NUMERIC_RANGE = 2` -- per-column min/max for non-ts numerics.
- `EXT_BLOOM = 3` -- bloom-filter sketch per column.
- `EXT_SINGLE_SERIES = 4` -- marker that the slice contains exactly
  one time series.

PR-1 emits the header only; extensions are envelope-only.

### Invariants

- CommitStats are append-only and immutable once a commit is durable.
- `physRowStart` values are strictly increasing across the array
  (insertion-order writes push each commit's rows to the tail).
- Adjacent CommitStats are disjoint in
  `[physRowStart, physRowStart + rowCount)`.
- `minTs <= maxTs`. `SORTED_BY_TS` asserts in-slice monotonicity.

### File layout

`_ts.commits` is a flat array of variable-length CommitStat records.
Append-only. Header at file offset 0:

```
TsCommitsHeader {
  magic       : u32  = "QTCS"
  version     : u16
  flags       : u16
  reserved    : u8 * 56     // pad to 64 bytes
}
```

Records follow immediately. Each record's length is derivable from
its `extLength` field (36 + extLength bytes total).

No footer. The valid file size in `_txn` slot [3] tells the reader
where the array ends. Bytes past valid size are crash garbage
(ignored).

## 5. WAL apply algorithm

### 5.1 Block detection

In `ApplyWal2TableJob.applyOutstandingWalTransactions` (around
line 336 of the existing code), iterate pending transactions; pull
`(minTs, maxTs)` from `WalEventCursor.getMinTimestamp /
getMaxTimestamp`; coalesce consecutive same-partition transactions
into one block per partition; route spanning transactions to a
separate path.

### 5.2 Single-partition block (fast path)

For each transaction in the block, in WAL order:

1. Mmap the WAL segment for this transaction.
2. **Sort the transaction's `(ts, physRowId)` pairs by ts in memory**.
   `Vect.radixSortLongIndexAscInPlace` on a per-transaction scratch
   buffer. Cost: O(K log K) for K rows in the transaction, where K
   is small (typically 1 to 10K). The sort is per-commit, not
   global -- it does not merge against existing partition data.
3. Append the transaction's rows to the partition's column-file
   tails **in sorted order**. New rows occupy
   `[partitionRowCount, partitionRowCount + K)`. Symbol remap as
   today.
4. Append one CommitStat to `_ts.commits` with
   `(physRowStart = oldPartitionRowCount, rowCount = K, minTs,
   maxTs, flags = SORTED_BY_TS)`.

After all transactions in the block are processed:

5. Update `txWriter.setPartitionTsCommitsFileSize(partitionTs,
   newValidSize)` and set
   `PARTITION_MASK_TS_COMMITS_BIT_OFFSET` on the partition's
   masked-size long.
6. Bump `txWriter` partition row counts.
7. Single `txWriter.commit()` at the end of the block.

No global radix sort. No prefix/merge/suffix on column files.
`O3PartitionJob`, `O3CopyJob`, `O3OpenColumnJob` are not invoked.

### 5.3 Spanning transaction (slow path)

When a single transaction's `(minTs, maxTs)` span more than one
partition:

1. Load the transaction's rows into the existing `o3*` buffers.
2. Global radix sort over `o3TimestampMem` (existing
   `Vect.radixSortLongIndexAscInPlace`). Partition boundaries
   emerge from the sorted output.
3. For each partition slice in the sorted output:
   - Append the slice's rows to that partition's column-file tails
     (sorted by ts within the slice -- a side effect of the global
     sort).
   - Append one CommitStat for the slice with
     `(physRowStart, sliceRowCount, sliceMinTs, sliceMaxTs,
     SORTED_BY_TS)`.
   - Update `_txn` slot [3] and the flag bit for that partition.
4. Single `txWriter.commit()` for the whole transaction.

The spanning path reuses the existing radix-sort machinery but
bypasses the prefix/merge/suffix column-rewrite stages.

### 5.4 Dedup semantics

This is a semantic change from today. With v2, **dedup is applied at
read time (merge cursor) and at compaction time, not at write time**.

Concretely:
- The WAL apply path always appends new rows. It does not check for
  matching dedup keys against existing partition data.
- Rows with duplicate dedup keys may coexist physically in column
  files. Two rows with the same dedup key but different
  `physRowStart` will both be present until compaction reclaims one.
- The K-way merge cursor (§6) applies dedup as it merges streams:
  when consecutive emitted rows share dedup keys, keep the later
  one (highest physRowStart, equivalent to latest-wins).
- Compaction (§7) applies dedup as part of its merge step.
- The user-visible result of any SELECT is identical to today
  (deduplicated). The on-disk state differs (dupes accumulate until
  compaction).

The trade-off: faster writes (no write-time dedup probe) for a
larger on-disk footprint until compaction. Acceptable for the
experimental flag.

## 6. Read path overview

This section sketches the read-side design; the full read plan is in
`wal-ts-storage-read-plan-v2.md` (to be written).

### 6.1 Canonical ordered cursor: K-way merge

A `TsMergeCursor` maintains a priority queue over the partition's
sorted-run heads. Each step:
1. Pop the run-head with the smallest ts.
2. Emit the row (`physRowId` is the cursor's output to the column
   accessors).
3. If the next pop has matching dedup keys, drop it (latest-wins).
4. Advance the run that was popped.

For a partition with one sorted run (single in-order commit, or
fully-consolidated): degenerates to a sequential scan with zero
merge overhead.

For Tier 3: the cold parquet contributes one input to the priority
queue alongside the local sorted runs. The cursor is unchanged in
shape -- (K+1) inputs instead of K.

### 6.2 Order-insensitive reads

Aggregations, group-by, count, distinct, hash-join slave hashing
top-K by ts, sample-by: scan all surviving runs after CommitStat
pruning, in any order, with full SIMD on contiguous column memory.
No priority queue. No per-row indirection. This is the **fast path
for the majority of analytical queries**.

### 6.3 Time-range pruning

For `WHERE ts BETWEEN A AND B`:
- Scan the CommitStat array.
- Mark runs whose `[minTs, maxTs]` overlaps `[A, B]` as "interesting."
- Emit only those runs to the consumer.

Scan cost: O(K) bytes, ~36 bytes per CommitStat, sequential in
memory. At K = 1M commits, ~36 MB scanned in ~4 ms at memory
bandwidth -- typically dominated by the actual data scan that
follows.

If K becomes pathological for short queries, the follow-up
"hierarchical CommitStat skip" (super-stats every 256 entries)
reduces scan cost to O(K / 256). Tracked as a follow-up.

### 6.4 ASOF / horizon joins

For these joins both sides need to be in ts order. With sorted
runs, the slave probe ("most recent slave row with ts <= masterTs")
becomes a per-run binary search across the slave's runs -- each run
is internally sorted, so `Vect.binarySearch64Bit` works directly on
the slave's ts column for each run. Cost: K * log(runSize) per
probe. For typical workloads (low K after consolidation), this is
fast and SIMD-friendly.

### 6.5 Frame model

A simplified version of the v1 read plan's frame types:

```
NATIVE                       = 0    // existing legacy ordered contiguous
PARQUET                      = 1    // existing
NATIVE_UNORDERED_CONTIGUOUS  = 2    // one sorted run = one frame
```

The ordered cursor (`TsMergeCursor`) does **not** produce frames; it
composes over multiple `NATIVE_UNORDERED_CONTIGUOUS` frames (one per
run) and emits a single ordered record stream. JIT runs on each
frame at full SIMD; predicates have no order assumption.

A frame is a contiguous chunk of physical column memory, identical
in shape to today's frames. There is no per-frame `physRowId`
scratch buffer and no per-row indirection inside the frame; the
ordered cursor composes order externally over multiple flat frames.

## 7. Compaction

PR-1 ships the storage and write path. Compaction is a follow-up PR
(see §13) but the design is captured here for completeness.

### 7.1 Free fold (background, frequent)

Scan CommitStats; find runs of adjacent entries where:
- `maxTs[i] <= minTs[i+1]` (ts-order chain), AND
- `physRowStart[i] + rowCount[i] == physRowStart[i+1]`
  (physically adjacent).

Collapse such runs into a single CommitStat. No column data
movement. Rewrites only the `_ts.commits` file (or a tail portion
of it). Runs aggressively; common case for non-O3 ingestion.

### 7.2 Real merge (background, throttled)

Selection heuristic (ClickHouse-inspired):
- Prefer K adjacent CommitStats of similar size (log-structured
  levels emerge).
- Older runs first.
- Cap on CommitStat count per partition: target 256, hard cap 4096.
- Above hard cap, WAL apply blocks until compaction reduces count.

Merge process:
1. Read the K selected runs' column data via sequential reads.
   Each run is internally sorted; build a priority queue of size K.
2. Streaming K-way merge sort. Apply dedup during the merge.
3. Append the merged sorted stream to the column-file tails as a
   new sorted run at `[P, P + N')` where P is current row count and
   N' is post-dedup row count.
4. Replace the K CommitStats with one new CommitStat for the merged
   run (`MERGED` flag set).
5. Old physical regions become orphan column data; reclaimed by the
   orphan-reclaim job (§7.3).

Cost: O(N) sequential I/O. Amortised over many subsequent reads,
similar to ClickHouse.

### 7.3 Orphan reclaim (background, rare)

After many real merges, column files have orphan ranges. Trigger
when orphan-bytes ratio exceeds a threshold (e.g., 30%). Stream-
rewrite column files dropping orphans; remap surviving CommitStat
`physRowStart` values. This is the closest the new design gets to a
"convert partition" operation; like the existing parquet conversion,
it is rare and explicitly scheduled.

User-triggered alternative: `OPTIMIZE PARTITION <ts>` SQL command
mirrors ClickHouse's `OPTIMIZE`.

### 7.4 Promotion to Tier 2

Stream the partition through the K-way merge cursor (Tier 1 input
only, or Tier 3 with cold base + overlay), apply dedup, write a new
parquet, atomic swap. Extends the existing
`convertPartitionNativeToParquet` machinery.

## 8. New code

All under new sub-package `io.questdb.cairo.wal.tsruns` to keep the
experiment isolated. Members alphabetically sorted within each
class. No banner comments. ASCII-only log strings. Builder-pattern
logs ending in `.$()` / `.I$()`.

### PR-1 (this plan)

1. **`TsCommitsFormat`** -- format constants. Magic, version,
   record offsets, flag bits, reserved extension type ids.
2. **`TsCommitsRecord`** -- thin POJO wrapping a slice of mmap
   memory; field accessors only, no allocation.
3. **`TsCommitsFile`** -- per-partition handle. Owns a
   `MemoryCMARW` over `<partition>/_ts.commits`, tracks `validSize`
   from `_txn`. Methods: `append(physRowStart, rowCount, flags,
   minTs, maxTs, extensionBytes)`, `recordAt(index)`,
   `recordCount()`, `sync()`, `close()`.
4. **`WalTsRunsCommit`** -- write-side orchestrator invoked from
   `TableWriter`. Per-transaction sort, append to column tails,
   emit CommitStat. Holds reusable `LongList` scratch and a per-
   transaction sort buffer.

### Read PR (follow-up)

- `TsCommitsReader` -- read-side counterpart, opens `_ts.commits`
  and exposes prune-by-interval and iterate operations.
- `TsMergeCursor` -- K-way merge over runs, with dedup.

### Compaction PR (follow-up)

- `WalTsFoldJob` -- free fold pass.
- `WalTsMergeJob` -- real merge pass.
- `WalTsOrphanReclaimJob` -- orphan column-data reclaim.

### Promotion PR (follow-up)

- `WalTsPromoteJob` -- Tier 1/3 -> Tier 2.

## 9. Modified code

### `io/questdb/PropertyKey.java`

Add `CAIRO_WAL_TS_RUNS_ENABLED("cairo.wal.ts.runs.enabled")` next
to existing `CAIRO_WAL_*` entries.

### `io/questdb/cairo/CairoConfiguration.java`

Add `default boolean isWalTsRunsEnabled() { return false; }` near
`isWalApplyEnabled()`.

### `io/questdb/cairo/DefaultCairoConfiguration.java`

Override `isWalTsRunsEnabled()` reading the new property.

### `io/questdb/cairo/TxReader.java`

- Add constant `PARTITION_MASK_TS_COMMITS_BIT_OFFSET = 59`.
- Add `isPartitionTsCommits(int)` and `getPartitionTsCommitsFileSize(int)`.
- Existing parquet accessors unchanged; callers must check the
  partition format bit before treating slot [3] as parquet size
  vs. ts-commits size.

### `io/questdb/cairo/TxWriter.java`

- Add `setPartitionTsCommitsFileSize(timestamp, size)` -- writes
  slot [3] and sets the flag bit.
- Add `resetPartitionTsCommits(partitionIndex)` -- clears the
  flag bit and the slot. Used on partition drop / Tier 1 -> 2
  promotion.
- Update `setPartitionParquetFormat` to clear the ts-commits flag
  bit (Tier 2 supersedes Tier 1).

### `io/questdb/cairo/TableWriter.java`

- New private fields: `tsCommitsFile`, `walTsRunsCommit`. Lazily
  allocated on first WAL apply with the flag on.
- `close()` and `Misc.free` sequence updated to release them.
- `processWalCommit()` (around line 8706): top-of-method branch on
  `configuration.isWalTsRunsEnabled()`. New helper
  `processWalCommitTsRuns()` runs the §5.2 / §5.3 path.
- New helper to switch active partition forward/backward for blocks
  targeting non-tail partitions. Reuse existing partition-switch
  machinery used during attach.

### `io/questdb/cairo/wal/ApplyWal2TableJob.java`

- `applyOutstandingWalTransactions()` (around line 336): when the
  flag is on, install the block-detection loop. Otherwise fall
  through to existing per-transaction dispatch.
- Helpers `flushSinglePartitionBlock()` and
  `flushSpanningTransaction()` calling into `TableWriter`.

### Unchanged in PR-1

- `O3PartitionJob`, `O3CopyJob`, `O3OpenColumnJob` -- not modified.
  Bypassed entirely when the flag is on.
- Read-side cursors and frames -- handled in the read PR.

## 10. Lifecycle

- **First WAL apply touching a partition with the flag on**:
  `openPartition` checks for `_ts.commits`. If absent, creates it
  with header only; `validSize = 64` (header).
- **Writer restart**: `openLastPartition` reads slot [3] from `_txn`
  (when the ts-commits bit is set), opens `_ts.commits` at that
  offset, no chain to walk -- the file is a flat array, reader
  position is the file tail.
- **Partition switch mid-WAL-apply**: close current `TsCommitsFile`
  (flush + sync), open the next one. Same as column rotation.
- **Writer close**: `Misc.free(tsCommitsFile)`,
  `Misc.free(walTsRunsCommit)`.

## 11. Crash recovery

Choice: **do not truncate.** Read `validSize` from `_txn` slot [3];
open `_ts.commits` for append at `validSize`. Bytes past that point
are unreachable garbage from a crashed commit; ignored.

Justification:
- `_ts.commits` is append-only at the byte level. No live structure
  depends on bytes past `validSize`.
- Truncation introduces a write that itself can crash.
- Crash garbage is bounded (one failed commit's worth of bytes).

Recovery sequence on writer open per partition:
1. Read slot [3] -> `validSize`.
2. Open `_ts.commits`, seek to `validSize`.
3. (Implicit) the CommitStat array `[64, validSize)` is the durable
   live state. Loaded into memory as a `MemoryCMR`-backed view.

If the file header magic or version mismatches, log critical and
suspend the table. This is an experimental flag; silent recovery is
not appropriate.

## 12. Tests

All tests under `core/src/test/java/io/questdb/test/cairo/wal/tsruns/`.

### Unit tests

- `TsCommitsRecordTest` -- header roundtrip, record offsets,
  extension TLV parse.
- `TsCommitsFileTest` -- open/close/reopen, valid-size tracking,
  append-after-restart, header validation, file-grew-but-uncommitted
  bytes ignored.

### Integration tests

Extend `AbstractCairoTest`. Override `isWalTsRunsEnabled()` to
return `true`.

- `WalTsRunsCommitTest` -- apply a few WAL transactions, decode
  `_ts.commits` directly, assert:
  - One record per WAL transaction (or per affected partition for
    spanning).
  - `SORTED_BY_TS` always set.
  - `physRowStart` strictly increasing.
  - `[physRowStart, physRowStart + rowCount)` non-overlapping.
- `WalTsRunsBlockBatchingTest` -- multiple consecutive same-partition
  transactions interleaved with a spanning transaction. Assert
  CommitStat structure matches the block coalescing.
- `WalTsRunsCrashRecoveryTest` -- write CommitStats, inject trailing
  garbage past `validSize`, restart, append another commit, decode
  -- confirms garbage is ignored and next commit succeeds.
- `WalTsRunsTxnSlotTest` -- verify slot [3] and the new flag bit are
  set/cleared correctly across: new partition, append, restart,
  partition drop, parquet conversion (clears ts-commits bit).

### What is not tested in PR-1

Query results. With the flag on, columns hold unsorted-across-runs
data and the legacy read path will return rows in wrong order. The
read PR adds query-level tests. PR-1 verifies file contents
byte-wise.

## 13. Out of scope (follow-ups)

- **Read PR (PR-2)**: `TsCommitsReader`, `TsMergeCursor`, frame
  type and order-requirement plumbing on `RecordCursorFactory`,
  per-operator adaptation, JIT-on-unordered.
- **Compaction PR (PR-3)**: free fold, real merge, orphan reclaim.
  Throttle params in `CairoConfiguration`. `OPTIMIZE PARTITION` SQL.
- **Tier 3 promotion PR (PR-4)**: extend
  `convertPartitionNativeToParquet` to merge cold base + overlay.
  Tier 3 read path.
- **`_ts.idx` secondary index PR (PR-N)**: optional K-shrinking
  optimisation for active partitions with pathologically high K.
  A bit-packed sort-position-to-physRowId index per partition,
  maintained eagerly alongside `_ts.commits`. The merge cursor
  picks the index walk over K-way merge when K crosses a threshold.
- **Extension TLV implementations**: symbol presence, numeric range,
  bloom, single-series markers.
- **Hierarchical CommitStat skip**: super-stats every 256 entries
  for very high K pruning.
- **Bitmap-index remapping** for ordered consumers (the §3(j) item
  in the v1 read plan).
- **Non-WAL tables** -- explicit out of scope.
- **LAG support with the flag on** -- PR-1 forces full-commit.
- **Removing the classic O3 sort path** -- must coexist behind the
  flag for a long time.
- **Schema migration** for existing partitions written without
  `_ts.commits`.

## 14. Open questions

1. **WAL writer sort vs WAL apply sort.** PR-1 sorts at WAL apply
   (one place to change, no WAL format impact). An alternative is
   to sort at WAL writer time so apply just streams bytes. The
   apply-side sort is the simpler PR; the writer-side sort is the
   future-perf-friendly choice (frees apply threads). Decide later.

2. **Tier 3 `_txn` slot encoding.** Tier 3 needs both a parquet
   file size and a `_ts.commits` valid size on the same partition.
   Options: bump `_txn` format version to add a fifth slot per
   partition (cleanest, on-disk format break); reuse reserved bits
   in the existing masked-size long; pack both lengths into one
   slot with bit fields. Defer to Tier 3 PR.

3. **Compaction count thresholds.** Target 256 CommitStats per
   partition, hard cap 4096. Tunable per
   `CairoConfiguration` once compaction lands. Validate against
   real workloads.

4. **Dedup latency contract.** Dedup applies at read merge -- on-
   disk dupes accumulate until compaction. If a user expects
   compaction-current state, they need to trigger
   `OPTIMIZE PARTITION` or wait for the background job. Document
   clearly so user expectations match implementation.

5. **K growth on long-lived high-rate partitions.** Even with
   compaction, K can spike between background passes. The
   hierarchical CommitStat skip is one band-aid; a smaller, denser
   CommitStat (drop `extLength` for the common case) is another.
   Reserve a flag bit for a "tiny record" variant if needed.

6. **Footer fsync ordering.** Recommended: write column tails
   first, sync; then append CommitStat, sync; then advance
   `_txn` slot [3] via `txWriter.commit()`. Durable column data
   before durable metadata before durable txn pointer.

## 15. Critical file references

- `core/src/main/java/io/questdb/cairo/TableWriter.java`
  - O3 trigger: lines 2540-2542
  - `commitWalInsertTransactions()`: lines 1428-1521
  - `processWalCommit()`: line 8706 (top-of-method branch)
  - close sequence: around line 5810
- `core/src/main/java/io/questdb/cairo/wal/ApplyWal2TableJob.java`
  - `applyOutstandingWalTransactions()`: around line 336
  - `commitWalInsertTransactions()`: line 484 call site
- `core/src/main/java/io/questdb/cairo/wal/WalEventCursor.java`
  - per-transaction metadata (`getMinTimestamp`, `getMaxTimestamp`,
    `isOutOfOrder`): lines 385-398
- `core/src/main/java/io/questdb/cairo/TxReader.java`
  - `LONGS_PER_TX_ATTACHED_PARTITION`,
    `PARTITION_PARQUET_FILE_SIZE_OFFSET`, partition mask bits:
    lines 49-58
- `core/src/main/java/io/questdb/cairo/TxWriter.java`
  - parquet-size set/reset (template for ts-commits accessors):
    lines 374, 452-464
- `core/src/main/java/io/questdb/cairo/CairoConfiguration.java`
  - `isWalApplyEnabled()`: around line 1018
- `core/src/main/java/io/questdb/cairo/DefaultCairoConfiguration.java`
  - around line 1669
- `core/src/main/java/io/questdb/PropertyKey.java`
  - `CAIRO_WAL_*` entries: lines 544-551
- `core/src/main/c/share/jit/compiler.cpp`
  - x86 AVX2 codegen: lines 401-541; arm64 codegen: lines 60-200.
    Both run unchanged on `NATIVE_UNORDERED_CONTIGUOUS` frames.