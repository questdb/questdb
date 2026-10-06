# Squash on the compaction job

Status: BUILT. `PartitionCompactionScanJob`, `CompositePartitionSwapCommand` (MERGE mode) and
`TableWriter.swapMergedLogicalPartition` implement what follows; `PARTITION_COMPACTION_JOB.md` describes the
result. Where the build deviates from this design, the section "What was built" at the bottom says so.

## Why

A logical partition is one period of the table's PARTITION BY unit: an hour, day, week, month or
year. A MOVE-TAIL leaves a logical partition as several folders: the main folder (`2024-01-01`) and
one or more split folders later in the same period (`2024-01-01T050000-...`). Each is its own `_txn`
entry. Today
`PartitionCompactionScanJob` handles one folder at a time, and it does that wrongly for splits.

### The bug this replaces

`PartitionCompactionScanJob.scanTable` rounds each folder's start time down to the logical partition start
(`getLogicalPartitionTimestamp`) and uses that rounded time for two things:

1. The key of the pending-swap note (`inFlightSwaps`).
2. The lookup in `dispatchComposite` / `dispatchMakePlain` (`getPartitionIndex(logicalStart)`).

A split folder rounds to the same logical partition start as the main folder. So:

1. Sweep N builds a REWRITE of the main folder into `<partition>.compacting<gen>`. The writer is busy, so
   the swap is queued, and a note keyed by the logical partition start is saved.
2. Sweep N+1 skips the main folder, since its note matches. It then reaches the split folder, finds
   the same note, sees a different nameTxn/generation, treats the note as stale and deletes it.
3. `dispatchComposite(logicalStart)` resolves to the MAIN folder, not the split. With the note gone, it
   rebuilds: `rmdir` of `<partition>.compacting<gen>` and a fresh copy into it.
4. The queued swap runs on the writer and renames the half-built folder into place. The next read
   or squash of that folder fails on a missing or empty file, and the table is suspended.

Seen in CI build 272575 (`MatViewFuzzTest.testStressWalPurgeJob`) and reproduced locally.
Deterministic test, red today:
`PartitionCompactionScanJobTest.testScanDoesNotRebuildAQueuedSwapWhenASiblingSplitSharesTheDay`.

When a logical partition has no main folder, its first folder is a split. The lookup by the
logical partition start then finds nothing, so the job never cleans that logical partition.

## Design

The unit of work is the LOGICAL PARTITION: the run of neighbouring `_txn` entries that share one
logical partition start. That
includes the main folder and every split, both composite and plain ones.

### Idle time of a folder

- Composite folder: `PartitionGeometry.getLastWriteMicros`.
- Plain folder: modification time of the designated timestamp column file (`ff.getLastModified`),
  the same way `isParquetPartitionIdle` reads a parquet file.
- Parquet folder: as today.

### Two thresholds

Two settings replace the single `cairo.partition.compaction.idle.timeout`:

- squash idle, default 30 min
- single idle, default 60 min

| Logical partition state | Action |
|---|---|
| Every folder idle >= squash idle (including >= single idle) | Clean and merge the whole logical partition into one folder |
| Some folder idle < squash idle | Compact alone each COMPOSITE folder idle >= single idle. Leave plain folders and the rest alone |

So there are only two outcomes: squash and compact everything, or compact only the older composite
folders. A logical partition with only one folder that is composite is the "clean alone" case, as
today.

### Merge the whole logical partition

- Build one staging folder from the live rows of every folder in the logical partition, in folder order and then
  piece order. Folders do not overlap in time and pieces ascend by `tsLo`, so that is timestamp order.
- Queue one swap that replaces the logical partition's N `_txn` entries with one entry.
- Do not reuse the writer's squash. The job builds the merged folder itself, off a `TableReader`
  snapshot, with no writer held, the same way it builds a REWRITE today. The writer only runs the
  swap command: it checks the logical partition is still in the state the copy was built from, and replaces the
  N `_txn` entries with one.
- The swap does not wait for readers. It commits the `_txn` change through the `TableWriter`, either
  inline or as a queued command. The old folders are then freed by the existing partition purge
  mechanism, the same as any other retired partition directory.
what
### Clean one folder alone

- Only composite folders. A plain folder is never processed alone.
- Dispatch by the folder's own start time (`getPartitionTimestampByIndex`), never the logical partition start.
  `getPartitionIndex(rawTs)` is an exact hit on that folder.
- Otherwise as today: MAKE-PLAIN when the shape allows, REWRITE otherwise.

### Tracking swaps in flight

- The job tracks swaps in flight per LOGICAL PARTITION, not per folder. Key: table id +
  logical partition start.
- While a logical partition has a swap in flight, the job skips every folder of it: the main folder and all
  splits. It starts no merge and no single-folder clean there.
- So at most one swap per logical partition is in flight. If several of its folders qualify for a single clean,
  they go one after another, each after the previous swap is done or failed.
- The entry is removed when the swap is done or failed:
  - done: the logical partition's `_txn` state moved on (folder set, a nameTxn or a generation changed);
  - failed: the writer dropped or refused the command, or the writer that owned the queue is gone
    (the existing writer-identity check).

## Behaviour change

After this change the job starts squashing split folders that are NOT composite. Today it never
touches a plain folder (`!isComposite && parquetFileSize <= 0` skips it); plain splits are merged
back only by the writer's own squash during commits.

- Any WAL table with plain splits is now in scope, even one that never had a composite partition,
  for example a table with merge-append off. Once every folder of a logical partition has been idle
  >= squash idle, the job copies the whole logical partition into one folder.
- That is new background I/O: one full copy of each such logical partition, where today there was
  none once ingestion stopped.
- A plain folder that is alone in its logical partition has no dead space and no split to merge. The
  job still skips it.
- A plain folder is never processed alone, however cold. Plain splits are only handled by the
  whole-partition squash.

## To check before building

- A swap the writer refuses (stale, or MAKE-PLAIN declined) leaves `_txn` unchanged. Without a
  failure signal the logical partition would be skipped forever. The writer already publishes an async command
  completion event with an error code; the job needs to consume it, or the entry needs a timeout.
- The writer side of `CompositePartitionSwapCommand` and MAKE-PLAIN looks the folder up with
  `getPartitionIndex(partitionTimestamp)`. Confirm an exact start time lands on the split, not the
  main folder.
- Commit `b11e49a651` ("Track compaction swaps by writer identity") introduced the logical-start key.
  That key stays for tracking; only the dispatch lookup moves to the exact start time.
- Plain splits are skipped today (`!isComposite && parquetFileSize <= 0`). The logical partition grouping must
  include them.
- The idle-window check that uses the next partition's start (`upperBound`) must use the logical partition's end
  for the merge case, not the next split's start.

## What was built

Every point above holds, with these answers to the open questions and these deliberate narrowings:

- **Exact start times do land on the split.** `TxReader.getPartitionIndex` floors through
  `getPartitionTimestampByTimestamp`, which returns an EXACT lo-timestamp match unchanged, so
  `getPartitionIndex(splitStart)` resolves the split. The writer side of `CompositePartitionSwapCommand` and
  MAKE-PLAIN needed no change; only the dispatch lookup moved from the logical start to the folder's own start.
- **Failure signal.** Neither a completion-event consumer nor a plain TTL: the record carries a 30-minute
  expiry, and on expiry the sweep takes the writer out of the pool and ticks it, which consumes whatever is
  still queued. Only then are the records forgotten. Nothing is rebuilt on the strength of elapsed time alone,
  so the timeout cannot resurrect the rebuild-under-a-live-command bug this design exists to fix. A writer too
  busy to hand over keeps its records for the next sweep.
- **Two thresholds.** `cairo.partition.compaction.idle.timeout` keeps its name and its 60-minute default and is
  now the SINGLE-folder threshold (it is also still the per-commit AGE rule's key, which is why it kept the
  name). `cairo.partition.compaction.squash.idle.timeout` is new, defaults to 30 minutes, and is clamped to at
  most the single-folder one.
- **A logical partition of ONE folder keeps today's behaviour**: composite and idle past the single threshold
  gets MAKE-PLAIN or REWRITE, plain is skipped. It is not squashed at the lower threshold, since one folder has
  no split to fold and the copy would be the same REWRITE.
- **The merge stands down on the ACTIVE logical partition** - the one holding the last `_txn` entry - and on
  any logical partition holding a Parquet, READ ONLY or REMOTE folder. The active partition's files carry the
  WAL lag rows past the live ones, which no piece accounts for and a reader snapshot never sees; the writer's
  own squash folds that one on commit. Both the sweep and the writer enforce this. Lifting the active-partition
  carve-out means teaching the swap about lag rows, `transientRowCount`/`fixedRowCount` and the open partition's
  column memories, and is left for later.
- **The merge's staging directory has its own marker**, `.merging<folderCount>` rather than
  `.compacting<generation>`: the writer's startup purge tests a staging directory for liveness, and a merge's
  liveness is "the logical partition still holds exactly this run of folders", not "this folder is still
  composite at this generation". Sharing the marker made the purge delete a live merge's staging directory,
  and the swap then failed its rename.
- **In-flight records are keyed by (table, logical partition start)** as planned, but their payload is a hash
  over the whole run - each folder's start, name txn, generation and row count. One word then covers "the swap
  landed", "a folder was added or dropped" and "ingestion rewrote a folder" for every dispatch kind.
