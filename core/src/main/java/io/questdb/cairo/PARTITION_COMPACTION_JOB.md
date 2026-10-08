# The background compaction sweep

`PARTITION_COMPACTION.md` describes the moves and the per-commit policy. That path only fires when a commit
lands on the table, so a partition that goes cold still holding dead space - a composite folder with
superseded pieces, a logical partition left in several folders, a Parquet partition carrying dead row groups
from in-place O3 updates - is never revisited. `PartitionCompactionScanJob` is the safety net: a periodic
sweep that finds idle wasteful partitions and hands each to its writer.

The sweep never copies bytes under the writer. Every copy - a whole-partition merge, a single-folder REWRITE,
a Parquet rewrite - is built into a staging directory off a `TableReader` snapshot on the sweep's own thread,
POSTING indexes included (`NativePartitionIndexBuilder`). The writer only re-checks the snapshot and swaps the
result in. MAKE-PLAIN, the one step the writer runs itself, copies nothing.

## Scope and placement

- **WAL tables only.** A non-WAL writer holds its transaction open across ticks, so a swap built off a reader
  snapshot could land on a writer carrying rows the snapshot never saw. Non-WAL tables keep per-commit
  compaction only.
- **Its own single-thread worker pool** (`PartitionCompactionPoolConfiguration`), never the shared write
  pool: a REWRITE copies a whole partition inline on the worker that picks it up. Wired in `ServerMain` when
  the instance is not read-only.
- A `SynchronizedJob` gated by `cairo.partition.compaction.check.interval` (default 2 min; negative disables
  the sweep, zero sweeps every call). The interval restarts when a sweep ends.

## The unit of work: a LOGICAL partition

The sweep takes the run of `_txn` entries sharing one logical start as one unit and dispatches at most one
command for it per sweep:

| state | action |
|---|---|
| two or more folders, every one idle >= `squash.idle.timeout`, not the active period, no Parquet/READ ONLY/REMOTE folder | **merge** the whole logical partition into one folder |
| otherwise | compact alone each COMPOSITE folder idle >= `idle.timeout`; plain folders are left alone |

The active logical partition is never merged: the writer holds it open and its files carry WAL lag rows no
piece accounts for; the writer's own commit-time squash folds it. A plain folder alone in its logical
partition is skipped outright.

## Gates, cheapest first

Per partition, in `scanTable`:

1. Skip READ ONLY and REMOTE partitions.
2. Read `_txn` standalone (short-lived mmap, no lock). A plain single-piece native partition is skipped
   without opening `_geometry` or `_pm`.
3. **Recency filter** - skip a partition whose upper time bound (the next logical partition's start, or the
   table's max timestamp - never the next split's start) is inside `[now - idle.timeout, now]`.
4. **Confirm idle** with one targeted read: a composite folder's `_geometry` `lastWriteMicros`, a plain
   folder's timestamp-column mtime, a Parquet partition's `_pm` footer. Parquet "clean" answers are memoised.

Each tick starts at a seeded random table, pre-charges each copy against `io.budget` at
`io.cost.multiplier x liveRows x estimatedRecordSize`, and stops before starting another when that budget or
`time.budget` is spent. The first dispatch always runs, so a partition larger than the budget cannot starve.

## Dispatch

| entry point | partition | result |
|---|---|---|
| `dispatchMerge` | a whole logical partition past the squash threshold | one directory holding every folder's live rows, staged as `.merging<folderCount>`; the swap replaces the run of `_txn` entries with one |
| `dispatchMakePlain` | composite, one piece at row 0 with dead space above (`isMakePlainShape`) | MAKE-PLAIN + TRIM-FILES in place on the writer |
| `dispatchComposite` | composite, otherwise | REWRITE: live rows in timestamp order, staged as `.compacting<generation>` |
| `dispatchParquet` | Parquet with dead row groups or a stale schema | live row groups re-encoded into staging |

Single-folder commands dispatch by the folder's OWN start timestamp, not the logical start - for a split
those are different directories.

## Swap protocol

`engine.getWriterOrPublishCommand` applies the swap inline on the sweep thread when the writer is idle, or
queues it on the busy writer's `TableWriterTask` queue to be applied in `tick()`. The sweep records seven
longs per in-flight swap, sorted by table and logical partition: `(tableId, logicalPartitionTimestamp,
targetTimestamp, targetNameTxn, targetGeneration, expiry, writerId)`.

- The record stands the sweep down on EVERY folder of that logical partition: at most one swap per logical
  partition is outstanding, and siblings take turns one sweep after another.
- A record is pruned when its target has moved on (which landing the swap does), when its writer is closed,
  distressed, evicted or replaced, or when the table is dropped. A write into a different folder of the same
  logical partition does not prune it.
- The expiry is not a TTL. A command a writer consumed without moving `_txn` - refused, or its rename failed
  - would otherwise park its logical partition for the life of that writer. `swap.timeout` (30 min) after the
  record was taken, the sweep takes the writer out of the pool and ticks it, which drains the queue; only then
  does it forget the record.
- After an inline swap, `notifyWalApplyIfLagging` re-sends the WAL apply notification dropped while the
  writer was out of the pool.

## Staleness and safety

Each command carries the source fingerprint - `(nameTxn, generation / parquetFileSize, metadataVersion)` -
and the writer drops a command whose partition has moved on; the next sweep decides again. A build that fails
before its swap removes its own staging directory. MAKE-PLAIN's reader-safety wait still applies, so the
sweep's reader is closed before the writer runs.

## Config

| key | default | meaning |
|---|---|---|
| `cairo.partition.compaction.check.interval` | 2 min | sweep cadence; negative disables |
| `cairo.partition.compaction.idle.timeout` | 60 min | a composite folder must be untouched this long to be compacted alone |
| `cairo.partition.compaction.squash.idle.timeout` | 30 min | every folder of a logical partition must be untouched this long to merge it; clamped to at most `idle.timeout` |
| `cairo.partition.compaction.io.budget` | 1 GiB | estimated bytes a sweep may start copying; the first dispatch always runs |
| `cairo.partition.compaction.io.cost.multiplier` | 2 | a copy's cost against the budget, as a multiple of its live bytes |
| `cairo.partition.compaction.time.budget` | 1 s | elapsed-time backstop between dispatches |
| `cairo.partition.compaction.swap.timeout` | 30 min | how long an unanswered swap parks its logical partition |

The waste thresholds (`dead.rows.ratio`, `dead.min.size`, `piece.threshold`, `avg.rows.piece.lim`, the
`table.dead.*` knobs) are shared with the per-commit policy - see `PARTITION_COMPACTION.md`.
