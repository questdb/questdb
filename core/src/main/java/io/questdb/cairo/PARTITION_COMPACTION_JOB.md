# The background compaction sweep

`PARTITION_COMPACTION.md` describes the four moves and the per-commit policy that picks one partition to
compact on the writer thread every commit (`PartitionCompactionPolicy`). That path only fires when a commit
lands on the table. A partition that goes cold still holding dead space is never revisited: a composite
partition left with superseded pieces, or a Parquet partition carrying dead row groups from in-place O3
updates, gets no more commits to trigger its own reclamation.

`PartitionCompactionScanJob` is that safety net - a periodic sweep that finds idle wasteful partitions and
hands each to its writer to compact. It reclaims exactly the same waste the per-commit path would have, off
the writer thread, for partitions the per-commit path can no longer reach.

## The unit of work: a LOGICAL partition

One period of the table's PARTITION BY unit - an hour, day, week, month or year - can be several directories:
the main folder (`2024-01-01`) and the splits a MOVE-TAIL or an O3 partition split leaves later in the same
period (`2024-01-01T050000-...`). Each is its own `_txn` entry. The sweep takes the whole run of entries
sharing one logical start as one unit, and dispatches at most one command for it per sweep.

| logical partition state | action |
|---|---|
| every folder idle >= squash idle, two folders or more, not the active period | merge the whole logical partition into one folder |
| anything else | compact alone each COMPOSITE folder idle >= single idle; leave plain folders alone |

The merge stands down on three shapes, which keep their per-folder treatment: a logical partition holding a
Parquet, READ ONLY or REMOTE folder (none of them rows this native copy may read or replace), and the ACTIVE
logical partition - the one holding the last `_txn` entry. That partition is the one the writer holds open,
and its files carry the WAL lag rows past the live ones, which no piece accounts for and a copy built off a
reader snapshot would drop; the writer's own squash folds it on commit. The writer re-checks all of this.

A plain folder alone in its logical partition has no dead space and no split to merge, so the sweep still
skips it outright. A plain folder is never compacted on its own, however cold: only the whole-partition merge
has anything to gain there.

## Where it runs

Its own single-thread worker pool (`PartitionCompactionPoolConfiguration`), never the shared write pool: a
composite REWRITE copies the whole partition inline on the worker that picks the job up, and on the shared
pool that worker is also running WAL apply and the O3 jobs, so the copy would stall ingestion for as long
as it takes. Wired in `ServerMain`, and only when the instance is not read-only.

**WAL tables only.** A non-WAL writer holds its transaction open across ticks, so a swap built off a reader
snapshot could be handed to a writer carrying uncommitted rows the snapshot never saw. Non-WAL tables keep
their per-commit compaction; only this out-of-band path stands down for them.

## The tick

A `SynchronizedJob` gated by `cairo.partition.compaction.check.interval` (default 2 min). A negative interval
disables the sweep entirely - per-commit compaction is unaffected; zero means sweep on every call.

Each tick starts at a configuration-seeded random table. It pre-charges rebuilds against an estimated disk
IO budget (`2 * liveRows * estimatedRecordSize`) and stops before starting another rebuild when that budget
or the elapsed-time backstop is spent. The first dispatch always runs, so a partition larger than the budget
cannot starve. The interval starts when the sweep ends, preventing an overrun from causing back-to-back sweeps.

## Gates, cheapest first

Per partition, in `scanTable`, each gate avoids the cost of the next:

1. Skip READ ONLY and REMOTE partitions - not this job's to rewrite.
2. Read `_txn` standalone (no reader or writer lock, a short-lived mmap). A plain single-piece native
   partition has no dead space and is skipped outright, never opening `_geometry`/`_pm`.
3. **Recency filter** - skip any partition whose upper time bound is inside `[now - idleTimeout, now]`. For
   time-partitioned data only the newest partitions can still take O3 writes, so this rules out most
   survivors using fields already in `_txn`. `cairo.partition.compaction.idle.timeout`, default 60 min.
4. **Confirm idle** with one targeted read of the survivors: a composite folder's `_geometry`
   `lastWriteMicros`, a plain folder's designated timestamp column file modification time, or a Parquet
   partition's `_pm` footer for dead bytes / a stale schema. Parquet "clean" answers are memoised
   (`cleanParquetPartitions`) so a compacted partition is not re-read every pass.

Gate 3 reads the END of the logical partition - the next logical partition's start, or the table's max
timestamp - never the next split's start, which is inside the run being decided.

## Dispatch

Three entry points, chosen by the partition's shape:

| entry point | partition | what it asks for |
|---|---|---|
| `dispatchMerge` | a whole logical partition, every folder idle past the squash threshold | one directory holding every folder's live rows, built off a `TableReader` snapshot into a `.merging<folderCount>` staging directory; the swap replaces the run of `_txn` entries with one |
| `dispatchMakePlain` | composite, already one piece at row 0 with dead space above (`isMakePlainShape`) | MAKE-PLAIN + TRIM-FILES in place - nothing staged, nothing copied |
| `dispatchComposite` | composite, otherwise | a REWRITE: all live rows in timestamp order, built off a `TableReader` snapshot into a `.compacting<generation>` staging directory |
| `dispatchParquet` | Parquet with dead row groups or a stale schema | live row groups copied (re-encoded under the current schema when stale) into staging |

The three single-folder entry points dispatch by the folder's OWN start timestamp. The logical start resolves
to the main folder, which for a split is the wrong directory entirely - and a logical partition whose main
folder is missing resolves to nothing at all.

The merge does not reuse the writer's own squash: the job builds the merged directory itself, off a reader
snapshot, holding no writer, exactly as a REWRITE does. The writer only runs the swap.

## Swap protocol

`engine.getWriterOrPublishCommand` decides how the result lands. An idle writer applies the swap inline on
the sweep thread. A busy writer instead gets the command queued onto its own `TableWriterTask` queue and
applies it on its own thread via `tick()`. The pool captures that writer's monotonic instance id while its
publish fence holds the writer live. The sweep records seven longs, sorted by table and LOGICAL partition:
`(tableId, logicalPartitionTimestamp, targetTimestamp, targetNameTxn, targetGeneration, expiry, writerId)`.
The target is what the command's staging directory is named after: for a single-folder command the folder's
start, name txn and generation; for a MERGE a whole-run marker, the first folder's name txn and the folder count.

The record stands down the sweep on EVERY folder of that logical partition, not just the one the swap was
built from: rebuilding a sibling would take the writer's queued command down a path that no longer matches
what it is about to rename. At most one swap per logical partition is therefore outstanding, and folders that
each deserve their own compaction take their turns one sweep after another.

When the job next visits the table, it prunes records whose target has moved on - which is what landing the
swap does. A write into a DIFFERENT folder of the same logical partition does not prune the record: while the
target keeps its identity, a rebuild would clear and refill the very staging directory the queued command is
about to rename, and the command re-checks only its target, so it would publish a half-built copy. A closed, distressed, evicted, or
replaced writer also invalidates its records, and every sweep removes records for dropped tables. Records
belonging to existing tables skipped because a sweep spent its budget remain untouched. No staging-directory
existence check participates: it cannot establish command ownership safely.

The expiry is not a TTL on the record. A command a writer consumed without moving `_txn` - one it refused, or
one whose rename failed - would otherwise park its logical partition for the life of that writer instance. 30
minutes after the record was taken, the sweep takes the writer out of the pool and ticks it, which consumes
whatever is still queued; only then does it forget the records. A writer too busy to hand over keeps them,
and the next sweep tries again. Nothing is rebuilt on the strength of the elapsed time alone.

After an inline swap, `notifyWalApplyIfLagging` re-sends the WAL apply notification that was dropped while the
writer was out of the pool.

## Staleness and safety

The build runs off a read snapshot holding no writer, so ingestion may advance the partition's `nameTxn` or
geometry generation before the swap lands. Each swap command carries the source fingerprint - `(nameTxn,
generation / parquetFileSize, metadataVersion)` - and the writer drops a command that lands on a partition
that has moved on; the next sweep decides again. A build that fails before its swap removes its own staging
directory. MAKE-PLAIN's own reader-safety wait (see `PARTITION_COMPACTION.md`) still applies: the sweep's
reader must be closed before the writer runs, because MAKE-PLAIN waits out the readers still resolving the
record it retires.

## Config

| key | default | meaning |
|---|---|---|
| `cairo.partition.compaction.check.interval` | 2 min | sweep cadence; negative disables the sweep |
| `cairo.partition.compaction.idle.timeout` | 60 min | a composite folder must be untouched this long to be compacted on its own |
| `cairo.partition.compaction.squash.idle.timeout` | 30 min | EVERY folder of a logical partition must be untouched this long for the whole partition to be merged; clamped to at most the key above |
| `cairo.partition.compaction.io.budget` | 1 GiB | estimated read-plus-write bytes a sweep may start; the first dispatch always runs |
| `cairo.partition.compaction.time.budget` | 1 s | elapsed-time backstop checked between dispatches |

The thresholds that decide *whether a partition is wasteful* (`dead.rows.ratio`, `piece.threshold`,
`avg.rows.piece.lim`, the `table.dead.*` pressure knobs, and so on) are shared with the per-commit policy
and documented in `PARTITION_COMPACTION.md`.
