# The background compaction sweep

`PARTITION_COMPACTION.md` describes the four moves and the per-commit policy that picks one partition to
compact on the writer thread every commit (`PartitionCompactionPolicy`). That path only fires when a commit
lands on the table. A partition that goes cold still holding dead space is never revisited: a composite
partition left with superseded pieces, or a Parquet partition carrying dead row groups from in-place O3
updates, gets no more commits to trigger its own reclamation.

`PartitionCompactionScanJob` is that safety net - a periodic sweep that finds idle wasteful partitions and
hands each to its writer to compact. It reclaims exactly the same waste the per-commit path would have, off
the writer thread, for partitions the per-commit path can no longer reach.

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

Each tick hands out at most `MAX_DISPATCH_PER_SWEEP` (32) dispatches and resumes the table walk from where
the last one stopped (`sweepStartTableIndex`), so a table with more qualifying partitions than the budget
cannot starve the tables behind it - every table gets its turn while the per-tick bound holds.

## Gates, cheapest first

Per partition, in `scanTable`, each gate avoids the cost of the next:

1. Skip READ ONLY and REMOTE partitions - not this job's to rewrite.
2. Read `_txn` standalone (no reader or writer lock, a short-lived mmap). A plain single-piece native
   partition has no dead space and is skipped outright, never opening `_geometry`/`_pm`.
3. **Recency filter** - skip any partition whose upper time bound is inside `[now - idleTimeout, now]`. For
   time-partitioned data only the newest partitions can still take O3 writes, so this rules out most
   survivors using fields already in `_txn`. `cairo.partition.compaction.idle.timeout`, default 60 min.
4. **Confirm idle** with one targeted read of the survivors: a composite partition's `_geometry`
   `lastWriteMicros`, or a Parquet partition's `_pm` footer for dead bytes / a stale schema. Parquet "clean"
   answers are memoised (`cleanParquetPartitions`) so a compacted partition is not re-read every pass.

## Dispatch

Three entry points, chosen by the partition's shape:

| entry point | partition | what it asks for |
|---|---|---|
| `dispatchMakePlain` | composite, already one piece at row 0 with dead space above (`isMakePlainShape`) | MAKE-PLAIN + TRIM-FILES in place - nothing staged, nothing copied |
| `dispatchComposite` | composite, otherwise | a REWRITE: all live rows in timestamp order, built off a `TableReader` snapshot into a `.compacting<generation>` staging directory |
| `dispatchParquet` | Parquet with dead row groups or a stale schema | live row groups copied (re-encoded under the current schema when stale) into staging |

## Swap protocol

`engine.getWriterOrPublishCommand` decides how the result lands. An idle writer applies the swap inline on
the sweep thread. A busy writer instead gets the command queued onto its own `TableWriterTask` queue and
applies it on its own thread via `tick()`. A queued swap is remembered in `pendingSwaps` with a TTL
(`PENDING_SWAP_MEMO_TTL_MICROS`, 60 min) so the next sweep does not rebuild work already staged; `isSwapPending`
also confirms the staging directory still exists before standing down. After an inline swap,
`notifyWalApplyIfLagging` re-sends the WAL apply notification that was dropped while the writer was out of
the pool.

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
| `cairo.partition.compaction.idle.timeout` | 60 min | a partition must be untouched this long to qualify |

The thresholds that decide *whether a partition is wasteful* (`dead.rows.ratio`, `piece.threshold`,
`avg.rows.piece.lim`, the `table.dead.*` pressure knobs, and so on) are shared with the per-commit policy
and documented in `PARTITION_COMPACTION.md`.
