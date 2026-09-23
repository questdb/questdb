# Partition compaction sweep: swap ownership, scheduling and IO budgeting

Status: **implemented**.
Context: PR #7595 (branch `lazy-geometry2`), base `e2a03853184fa8c04fb446e7876e31c61b27be12`.
Supersedes the `consumed`-flag design currently in the working tree (see "What this replaces").

---

## 1. Problem

`PartitionCompactionScanJob` rebuilds an idle composite or parquet partition into a staging directory
`<partition>.<nameTxn>.compacting<generation>`, then hands a swap command to the table's writer. When the
writer is busy, the command goes onto its `commandQueue` and **ownership of that staging directory transfers
to the command**: the sweep must not touch the directory again until the writer is done with it.

The staging name is a pure function of `(table, partition, srcNameTxn, generation)` (`setStagingPath`), so
any later rebuild for the same generation targets the *same* directory. The sweep therefore needs to know
whether a command is still outstanding.

Shipped behaviour answers that with a 60-minute TTL memo (`PENDING_SWAP_MEMO_TTL_MICROS`). The TTL is a
guess, and when it expires early the sweep deletes and re-creates a directory a queued command still owns;
the writer's staleness battery checks generation identity, never staged *content*, so it renames an empty
directory into the table and the partition becomes unreadable.

Two further weaknesses of the shipped design:

- suppression is keyed on a fingerprint plus `ff.exists(stagingDir)`, so a directory left behind by a
  terminal writer path (failed rename; stale decline whose `rmdir` failed) reads as "still pending";
- the sweep meters work by partition count (`MAX_DISPATCH_PER_SWEEP = 32`), which says nothing about the
  resource a rebuild actually consumes.

## 2. Design goals

1. A rebuild never touches a staging directory an outstanding command owns — no timer, no guess.
2. No unbounded state: the in-flight set is bounded by the number of partitions with an outstanding command.
3. A partition is never pinned out of compaction permanently by a lost command.
4. No participation from `TableWriter.doClose()`, and nothing special on a distressed writer.
5. Compaction consumes a bounded, configured share of **disk IO**, the scarce resource.

## 3. In-flight tracking

### 3.1 Record

One record per partition, **5 longs**:

```
tableId, partitionTimestamp, srcNameTxn, generation, writerId
```

`partitionTimestamp` is the logical (floored) timestamp, so the parquet path's
`txWriter.getLogicalPartitionTimestamp` normalisation and the composite path key the same partition
identically.

`writerId` is a new monotonic id assigned in the `TableWriter` constructor, captured **at publish time**.

### 3.2 Storage

A flat `LongList`, stride 5, kept sorted by `(tableId, partitionTimestamp)`.

- Lookup: binary search on the composite key.
- Per-table operations: every record for a table is **contiguous**, so one binary search finds the block and
  the prune walks it. This is the dominant access pattern; a hash map would scatter it.
- Mutation: binary-search insert/remove in place, done immediately as each fact is established — no scratch
  list, no block splicing. A few hundred bytes of memmove is noise beside a dispatch that copies megabytes
  to gigabytes, and per-item mutation leaves the list consistent even when `scanTable` throws mid-way.
- Zero-GC, `io.questdb.std`, no allocation after growth.

### 3.3 Suppression rule

`dispatchComposite` / `dispatchParquet` skip a partition when a record exists for it **and**

- the record's `srcNameTxn` and `generation` equal the partition's current values, **and**
- the table's live writer id equals the record's `writerId`.

Otherwise the record is obsolete and the dispatch proceeds.

### 3.4 Pruning

Two independent, cheap signals — no TTL:

- **Completion.** `scanTable` already reads `_txn` and the geometry for every table it visits. While walking
  that table's contiguous record block it drops any record whose partition is now plain, or whose
  `nameTxn`/generation has moved. A successful swap makes the partition plain, so the record clears itself
  with no extra IO — and because the prune walks the memo by table rather than by qualifying partition, the
  early skip of plain partitions cannot hide it.
- **Loss.** A different live `writerId`, or no writer open, proves the queue that held the command is gone
  (distressed release, `TableWriter.destroy()`, pool eviction). The record is dropped and the partition
  becomes eligible again.

**Per visited table only.** Records for tables this sweep did not visit are carried over untouched. The
dispatch budget and the random start mean tables are routinely skipped; rebuilding the list wholesale would
drop their in-flight records and re-open the clobbering bug.

## 4. Scheduling

- **Random start table** each sweep. This removes the resume pointer and its known wart: it is an *index*,
  so a table created or dropped shifts it and can repeat or skip a table for a tick.
  The RNG must be seeded through configuration, not `Math.random()`, or the starvation test and the fuzz
  suites lose reproducibility.
- **Duty cycle.** Stamp `last` at sweep **end**, not sweep start. Today it is stamped at the start, so a
  sweep that overruns the interval is immediately eligible again and sweeps run back-to-back.
- Starvation is accepted deliberately: at these budgets the backlog is only saturated when there is far more
  reclamation work than device capacity, and compaction is reclamation, not user-visible latency.

## 5. IO budgeting

Disk IO, not thread time and not partition count, is what compaction takes from WAL apply and O3 writes on
the same device.

- **Primary meter: bytes per sweep.** Pre-charge each dispatch before it starts, from `liveRows ×
  avgRecordSize()` (already used by the threshold policy) or the summed column file sizes. Charge **~2×
  live bytes**: a rebuild reads the live rows and writes them, plus sidecar/index work on the parquet path.
  `budget / check.interval` is the MB/s ceiling granted to compaction — a number an operator can reason
  about against their device.
- **Backstop: elapsed-time cap.** The byte figure is an estimate and mispredicts on wide or
  variable-length columns and on parquet re-encode. Read the clock through the `Clock` the job already
  takes, so tests stay deterministic under `setCurrentMicros`.
- **Floor: always permit the first dispatch of a sweep**, whatever the budgets say, or a partition larger
  than the budget is never compacted.
- Both budgets are "do not start another" gates. A copy in flight is never interrupted, so overrun is
  bounded by a single partition.
- The parquet probe budget (`MAX_PROBE_PER_SWEEP`) is unchanged: it meters footer mmaps, not copies, and a
  memo hit is already free.

## 6. What this replaces

Reverts to untouched, relative to the current working tree:

- `io/questdb/cairo/sql/AsyncWriterCommand.java` — no `isConsumed()` / `markConsumed()` defaults
- `CompositePartitionSwapCommand.java`, `ParquetPartitionSwapCommand.java` — no `volatile consumed`
- `TableWriter.processCommandQueue` — no hoist, no `try/finally`
- `TableWriter.doClose` / `drainCommandQueueOnClose()` — deleted entirely, and with it the unwired-barrier
  hang that method introduced and the guard added to contain it

Retained, unrelated to this mechanism: the two `case APPEND` forecast arms in
`wouldBreachCompactionThresholds` / `wouldMoveTailSucceed`, and the rename-failure message fix on both swap
paths.

New plumbing required:

- monotonic writer id assigned in the `TableWriter` constructor;
- an engine call to read the live writer id for a table **without acquiring the writer**;
- the id captured at publish time, not after — a close/reopen between publish and record would otherwise
  pin the record until the *new* writer closes.

## 7. Why not the alternatives

- **Keep the TTL, make it longer.** Narrows the window, restores no invariant; the expiry is still a guess.
- **Suppress on `ff.exists(stagingDir)`.** A directory left by a failed rename or a failed `rmdir` then
  vetoes that partition forever and leaks a full copy, because the generation never moves and the writer's
  stale purge deliberately keeps live-generation directories.
- **Consumer-signalled `consumed` flag + close-time drain** (the version in the tree). Correct, but it puts
  a drain in `doClose`, which on a **distressed** release — the code names "disk is full" as a cause —
  marks the swap consumed and invites a full-partition rebuild 120 s later on the disk that just failed.
  It also introduced a non-terminating loop when the constructor throws before the ring barrier is wired.
- **Hold the command object, or the published cursor plus the writer's `SCSequence`.** Both work and the
  sequence variant is elegant (the sequence is heap, safe to read after close, and doubles as an
  instance identity), but both hold a reference to writer-owned state and require plumbing the cursor back
  through `WriterPool.addCommandToWriterQueue`. Five longs and a writer id carry the same information.
- **Immutable staging incarnations (unique suffix per build), fully fire-and-forget.** Removes the clobber
  by construction, but `TableWriter.removeCompactingPartitionDirIfStale` parses the whole name tail with
  `Numbers.parseLong` and *leaves* anything it cannot parse, so it must learn the new format; and with no
  suppression at all the sweep rebuilds a fresh full copy every interval while a command is queued.

## 8. Risks and residuals

- **Retry cadence.** After a writer dies the record clears, so a partition whose swap keeps failing is
  retried at the sweep cadence. This is bounded by the byte budget rather than unbounded, which is the point
  of §5, but it is still repeated work against a possibly-failing device.
- **Second job instance.** An engine running two `PartitionCompactionScanJob` instances (the enterprise
  tests construct one against a live engine) keeps per-instance memos, so instance B can rebuild a directory
  instance A's live command owns. Pre-existing and unchanged; only per-build incarnations or an on-disk
  completeness token would close it.
- **Writer id races.** The id must be read without acquiring the writer, so it is a snapshot. A stale read
  can only cause an extra rebuild or one sweep of extra suppression, never a clobber, provided the id is
  captured at publish time.
- **Byte estimate accuracy.** `liveRows × avgRecordSize()` is the same estimate the threshold policy already
  trusts; the time backstop exists because it can be wrong.

## 9. Test plan

- Clobber regression, composite and parquet: queue a swap on a busy writer, force a second sweep, assert one
  staging `mkdirs` and intact partition data.
- Completion prune: after a deferred swap applies, assert the in-flight list is empty and the partition is
  plain.
- Loss prune: close the writer with the swap still queued (drive `TableWriter.destroy()`, which reaches
  `doClose` with no preceding tick or rollback), assert the record clears and the next sweep re-dispatches.
- Distressed release: assert no rebuild is attempted while the same writer instance is alive.
- Carry-over: with more qualifying tables than the budget, assert an unvisited table keeps its records and
  its staging directory is untouched.
- Budget: assert the sweep stops dispatching once the byte budget is spent, and that a single oversized
  partition is still dispatched (the floor rule).
- Determinism: seeded RNG for the random start; `setCurrentMicros` for the duty cycle and the time backstop.
