# WAL ts-storage read-side implementation plan

Status: design, not yet implemented.
Companion to `wal-ts-storage-plan-v2.md` (write side).

## 0. Approach

The read side is the riskier half of this work because correctness is
verified against the full SQL surface, not a focused new code path.
The plan accepts that risk explicitly by sequencing the work in four
phases, each with a clear completion contract:

- **Phase 0**: a mock writer that produces partitions in the new
  format from test code, so read-side development does not depend on
  the write-side PR landing first.
- **Phase 1**: a transparent frame replacement. Every existing query
  works against the new storage with **identical results**, hiding
  storage complexity behind a frame that materialises sorted-order
  access through indirection. Performance is not the goal; the goal
  is "all SQL tests pass with the flag on."
- **Phase 2**: storage-aware fast paths for priority operators
  (count, group-by, sample-by, ts-range aggregations). These
  operators learn that order can be skipped and run on contiguous
  unordered frames with full SIMD.
- **Phase 3**: continue expanding to remaining operators. ASOF,
  LATEST BY, window functions, top-k, distinct, hash join slave.

The phasing matters because Phase 1 is the safety net: if Phase 2 or
3 stalls on a particular operator, that operator keeps working via
Phase 1's slow path until we fix it. This means the feature can ship
behind the flag without all operators being optimised, and we can
ship operator optimisations incrementally without breaking the rest.

## Phase 0: Mock writer for test data generation

### What it produces

A test-only utility that constructs a partition in the new format
without invoking the WAL apply machinery. It writes binary-correct
column files, a `_ts.commits` file, and updates the test
`TableReader`'s `_txn` metadata.

### Why we need it before the write-side PR lands

The read-side work is large and we want to start it in parallel with
write-side work. Coupling read-side dev to write-side completion
serialises them and bottlenecks on the write PR landing. Mock data
generation lets read-side proceed independently.

### Design

```
TsMockPartitionBuilder builder = new TsMockPartitionBuilder(ff, root, tableToken, partitionTs)
    .columns(int.class, long.class, double.class)
    .addCommit(rows -> rows
        .row(ts(1000), 1, 1.0)
        .row(ts(1010), 2, 2.0)
        .row(ts(1020), 3, 3.0))               // commit 1: in-order
    .addCommit(rows -> rows
        .row(ts(1015), 4, 4.0)                // commit 2: O3, ts falls inside commit 1's range
        .row(ts(1025), 5, 5.0))
    .addCommitWithDupes(...)                  // commit 3: dedup test material
    .build();
```

The builder:
- Sorts each commit's rows by ts internally before "writing" (matches
  WAL apply semantics from `wal-ts-storage-plan-v2.md` §5.2 step 2).
- Appends each commit's rows to the column files at the partition's
  current tail.
- Appends one `CommitStat` per commit to `_ts.commits`.
- Updates `_txn` slot [3] valid-size + `PARTITION_MASK_TS_COMMITS_BIT_OFFSET`.

Lives under `core/src/test/java/io/questdb/test/cairo/wal/tsruns/`.

### Sub-components

- `TsMockPartitionBuilder` -- fluent API above.
- `TsCommitStatDecoder` -- read-only decoder for `_ts.commits`, used
  by tests to assert the file's content.
- `TsTestData` -- factory of common test fixtures (single in-order
  run, K in-order runs, K mixed-order runs, runs with dupes, etc.)
  for parameterised tests.

### Completion contract for Phase 0

- `TsMockPartitionBuilder` produces partitions that `TableReader`
  recognises as TsRuns-format (the flag bit lights up).
- `TsCommitStatDecoder` round-trips the file bytes correctly.
- A handful of golden-bytes tests assert binary stability of the
  `_ts.commits` format under known inputs.
- No read-side cursor code yet; Phase 0 deliverable is data
  generation only.

## Phase 1: Transparent frame replacement

### Goal

Every query that works today on standard partitions works identically
on TsRuns partitions. Correctness only; performance comes in later
phases.

The mechanism: a new partition format value
`PartitionFormat.NATIVE_TS_RUNS` and an "ordered indexed frame" that
materialises the sort order into a per-frame scratch array. All
existing column-access code remains unchanged. The cost is one
per-row indirection on every column read.

### What changes

#### New: `TsCommitsReader`

Per-partition read-side handle. Opens `_ts.commits` at partition open,
loads the `CommitStat[]` into memory. Methods:

- `commitCount()` -- number of CommitStats.
- `commitAt(int i)` -- decode the i-th CommitStat into a thin view.
- `findRunsOverlapping(intervalLo, intervalHi)` -- scan stats, return
  the contiguous run-index range whose ts ranges overlap the
  interval. For ts-range pruning by aggregators.
- `materialiseSortedSlice(logicalLo, logicalHi, long[] scratch)` --
  walk the runs in K-way merge order across logical positions
  `[logicalLo, logicalHi)` and write each row's physRowId into
  `scratch`. Used by Phase 1 frames.

The K-way merge inside `materialiseSortedSlice` is the heart of
Phase 1's slow path. A priority queue over run heads; pop and write
the smallest ts. Implementation should be zero-GC: reuse the queue
and head state across calls.

Lives under `io.questdb.cairo.wal.tsruns`.

#### New: `PartitionFormat.NATIVE_TS_RUNS`

A new value in `PartitionFormat` (or its successor enum). Set on the
PartitionFrame when `_txn`'s ts-commits flag bit is on.

#### New: ordered-indexed frame variant

The simplest implementation: extend `PageFrameMemoryRecord` to know
about a per-frame `physRowIds[]` scratch buffer. When the frame is
`NATIVE_TS_RUNS`, `setRowIndex(logicalIdx)` reads
`physRowIds[logicalIdx - frameRowLo]` and stores it in the existing
`rowIndex` field. Column accessors continue to compute
`address + (rowIndex << shift)` -- unchanged.

The scratch buffer is materialised once per frame opening, by the
partition-frame cursor calling
`tsCommitsReader.materialiseSortedSlice(rowLo, rowHi, scratch)`.

Per-row cost on read: one extra L1/L2-resident memory load
(scratch dereference) per column access.

#### Modified: partition-frame cursors

`FullFwdPartitionFrameCursor`, `FullBwdPartitionFrameCursor`,
`IntervalFwdPartitionFrameCursor`, `IntervalBwdPartitionFrameCursor`
gain a branch: when the partition is `NATIVE_TS_RUNS`, the frame's
`rowLo`/`rowHi` are *logical* positions (cumulative across runs).
The frame additionally carries a pointer into a per-frame scratch
buffer of `physRowId` values, pre-populated by the cursor.

For interval cursors, the lo/hi computation must use the merge
cursor's ts-order, not the underlying ts column. Concretely:
`TsCommitsReader.findRunsOverlapping(intervalLo, intervalHi)` plus a
within-run binary search on each boundary run's ts column produces
the logical row range covering the interval.

#### Modified: `Rows` / packed row ids

`Rows.toRowID(partition, localRowId)` keeps its current shape
`partition(20) | localRowId(44)`. In Phase 1, `localRowId` is the
physRowId (after the scratch dereference), so all downstream
consumers of packed row ids see the same semantic as today.

This is important: bitmap indexes (which store physRowIds) continue
to work without remapping for queries that don't require ts order.

### What does not change in Phase 1

- JIT compilation. JIT continues to operate on contiguous column
  memory exactly as today. For frames that materialise scratch, JIT
  is invoked on the *underlying* column data (i.e., on the physical
  layout), not on the ordered view. Phase 1 simply does not invoke
  JIT for `NATIVE_TS_RUNS` frames when the consumer requires order
  -- it falls back to the existing Java filter loop, with the same
  per-row scratch deref.
- Operator implementations. Every existing operator works on the new
  frame because the frame exposes the same interface (`setRowIndex` +
  column accessors) and the same row range semantics (`rowLo`/
  `rowHi` describe a contiguous, in-ts-order range).

### Completion contract for Phase 1

- All SQL tests under `core/src/test/java/io/questdb/test/griffin/`
  pass on TsRuns partitions when the flag is on.
- Per-query result equality is the bar: a query Q over a standard
  partition and over an equivalent TsRuns partition (same logical
  data, different on-disk layout) returns identical results.
- Performance is degraded but not pathological. Targeting "within
  5x of standard partitions" for sequential scans is a reasonable
  guard against accidental quadratic behaviour.
- Phase 1 is the all-green milestone. After Phase 1, the flag could
  ship as experimental without breaking any query.

### Test strategy for Phase 1

- Parameterised SQL tests: each existing query test class runs twice,
  once with the flag off (standard) and once with the flag on
  (TsRuns), asserting identical results.
- Result-equality tests across pairs of partitions built by
  `TsMockPartitionBuilder` with the same logical data but different
  CommitStat structure (one big run vs. K small runs, in-order vs.
  O3-history, etc.).
- Crash-safety tests: open/close cycles, restart between commits,
  garbage tail bytes ignored.
- Edge cases: empty partition, single-row partition, partition with
  one run, partition with thousands of runs, dupes at run boundaries,
  dupes within a single run.

## Phase 2: Storage-aware fast paths

### Goal

Identify the queries that QuestDB users run most often, and make them
exploit the new storage's strengths instead of paying for Phase 1's
slow indirection. Specifically: when an operator doesn't need ts
order, it should scan runs as flat contiguous frames with full JIT
and SIMD, prune by CommitStat min/max, and parallelise across runs.

### Why these queries first

Phase 2 prioritises queries where the new storage can be **faster
than the current implementation** because today's path pays for
write-time sort that the operator does not need. SAMPLE BY is the
flagship -- it bucket-aggregates by `floor(ts / width)` and doesn't
care about row order during the scan. Today, SAMPLE BY benefits
incidentally from sorted storage; under TsRuns storage, freeing it
from the order requirement lets it scan unordered runs at the
maximum disk throughput.

### Priority operators

| Operator                              | Why fast under TsRuns                         |
|---------------------------------------|-----------------------------------------------|
| `COUNT(*)` no filter                  | Sum CommitStat.rowCount across runs. O(K).    |
| `COUNT(*) WHERE ts BETWEEN ...`        | Prune runs by CommitStat ts range, sum survivors. |
| `COUNT(col_filter) WHERE ...`         | Prune runs, JIT-evaluate predicate per run, sum matches. |
| Non-keyed aggregates (SUM/AVG/MIN/MAX)| Same shape as COUNT with filter.              |
| GROUP BY <non-ts cols>                 | Same shape; aggregator is keyed.              |
| SAMPLE BY <width>                      | Aggregator keys by `floor(ts/width)`; row order during scan is irrelevant. Prune runs by ts, scan survivors unordered. |
| `WHERE ts BETWEEN ... GROUP BY ...`    | Combines the above.                           |

### The Phase 2 mechanism: order requirement signal

A new method on `RecordCursorFactory`:

```java
default int getOrderRequirement() {
    return ORDER_TS_ASC_REQUIRED;     // default keeps backwards compat
}
```

with values `ORDER_TS_ASC_REQUIRED`, `ORDER_TS_DESC_REQUIRED`,
`ORDER_NONE`.

Factories that don't need order override to return `ORDER_NONE`. The
partition-frame cursor reads it and chooses the frame mode per
partition:

- `ORDER_NONE` on a `NATIVE_TS_RUNS` partition -> emit one
  unordered-contiguous frame per surviving run. No scratch
  materialisation. JIT runs at full SIMD on each run's physical
  column memory.
- Anything else on a `NATIVE_TS_RUNS` partition -> Phase 1's
  scratch-buffer ordered frame.

The operators in the table above each override `getOrderRequirement`
to return `ORDER_NONE` (or its keyed variants if dialect matters).

### Operator changes

For each priority operator, the work is narrowly scoped:

1. **Override `getOrderRequirement` to `ORDER_NONE`** on the
   relevant `RecordCursorFactory`.
2. **Add a fast path that consumes multiple frames per partition**
   instead of one (since unordered mode produces one frame per run).
   Some operators already do this via the existing
   `UnorderedPageFrameSequence` plumbing; they need no further
   change.
3. **For SAMPLE BY specifically**: the aggregator must key by ts
   bucket explicitly rather than implicitly relying on input
   ordering. The existing implementation already keys by computed
   bucket id; the change is making sure the planner can pick this
   path when the base is `NATIVE_TS_RUNS`.

### Interval pruning in unordered mode

For `WHERE ts BETWEEN A AND B`:

- `IntervalFwdPartitionFrameCursor` (and Bwd) checks the partition
  format. For `NATIVE_TS_RUNS` + `ORDER_NONE` consumers, it:
  1. Calls `tsCommitsReader.findRunsOverlapping(A, B)`.
  2. Emits one unordered-contiguous frame per surviving run.
  3. Each emitted frame's `rowLo`/`rowHi` is the run's physical
     range; the operator (filter, aggregator) sees a flat
     contiguous slice of column memory.
- The JIT/filter step inside the emitted frame must still evaluate
  the exact ts predicate per row (the CommitStat bounds are
  approximate). For tight intervals over a coarse run this is a
  small cost; for runs entirely inside the interval, the predicate
  trivially holds.

### Completion contract for Phase 2

- The priority operators perform within parity (or better) versus
  standard storage on a representative benchmark suite.
- The benchmark suite includes:
  - 1B-row table, partitioned daily, no O3 history (single run per
    partition).
  - 1B-row table with 10% O3 history (multiple runs per partition).
  - 100M-row table with 1M tiny commits (worst-case K).
- For each, run the priority operators with and without the flag,
  compare wall-clock and CPU.
- "Parity or better" is the bar for shipping Phase 2. Where parity
  is not achievable, document why and identify the follow-up.

## Phase 3: Expansion to remaining operators

Once Phase 2 ships the framework and the priority operators, the
rest follows by repeated application of the same pattern:
identify the order requirement of the operator, add the appropriate
override, write the storage-aware fast path if it helps, fall back to
Phase 1's slow path otherwise.

### Tier A: order-insensitive, mostly mechanical

| Operator             | Approach                                        |
|----------------------|-------------------------------------------------|
| TOP K BY ts          | Override to `ORDER_NONE`, use top-K heap keyed by ts. The existing `AsyncTopKRecordCursorFactory` is already on the unordered path. |
| DISTINCT (no ts)     | Override to `ORDER_NONE`, hash-set dedup.        |
| Hash join slave hash | Slave side is order-insensitive. Override on the slave factory. |
| Nested loop / cross  | Order-insensitive.                              |
| UNION ALL            | Order propagates from outer.                    |
| EXCEPT / INTERSECT   | Hash-set, order-insensitive.                    |

Estimated cost: small per operator. Tier A could ship as a single PR.

### Tier B: order-required, needs the ordered merge cursor

| Operator             | Approach                                        |
|----------------------|-------------------------------------------------|
| ORDER BY ts          | The base cursor under `ORDER_TS_ASC_REQUIRED` already gives ordered output via Phase 1's scratch frame. No further change beyond keeping the order requirement set. |
| ORDER BY ts LIMIT N  | Two options. (a) Use Phase 1's ordered frame, terminate after N. (b) Use Phase 2's unordered path with a top-K heap by ts. (b) is faster for filtered cases. Planner decision. |
| `DistinctTimeSeriesRecordCursorFactory` | Override correctly to ts-ascending; falls into Phase 1's slow path. |
| Window functions     | Required order via Phase 1 slow path. SIMD lost. |

Tier B operators land via the slow path automatically (the
inherited `ORDER_TS_ASC_REQUIRED` default already routes them there);
this tier's work is mostly testing that the slow path is correct for
each operator's specific semantics.

### Tier C: ts-order joins (the hardest)

ASOF, horizon, LT joins are the hottest workload and the most
complex to adapt. The fast path uses **per-run binary search on the
slave's ts column**: each run is internally sorted, so for a master
row at ts T the slave probe is a binary search across each run's ts
column for "largest ts <= T", combined across runs.

Implementation:
- New helper `TsCommitsReader.findMatchingSlaveRow(masterTs)` that
  performs K binary searches and returns the best (largest-ts <= T)
  physRowId across all runs.
- ASOF/horizon cursors call this helper instead of today's
  monolithic binary search.

Cost per probe: K * log(runSize). For typical post-compaction K (a
handful of runs), this is competitive with today's single binary
search. For pre-compaction K (many small runs), this is slower but
bounded.

The optimisation is meaningful enough that it justifies its own PR
once Phase 2 lands. Until then, ASOF works via Phase 1's slow path
(per-row indirection through the ordered scratch).

### Tier D: bitmap-index-driven queries with ts order

`WHERE sym = 'X' AND ts BETWEEN A AND B ORDER BY ts` is the
remaining hard case. The bitmap index returns physRowIds in append
order, not ts order. Producing ts-ordered output from those rows
requires either:

1. A reverse lookup `physRowId -> logical sort position`, used to
   re-sort the bitmap matches.
2. Adding a `physRowMin`/`physRowMax` per BlockRef in the future
   `_ts.idx` index, allowing bitmap matches to be filtered by run
   and then sorted within run.
3. A top-level sort after the bitmap-driven scan.

Option 3 is the Phase 3 default (it works today, just slower than
option 2 would be). Options 1 and 2 are follow-up perf work tied to
the `_ts.idx` PR.

### Tier E: LATEST BY

Needs reverse ts order. The existing C++ kernel takes a flat
physical column. Under TsRuns, the simplest path is:

1. Materialise the ts-reverse scratch via a reverse K-way merge.
2. Call the existing kernel against the scratch.

Slower than today's reverse-physical scan but bounded.

Optimisation later: rewrite the LATEST BY core in Java around the
merge cursor's reverse iterator.

## Query class taxonomy and required adaptations

| Class                          | Order needed?      | Implementation                       |
|--------------------------------|--------------------|--------------------------------------|
| COUNT(*) / COUNT(filter)       | No                 | CommitStat sum or JIT per run        |
| GROUP BY non-ts                | No                 | UnorderedPageFrameSequence + per-run JIT |
| SAMPLE BY                       | No (bucket by ts)  | Same as GROUP BY                     |
| WHERE ts BETWEEN ... aggregate | No                 | Run pruning + per-run JIT            |
| WHERE filter, no order out     | No                 | Per-run JIT                          |
| WHERE filter ORDER BY ts LIMIT N| Yes (output)      | Top-K heap or ordered scratch frame  |
| ORDER BY ts no limit           | Yes                | Phase 1 ordered scratch frame        |
| ORDER BY ts DESC LIMIT N        | Yes (rev)         | Reverse top-K or ordered scratch     |
| DISTINCT (hash)                | No                 | Per-run scan + hash set              |
| DistinctTimeSeries             | Yes                | Phase 1 ordered scratch              |
| Window functions               | Yes                | Phase 1 ordered scratch              |
| ASOF / horizon (driver)         | Yes (both sides) | Phase 1 + per-run binary search probe (Tier C) |
| LATEST BY                       | Reverse           | Reverse scratch + existing kernel    |
| Bitmap-index equality           | No (default)      | physRowIds direct, unchanged         |
| Bitmap-index + ORDER BY ts     | Yes                | Bitmap scan + top-level sort (Tier D) |
| Hash join slave hash           | No                 | Per-run scan                         |
| Nested loop                    | No                 | Per-run scan                         |
| UNION / EXCEPT / INTERSECT     | Propagated         | Inherited                            |
| `MAX(ts)` / `MIN(ts)`           | No                 | Read CommitStats min/max global; fast-path opportunity beyond current implementation |

## Test strategy across phases

### Phase 0
- Golden bytes tests on `_ts.commits` file format.
- `TsMockPartitionBuilder` produces files that pass `TableReader`
  open + close cycle.

### Phase 1
- All existing SQL tests run parameterised on standard and TsRuns
  partitions; results identical.
- New tests under `core/src/test/java/io/questdb/test/cairo/wal/tsruns/`:
  - Multi-run partition scan correctness.
  - Run boundary handling.
  - Dedup at run boundaries.
  - Crash recovery (trailing garbage past `_txn` slot [3] valid size).
  - Empty / single-row / huge-K partitions.

### Phase 2
- Benchmark suite added under `benchmarks/` for priority operators.
- Performance gate: priority operators within parity of standard.
- Regression tests: every priority operator's existing test class
  also runs on TsRuns.

### Phase 3
- Per-operator test classes extended to TsRuns coverage as each tier
  lands.
- ASOF specifically gets a dedicated test class because the binary-
  search-per-run path is new code with subtle invariants.

## Critical files for the read side

- `core/src/main/java/io/questdb/cairo/sql/PartitionFrame.java`
- `core/src/main/java/io/questdb/cairo/sql/PageFrame.java`
- `core/src/main/java/io/questdb/cairo/sql/PageFrameMemoryRecord.java`
- `core/src/main/java/io/questdb/cairo/sql/PageFrameMemoryPool.java`
- `core/src/main/java/io/questdb/cairo/sql/RecordCursorFactory.java`
- `core/src/main/java/io/questdb/cairo/sql/async/UnorderedPageFrameSequence.java`
- `core/src/main/java/io/questdb/cairo/FullFwdPartitionFrameCursor.java`
  (and Bwd / Interval variants)
- `core/src/main/java/io/questdb/cairo/TimestampFinder.java` /
  `NativeTimestampFinder.java`
- `core/src/main/java/io/questdb/griffin/engine/groupby/*` (SAMPLE BY,
  GROUP BY)
- `core/src/main/java/io/questdb/griffin/engine/table/*` (COUNT,
  LATEST BY, filter cursors)
- `core/src/main/java/io/questdb/griffin/engine/join/*` (ASOF,
  horizon, hash)
- `core/src/main/java/io/questdb/griffin/engine/window/*` (window
  functions)
- `core/src/main/java/io/questdb/jit/CompiledFilter.java` (JIT entry)
- `core/src/main/c/share/jit/compiler.cpp` (JIT codegen, unchanged in
  Phase 1; investigated in later perf work)

## Open questions

1. **Phase 1 scratch buffer lifetime.** Is the `physRowIds[]` scratch
   buffer per-frame, per-cursor, or per-thread? Per-frame is the
   simplest but allocates on each frame open; per-cursor reuses
   across frames; per-thread reuses across cursors. Lean toward
   per-cursor with growth as needed. Confirm with profiler.

2. **Sub-frame split granularity.** When a partition's logical row
   range is sliced into parallel sub-frames, does the slice align
   to run boundaries (clean, possibly uneven) or to fixed row
   counts (even, may straddle runs)? Run-aligned is simpler and
   matches Phase 2's per-run frame model.

3. **`getOrderRequirement` migration order.** Phase 2 wants operators
   to declare order requirements. Some operators today implicitly
   rely on ordered input but don't crash on unordered. Audit before
   flipping defaults.

4. **Bitmap-index ts-order question.** Tier D's three options should
   be evaluated against a real workload before committing. The
   reverse lookup (option 1) is the most general but the most memory.

5. **Reverse cursor performance.** Phase 1's reverse path uses a
   reverse K-way merge. For long partitions with many runs, this is
   not cheap. LATEST BY is the canonical consumer; measure once
   Phase 3 Tier E lands.

6. **JIT-on-indexed possibility.** Phase 1 disables JIT for ordered
   frames. A future PR could revisit this if the slow Java filter
   loop becomes a bottleneck. AVX-512 gather (`vpgatherqq`) is
   plausible but needs measurement.

7. **Cold + overlay (Tier 3) reads.** This plan focuses on Tier 1
   (active partition). Tier 3 reads add a parquet input to the
   merge cursor and are deferred to the Tier 3 PR.
