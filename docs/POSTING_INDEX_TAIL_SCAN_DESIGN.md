# Posting Index Tail-Scan Fallback — Design Sketch

Working notes on `bench/posting-wal-fastlag`. Started as a benchmark for WAL
fast-lag posting-index commit cost (commit `7c5236be80`); investigation
revealed that fast-lag commits trigger a full posting-index seal on the
active partition that dominates per-commit cost. This document tracks a
candidate redesign that replaces seal-on-fast-lag with a reader-side
tail-scan, plus what's done, what's measured, and what's blocking
implementation.

## Problem

Every WAL fast-lag commit (the path `applyLagToLastPartition` →
`sealPostingIndexesForLastPartitionFastLag`,
`core/src/main/java/io/questdb/cairo/TableWriter.java:11223`) does:

- For each posting-indexed column (lines 11371-11373): `setNextTxnAtSeal` +
  `indexer.seal()` + `publishPendingPurges`.
- For each *covering* posting-indexed column (lines 11248-11369): same seal
  call (line 11354), wrapped in extra work — opens/maps each covering
  column file (`mapRO`), calls `configureCovering`, then `seal()` writes
  both the new `.pv.<sealTxn>` AND the `.pc<N>` sidecar files, then munmaps.

The earlier reading that said "covering skips seal on fast-lag" was wrong;
the `continue` at line 11369 is the loop fallthrough *after* the covering
seal block, not a skip.

`indexer.seal()` (`PostingIndexWriter.java:1081-1217`) re-encodes all
in-memory generations from sparse-gen format into the stride-indexed dense
format, writes a new `.pv.<sealTxn>` file, and publishes a chain entry in
`.pk`. Cost is super-linear in cumulative key/value count.

### Bench evidence

`PostingIndexBenchmarkSuite.walFastLagInsert` measures one fast-lag commit
on a WAL-enabled table preloaded with 100k rows. Selected results (ms/op,
2 warmup x 1s + 3 measure x 1s, GraalVM CE 25.0.2 + JVMCI):

| batchRows | keyCount | no_index | bitmap | posting | posting_covering |
|----------:|---------:|---------:|-------:|--------:|-----------------:|
| 100       | 50       | 0.39     | 0.52   | 5.84    | 5.44             |
| 1000      | 1000     | 2.25     | 5.65   | 21.91   | 20.54            |
| 1000      | 100k     | 7.15     | 10.37  | 26.70   | 24.49            |
| 10000     | 10k      | 15.54    | 55.73  | 69.83   | 74.65            |

The posting-vs-no-index gap is the seal cost the candidate would skip.
`posting` and `posting_covering` are in the same band — both seal; the
covering branch adds map-RO + sidecar-write + munmap on top of seal,
which is small relative to the seal's re-encode work.

Variance is high (some +/- errors > means at the noisy combos); the
order-of-magnitude conclusion holds, but specific numbers shouldn't be
quoted without a longer re-run (`-i 10 -r 5s`).

`tailScanEquality` (synthetic native int-array linear scan, equality, emit
matching rowIds): ~0.63 ns/row, independent of `keyCount`. Linear in tail
size: 1k rows = 0.6us, 1M rows = 0.6ms. Effective throughput ~1.6 GB/s
(L1-resident).

### Decision

For any realistic queries-per-commit ratio, skip-seal + tail-scan wins by
1-2 orders of magnitude.

Break-even examples:
- `batchRows=1000, keyCount=1000`: seal saves ~20ms; 1M-row tail-scan =
  0.6ms -> break-even ~32 queries/commit. At 100k-row tail (0.063ms) ->
  ~317 queries/commit.
- `batchRows=10000, keyCount=10000`: seal saves ~54ms; 1M-row tail-scan =
  0.63ms -> break-even ~86 queries/commit.

Production query-per-commit ratios are typically single digits.

## Candidate design: tail-scan fallback

### Idea

Don't seal on fast-lag. Leave the writer's `.pv` accumulating sparse-gen
data on disk, but don't publish a new chain entry. Readers pin at the new
committed `_txn = N`, walk the `.pk` chain, find the latest entry with
`txnAtSeal = M` whose `entry.maxValue` records the row-id high-water of
that entry's `.pv`. If `entry.maxValue < pinnedRowMax`, the reader scans
`(entry.maxValue, pinnedRowMax]` directly on the indexed SYMBOL column,
with an equality predicate against the looked-up key, emitting matching
rowIds. Results are merged with what the index produced.

### What's already in the code

The chain entry already has a `MAX_VALUE` field
(`V2_ENTRY_OFFSET_MAX_VALUE` at byte 32, documented in
`PostingIndexUtils.java:244`). The writer already has machinery to update
it on the head entry in-place under seqlock without resealing —
`PostingIndexWriter.setMaxValue()` (line 1237) calls
`chain.updateHeadMaxValue(keyMem, maxValue)`.

Five+ callers in production today:
- `TableWriter.java:4751, 6594, 10229`
- `O3PartitionJob.java:3381`
- `O3CopyJob.java:864`
- `TableSnapshotRestore.java:1274`
- `ContiguousFileIndexedFrameColumn.java:64, 79`

Currently these callers pass `partitionRowCount - 1` — meaning "writer's
known partition row max." The candidate redefines this to "highest rowId
whose values are present in the entry's `.pv`," advanced only on actual
seals. **This semantic shift is the central audit task before
implementation.**

### Reader-side changes

- `PostingIndexChainPicker.pick()`
  (`PostingIndexChainPicker.java:88-148`): no logic change. Reader still
  picks the latest entry with `txnAtSeal <= pinnedTableTxn`.
- Reader integration: lookups currently produce row IDs from the picked
  entry's `.pv`. New contract: reader returns
  `(indexedRowIds[], tailRowMin = entry.maxValue + 1)`. Caller scans
  `[tailRowMin, pinnedRowMax]` with equality, merges results.
- Closest existing pattern:
  `DeferredSingleSymbolFilterPageFrameRecordCursorFactory`
  (`griffin/engine/table/`, lines 46-150). Extend it to consult
  `entry.maxValue` and emit a tail-scan cursor chained after the
  index-driven cursor. No existing factory has a tail-scan fallback shape;
  this is a new query-execution shape.

### Tail-scan primitive

No existing row-range column scan API. `PageFrame` only exposes
partition-level lo/hi (`cairo/sql/PageFrame.java:123-133`),
`TableReader.getColumn()` hands back the whole mmap (line 342), and
`RowCursor` (`cairo/sql/RowCursor.java`) is index-driven. Need a new
abstraction — call it `ColumnRangeRowCursor` — that takes
`(partitionIndex, columnIndex, rowLo, rowHi, symbolKey)` and walks the
mmap with int-equality. SIMD-friendly (fixed-width int keys). Bench shows
0.63 ns/row; real implementation may add page-fault cost on cold ranges,
but at 4KB / 1024 ints per page even cold-HDD costs are dominated by
tail size.

### Visibility prerequisites — verified

`TableReader.reload()` -> `reconcileOpenPartitions` ->
`reloadColumnFiles` -> `growColumn` -> `MemoryCMR.tryChangeSize`
(`TableReader.java:582-587, 704-727, 773-806`) atomically remaps the
active partition's column files to the writer-appended size before the
cursor iterates. A reader pinned at the post-fast-lag `_txn` will see
the tail rows mmapped. Writer/reader share `MemoryCMR`, no exclusivity
issue.

### Covering tail wrinkle

Covering posting indexes also won't write `.pc<N>` sidecars on fast-lag
if seal is skipped. So covering queries against tail rows can't use
sidecars — they have to:

1. Scan SYMBOL column for matching rowIds (the tail-scan above), then
2. Materialize covering column values via regular column reads at each
   matched rowId.

This is the same machinery a non-covering posting query uses today
after the lookup. The covering vs non-covering distinction collapses
in the tail window — no new primitives needed beyond the SYMBOL
tail-scan.

## Design alternatives considered and rejected

1. **Delta entries (append-only chain)** — publish a tiny
   `.pv.<sealTxn>` per fast-lag commit containing only new rows; reader
   merges N entries. Cleanly avoids re-encode cost but creates read
   amplification, more files, and a background-compactor requirement.
   Worth revisiting if tail-scan turns out too slow on cold pages.

2. **Async seal off the commit path** — hand seal to a background worker.
   Doesn't reduce total CPU; just shifts when it happens. The gap
   between fast-lag and seal-completion still needs a fallback for
   readers — same tail-scan as the candidate. Worth combining with the
   candidate as a future refinement.

3. **Two-horizon visibility (`dataTxn` vs `indexTxn`)** — pin indexed
   queries at the older `indexTxn`. Hides freshly-committed rows from
   indexed queries; silently produces different result sets based on
   planner choice. Footgun; rejected.

## What's done on this branch

- Bench: `benchmarks/src/main/java/org/questdb/PostingIndexBenchmarkSuite.java`
  extended with parameterized `walFastLagInsert` (added `batchRows`,
  `keyCount` params + 100k-row preload), new `walFastLagQuery`,
  `walFastLagInsertAndQuery`, and synthetic `tailScanEquality`. Added
  `sampleKeys` helper. Plus per-method `@BenchmarkMode` /
  `@OutputTimeUnit` annotations on the four new benches (the State-level
  ones are pre-existing no-ops for outer-class methods).
- JSON results in `jmh-walfastlag.json`.
- PR `#7070` ("fix(core): fix JVM crash when reading covering posting
  indexes") merged into this branch — was unblocking a SIGSEGV in
  `MemoryCR.getLong` during tight read-after-write loops in
  `walFastLagInsertAndQuery`.

## Open items before implementation

In priority order. Items 1-3 are pure reading work; (4) is grep + trace;
(5) needs stable bench numbers.

1. **`setMaxValue` callsite audit (~half day).** Five+ callers in
   production. Each needs reclassification: real seal event (keep call)
   vs fast-lag advance (drop call or rename to a different setter, e.g.
   `setPartitionRowMax`). Without this, semantic drift is guaranteed.
   Earlier grep confirmed no reader currently uses `entry.maxValue` for
   visibility, so the field is currently writer-only metadata; the
   semantic change is reader-introducing rather than reader-breaking,
   which lowers the audit risk.

2. **Concrete column-read prototype (~1 day).** Synthetic
   `tailScanEquality` measured a native int array, not a real
   `MemoryCR` mmap with grow-on-extend semantics. Build a minimal
   standalone reader that opens a SYMBOL column on an active partition,
   accepts `(rowLo, rowHi)`, and emits matches into a long buffer.
   Validates the abstraction composes with what exists.

3. **Concurrency safety story (~half day).** Reader pins `rowMax = N`
   at lookup start, scans `(entry.maxValue, N]`, and writer is
   concurrently extending the column file via fast-lag. Reader must not
   read past `N`. Trace existing `pinnedRowMax` propagation, confirm
   it's a strict upper bound at scan time and not subject to drift
   mid-cursor.

4. **Worst-case lag bound + forced-seal fallback.** At ~0.63 ns/row, a
   1B-row tail = 600ms (catastrophic for a query). Need a configurable
   max-tail-rows threshold above which the writer forces a seal anyway.
   Default value to be picked from stable bench numbers.

5. **Re-run noisy bench combos** with `-i 10 -r 5s` (or longer) to get
   publishable numbers. Order-of-magnitude conclusion is robust;
   specific table values aren't. Quote stable numbers in the
   implementation PR.

## Nice-to-haves (not blocking)

- Extend the fuzz harness with a "query during fast-lag" scenario before
  the candidate ships. Recent commits (`8735521078`, `20a30a021e`)
  show fuzz catches subtle bugs in this exact area.
- Decide rollout: feature flag (so production A/B is possible) vs.
  straight replacement. Probably flag-gated on first ship given the
  blast radius.

## Key file paths (cross-reference)

- `core/src/main/java/io/questdb/cairo/TableWriter.java`
  - `applyLagToLastPartition`: 3803-3851
  - `sealPostingIndexesForLastPartitionFastLag`: 11223-11380 (the seal
    call site for both covering and non-covering posting)

- `core/src/main/java/io/questdb/cairo/idx/PostingIndexWriter.java`
  - `seal()`: 1081-1217 (re-encode + write `.pv` + publish chain entry)
  - `setMaxValue()`: 1237 (mutable head-entry MAX_VALUE primitive,
    already exists)
  - `commit()` / `flushAllPending()`: 404-410

- `core/src/main/java/io/questdb/cairo/idx/PostingIndexChainPicker.java`
  - `pick()`: 88-148 (entry selection — no logic change in candidate)

- `core/src/main/java/io/questdb/cairo/idx/PostingIndexUtils.java`
  - V2 chain entry layout doc: 234-275

- `core/src/main/java/io/questdb/cairo/TableReader.java`
  - reload + remap path: 582-587, 704-727, 773-806

- `core/src/main/java/io/questdb/griffin/engine/table/DeferredSingleSymbolFilterPageFrameRecordCursorFactory.java`
  - 46-150 (closest existing index-driven factory shape)

## Bench reproduction

```
mvn -pl core install -DskipTests -P local-client -q
mvn -pl benchmarks package -DskipTests -q

java -Xmx4g -Dquestdb.log.level=E \
  -jar benchmarks/target/benchmarks.jar \
  'PostingIndexBenchmarkSuite\.(walFastLag|tailScan)' \
  -wi 2 -w 1s -i 3 -r 1s -f 0 \
  -rf json -rff jmh-walfastlag.json
```

For publishable numbers, increase `-i 10 -r 5s` and consider
`-XX:-UseJVMCICompiler` if stability matters more than peak.
