# Replace commits on Parquet partitions — design

Date: 2026-09-29
Branch: `feat/parquet-replace-commit` (base: OSS `origin/master` f3788a1b64; JDK 25)

## Problem

`TableWriter` rejects any replace-range commit (`WAL_DEDUP_MODE_REPLACE_RANGE`)
that reaches a Parquet partition:

```
commit replace mode is not supported for Parquet partitions [table=..., partition=...]
```

The throw fires for every Parquet partition the replace loop visits: existing
converted partitions, and brand-new partitions of a `FORMAT PARQUET` table.
Any replace-range WAL commit whose range reaches a converted partition, and
every replace-range commit against a `FORMAT PARQUET` table, therefore suspends
the table.

## Semantics to preserve

A replace commit with range `[lo, hi)` and batch `B` (every row of `B` lies in
`[lo, hi)`) makes the partition contain: existing rows with `ts < lo`, then `B`
sorted, then existing rows with `ts >= hi`. `B` may be empty (a range delete).
Replace mode and dedup mode are mutually exclusive (`dedupMode` is a single
byte), so no dedup runs against existing rows.

Per partition, `TableWriter` clips the range to
`[o3TimestampLo, o3TimestampHi]` (inclusive) and passes it to
`O3PartitionJob.processPartition`.

## What already works (no change needed)

- `o3ConsumePartitionUpdateSink`: a replace result of size 0 with
  `partitionMutates=1` removes the partition (attached list, column versions,
  `partitionRemoveCandidates` by name txn — the directory layout is shared by
  native and Parquet partitions).
- First/last partition removal or shrink re-reads min/max through
  `readPartitionMinMaxTimestamps`, which handles Parquet partitions, and
  re-syncs `partitionTimestampHi` for a Parquet last partition.
- Brand-new partitions on `FORMAT PARQUET` tables go through
  `writeFreshParquetFromO3`. An empty replace over a partition with no data is
  skipped in the writer loop before dispatch.

## Design

### 1. TableWriter

Remove the `isParquet` throw in the replace branch of the O3 partition loop.
`o3TimestampLo/Hi` are then computed as for native partitions and forwarded.

### 2. O3PartitionJob dispatch

`processPartition` forwards `o3TimestampLo/Hi` to `processParquetPartition`
(new parameters `replaceLo`, `replaceHi`; `replaceLo > replaceHi` means "not a
replace commit").

### 3. Merge strategy — `O3ParquetMergeStrategy.computeMergeActions`

New parameters `replaceLo`, `replaceHi` (inclusive). When a replace range is
given, after the existing O3-to-row-group assignment:

- A row group with no O3 rows that intersects `[replaceLo, replaceHi]`:
  - fully inside the range → new action `DROP(rg)`;
  - partially inside → `MERGE(rg)` with an empty O3 slice (`o3Lo > o3Hi`),
    i.e. a filter-only merge.
- A row group with O3 rows is a `MERGE` as today.
- Row groups not intersecting the range and gap O3 data are unchanged
  (`COPY_ROW_GROUP_SLICE`, `COPY_O3`).

Ordering stays valid: every O3 row lies inside the range, and every surviving
existing row lies outside it, so no ties between O3 and surviving rows exist.

The dedup-only boundary-tie coalescing never runs in replace mode
(`isCommitDedupMode()` is false).

### 4. Merge execution — `mergeRowGroup`

In replace mode the merge index is built from three contiguous segments:
decoded row-group rows with `ts < replaceLo`, the O3 slice, and row-group rows
with `ts > replaceHi` (bounds via binary search over the decoded timestamp
column). Either side may be empty; the O3 slice may be empty. The result row
count may be smaller than the row-group size. A merge that yields zero rows is
impossible by construction: that case is a `DROP`.

`duplicateCount`-style accounting gets a replace counterpart: rows removed per
merge are subtracted from the partition's new size.

### 5. Update vs rewrite (hybrid)

`isRewrite |= (any DROP action)`. Update mode's `replace_row_group` can shrink a
row group, but the updater has no remove primitive. Otherwise the existing
rewrite gates (schema change, single row group, unused-bytes ratio, legacy
encodings) apply unchanged.

In rewrite mode `DROP` writes nothing (the row group is not copied). Update mode
never sees a `DROP`.

### 6. Short-circuits (mirror native)

Computed from row-group bounds before opening any writer fd:

- **No-op**: no O3 rows and no row group intersects the range → report the
  partition unchanged (`partitionMutates=0`, same size, parquet file size `-1`)
  and return without writing.
- **Full removal**: every row group is fully inside the range and there are no
  O3 rows → report size 0 with `partitionMutates=1` and return without writing;
  the consumer removes the partition.

### 7. Partition-update sink

- Size: `oldSize − removedRows + o3Rows`.
- `timestampMin`: in replace mode, the minimum timestamp of the resulting data
  (first surviving row or first O3 row), because the consumer sets
  `txWriter.minTimestamp` from it when the first partition is replaced.

## Error handling

Unchanged. Update-mode failures truncate the data file to its pre-merge size and
leave the `_pm` tail dead; rewrite-mode failures remove the new txn directory.
Both bump the writer error count, which suspends the table.

## Testing

A new test class under `core/src/test/java/io/questdb/test/cairo/parquet/` uses a
small `cairo.partition.encoder.parquet.row.group.size` so partitions hold
several row groups. It covers converted partitions and `FORMAT PARQUET` tables,
asserting query results with the fluent `assertQuery(...).returns(...)`:

1. Range strictly inside one row group, with and without new rows (update mode).
2. Range spanning a row-group boundary (update mode, two filtered merges).
3. Range fully covering a middle row group, without new rows (DROP → rewrite).
4. Range fully covering a row group, with new rows (update mode).
5. Range covering the whole partition, empty batch → partition removed.
6. Replace touching the first partition → table min timestamp updates;
   replace trimming the last partition → max timestamp updates.
7. One replace spanning native and Parquet partitions.
8. Replace after `ADD COLUMN` (schema-change rewrite combined with a filter).
9. Range missing all data with an empty batch → no-op (partition name txn and
   file size unchanged).

Unit tests for `computeMergeActions` cover the DROP and filter-only MERGE
classification.

Fuzz: Parquet variants of `ReplaceInsertFuzzTest` (partition-to-Parquet
probability > 0, and `setCreateWalAsParquet(true)`).

## Out of scope

- A mat-view refresh test over Parquet view partitions: OSS has no
  `ALTER MATERIALIZED VIEW ... CONVERT PARTITION` and rejects `FORMAT PARQUET` on
  mat views, so OSS cannot put a view partition in Parquet. Mat-view and
  live-view replace suites still run as regressions.

- Native's "replace produced identical data → skip rewrite" optimisation.
- A `remove_row_group` primitive in the Rust updater / `_pm`.
