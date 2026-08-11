# Composite Partitioning — Parquet and Format Conversion (Sub-project 3) — Design

**Status:** drafted 2026-08-11, awaiting review. Sub-project 3 of 8.

## 1. Scope

Six gated operations, all about a partition's storage form:

`CONVERT PARTITION TO PARQUET` · `CONVERT PARTITION TO NATIVE` ·
`switchNativePartitionWithParquet` · `commitPendingParquetToNativeConversions` ·
POSTING index seal on a partition · covering POSTING index reseal on a PARQUET partition

## 2. The policy decision

The original design (`2026-07-15-composite-partitioning-design.md` §14) left two options open:
Hive-style parquet **per cell**, or a single internally `(cluster, ts)`-sorted parquet **per day**.

**Decision: one parquet file per cell now; the single sorted file per day is a later, per-table
storage option.**

Rationale — the per-cell form is the only one consistent with the addressing model sub-project 1
establishes:

- A cell is a partition record in `_txn`. Per-cell parquet preserves that 1:1, so `DROP`, `DETACH`
  and `ATTACH` of a single cell keep working after conversion, using the same `(ts, cellKey)`
  addressing.
- Cell pruning stays **directory** pruning. Under the sorted-per-day form it would have to become
  row-group pruning, a different mechanism that the read path does not have.
- `table_partitions()` keeps one row per cell whatever the storage form.

The cost is real and should be stated: at high cell cardinality this produces many small parquet
files, which is exactly the pathology the sorted-per-day form was invented to avoid. That form
remains the right answer for wide, high-cardinality dimensions and is deferred rather than rejected
(§7).

## 3. Semantics

| Operation | Composite behaviour |
|---|---|
| `CONVERT PARTITION TO PARQUET WHERE …` | predicate selects cells (sub-project 1 rule); each selected cell converts to its own parquet file |
| `CONVERT PARTITION TO NATIVE WHERE …` | inverse, per cell |
| Mixed forms within a day | **permitted** — cells of one day may independently be native or parquet |
| `table_partitions()` | `isParquet` is per cell, as it is per partition today |
| POSTING seal / covering reseal | per cell |

Mixed forms within a day follow directly from "a cell is a partition record": each record already
carries its own format bit and its own slot-3 word (parquet file size for parquet, last-modifying
seqTxn for native — the multiplexed `PARTITION_VERSION_OFFSET` master introduced). Nothing in the
`_txn` layout needs to change to represent a half-converted day.

The read path already tolerates this: `CompositePageFrameRecordCursorFactory` refuses non-native
partitions loudly today (`composite cross-cell merge supports native partitions only`), so the work
is to make the cross-cell merge and the time-frame permutation read parquet cells, not to invent a
representation.

## 4. Interaction with the covering POSTING index

The gated reseal (`resealParquetCoveringForPartition`) exists because a covering POSTING index over
a parquet partition keeps parquet-backed rowids in sync with the committed partition size. Its gate
comment is explicit that skipping it is *not* provably safe — it risks a stale index and therefore
wrong answers, which is why it throws rather than skips.

Per-cell parquet keeps this tractable: the reseal is per cell, over that cell's own parquet file and
its own sidecars, exactly as it is per partition today. The day is never the unit.

## 5. Implementation surfaces

| File | Change |
|---|---|
| `cairo/TableWriter.java` `convertPartitionNativeToParquet` | per-cell conversion; drop the gate |
| `cairo/TableWriter.java` `convertPartitionParquetToNative` | per-cell; drop the gate |
| `cairo/TableWriter.java` `switchNativePartitionWithParquet` | cell-aware paths; drop the gate added by the 2026-08-10 merge audit |
| `cairo/TableWriter.java` `commitPendingParquetToNativeConversions` | cell-aware; drop the audit gate; the pending queue carries `(ts, cellKey)` |
| `cairo/TableWriter.java` `resealParquetCoveringForPartition` | per cell; drop the gate |
| `cairo/TableWriter.java` `sealPostingIndexForPartition` | per cell; drop the gate |
| `griffin/engine/table/CompositePageFrameRecordCursorFactory` | read parquet cells in the cross-cell merge |
| `griffin/engine/table/CompositeTimeFrameRecordCursor` | same for the time-frame permutation |
| `cairo/TableUtils.java` `produceParquetFromParquetWithConversions` | reachable only from tests today; make cell-aware if a production caller appears |

## 6. Testing

- **Differential twin** for every conversion path (sub-project 8 harness), including a day left in a
  **mixed** state: some cells parquet, some native, read back equal to the plain twin.
- **Round-trip:** native → parquet → native per cell, asserting row-level equality and that
  `_txn` slot 3 returns to the cleared sentinel for the native form.
- **Cell-addressed conversion:** `CONVERT … WHERE exchange = 'BTC'` converts only BTC cells; siblings
  stay native and stay readable.
- **Covering POSTING over parquet cells:** a covered read after reseal returns the same values as the
  plain twin — the stale-index risk the current gate names.
- **DROP/DETACH of a parquet cell** (sub-project 1 addressing over a converted cell).
- **Crash mid-conversion:** the cell is either fully converted or fully native; never both, never
  neither.
- **Plain byte-identity** across all conversion paths.

## 7. Out of scope, and the deferred alternative

**Single `(cluster, ts)`-sorted parquet per day.** Deferred, not rejected. It is the right storage
form for high-cardinality dimensions where per-cell files become too small and too numerous. Adding
it later is additive — a per-table storage option — but it requires: clustering-aware row-group
boundaries, row-group-level cell pruning to replace directory pruning, and a decision about what
per-cell `DROP`/`DETACH` mean when cells share one file. None of that is needed to close these six
gates, and doing it now would fork the addressing model sub-project 1 just fixed.

Enterprise tiering of parquet cells is sub-project 6.
