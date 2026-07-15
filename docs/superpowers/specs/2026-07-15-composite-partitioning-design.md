# Composite Partitioning for QuestDB — Design Spec

**Status:** Draft for review · **Date:** 2026-07-15 · **Scope:** OSS core, WAL tables
**Grounding:** every load-bearing claim is anchored to `file:line` in `/home/nick/claude/hub/questdb`
(@ `2041d82a18`), verified by direct source reads. Prior-art from cited web sources.

> **Naming:** "sub-partition" is already taken internally (the O3 *temporal* splits of one time
> partition, `TableWriter.java:13589`). This feature is **composite partitioning**; its physical unit
> is a **cell**; its within-partition sort variant is **clustering**.

---

## 1. Motivation & goals

Partitioning today is **time-only**: `PartitionBy` is a single `int` time-unit, threaded end to end
(`cairo/PartitionBy.java:46-105`). This feature adds one or more **categorical dimensions** to a
partitioned table. Drivers (all confirmed with the requester):

1. **Query pruning** — skip whole partition directories for `symbol='X'` filters. Today a symbol
   filter prunes by **time only** (`SqlCodeGenerator.java:10344`) then bitmap-index-seeks *every*
   time-selected partition; composite partitioning replaces N seeks with directory pruning. Biggest
   single win: `LATEST ON … WHERE symbol='X'` → open one newest cell, read one row.
2. **Eliminate cross-feed O3** — when multiple feeds with independent clocks interleave into one
   table, O3 fires constantly (`TableWriter.java:2727`, `timestamp < maxTimestamp`). Routing each feed
   to its own cell makes each cell a monotonic **append** — the interleave O3 vanishes. (Sleeper win.)
3. **Parallel ingest / I/O** — spread one time window across cells (Timescale "space dimension").
4. **Storage tiering & lifecycle** — per-cell drop/retention/TTL, cold-tier some symbols (Enterprise).
5. **Multi-tenant isolation** — physically separate tenants' rows within one logical table.

## 2. Non-goals / explicitly deferred

- Non-SYMBOL partition dimensions (INT/BOOLEAN/VARCHAR) — Phase 4.
- Partition **evolution** (ALTER add/drop a dimension without rewrite) — Phase 4.
- Reference-table / cross-row / stateful partition expressions — out of scope (per requirement, the
  partition expression is a **deterministic scalar function of the current row's columns only**).
- Mat-view refresh *pruning* by symbol — Phase 3 (Phase 1 refresh stays correct but unpruned).

## 3. Scope & phasing

- **Phase 1 (this spec) — composite-partitioning core.** Grammar + composite carrier; additive `_meta`
  persistence; `(ts, cellKey)` time-major txn/reader model; WAL-apply routing via a generalized
  `processO3Block`; nested Hive/plain cell directories; read-side symbol→cell pruning + the cross-cell
  k-way merge; index-skip on partition-key dims; `SHOW CREATE TABLE`; live-cell cardinality guard.
  Transforms `identity`/`hash`/`truncate` + aliased escape-hatch expression (routes & stores; pruned
  only on filter-on-expression). Parse + persist the `ORDER BY` clustering spec and build the **run
  abstraction**, though Phase 1 only *implements* the directory-run (chunked) form.
- **Phase 2 — lifecycle, tiering & Parquet forms.** Per-cell `DROP`/`DETACH`/`ATTACH`/`CONVERT`/TTL;
  per-cell placement (tiering); Parquet conversion policy: (a) Hive-per-cell Parquet vs (b) single
  internally-`(cluster,ts)`-sorted Parquet per time partition (encoder sort + clustering-aware
  row-group boundaries + the ts-fast-path guard).
- **Phase 3 — pruning depth & mat-view refresh.** Per-cell min/max value stats for escape-hatch
  pruning from underlying-column filters; symbol-aware mat-view refresh (recompute only touched cells);
  close the known base-`DROP PARTITION` gap-skip (`WalTxnRangeLoader.java:153-157`).
- **Phase 4 (optional) — non-symbol dimensions & partition evolution.**

## 4. Syntax & grammar

Every partition element is a **transform**; the time bucket always leads; an optional `ORDER BY`
declares clustering.

```
PARTITION BY <time-dim> [ , <dim> ]*  [ ORDER BY <col> [, <col>]* ]

<time-dim> := DAY|HOUR|WEEK|MONTH|YEAR            -- sugar, byte-identical to today
            | timestamp(DAY|HOUR|WEEK|MONTH|YEAR)  -- canonical, on the designated timestamp
<dim>      := col | identity(col)                  -- raw value per cell
            | hash(col, N)                          -- N hash buckets  (bounded)
            | truncate(col, N)                      -- first N chars / numeric bin (prunes on '=' and LIKE 'p%')
            | ( <scalar expr over this row> ) AS name   -- escape hatch, must be aliased
```

**Rules.**
- Bare `DAY` desugars to `timestamp(DAY)` ⇒ **existing DDL unchanged**; the whole feature is additive.
- Time always first; `…, sym, DAY` normalizes to `timestamp(DAY), sym`. Keeps the designated timestamp
  as the primary ordering axis; there is never a purely-categorical table (so WAL/O3/mat-view
  machinery all still hold).
- Each non-time `<dim>` must resolve to a **SYMBOL / bounded value over the current row's columns**
  (no aggregates, no subqueries — enforced at compile). Escape-hatch expressions must be `AS`-aliased.
- Parser chokepoint today reads exactly one literal — `SqlParser.parseCreateTablePartition`
  (`SqlParser.java:2227-2233`); becomes a comma-list + transform parser. `ORDER BY` is a new
  CREATE TABLE clause; clustering order is `(orderCols…, designatedTs)` so each run stays ts-ordered.
- Validation extends `CreateTableOperationImpl.java:688` / `SqlParser.java:1706-1759`.
- `SHOW CREATE TABLE` reconstructs the full transform list + `ORDER BY` (`ShowCreateTableRecordCursorFactory.java:394`).

## 5. Core model

A **cell** is the physical unit `(timePartition, cellKey)`. Conceptually `cellKey` is a tuple of dense
ints, one per non-time dimension; it is **materialized as a single dense cell ordinal** — a table-root
**cell registry** assigns one ordinal per distinct dimension-tuple — so the `_txn`/`_cv` blocks stay
**fixed-stride** (one extra `int` slot, not a variable-length key). The registry maps ordinal ↔
dim-tuple; each dimension dictionary maps value ↔ dense int. Equality/`IN` pruning resolves
value→dim-int→matching ordinals; range pruning consults the dimension dictionary then maps to ordinals.

**Per-dimension dictionaries.** Each dimension owns a value dictionary (distinct transform outputs →
dense int) at table root, **reusing the existing `SymbolMapWriter`/`Reader` machinery**
(`SymbolMapWriter.java:40-70`; global symbol dict is already table-root and partition-independent —
`TableWriter.java:5542`). At write time each row evaluates each dimension's transform, interns the
result, and gets a dense int; the tuple is the cellKey.
- `identity(symbolCol)` **reuses that column's existing dictionary** ⇒ `WHERE symbolCol='X'` prune is a
  direct dict lookup → cellKey.
- `hash(col,N)` needs no dictionary — the key *is* `0..N-1`.
- `truncate(col,N)` / `(expr)` get a **dedicated** dimension dictionary.

**The run abstraction (unifies chunking & clustering).** A partition is a set of ts-ordered **runs**,
each tagged with its dim value (chunked) or a row-group min/max (clustered). Two layout-agnostic
primitives: **prune** (directory-skip *or* row-group-skip via `ParquetRowGroupFilter.canSkipRowGroup`)
and **merge** (the k-way ts heap of §9). Phase 1 implements directory-runs; Parquet row-group-runs
slot into the same interfaces in Phase 2.

## 6. Metadata & on-disk format (additive, backward-compatible)

- Keep the single `int` at `META_OFFSET_PARTITION_BY=4` holding the **time** unit, so old readers see a
  normal time-partitioned table (`TableUtils.java:128`).
- Write the **dimension list** (count; per-dim: transform kind, source column index, param N, alias,
  serialized expression for the escape hatch), the **naming mode** (Hive|plain), and the **`ORDER BY`
  clustering spec** into a **new trailing metadata block** — precedent: the covering-index block
  (`TableUtils.java:2670-2678`) — gated by a `META_FORMAT_MINOR_VERSION` bump (`TableUtils.java:117`).
  **No `ColumnType.VERSION` bump, no `mig/` migration** (this is how TTL and TABLE_FORMAT were added).
- Old binaries reject composite tables via the minor-version gate; existing tables are byte-identical.

## 7. Disk layout

Nested cell subdirectories under the (unchanged-format) time partition — Layout A; the flat
`2023-01-01.<key>/` form collides with the O3 split `.nameTxn` suffix grammar
(`TableUtils.setSinkForNativePartition:2452`) and is rejected. **Per-table naming mode** (stored in the
metadata block); internal `_txn`/`_cv` always key on the **compact dense-int cellKey** — the value
string is *only* the on-disk directory label, derived via the dimension dictionary; reverse-parse
(attach/recovery) reads the label and dict-looks-up the int:

```
Hive mode (external hive_partitioning=1 reads directly):
  trades~42/ts=2023-01-01/exchange=NYSE/symbol_trunc=BTC/{ts.d,price.d,…,venue.d,venue.k,venue.v}
Plain mode (compact, familiar time dir):
  trades~42/2023-01-01/NYSE/BTC/{…}
```

Per-transform Hive label: `identity(exchange)`→`exchange=NYSE`; `hash(symbol,32)`→`symbol_hash=7`;
`truncate(symbol,3)`→`symbol_trunc=BTC`; `(expr) AS asset_class`→`asset_class=crypto`. Values are
**percent-encoded** (Hive convention) for filesystem safety; canonical value + dense int live in the
dimension dictionary. O3 split `.nameTxn` suffixes still apply at the **leaf (per-cell)** level, so
split/squash stays intact within a cell.

## 8. Write & routing path (WAL tables only)

- **One generalized chokepoint.** Both direct commit (`commit→o3Commit→processO3Block`, `:8161`) and
  WAL apply (`commitWalInsertTransactions→processWalCommitFinishApply→processO3Block`, `:11056`) funnel
  through **`TableWriter.processO3Block`** (`:9404`). It sorts/groups the batch so each
  `(cell, timePartition)` run is contiguous and **ts-ordered within**, then appends/merges each run into
  its cell — vs. slicing by `partitionTimestamp` alone today. **WAL segments are partition-agnostic — no
  WAL format change** (`WalWriter.java:459-479`).
- **The 5 load-bearing rewrites** (each widens `long ts` → composite `(ts, cellKey)`):
  1. `TxReader.attachedPartitions` — flat 4-long/partition array sorted by a single timestamp,
     binary-searched everywhere (`TxReader.java:74-75, 858-861`) → time-major `(ts, cellKey)`.
  2. `processO3Block` slice loop (`TableWriter.java:9448-9782`) → group by `(ts, cellKey)`.
  3. `TxWriter.switchPartitions` (`:510-530`) — appends at tail assuming increasing ts → composite insert.
  4. `ColumnVersionWriter` key `(partitionTimestamp, columnIndex)` (`:283`) → `(ts, cellKey, col)`.
  5. Scalar cursors `partitionTimestampHi`/`lastOpenPartitionTs`/`maxTimestamp` → per-cell frontiers.
- **cellKey at apply:** evaluate each dim transform per row → intern → dense int tuple.
- **Anti-O3:** each feed lands in its own cell; within a cell timestamps stay monotonic → pure append;
  cross-feed O3 never fires. Same routing change delivers pruning *and* anti-O3.
- **Split / squash / dedup** keep 1-D-timestamp logic scoped within a `(ts, cell)`; dedup already
  supports multi-column keys within a timestamp group (`O3PartitionJob.java:1875-2098`).
- **Legacy non-WAL in-order append path is out of scope** (it uses a single scalar `partitionTimestampHi`
  + one open column set; making it 2-D would need N concurrent open column sets). WAL is already
  required for dedup, mat-views, and is the modern default.

## 9. Read & pruning path

- **Time pruning unchanged** — time-major sort preserves it; a `ts` interval finds a partition-index
  range via `getPartitionIndexByTimestamp` (`TableReader.java:487`) and
  `AbstractIntervalPartitionFrameCursor.cullPartitions` (`:196-207`).
- **New symbol→cell pruning** — a `WHERE` on a partition-key dim resolves to cellKey(s) via the
  dimension dictionary and restricts the `(ts, cell)` enumeration. Hook: `SqlCodeGenerator.java:10375-10631`
  (thread resolved keys into the **frame-cursor factory** `:10344/10357`, not just a row cursor);
  `WhereClauseParser.columnIsPreferred…` (`:2443`) declines to promote a partition-key dim to index scan.
- **LATEST ON** — `LATEST ON … WHERE symbol='X'` opens just the newest cell for X.
- **Merge is opt-in by query shape** (see §10). Single-cell filters, per-cell aggregation/`GROUP BY`,
  and `LATEST ON` **skip** the merge and are strictly faster.

## 10. Cross-cell k-way merge (validated covering-index reuse)

For queries needing a single globally-timestamp-ordered stream across ≥2 cells (`ORDER BY ts`, plain
time-ordered scan, ASOF driver, multi-cell `SAMPLE BY`):

- **Reuse the covering-index merge skeleton + `IntLongSortedList` heap verbatim**
  (`MultiKeyCoveringCursor.hasNext:2258`; `HeapRowCursor` + `IntLongSortedList`), with the one required
  correction: those merge on **row-id**, valid only because inputs share one partition's column files;
  **cells have separate column files**, so the comparator becomes the **designated-timestamp value**,
  at the **record-cursor** layer. Seed `(childIndex, timestampValue)`, emit smallest, advance winner,
  tie-break by cell index.
- **New work:** K live records across K readers/column-file sets (vs. the heap's single shared frame).
- **Cost:** O(N log K) per bucket over already-sorted streams. Async caveat: the parallel collect
  assumes monotonic frame order (`PageFrameSequence.java:496-571`), so Phase 1 runs the merge serially
  above the async operators; a Phase-3 **eager** path can materialize ts-ordered frames via the O3
  primitive `Vect.mergeLongIndexesAsc` (`Vect.java:228`) for vectorized downstream.

## 11. Indexes

- **Skip building an index on any partition-key dimension** — the cell already isolates that value; the
  index would hold one key with a whole-partition value chain (nothing detects this today). Write gate:
  `O3CopyJob.java:619` / `metadata.isColumnIndexed`.
- **Other indexed symbols** still get **per-cell** `.k/.v` (`BitmapIndexUtils.keyFileName`); this is
  where the multiplicative cost lives (~2 KB min per distinct value **per cell**,
  `BitmapIndexWriter.java:471-487`) — bounded by the §13 guard.

## 12. Materialized views & WAL

- A mat view **is** a normal partitioned WAL table with its own `PARTITION BY`
  (`CairoEngine.createMatView → createTableOrViewOrMatViewUnsecure`), so it can be composite-partitioned
  with no new storage machinery.
- Phase 1 refresh stays **time-interval driven** (`WalTxnRangeLoader.java:163-172`,
  `MatViewRefreshJob.java:1132`): correct over a composite base, but recomputes all cells in a touched
  time range. Symbol-aware refresh pruning = Phase 3. WAL apply rides the same generalized `processO3Block`.

## 13. Cardinality guardrail

Live cells ≈ (time buckets) × Π card(dimᵢ). `identity`/`truncate`/`(expr)` cells are discovered
dynamically; `hash(N)` is bounded by construction. Enforce a **runtime live-cell cap/warning** (new
`cairo.partition.max.cells`-style config) at apply when a new cell would be created; warn/reject past
threshold. This is the concrete guard against the universal high-cardinality failure mode (ClickHouse
`Too many parts`, Hive small-files, Influx 10k-partition ceiling — all cited in prior-art). **Clustering
(`ORDER BY`) is the recommended high-cardinality alternative** — one sorted Parquet/day with row-group
pruning instead of a directory per value.

## 14. Parquet conversion & clustering (Phase 2, feasibility grounded)

At `CONVERT … TO PARQUET`, choose the cold form per partition, independent of the hot form:
- **(a) Hive + Parquet at the leaf** — each cell dir → its own `data.parquet`. Directory pruning +
  external Hive read.
- **(b) Single internally-`(cluster,ts)`-sorted Parquet per time partition** — merge the day into one
  `data.parquet` sorted by `(ORDER BY cols, ts)`, row groups aligned to the clustering key + min/max stats.

**Already exists (reusable):** one `data.parquet` + `_pm` per partition is the norm
(`TableUtils.java:143`); full per-column, per-row-group min/max/null_count incl. SYMBOL as
truncated-UTF-8 ByteArray bounds (`parquet_write/…`; `symbol.rs:161-185`); **read-side row-group
pruning already works on symbol min/max for `=`/`IN`/range** (`parquet_read/row_groups.rs:2478, 3017`)
+ optional bloom filters — so a `(sym,ts)`-clustered file prunes with **no read-path change**; a
multi-source row-group merge primitive (`parquet_write/file.rs:301-321`).

**Missing (load-bearing, in the Rust encoder):** (1) a `(sym,ts)` row **permutation** applied across
all columns (today straight native-order copy, `file.rs:267-299`; only ts-merge-index pre-sort exists);
(2) **clustering-aware row-group boundaries** (today fixed 100k-row steps, `file.rs:268-282`);
(3) emit clustering keys as `SortingColumn` metadata (`jni.rs:542`); (4) **the correctness item** —
`(sym,ts)` clustering breaks intra-partition ts monotonicity, so the timestamp fast-path binary-search
/ stats-absent fallback (`row_groups.rs:3500-3560`, `validate_timestamp_sorting_key:3623`) must be
guarded off for clustered files. This engine-order-assumption break is precisely why clustering is
Phase-2+ and Parquet-only (native partitions stay ts-ordered).

## 15. Migration & backward compatibility

Additive metadata + minor-version bump; existing tables byte-identical and unaffected; only new
composite tables use the shape; no `mig/` migration. `PARTITION BY DAY` semantics and on-disk form are
unchanged.

## 16. Testing strategy (TDD, per QuestDB fluent `assertQuery`/`QueryAssertion` style)

1. **Unit** — composite `PartitionBy` carrier; transform evaluation (`identity`/`hash`/`truncate`/expr);
   dimension dictionary; cellKey computation; path build **and** reverse-parse for both naming modes.
2. **On-disk** — create composite table, assert the directory tree + dictionary files (both modes).
3. **Routing / anti-O3** — interleave multiple feeds; assert **zero** O3 (per-cell append) + correct
   cell placement.
4. **Pruning** — `WHERE symbol='X'` asserts the partition/cell scan count drops; `LATEST ON` opens one cell.
5. **Ordering correctness** — cross-cell `ORDER BY ts` / `SAMPLE BY` vs a single-partition oracle table
   (exercises the k-way merge); property test: composite result ≡ equivalent simple-table result.
6. **Perf** — pruning speedup + anti-O3 ingest throughput vs a single-partition baseline.
7. **Compat** — old table unaffected; `SHOW CREATE TABLE` round-trip; minor-version gate rejects on old binary.

## 17. Risks & open questions

- **Multiplicative cardinality / small files** — mitigated by §13 guard + clustering alternative.
- **Merge perf for large K** — keep K small (prune first); Phase-3 eager path for vectorized downstream.
- **Filesystem path length / encoding / case-insensitive collisions** — percent-encode; cap; dict holds canonical value.
- **Mat-view refresh unpruned in Phase 1** — correct but slower over composite base; Phase 3.
- **Escape-hatch pruning** limited to filter-on-expression in Phase 1; underlying-column pruning Phase 3.
- **Open:** default naming mode (proposed **Hive**, with `LAYOUT PLAIN` opt-out); exact `ORDER BY`
  interaction when combined with chunk dims (`PARTITION BY DAY, exchange ORDER BY symbol` clusters
  within each exchange cell).

## Appendix — load-bearing chokepoints (file:line index)

| Area | Anchor |
|---|---|
| PartitionBy int-enum (no column notion) | `cairo/PartitionBy.java:46-105` |
| Parser reads one literal | `griffin/SqlParser.java:2227-2233` |
| `_meta` partitionBy offset + reserved bytes + minor-version | `cairo/TableUtils.java:128, 117, 2670-2678` |
| Attached-partitions array (1-D ts key) | `cairo/TxReader.java:74-75, 858-861` |
| Column version key | `cairo/ColumnVersionWriter.java:283` |
| Routing chokepoint (both write paths) | `cairo/TableWriter.java:9404` (`processO3Block`) |
| Append O3 trigger | `cairo/TableWriter.java:2727` |
| WAL segments partition-agnostic | `cairo/wal/WalWriter.java:459-479` |
| Read pruning: factory chosen by time only | `griffin/SqlCodeGenerator.java:10344` |
| Pruning insertion point (keyColumn block) | `griffin/SqlCodeGenerator.java:10375-10631` |
| Interval partition culling | `cairo/AbstractIntervalPartitionFrameCursor.java:196-207` |
| Covering-index k-way merge (reuse) | `griffin/engine/table/CoveringIndexRecordCursorFactory.java:2258` |
| ts-heap for merge | `std/IntLongSortedList.java`; `HeapRowCursor.java` |
| Per-partition bitmap index cost | `cairo/idx/BitmapIndexWriter.java:471-487` |
| Parquet encoder (native-order copy) | `core/rust/qdbr/src/parquet_write/file.rs:267-299` |
| Parquet symbol row-group pruning (exists) | `core/rust/qdbr/src/parquet_read/row_groups.rs:2478, 3017` |
| Parquet ts-fast-path to guard for clustering | `core/rust/qdbr/src/parquet_read/row_groups.rs:3500-3560, 3623` |
| Mat-view refresh time-interval only | `cairo/mv/WalTxnRangeLoader.java:163-172` |
