# Composite Partitioning — Partition Lifecycle (Sub-project 1) — Design

**Status:** approved 2026-08-11. Second of eight sub-projects closing out composite partitioning;
see `2026-08-11-composite-supported-surface-verification-design.md` (sub-project 8) for the
decomposition and build order.

## 1. Scope

Seven currently-gated operations, all refusing composite tables today:

`DROP PARTITION` · `FORCE DROP PARTITION` · `DETACH PARTITION` · `ATTACH PARTITION` ·
`SQUASH PARTITIONS` (split-fragment squash, currently a silent skip) · TTL eviction

The crux is not the individual operations — it is **how a partition is addressed** once a day
contains N cells. That decision propagates into sub-project 3 (parquet conversion), 6 (Enterprise
tiering) and 7 (materialized views), which is why this sub-project comes before them.

## 2. The unifying rule

> **A partition predicate selects cells. Dropping every cell of a day drops the day.**

There is no day-versus-cell granularity switch, and no new grammar. Existing behaviour is not
preserved by special-casing it — it *falls out of the rule*:

| Predicate | Cells selected | Effect |
|---|---|---|
| `timestamp < '2024-02-01'` | every cell of the matched days (no dimension constraint) | whole days removed — **identical to today** |
| `exchange = 'BTC'` | only BTC cells | precise; sibling cells untouched |
| `timestamp < X AND exchange = 'BTC'` | BTC cells within those days | precise |
| any predicate, plain table | the day is its own single cell | **unchanged** |

The backward-compatibility argument is exact rather than empirical: on a plain table a day *is* one
cell, so "select cells, remove what is selected" and "remove the day" are the same operation.

### 2.1 Rejected alternative

Day-granular predicates (a matching predicate removes the whole day, unmatched cells included) were
considered and rejected: `DROP PARTITION WHERE exchange = 'BTC'` would silently destroy that day's
ETH data. A destructive statement must not do visibly more than it names. It also made `LIST` and
`WHERE` disagree on granularity.

## 3. Addressing surfaces

Both existing forms are kept; neither gains new syntax.

```sql
-- WHERE: an arbitrary boolean predicate (mechanism already exists, see §5.1)
ALTER TABLE trades DROP PARTITION WHERE timestamp < '2024-02-01';
ALTER TABLE trades DROP PARTITION WHERE exchange = 'BTC';
ALTER TABLE trades DROP PARTITION WHERE timestamp < '2024-02-01' AND exchange IN ('BTC','ETH');
ALTER TABLE trades DROP PARTITION WHERE glob(name, '2024-01-*');

-- LIST: exact names, as today
ALTER TABLE trades DROP PARTITION LIST '2024-01-01';                          -- whole day, all cells
ALTER TABLE trades DROP PARTITION LIST '2024-01-01/exchange=BTC/symbol=17';   -- one subpartition
```

`glob()` is the existing SQL function backed by `GlobFilesFunctionFactory.globMatch`, which already
supports `*`, `?`, `[…]` and `\`-escaping. Reusing it means no new matcher, no new escaping rules,
and no decision about literal-versus-wildcard: `glob(name, 'exch=\*')` matches the cell whose value
is literally `*`.

`putPathSafe` is **unchanged**. Path-pattern matching is not the primary addressing mechanism —
typed dimension predicates are — so on-disk escaping does not need to carry the wildcard question.

## 4. Predicate columns

The predicate compiles against a synthetic record exposing:

| Column | Type | Notes |
|---|---|---|
| designated timestamp | table's timestamp type | exists today; the only column currently exposed |
| `name` | STRING | the rendered partition name, e.g. `2024-01-01/exchange=BTC` |
| one column per dimension | the dimension's source column type | `exchange`, `symbol`, … ; absent for plain tables |

For an `identity` dimension the value is the symbol value. For `hash(col, N)` it is the bucket
ordinal, and for `truncate(col, N)` the truncated prefix — i.e. **the value the cell is keyed by**,
not the underlying row value, because a partition is only addressable by what it is keyed on. For an
expression dimension the column is named by its alias.

The identical column set is added to `table_partitions()`, which makes the dry run textually
identical to the statement:

```sql
SELECT * FROM table_partitions('trades') WHERE exchange = 'BTC';   -- preview
ALTER TABLE trades DROP PARTITION WHERE exchange = 'BTC';          -- execute
```

**Invariant (tested):** the set of partitions the `SELECT` returns is exactly the set the `ALTER`
removes, for any predicate. This is the safety mechanism — no `DRY RUN` keyword and no
`partition_match()` function is introduced, because the existing catalog function plus the existing
`WHERE` already provide the preview with one shared evaluation path.

## 5. Per-operation semantics

### 5.1 DROP PARTITION

The mechanism already exists and is more general than it appears: `SqlCompilerImpl` (~`:1598–1650`)
parses an arbitrary expression, builds a `GenericRecordMetadata` containing **exactly one column**
(the designated timestamp), compiles it to a boolean `Function`, and `filterPartitions`
(~`:5198`) evaluates it per partition. The change is what the record exposes and at what granularity
the loop iterates — not the mechanism.

Two failure modes are already documented in the Plan 4a gate comment at `TableWriter:3814–3826` and
must be fixed, not worked around:

1. **Active-partition branch.** `dropPartitionByExactTimestamp`'s "removing active partition" path
   resolves the new tail's min/max through the cell-blind 5-arg `setPathForNativePartition`, and
   throws "file does not exist" for a routed composite tail. It must resolve the cell's own path.
2. **Infinite loop.** `TxWriter.removeAttachedPartitions(long)` defaults to `cellKey = 0`, so on a
   day with 2+ cells the `getLogicalPartitionTimestamp`-driven loop re-probes the same raw index
   forever once cell 0's entry is gone. This was empirically reproduced during the Plan 4a sweep (a
   forked test JVM spinning), and is the single most important regression test in this sub-project.

The `_txn` primitive needed already exists: `TxWriter.removeAttachedPartitions(long, int cellKey)`
(`TxWriter:429`), added by Plan 3 Task 4, resolves the exact `(ts, cellKey)` record via
`findAttachedPartitionRawIndexBy` so removing one cell cannot delete a sibling at the same
timestamp. The one-argument form is a `cellKey = 0` delegate and stays byte-identical for plain
tables.

**Row-count arithmetic is reused, not reinvented.** `rowCount = transientRowCount + fixedRowCount`,
and because the attached-partition array is sorted `(ts ASC, cellKey ASC)` at most one entry is the
transient tail. Removing cells adjusts the same two counters the multi-cell fast-append already
folds (spec 2's N-fold), so this is existing, crash-tested reasoning.

**Atomicity.** Removing N cells is **one `_txn` commit**. A partially-applied multi-cell drop must be
impossible; the commit is the single linearization point, exactly as for multi-cell fast-append.

**Housekeeping.** When the last cell of a day is removed, the day container directory is removed too.
Purge is already cell-aware (`partitionRemoveCandidates` carries cellKey, Plan 4b, 15 sites). A cell
holding a live fast-append segment is closed **non-truncatingly** before removal — the close
discipline established by fast-append T3, where a truncating close on a partially-opened cell
shrank a committed cell to zero bytes.

**min/max recomputation** uses the remaining cells, not the day floor.

### 5.2 FORCE DROP PARTITION

Identical addressing; it exists to bypass the safety checks, so it bypasses them for cells too.

### 5.3 DETACH PARTITION

Follows the same predicate rule: it detaches the selected **cells**.

Detached artifacts use a nested container mirroring the day's shape:

```
2024-01-01.detached/exchange=BTC/          <- detaching BTC
2024-01-01.detached/exchange=ETH/          <- detaching ETH later joins the same container
```

`DETACHED_DIR_MARKER` (`TableUtils:117`) is unchanged; the marker moves to the day container. A
whole-day detach produces the container holding every cell, which is the plain-table shape when a
day has one cell.

### 5.4 ATTACH PARTITION

ATTACH takes names, not predicates, consistent with today. It accepts either the whole detached day
container or one named cell within it.

**ATTACH must learn to merge into an existing day.** Today it refuses when the partition already
exists; with cell-level detach, re-attaching BTC to a day still holding ETH is legitimate. Attaching
a cell whose values resolve to a cellKey already live in that day remains an error — that is a
genuine conflict, not a merge.

**Dimension values are re-interned by value, never by ordinal.** `cellKey` is a dense ordinal local
to one table, so a cell directory produced by another instance carries a meaningless ordinal. Its
directory name carries the *value* (`exchange=BTC`), so ATTACH resolves that value through this
table's dedicated dictionaries and `_cell` registry, minting a cellKey locally. This is what makes
cross-instance attach possible at all, and it is the reason the `key=value` naming earns its verbosity.

Metadata validation is per attached cell, as it is per partition today.

### 5.5 SQUASH PARTITIONS

Split fragments belong to a cell, so squash merges fragments **within** a cell. The current silent
skip ("composite table, skipping split-fragment squash") becomes real behaviour. Both
`squashSplitPartitions` (`TableWriter:17967`) and `squashPartitionForce` (`:17895`) become
cell-aware.

### 5.6 TTL eviction

Whole days only. When a day ages out, every cell in it is evicted; `enforceTtl`
(`TableWriter:9041`) becomes cell-aware in *how* it removes, not in *what* it selects. One retention
policy per table, no new DDL.

Per-dimension retention (keep BTC 90 days, others 7) is a genuinely useful feature and is explicitly
**out of scope** — it needs new DDL and per-cell retention metadata, and can be added later without
changing what TTL means today.

## 6. Implementation surfaces

| File | Change |
|---|---|
| `griffin/SqlCompilerImpl.java` ~`1598–1650` | synthetic metadata grows to timestamp + `name` + per-dimension columns |
| `griffin/SqlCompilerImpl.java` ~`5198` | `filterPartitions`/`filterApply` iterate `(ts, cellKey)` records and bind dimension values |
| `griffin/engine/ops/AlterOperationBuilder.java` | partition list carries `(timestamp, cellKey)` pairs |
| `griffin/engine/table/ShowPartitionsRecordCursorFactory.java` ~`116–132` | same dimension columns added to `table_partitions()` |
| `cairo/TableWriter.java` `3809` `removePartition` | cell-aware; remove the two documented failure modes |
| `cairo/TableWriter.java` `2663` `forceRemovePartitions` | cell-aware |
| `cairo/TableWriter.java` `2271` `detachPartition` | per-cell, nested `.detached` container |
| `cairo/TableWriter.java` `1182` `attachPartition` | accept cell or whole container; merge into existing day; re-intern by value |
| `cairo/TableWriter.java` `17895`/`17967` squash | per-cell fragment squash |
| `cairo/TableWriter.java` `9041` `enforceTtl` | cell-aware removal, day-granular selection |
| `cairo/TxWriter.java` `429` | **already exists** — `removeAttachedPartitions(long, int)` |

Each gate removed must have its `isRoutedComposite()` throw deleted only once the corresponding
operation is cell-correct and covered.

## 7. Testing

- **Differential twin** for every operation: composite versus plain twin, asserting equal results
  after the same logical operation. This is the sub-project 8 harness; each operation implemented
  here flips its entry in that harness's classification table from `GATED` to `SUPPORTED`, which is
  the acceptance criterion.
- **Preview equals removal**: for a generated predicate, the rows returned by
  `SELECT … FROM table_partitions(…) WHERE p` are exactly the partitions removed by
  `DROP PARTITION WHERE p`. Property-tested across random predicates.
- **The infinite-loop regression**: a day with 2+ cells, dropping cell 0, must terminate. Bounded by
  a test timeout so a regression fails rather than hangs CI.
- **Active-cell drop**: dropping the transient tail cell, and dropping a non-tail cell of the last
  day, each asserting `rowCount == transient + fixed` afterwards.
- **Atomicity under crash**: fault injection during a multi-cell drop must leave either all N cells
  removed or none.
- **Detach/attach round-trip**, including cross-instance: detach a cell, wipe the source table's
  symbol dictionary ordering, attach into a table whose ordinals differ — proving re-interning by
  value rather than ordinal.
- **Plain byte-identity**: every operation on a plain table produces a byte-identical `_txn` to
  before this sub-project.

## 8. Out of scope

- `CONVERT PARTITION` per-cell versus per-day — belongs to sub-project 3, since it depends on the
  Hive-parquet-per-cell versus one-sorted-parquet-per-day policy.
- Per-dimension TTL (§5.6).
- **Windows portability.** `putPathSafe` escapes `/ \ . %` and control characters, so `* ? : | " < >`
  reach directory names. All are illegal in Windows filenames, so a composite table with such a
  dimension value cannot create its directories there. This is a pre-existing limitation, correct on
  Linux, documented here rather than discovered later. Fixing it is a one-line escape-set change that
  is free while composite is unreleased and an on-disk format break afterwards — worth deciding
  before release, but not part of this sub-project.
- Column DDL, row-level ops, Enterprise tiering, materialized views — sub-projects 2, 4, 6, 7.
