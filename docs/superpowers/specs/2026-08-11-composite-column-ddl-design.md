# Composite Partitioning — Column DDL (Sub-project 2) — Design

**Status:** drafted 2026-08-11, awaiting review. Sub-project 2 of 8; see
`2026-08-11-composite-supported-surface-verification-design.md` §1 for the decomposition.

## 1. Scope

Seven gated operations, all column-shaped:

`DROP COLUMN` · `RENAME COLUMN` · `ALTER COLUMN TYPE` · `ADD COLUMN` of type SYMBOL ·
`ADD INDEX` · `DROP INDEX` · `REINDEX`

`ADD COLUMN` of a non-SYMBOL type already works on composite tables and stays working.

## 2. The two problems

Column DDL on a composite table has exactly two difficulties. Everything else is mechanical.

### 2.1 N directories instead of one

Every column operation touches per-partition column files. On a plain table a partition is one
directory; on a composite table a day is N cell directories, each holding a full column set. A
column operation must therefore apply to every cell of every day, **atomically** — a DROP COLUMN
that succeeds for cell BTC and fails for cell ETH leaves a table whose cells disagree about their
own schema, which no reader can reconcile.

### 2.2 The interner slot-ordering hazard

This is why `ADD COLUMN … SYMBOL` is gated, and it is a genuine format-level problem rather than
missing plumbing. From `TableWriter:921–925`:

> A new symbol writer is appended at `denseSymbolMapWriters.size()`, i.e. AFTER the composite
> interners (dedicated dicts + `_cell` registry). On next writer reopen `configureColumnMemory()`
> always rebuilds the dense order as `[realSymbols…, <new column>, dedicatedDicts…, registry]`,
> which does not match the order the `_txn` symbol-count slots were written under at ALTER time.
> That desyncs the counts silently.

Composite tables carry their dimension dictionaries and `_cell` registry as ordinary symbol map
writers with `columnIndex = -1`, living in the tail of `denseSymbolMapWriters`. `_txn` stores one
symbol count per dense slot. Adding a real SYMBOL column inserts into the middle of that ordering on
reopen but appends at ALTER time, so slot *k* means one thing when written and another when read.
The failure is silent count corruption, not an exception.

## 3. Decisions

**D1 — Interner slots are pinned to the tail by construction, not by insertion order.**
`configureColumnMemory()` and the ALTER-time append must agree on one rule: real column symbol
writers occupy dense slots `[0, realSymbolCount)`, and composite interners always occupy the slots
after them, in a stable, persisted order (dedicated dicts by dimension index, then the `_cell`
registry). Adding a real SYMBOL column shifts the interners *consistently in both paths*, so the
`_txn` slot written at ALTER time is the slot read at reopen.

The alternative — keeping `ADD COLUMN … SYMBOL` permanently gated — was rejected because
sub-project 2 exists precisely to close these gates, and because the same ordering rule is what makes
`DROP COLUMN` of a SYMBOL safe (it removes a slot from the middle of the same list).

**D2 — Column operations are all-or-nothing across cells.** One `_txn` commit covers every cell.
Column files are prepared per cell first; the commit is the single linearization point. This mirrors
the multi-cell fast-append rule already established and crash-tested.

**D3 — Dimension source columns and cluster columns remain undroppable and unrenameable.** This is
already enforced (Plan 2 DDL guards) and is not relaxed here: dropping the column a dimension is
derived from would leave the partition spec dangling, and renaming it would break the `key=value`
directory names that ATTACH relies on for cross-instance re-interning (sub-project 1 §5.4).
`ALTER COLUMN TYPE` on a dimension source column is likewise refused.

**D4 — Indexes are per cell.** `ADD INDEX`/`DROP INDEX`/`REINDEX` build or drop the index for the
column in every cell directory. An index is a per-partition artifact today; a cell is a partition.

## 4. Semantics

| Operation | Composite behaviour |
|---|---|
| `ADD COLUMN` non-SYMBOL | already works; unchanged |
| `ADD COLUMN` SYMBOL | works once D1 lands; new column's dense slot precedes all interners |
| `DROP COLUMN` | applies to every cell; refused for a dimension source or cluster column (D3) |
| `RENAME COLUMN` | applies to every cell; refused for a dimension source or cluster column (D3) |
| `ALTER COLUMN TYPE` | applies to every cell; refused for a dimension source column (D3) |
| `ADD INDEX` / `DROP INDEX` | applies to every cell |
| `REINDEX` | rebuilds per cell |

Column-version bookkeeping (`_cv`) is already cell-aware: it packs cellKey into the columnIndex high
32 bits (Plan 3), so per-cell column tops and name txns are already representable. Plan 4b already
fixed per-cell column-versions on ADD COLUMN.

## 5. Implementation surfaces

| File | Change |
|---|---|
| `cairo/TableWriter.java` `~921` `addColumn` | remove the SYMBOL gate once D1 lands |
| `cairo/TableWriter.java` `configureColumnMemory` | stable interner slot rule (D1) |
| `cairo/TableWriter.java` `removeColumn` | iterate cells; keep the D3 guard |
| `cairo/TableWriter.java` `renameColumn` | iterate cells; keep the D3 guard |
| `cairo/TableWriter.java` `changeColumnType` | iterate cells; keep the D3 guard; drop the broad `isRoutedComposite()` gate |
| `cairo/TableWriter.java` `addIndex` / `dropIndex` | iterate cells |
| `griffin/engine/ops/*` REINDEX path | iterate cells |
| `cairo/ColumnVersionWriter` | no change — already cellKey-aware |

Each `isRoutedComposite()` throw is deleted only once its operation is cell-correct and covered.

## 6. Testing

- **Differential twin** per operation via the sub-project 8 harness; each operation flips its
  classification entry from `GATED` to `SUPPORTED`, which is the acceptance criterion.
- **Interner ordering regression (D1):** add a real SYMBOL column to a composite table, close and
  reopen the writer, and assert every symbol count in `_txn` still matches its writer — the exact
  silent desync the current gate prevents. Must fail before D1 and pass after.
- **Multi-cell atomicity under crash:** fault injection mid-DDL must leave every cell either
  converted or untouched; no cell may disagree with its siblings about the column set.
- **Dimension-column guards:** DROP/RENAME/ALTER TYPE of a dimension source and of a cluster column
  each rejected loudly, on a routed composite table and on a dormant one.
- **Plain byte-identity** for every operation.
- **Cross-check with ATTACH:** rename guard proven necessary by attempting an attach after a rename
  would have broken the `key=value` name.

## 7. Out of scope

- Relaxing D3 (dimension/cluster column mutability) — that is partition-spec evolution, Phase 4 of
  the original design.
- Covering-index (`POSTING`) interactions on parquet partitions — sub-project 3.
