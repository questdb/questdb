# Composite Partitioning — Row-Level Operations (Sub-project 4) — Design

**Status:** drafted 2026-08-11, awaiting review. Sub-project 4 of 8.

## 1. Scope

`UPDATE` · `DEDUP` (`DEDUP UPSERT KEYS`, at `CREATE TABLE` and via `ALTER TABLE … DEDUP ENABLE`) ·
and the REPLACE-range commit mode, which shares dedup's machinery.

Both are gated today. Unlike the other sub-projects, neither gate is about missing plumbing — each
hides a genuine correctness trap that composite routing creates.

## 2. DEDUP — the cross-cell trap

Dedup is enforced during the per-partition merge: rows with equal dedup keys within the partition
being written collapse. Composite routing breaks the premise that "same key ⇒ same partition".

If the dedup key does **not** include every dimension source column, two rows with identical dedup
keys can carry different dimension values, route to **different cells**, and therefore never be
compared. The duplicate survives, silently, with no error at any layer.

```sql
-- dedup key is (ts, sym); the table is partitioned by day, exchange
INSERT INTO t VALUES ('2024-01-01T00:00:00Z', 'AAPL', 'NASDAQ', 1.0);
INSERT INTO t VALUES ('2024-01-01T00:00:00Z', 'AAPL', 'NYSE',   2.0);
-- same dedup key, different cells → both rows persist
```

**Decision (D1): DEDUP is permitted only when every dimension source column is part of the dedup
key.** Then same-key rows always route to the same cell, and per-cell dedup is correct by
construction — no cross-cell comparison, nothing added to the ingest hot path.

The rule is enforced at DDL time, in both `CREATE TABLE … DEDUP UPSERT KEYS(…)` and
`ALTER TABLE … DEDUP ENABLE UPSERT KEYS(…)`, with an error naming the missing columns:

```
dedup keys must include every partition dimension column for a composite table
  [table=trades, missing=exchange]
```

Rejected alternatives: dedup across all cells of a day (requires a cross-cell merge on every commit
— precisely the machinery composite exists to avoid, and a permanent ingest cost); leaving DEDUP
gated (makes two major features mutually exclusive for no correctness reason once D1 holds).

The same rule governs REPLACE-range commits, which resolve ranges through the dedup key.

## 3. UPDATE — the row-migration trap

`UPDATE` rewrites column files in place, per partition. It has never moved a row between partitions.

On a composite table, updating a column that a dimension is derived from changes the row's cell:
its correct location becomes a different directory. Supporting that would make UPDATE a
cross-partition data-movement operation, with its own crash-safety story, its own interaction with
the fast-append open-cell cache, and its own atomicity requirements.

**Decision (D2): UPDATE is fully supported on a composite table for every column except a dimension
source column; updating a dimension source column is refused loudly.**

```
cannot UPDATE a partition dimension column on a composite-partitioned table
  [table=trades, column=exchange]
```

This mirrors D3 in sub-project 2, where dimension source columns are already undroppable and
unrenameable — the same principle: a table's routing keys are immutable while the table exists.

Everything else about UPDATE becomes cell-aware mechanics: the update runs per cell, over the cells
the row set actually touches, and the existing per-partition guards (no update of a parquet
partition, no update of a read-only partition) apply per cell.

Rejected alternative: implementing migration (delete from source cell, append to target). It is the
complete semantics, and a legitimate future feature, but it is a substantially larger piece of work
than the rest of this sub-project combined and is not needed to close the gate.

## 4. Semantics summary

| Operation | Composite behaviour |
|---|---|
| `UPDATE … SET <non-dim col> = …` | supported; applies per cell |
| `UPDATE … SET <dim source col> = …` | refused loudly (D2) |
| `UPDATE` touching a parquet or read-only cell | refused, as per partition today |
| `CREATE TABLE … DEDUP UPSERT KEYS(k…)` | permitted iff `k` ⊇ all dimension source columns (D1) |
| `ALTER TABLE … DEDUP ENABLE UPSERT KEYS(k…)` | same rule, checked against the live spec |
| REPLACE-range commit | permitted under the same D1 rule |

## 5. Implementation surfaces

| File | Change |
|---|---|
| `griffin/UpdateOperatorImpl.java` | per-cell update; drop the composite gate |
| `cairo/TableWriter.java` update path | cell-aware column-file rewrite |
| `griffin/SqlCompilerImpl.java` update compile | reject dimension-source-column assignment (D2) |
| `griffin/engine/ops/*` dedup DDL | enforce D1 at CREATE and ALTER |
| `cairo/TableWriter.java` dedup commit path | drop the composite gate once D1 is enforced |
| `cairo/TableWriter.java` REPLACE path | drop the gate under the same rule |

## 6. Testing

- **The dedup trap, as a red test first:** two rows with identical dedup keys and different
  dimension values. Before D1 they both persist (proving the trap is real); after D1 the DDL is
  refused, and with a compliant key exactly one row survives — equal to the plain twin.
- **Differential twin** for UPDATE across cells via the sub-project 8 harness; `FuzzInsertOperation`
  with dedup enabled flips from `GATED` to `SUPPORTED` under a compliant key.
- **D2 rejection** for every dimension kind — identity, hash, truncate, and an expression dimension
  whose source column is updated.
- **UPDATE spanning multiple cells** in one statement, asserting per-cell atomicity: one `_txn`
  commit, all cells or none.
- **UPDATE touching a parquet cell** refused per cell, with sibling native cells unaffected.
- **Plain byte-identity** for both features.

## 7. Out of scope

- Row migration between cells on dimension-column UPDATE (§3) — a future feature, not a gap.
- Cross-cell dedup for keys that omit dimension columns (§2) — deliberately excluded by D1.
- `DELETE` — not part of the gated set; QuestDB's DELETE work is tracked separately.
