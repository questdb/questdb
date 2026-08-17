# Composite Partitioning — Read-Side Query Surface (Sub-project 9) — Design

## 1. Scope

Every remaining restriction a user meets while **reading** a composite table.

| # | Gate | Site | Tranche |
|---|---|---|---|
| 1 | multiple sub-day time intervals over a single multi-cell day | `AbstractIntervalPartitionFrameCursor:132` | 9A |
| 2 | indexed `WHERE` predicate | `SqlCodeGenerator:11596` | 9B |
| 3 | `ORDER BY` on an indexed symbol column | `SqlCodeGenerator:11964` | 9C |
| 4 | cross-cell merge supports native partitions only | `CompositeMergePartitionRecordCursor:237` | 9D |
| 5 | time-frame permutation supports native partitions only | `CompositeTimeFrameRecordCursor:565` | 9D |

The tranches have different prerequisites and are scheduled apart (§6): 9A depends on nothing, 9B/9C
depend on sub-project 2 (column DDL owns index DDL), 9D depends on sub-project 3 (parquet cells).
They share one spec because they share one mechanism (§3).

Out of scope: everything owned by another sub-project in the scope-closure index; EXPRESSION
dimensions (deferred by decision — an unbuilt feature, not a restriction on an available action).

## 2. Why these gates exist — one cause, not five

`IntervalFwdPartitionFrameCursor` and `IntervalBwdPartitionFrameCursor` walk partitions and intervals
as a **single merge of two sorted sequences** with a monotonic `partitionLo`. That is correct when one
partition is one day. A composite day is several partitions — one per cell — and the whole gate class
follows from the mismatch:

- A cell abandoned to visit a sibling for interval *i* can never be revisited for interval *i+1*,
  because `partitionLo` only moves forward. Rather than drop those rows silently, the cursor throws:
  that is gate 1.
- Any exit that retires an interval while a same-day sibling remains unvisited drops that sibling's
  rows. Three such exits existed. The fragment exit was fixed in Task 6c; the "empty frame" and
  "wholly above/below" exits were fixed on 2026-08-13 in both directions, after the backward cursor
  was found returning **no rows at all** for `ORDER BY ts DESC` with a timestamp filter.

Three defects from one design assumption, each found separately, each patched separately. A fourth
exit would be found the same way. **This sub-project removes the assumption rather than adding a
fourth patch.**

## 3. Design — per-cell cursors, merged

Give each cell of a day its own interval cursor; merge their output by timestamp.

This is not a new architecture: composite reads already merge per-cell streams through
`CompositeMergePartitionRecordCursor`. The change extends that shape down to interval scanning
instead of asking one cursor to interleave cells and intervals simultaneously.

Consequences, in order of importance:

1. **The monotonic-walk constraint disappears.** Each cell sees every interval, so gate 1 is *deleted*,
   not relaxed. The `multipleSubDayIntervalsOverMultiCellDayUnsupported` throw and its four call sites
   go with it.
2. **Plain tables keep the existing cursor untouched.** No surgery on a loop shared with plain, so
   invariant 1 (plain byte-identity) is not at risk, and plain's frame emission order is unchanged.
3. **The defect class ends.** There is no "exit that retires an interval" to get wrong, because
   intervals are no longer retired on behalf of other cells.

**Cost, stated up front:** one cursor per open cell. Bounded by the existing open-cell cap
(`cairo.wal.composite.fastappend.max.open.cells`, default 64) — the spec commits to that bound rather
than discovering it under a high-cardinality table. Memory per cursor is small (bounds and indices, no
row buffers), but the bound must be enforced and tested at the cap, not merely assumed.

### Alternatives considered and rejected

- **Reset `partitionLo` to the cell-run start when retiring an interval.** Keeps the per-(cell,
  interval) residual-limit bookkeeping that produced all three defects, and needs it to become *more*
  complex. Rejected: it preserves the cause.
- **Invert the loop to cell-major (for each cell, walk all intervals).** Natural for composite, but the
  loop is shared with plain tables and inversion changes plain's frame emission order. Rejected on
  invariant 1.

### 9B/9C — index factories

Index data is stored **per partition**, therefore already per cell. Symbol *keys* are table-wide, so
key resolution needs no cell awareness at all — worth stating plainly because it is the part that
looks hardest and is not.

The work is that the indexed factories combine per-partition results assuming partition == day. Each
cell's index row cursor becomes a stream, feeding the same merge as §3. `ORDER BY` on an indexed
symbol (9C) then falls out of 9B: ordering across cells is the merge's responsibility, not the
index's.

Families to convert, from the factory audit (`CompositeFactoryCoverageTest`):
`DeferredSingleSymbolFilterPageFrame` (already correct for dimension equality — the predicate selects
one cell per day), `FilterOnValues`, `FilterOnExcludedValues`, and the indexed `LatestBy` families.

### 9D — parquet cells

Both remaining gates say the same thing: the merge and the time-frame permutation handle native
partitions only. They are read-side, so they live here, but they cannot be lifted until sub-project 3
makes a parquet cell addressable. Listed here, scheduled after 3.

## 4. Error handling

Gates move; they do not vanish. Until a tranche lands its gate stays and stays **loud** — a refusal a
user can act on, never a silent wrong answer. Two specific rules:

1. A gate must fire at the statement that caused it (see wave 0 in the closure index). No gate in this
   sub-project may be lifted by deferring its failure to a later operation.
2. Where a shape is genuinely unsupportable, it becomes a documented permanent limitation in the
   deferred table — not a "not yet" that never resolves.

## 5. Testing

Acceptance is differential: each operation flips `GATED → SUPPORTED` in sub-project 8's classification
table and passes the twin comparison. Flipping that classification **enrols the operation in the
differential fuzz automatically**, so a lifted gate gains coverage by construction; a gate lifted
without the flip is a gate lifted without coverage, and `CompositeFuzzOpCoverageTest` fails on any
unclassified operation.

**Regression net for the 9A rewrite** (all built 2026-08-13, all verified to fail when the current
fixes are reverted): `CompositeIntervalSiblingCellTest`, `CompositeMultiIntervalTest`,
`CompositeIntervalLimitTest`, `CompositeIntervalNullCellTest`, `CompositeIntervalAggregateTest`,
`CompositeIntervalCellPruningTest`, `CompositeIntervalHiveLayoutTest`,
`CompositeIntervalMultiDimensionTest`, `CompositeIntervalReaderReloadTest`,
`CompositeIntervalConcurrentReadTest`, `CompositeIntervalCursorUnitTest`, and fuzz shapes 6b, 9, 10
and 11. A rewrite of the cursor architecture is precisely what they exist to catch.

**Planned test-contract flips** — named here so they are deliberate, not discovered when something
goes red:

| Test | Today asserts | After |
|---|---|---|
| `CompositeMultiIntervalTest` | multi-interval shapes answer correctly; guard documented | plus the previously-gated shape answers correctly |
| `CompositeFactoryCoverageTest#testUnsupportedIndexedPredicatesAreRefused` | indexed predicates refused | indexed predicates twin-equal |
| `CompositeFactoryCoverageTest#testIndexedDimensionEqualityMatchesTwin` | index scan outside the merge | index streams through the merge |

**Harness gaps to close as part of this work:** the fuzz generates no indexed tables (9B/9C need
that), and no parquet cells (9D needs that, shared with sub-project 3).

**Per-test discipline**, from what this session cost: every new test must be shown to FAIL with its fix
reverted and the result recorded in the commit message; a backward-scan test must use a single sort
key, project only `ts`, and assert the plan, because a multi-key `ORDER BY ts DESC` silently plans as a
sort over a forward scan and an outer `ORDER BY` lets the optimiser drop an inner one. Three tests
written this session passed against a defective build before this was checked.

`io.questdb.test.griffin.**` must be green before any PR — the suite that hid seven failures for weeks
because no regression filter on this branch included it.

## 6. Sequencing

```
Wave 0 → 8 → 9A → 1 → 2 → 9B/9C → 4 → 5 → 3 → 9D → 6 / 7
```

9A precedes sub-projects 1 and 2 deliberately. Composite reads are a **shipping** capability; that
cursor design has produced three defects, two returning silently wrong answers and one returning no
rows. Everything else on the roadmap adds new capability. Fixing what is already broken outranks
adding features, and 9A also removes the mechanism that would keep generating defects while 1 and 2
are built.

## 7. Out of scope

- Performance parity with plain. Measured and recorded per operation, never gating (project-wide
  decision, 2026-08-17).
- Cell-level read parallelism beyond what the existing page-frame distribution provides.
- EXPRESSION dimensions.
