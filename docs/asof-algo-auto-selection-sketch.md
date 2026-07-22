# ASOF JOIN algorithm auto-selection — design sketch

Status: sketch / RFC. Backed by JMH `AsOfJoinAlgorithmBenchmark` (commits `b69210dd6`…`660f2d33e`)
and `/data/asofbench/*_summary.txt`.

## Problem

Keyed single-symbol ASOF has several cursor implementations with wildly different cost profiles,
but the optimiser only ever auto-picks **Dense** (`AsOfJoinDenseSingleSymbolRecordCursorFactory`).
The faster paths (`asof_index`, `asof_memoized`) are reachable **only via an explicit hint**
(`SqlCodeGenerator.generateJoinAsof`, ~L5133–5200). Measured wins left on the floor:

| situation (single-symbol, selective master) | best algo | vs Dense default |
|---|---|---|
| slave symbol **indexed**, master ≲ 2% of slave | `asof_index` | **12–50×** |
| **not indexed**, sparse timestamps, master selective | `asof_memoized` | **2–25×** |
| dense timestamps, or master ≳ 5% of slave, or multi-key | Dense | — (safe floor) |
| `asof_fast` on illiquid symbol | — | **8.5× *slower* (trap)** |

Crossover data (fixed 2M slave, `asof_index` vs Dense, ms/op):

| master/slave | 0.05% | 0.5% | 5% | 25% | 100% |
|---|---|---|---|---|---|
| Dense  | 12.3 | 12.4 | 14.6 | 22.6 | 58 |
| index  | 0.51 | 5.47 | 20.0 | 48.9 | 162 |
| winner | index 24× | index 2.3× | Dense | Dense | Dense |

⇒ the deciding lever is **master/slave row ratio** (index = O(master lookups),
Dense = O(slave scan)). Symbol liquidity is NOT the lever — index wins 12–50× across the
whole cardinality range for a small master.

## The hard constraint

At the selection site (`generateJoinAsof`) the planner has only:
`masterMetadata` / `slaveMetadata` (types, timestamp index, **`isColumnIndexed`**), the query
models, and `slave.supportsTimeFrameCursor()`. There is **no row-count estimate and no
timestamp-density statistic** — QuestDB is rule-based here. That is exactly why these algos are
hint-gated today. Any auto-selection must either (a) work from signals that are free at plan time,
or (b) have cheap new signals plumbed in.

**Guiding principle: first, do no harm.** When a signal is missing, fall back to today's Dense
default. Auto-selection only ever *adds* a win when it is confident; it never risks a regression on
an unknown.

## Signals, in order of plumbing cost

1. **`isColumnIndexed(slaveSymbolColumnIndex)`** — free today. Strong intent signal: a user who
   indexed the symbol built it for point access.
2. **Master LIMIT / intrinsic time-bound** — *partially* free (verified against the code):
   - The master model **is reachable**: `model.getJoinModels().getQuick(0)` for the common
     `table ASOF table` case (`generateJoinAsof` is called with the parent `model` at L5720).
   - **LIMIT is free and clean**: `masterModel.getLimitLo()/getLimitHi()` — a constant limit is a
     definitive "small master" signal.
   - **The WHERE time-interval is NOT freely available**: by codegen the intrinsic interval has been
     consumed into a nested interval `RowCursorFactory` inside the master page-frame factory; there
     is no `master.isTimeBounded()` accessor. Surfacing it needs plumbing comparable in cost to #3
     (either capture the master model's intrinsic interval *before* it's consumed, or add an
     accessor to the interval factory). ⇒ treat the time-filter proxy as part of the #3 work, not a
     free win. LIMIT alone is the only free selectivity signal.
3. **`approxRowCount()` on `RecordCursorFactory`** — new, cheap. Default returns `-1` (unknown).
   Base-table page-frame factories override to return the reader's size (sum of partition row
   counts from the txn — cheap, no scan). Filter factories return `child.approxRowCount() ×
   defaultSelectivity` or `-1`. Joins/unknown return `-1`. Enables the true ratio gate for the
   common base-table case; everything else stays Dense.
4. **Timestamp density** — genuinely unknown at plan time. Do NOT gate on it. Instead, make the
   *memoized cursor* self-guard at runtime (below).

## Decision logic (slots into `generateJoinAsof`, after hint checks, before the Dense default)

```java
// single-symbol branch, after asof_index/asof_memoized/asof_fast hints, before the
// DenseSingleSymbol default return (~L5188).
if (configuration.isAsOfAutoAlgoEnabled()) {          // cairo.sql.asof.auto.algo, default true
    long slaveN  = approxRowCount(slave);             // -1 if unknown
    long masterN = approxRowCount(master);            // -1 if unknown
    IQueryModel masterModel = model.getJoinModels().getQuick(0);
    long masterLimit = constLimitOrMinus1(masterModel);   // signal #2 — free: getLimitLo/Hi
    boolean masterSelective =
            (masterLimit >= 0 && slaveN > 0 && masterLimit <= slaveN * cfg.asofIndexMasterRatioMax())
         || (masterN >= 0 && slaveN > 0 && masterN <= slaveN * cfg.asofIndexMasterRatioMax()); // #3, default 0.02
    // NOTE: WHERE-time-bound proxy deferred to the #3 plumbing (interval is consumed by codegen).

    // (A) indexed slave + confidently-small master -> index path (12-50x)
    if (slaveMetadata.isColumnIndexed(slaveSymbolColumnIndex) && masterSelective) {
        return new AsOfJoinIndexedRecordCursorFactory(/* … */
                reason("indexed-symbol", masterN, slaveN));   // carry reason into toPlan
    }
    // (B) no index, sparse-ts single symbol, selective master -> memoized (2-25x), self-guards on dense ts
    if (cfg.isAsOfAutoMemoizedEnabled() && masterSelective) {   // default guarded; see runtime guard
        return new AsOfJoinMemoizedRecordCursorFactory(/* … */
                reason("memoized-sparse", masterN, slaveN));
    }
}
// else: fall through to today's DenseSingleSymbol default (safe floor)
```

Notes:
- Placed **after** the explicit-hint returns, so hints always override auto-selection.
- Unknown estimates ⇒ `masterSelective` false ⇒ Dense. Zero regression vs today.
- Conservative `asofIndexMasterRatioMax = 0.02` (break-even sits ~2–2.5%; index still 2.3× at
  0.5%). Errs toward Dense near the crossover.

### Memoized runtime dense-timestamp guard (enables path B safely)

`asof_memoized` cliffs when many rows share a timestamp (`dense_sym` = 3372 ms). Since ts density
is unknown at plan time, gate it in the cursor: track the length of the current equal-timestamp
run; if it exceeds `K` (e.g. 4096), the cursor abandons memoization and finishes as a Dense
forward scan for the rest of the partition. Same "measure-then-fall-back" shape as the (now
dominated) adaptive prelude, but here it protects a path that is otherwise 2–25× and only has one
failure mode. Until this guard lands, keep path B behind `cairo.sql.asof.auto.memoized=false`.

## Getting the decision *into the plan* (EXPLAIN)

The factory already names itself in `toPlan` (`"AsOf Join Indexed Scan"`, `"AsOf Join Dense"`, …).
Extend each ASOF factory's `toPlan` to emit the **decision reason and estimates**, so the choice is
auditable:

```
AsOf Join Indexed Scan [on=sym, reason=indexed-symbol, master≈1000, slave≈2000000, ratio=0.0005]
```

And — importantly — when Dense is kept *despite* an available index, say why, so users learn when to
add a hint:

```
AsOf Join Dense [on=sym, note=indexed symbol available but master≈500000 ratio=0.25 > index-threshold 0.02]
AsOf Join Dense [on=sym, note=row-count estimate unavailable; add /*+ asof_index */ if master is selective]
```

This is the concrete answer to "get that information into the plan": the planner's reasoning
becomes visible in EXPLAIN instead of being invisible hint-gating.

## Correctness

All ASOF factories are result-equivalent (`AsOfJoinTest` 116/116, `AsOfJoinFuzzTest` 6/6 green
across forced algos). Auto-selection changes only performance. Add a fuzz mode that lets the
optimiser choose (varying index presence / master size / ts density) and asserts identical output
vs a forced-Dense oracle.

## Rollout

- **P1 (biggest win, lowest risk):** `approxRowCount()` for base-table + filter factories; auto-pick
  `asof_index` for indexed single-symbol with a selective master; EXPLAIN reason strings.
- **P2:** memoized runtime dense-ts guard, then enable auto path B for non-indexed sparse-ts.
- **P3:** multi-key ASOF has no index/memoized path today; defer (would need an indexed multi-key
  cursor).

## Explicitly out of scope

The adaptive Fast↔Dense prelude (`cairo.sql.asof.adaptive.backscan.budget`) is **dominated in every
measured regime** (~1.4–2× slower than Dense even on its best-case `sparse_tail`, and it never
touches the index). Recommend leaving it dormant/opt-in or removing it; it is not part of this
auto-selection.
