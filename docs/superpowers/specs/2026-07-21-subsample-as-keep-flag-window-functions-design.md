# SUBSAMPLE as keep-flag window functions

**Date:** 2026-07-21
**Status:** Design approved, pending implementation plan
**Branch:** `subsample-fixes` (off `jv/lttb` / PR #7013)

## Summary

Re-implement QuestDB's `SUBSAMPLE` downsampling clause as a family of
**boolean keep-flag window functions** plus a thin **desugaring** rewrite, instead
of a dedicated cursor + per-algorithm classes. Each algorithm (`lttb`, `m4`,
`minmax`, `uniform`, `cadence`, and `sdt`) becomes a **user-visible** window
function returning `true` on the rows it keeps; `SUBSAMPLE algo(args)` desugars to
a window subquery filtered on that flag. The SQL surface is unchanged.

## Motivation

- **Reuse the window framework instead of rebuilding it.** The dedicated
  `SubsampleRecordCursorFactory` (660 lines) re-implements what the window engine
  already does: streaming-vs-materialized routing, a parallel sort
  (`EncodedWindowSortBuffer`), paged random-access materialization (`RecordArray`,
  `CachedWindowLight`), and a two-pass driver (`pass1`/`preparePass2`/`pass2`).
- **Free wins the custom cursor lacked:** the window sort is **parallel** (settles
  the fused-vs-composed sort question — see the benchmark that showed the custom
  serial sort losing above ~10M rows), and `ZERO_PASS` functions run on a
  **non-materializing** streaming cursor.
- **Unifies the keep-flag family.** `sdt` (Swinging Door Trending, built on
  `feat/swinging-door` as a keep-flag window function) joins the same family;
  `SUBSAMPLE` and any future algorithm are just sugar over it.
- **Less code, battle-tested infra, same surface.** javier's 139 SUBSAMPLE tests
  are the regression oracle.

## Background: why not a dedicated two-cursor operator

We considered building two new cursors (sorted-input / unsorted-input) with a
custom algorithm interface. It maps almost 1:1 onto the window framework
(`WindowRecordCursorFactory` streaming vs `CachedWindowRecordCursorFactory`
materializing; `WindowSortBuffer` = the "fast sort, materialize only if needed"
mechanism). Rebuilding it would duplicate the window engine for marginal upside,
so we use the window framework directly.

Empirical basis (benchmark `SubsampleSortFusionBenchmark`, this branch):

| rows | custom serial sort | engine parallel sort |
|---|---|---|
| 1M | 108 ms | 123 ms |
| 5M | 665 ms | 698 ms |
| 10M | 1426 ms | **1379 ms** |
| 20M | 2883 ms | **2716 ms** |

The custom sort's edge is a compact-buffer artifact that vanishes and reverses
once the engine sort parallelizes (threshold ~1M rows). The window framework gets
the parallel sort for every algorithm for free.

## Architecture

Three parts, each independently testable.

### 1. Keep-flag window-function family

User-visible window functions, each returning `boolean` (keep this row?). The
SUBSAMPLE method name **is** the function name (no `_keep` suffix, consistent with
`sdt`):

| function | signature | keeps |
|---|---|---|
| `lttb` | `lttb(ts, value, target [, gap])` | max-triangle point per bucket (+ gap splits) |
| `m4` | `m4(ts, value, target)` | first/min/max/last per time bucket |
| `minmax` | `minmax(ts, value, target)` | min/max per time bucket |
| `uniform` | `uniform(target)` | rows at evenly spaced positions |
| `cadence` | `cadence(stride [, seed])` | every stride-th row (+ optional seed offset) |
| `sdt` | `sdt(ts, value, compdev)` | swinging-door archived points (error-bound) |

- Time-axis functions (`lttb`/`m4`/`minmax`/`sdt`) take `ts` explicitly (a window
  function does not receive its `ORDER BY` column; this mirrors the `sdt` design
  decision and avoids depending on a designated timestamp existing).
- Position-only functions (`uniform`/`cadence`) take no `ts`/`value` — they select
  by row position and read neither.
- All are used as `algo(...) OVER (ORDER BY ts)`; the boolean result is filtered.

### 2. Pass classification (minimum passes — the single-pass requirement)

Each function reports its **minimum** `getPassCount()`, so the framework routes
single-pass functions to the streaming (non-materializing) cursor:

| function | passes | rationale |
|---|---|---|
| `cadence` | **ZERO_PASS** (streaming) | stride is by row position; no global info needed. Last-row pinning is handled without forcing materialization (see Open Items). |
| `uniform` | **ONE_PASS** when the input cursor exposes `size()` (positions computed up front, marked in one forward pass); **TWO_PASS** fallback when the row count is not known until the scan ends. |
| `sdt` | **TWO_PASS** in framework terms, but a **single algorithmic data pass**: pass1 runs the swinging door once, back-patching the one-row lookback into an O(1)-per-row keep buffer; pass2 only *materializes* those buffered flags (it does not re-run the algorithm). The doors archive the *previous* point, so the flag for row i is known at i+1 — hence the buffered pass2 rather than pure streaming. |
| `m4`, `minmax`, `lttb` | **TWO_PASS** — bucket boundaries need global `min(ts)`/`max(ts)`; the max is unknown until the last row, so a single forward pass cannot bucket. `lttb` gap detection also lives in pass1. |

Consequence: `cadence` and (random-access) `uniform` run on the streaming
`WindowRecordCursorFactory` with **no materialization** when the input is already
ordered; the range/bucket functions materialize into `RecordArray` and sort via
the parallel `EncodedWindowSortBuffer`.

### 3. `SUBSAMPLE` desugaring

`SUBSAMPLE` keeps its current parser/model plumbing (the parsed `subsample` node
on `QueryModel`), but the code generator/optimiser **rewrites** it into a window
subquery instead of building a custom cursor:

```sql
SELECT <cols> FROM t SUBSAMPLE lttb(price, 500)
-- →
SELECT <cols> FROM (
  SELECT <cols>, lttb(ts, price, 500) OVER (ORDER BY ts) AS __keep FROM t
) WHERE __keep
```

- The desugarer supplies `ts` = the designated timestamp and the
  `OVER (ORDER BY ts)`. Per the position-only ordering decision
  ("time order, skip sort if absent"): use `OVER (ORDER BY ts)` when a
  timestamp/order exists, else `OVER ()` (input-order sampling) rather than
  erroring.
- `SUBSAMPLE uniform(price, 100)` drops the vestigial value arg →
  `uniform(100) OVER (ORDER BY ts)`.
- Placement preserves current semantics: the rewrite sits so `SUBSAMPLE` reduces
  **before** the outer `ORDER BY`/`LIMIT` (which operate on the reduced result).
  This is the same pipeline position the current clause occupies.

## Algorithm re-homing (reuse, don't rewrite)

javier's five `*Algorithm.select()` bodies are the selection logic and are
**preserved** — repackaged into each window function's pass driver:

- `pass1` gathers the global range/row-count and per-bucket candidate offsets
  (the current `select()` scan, adapted).
- `preparePass2` finalizes bucket structure.
- `pass2` writes `true` for selected rows, `false` otherwise.

The integer-overflow and `Math.floorMod` cadence fixes and the `RecordChain`
`maxPages` clamp from this branch carry into the new code where relevant.

`sdt` ports from `feat/swinging-door`: the pure `SwingingDoor` state machine and
its golden-vector unit tests move in unchanged; the window-function shell adapts
to this branch's window API.

## What is removed

- `SubsampleRecordCursorFactory` and its cursor glue (the 660-line cursor, the
  native buffer, `nativeSortBufferByTimestamp`, the fast/fallback split).
- The `SubsampleAlgorithm` interface and the per-method dispatch in
  `SqlCodeGenerator`/`SubsampleRecordCursorFactory` (name→method switch, arity,
  `isValueInspectingMethod`, the algorithm-construction switch, `toPlan`).

The algorithm **math** survives inside the window functions; only the bespoke
cursor/dispatch machinery is deleted.

## Components / file structure

- `core/.../engine/functions/window/LttbFunctionFactory.java` (+ `M4`, `MinMax`,
  `Uniform`, `Cadence`, `Sdt`) — the six keep-flag window-function factories, each
  self-registering via classpath scan, modeled on existing window factories.
- `core/.../engine/functions/window/SwingingDoor.java` — ported pure state machine.
- `core/.../griffin/SqlOptimiser.java` — the `SUBSAMPLE` → window-subquery rewrite
  (replacing the custom-cursor generation path).
- Deleted: `SubsampleRecordCursorFactory.java`, `SubsampleAlgorithm.java`,
  `{Lttb,M4,MinMax,Uniform,Cadence}Algorithm.java` (math re-homed).

## Testing

- **Regression oracle:** javier's 139 `SubsampleTest` cases must stay green — the
  SQL surface is identical.
- **New unit tests:** each keep-flag function tested directly as a window function
  (`assertQuery` on `algo(...) OVER (...)`), including the streaming-vs-cached
  routing (EXPLAIN plan shows the expected cursor per pass class).
- **`sdt`:** the ported `SwingingDoorTest` golden vectors + new SQL-level tests,
  plus `SUBSAMPLE sdt(value, compdev)`.
- **Performance:** `SubsampleSortFusionBenchmark` re-pointed to compare the old
  custom cursor (git-stashed reference) vs the window implementation at 1M–20M
  rows; the window path must be neutral-or-better (expected: better at scale via
  the parallel sort; verify no small-N regression from the cached-light cursor).

## Phasing (each phase ships green)

1. **Spine:** the `SUBSAMPLE` → window-subquery desugaring + `uniform` end-to-end;
   existing `uniform` tests pass. Proves the pattern and the rewrite.
2. **Count-based:** `cadence` (ZERO_PASS streaming), `m4`, `minmax`, `lttb`
   (TWO_PASS) as keep-flag functions; their existing SUBSAMPLE tests pass.
3. **`sdt`:** port `SwingingDoor` + `sdt`; add `SUBSAMPLE sdt(value, compdev)`.
4. **Retire + verify:** delete the old cursor/interface/dispatch; full SUBSAMPLE
   suite green; benchmark old-vs-new.

## Open items (resolved during planning)

- **`cadence` last-row pinning under ZERO_PASS streaming.** The streaming cursor
  has no "is-last-row" signal. Options: (a) keep the last-row pin by making
  `cadence` ONE_PASS (single materialized pass, count known at end); (b) provide an
  end-of-partition hook. Decide in the plan; default to (a) if no clean hook —
  ONE_PASS is still a single data pass.
- **Streaming eligibility for `uniform`** depends on the base cursor exposing
  `size()`; confirm the routing falls back to ONE_PASS/TWO_PASS cleanly when it
  does not.
- **Desugaring placement vs existing SUBSAMPLE optimiser hooks** (PIVOT/UNION
  rejection, the model-chain pull-up): the rewrite must land at the same model
  position the current codegen targets so composition (SAMPLE BY, JOIN, UNION
  arms) behaves identically. Reuse the fixes already made on this branch.
