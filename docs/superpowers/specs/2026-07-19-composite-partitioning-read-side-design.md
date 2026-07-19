# Composite Partitioning — Read Side (Plans 5 & 6) Design

**Status:** design (autonomous build, per standing "execute plan 5 and 6" directive). Builds on
Plans 1–4 (write side complete, `feat/composite-partitioning` @ `d2de63c781`). Grounded in
`.superpowers/sdd/plan56-research.md` (4-agent read-side map + a merge-mechanics pass — every anchor
verified against the live worktree).

## Goal
Make the READ side of a composite table correct and then fast. Today the write side physically routes
rows into per-cell partition directories (`2020-01-01/exch=BTC.3`), but the read side is cell-blind:
it treats the `(ts, cellKey)`-ordered partition list as if it were globally timestamp-ordered. It is
NOT — within a day, cells are concatenated (`cell0` rows ++ `cell1` rows), each internally ts-ordered
but not merged. Every query shape that assumes a globally-ts-ordered base is therefore **silently
wrong** on a composite table: `ORDER BY ts`, `SAMPLE BY`, `LATEST ON`, and ASOF/LT/SPLICE/WINDOW/HORIZON
joins. Plus a ts-range interval filter drops sibling cells of the last matched day.

After Plans 5 & 6, a composite table is a first-class citizen for every read shape (results byte-identical
to an equivalent single-column-partitioned "plain twin"), and dimension-predicate queries prune whole
cells (the performance payoff of the layout).

## The defects (all confirmed against live code — see research doc for `file:line`)
1. **I2 — interval-scan endpoint.** `AbstractIntervalPartitionFrameCursor.cullPartitions` maps the high
   interval boundary via a ts-only binary search that lands on the *first* cell (cellKey 0) of the high
   day and `+1`s past only it → all higher-cellKey siblings of the highest matched day are excluded.
   (Low boundary is fine — it lands on cellKey 0 and siblings sit at higher indices inside the range.)
2. **Cross-cell disorder (the core).** `PageFrameRecordCursorImpl` drains each page frame fully before
   advancing; frames arrive in `(ts, cellKey)` order → per day it emits `cell0 (ts-asc) ++ cell1 (ts-asc)`
   = concatenation, not a global ts merge. The root enabler is `PageFrameRecordCursorFactory.getScanDirection()`
   reporting FORWARD from the *requested* iteration order, never from whether the concatenated stream is
   actually globally ts-ordered. Downstream:
   - ORDER BY: sort-skip fast-path trusts the flag → returns the raw interleaved stream.
   - SAMPLE BY / FILL: eligibility gate passes (flag lies); the single forward-pass bucket loop folds a
     later cell's early rows into the wrong (already-advanced) bucket and emits each period once per cell.
   - LATEST ON: keeps the last-seen rowid per key with no ts compare → "latest" = highest cellKey, not
     highest ts.
   - Joins: master consumes `getCursor()`, light/splice slave consumes `getCursor()`, fast slave consumes
     `getTimeFrameCursor()`; all assume ascending ts with no monotonicity guard (only `-ea` post-condition
     asserts, silent in production).
3. **No dimension pruning.** A `WHERE exch='BTC'` predicate is applied only as a row-level filter inside
   every frame; it never prunes whole cells. Correct but leaves the layout's main performance win unrealized.

## Architecture

### Plan 6 — cross-cell timestamp merge (correctness core)
**One seam fixes the whole record path:** give a composite table a base scan whose **`getCursor()` returns
a globally-ts-ordered stream** produced by a **per-day k-way cross-cell merge record cursor**, and whose
**`getScanDirection()` then truthfully reports FORWARD/BACKWARD**. Because ORDER BY, SAMPLE BY, LATEST ON,
and master + light/splice-slave joins all consume `getCursor()`, they become correct with **zero
per-consumer change** the moment the stream is genuinely ordered and the flag is honest.

- **Merge cursor.** Per day, group that day's frames by cell, expose one ts-ordered sub-iterator per
  sibling cell, and heap-pop rows by the designated timestamp; when a day is exhausted, advance to the
  next day. The heap width = cells-per-day (small). Merge is **row-granular** (a page frame is one cell's
  contiguous native memory and cannot interleave two cells). Forward and backward variants (backward pops
  by descending ts). The exact row-access mechanism (mimic `PageFrameMemoryRecord` addresses vs. a
  copy-out candidate buffer) is fixed by the merge-mechanics research and specified in Plan 6 Task 1 —
  driven by whether ≥2 frames' column memory can be simultaneously addressable (pool size).
- **`getScanDirection()` truthful.** Report the real order of the merged `getCursor()` stream. MUST-VERIFY
  (Plan 6 Task 2): `getScanDirection` is consulted only for record-order plan-time decisions, not to
  describe *frame* order — otherwise a frame consumer would be misled. If any frame consumer reads it,
  split into a distinct "frames globally ordered?" query.
- **`getPageFrameCursor()` stays cell-blind.** Order-INDIFFERENT vectorized/async ops (SUM/COUNT/MIN/MAX/
  AVG, unkeyed & keyed hash GROUP BY, bare filters) keep the fast unmerged frame path — correct because
  they do not depend on order. A differential capstone (every shape == plain twin) is the safety net that
  catches any order-dependent consumer that slipped onto the frame path.
- **`getTimeFrameCursor()` (fast-join slave).** For a composite table, signal "no time-frame cursor" so
  ASOF/LT/WINDOW/HORIZON fall back to the LIGHT slave path (`getCursor()` → merged) — correct now. A merged
  time-frame cursor is a later performance optimization. If the join generator lacks a graceful fallback,
  add a loud composite gate on the fast path until the fallback exists (never silent).

### Plan 5 — read pruning
- **5a (correctness).** Fix the I2 endpoint: `cullPartitions` must set the high partition index to the
  *last* cell of the high day (a scan-down search returning the last index whose partition ts ≤ intervalHi,
  `+1`), via a new `TableReader.getPartitionIndexByTimestampScanDown` helper. One fix covers Fwd and Bwd
  (both consume the same `initialPartitionHi`). This must land before/with Plan 6 so the merge over an
  interval scan sees all sibling cells.
- **5b (performance — the payoff).** A new cell-pruning hook: resolve a partitioning-dimension predicate
  (`WHERE exch='BTC'`, `WHERE exch IN (...)`, and negations where tractable) to a set of cellKeys, then
  skip partition slots whose cellKey is not in the set — before frames are produced. Natural locus: a
  cell-aware `PartitionFrameCursor` (or a filter applied in `cullPartitions`/frame advance) that consults
  the `_cell` registry to map dimension values → cellKeys. Correctness is unaffected if omitted (the row
  filter still runs); this is pure speed. Compose with the interval cursor (prune by ts AND by cell).

## Non-goals (this phase)
- A merged **time-frame** cursor to restore the fast-join slave path (deferred perf; light-join fallback
  is correct).
- A perf "skip the merge when the result order is irrelevant" planner optimization (always-merge
  `getCursor()` is correct; skipping is later).
- Cardinality/entropy guard on dimension cardinality (that was the separate sub-plan 7).
- Non-symbol dimensions; ORDER-BY-clustering physical realization.
- Vectorized/JIT execution *over the merged stream* (the merge is a record cursor; vectorization stays on
  the order-indifferent frame path).

## The no-silent-wrong invariant (execution discipline)
The composite read side is *already* silently wrong today (pre-existing, documented, branch unmerged /
not-user-facing). Plan 6 Task 1 removes the silent-wrong for the record path. At **every commit**, any
read shape not yet made correct must be **loud-gated** (a clear `CairoException` "not yet supported on
composite tables"), never left silent — the same discipline that governed the write side. The differential
plain-twin capstone (6d) plus per-task reviews (opus for the merge cursor and the getScanDirection change)
are the verification spine.

## Sequencing
`5a` (I2 endpoint) → `6a` (merge cursor + truthful getScanDirection) → `6b` (getTimeFrameCursor / fast-join
fallback) → `6c` (audit frame-path order consumers; verify LATEST ON & SAMPLE BY over the merged stream) →
`6d` (differential capstone: every shape == plain twin, incl. checkpoint/restore + multi-commit) →
`5b` (dimension cell-pruning, performance).
