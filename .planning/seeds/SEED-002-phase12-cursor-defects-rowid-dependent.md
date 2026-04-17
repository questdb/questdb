---
id: SEED-002
status: dormant
planted: 2026-04-17
planted_during: Phase 12 completion (sm_fill_prev_fast_path branch)
trigger_when: rowId migration phase is planned
scope: Medium
---

# SEED-002: Phase 12 cursor-level defects (likely absorbed by rowId)

## Why This Matters

Two cursor-level wrong-output defects surfaced during Phase 12 but were
deferred because their fix likely overlaps with the parallel rowId-migration
work stream (moving PREV's copied-value storage to rowId-based lookup).

Defect 2 in particular (`toTop()` state corruption) sits squarely in the
iteration state machine that rowId migration is expected to rewrite —
fixing it independently now would likely get discarded. Defect 1 (CTE-wrap
+ outer projection corruption) is less clear — it may be in the factory
pipeline above the cursor, in which case rowId doesn't help.

"Should not lose them" — the user constraint when this seed was planted.

The user-facing consequence of these defects is that
`SampleByTest#testSampleByFillNeedFix` remains in its 6-row buggy form at
`SampleByTest.java:5943` (Phase 12 CONTEXT.md Success Criterion #1 asked to
restore master's 3-row form; descoped via Option A at a checkpoint when the
defects were discovered).

## When to Surface

**Trigger:** when rowId migration phase is planned.

This seed should be presented during `/gsd-new-milestone` or
`/gsd-new-phase` when the scope mentions:
- rowId migration / PREV rowId storage
- SampleByFillRecordCursorFactory redesign
- SampleByFillCursor iteration state machine
- Fast-path cursor `toTop()` / state reset
- SAMPLE BY FILL(PREV) mechanism rework

## Scope Estimate

**Medium** — investigation-heavy. Could be 0 LOC if rowId absorbs both
defects, or a full separate phase if independent. Estimate depends on two
open questions answered during rowId planning:

- **Q1:** Does rowId migration rewrite `SampleByFillCursor.toTop()` and the
  iteration state machine, or only the snapshot buffer encoding?
- **Q2:** Is Defect 1 a snapshot-buffer-level bug (absorbed) or a
  factory-plumbing bug above the cursor (independent)?

## Contents

### Defect 1 — CTE-wrap + outer projection corruption

Shape that breaks:
```sql
WITH sq AS (
  SELECT ts, symbol, first(price) AS open, sum(qty) AS cnt
  FROM t WHERE ... SAMPLE BY 1h FILL(PREV, 0) ORDER BY ts DESC
)
SELECT ts, cnt FROM sq   -- outer projects FEWER columns than CTE
```

Output: wrong timestamps (`1970-01-01T00:00:00`), duplicated rows per
bucket, garbled aggregate values. Raw SAMPLE BY (no CTE wrap) is correct.
`SELECT *` from the CTE is also correct. Only reducing the projection via
the outer SELECT triggers it.

**Reproduction:** `SampleByTest#testSampleByFillNeedFix` assertion #2
(currently in 6-row buggy form, lines 6018-6052).

**Hypothesis:** outer projection builds a different factory pipeline that
re-reads the CTE cursor in a mode the fast-path cursor does not support
(possibly `recordAt` / random-access / toTop re-use).

**Absorption probability:** UNCLEAR. Depends on where the bug lives. If in
factory plumbing above the cursor, independent. If in the cursor's response
to the factory's iteration pattern, likely absorbed.

### Defect 2 — Fast-path toTop() state corruption

`assertQuery(expected, query, timestampCol, sizeExpected=true)` iterates
cursor twice to verify `size()`. First iteration: correct output matching
master. Second iteration after `toTop()`: wrong values (e.g. `cnt=216`
instead of `cnt=0` for gap buckets; `high=0.0` instead of `0.00128`).

The cursor advertises `supportsRandomAccess=false` but `size() >= 0`
(advertises size), so the test framework does the double-iteration to
cross-check. `toTop()` does not fully reset some state — likely
`isGapFilling`, `hasDataForCurrentBucket`, `isEmittingFills`, or the
`simplePrev[]` snapshot.

**Reproduction:** `SampleByTest#testSampleByFillNeedFix` assertion #1.

**Absorption probability:** HIGHLY LIKELY. rowId migration rewrites how
PREV values are tracked. If the rewrite replaces `simplePrev[]` and the
associated `isInitialized`/`hasSimplePrev` flags, `toTop()` reset gets
redesigned in the process.

## Breadcrumbs

- `.planning/phases/12-replace-safety-net-reclassification-with-legacy-fallback-and/12-FOLLOWUPS.md` — bucket 2 details with fix scopes
- `.planning/phases/12-replace-safety-net-reclassification-with-legacy-fallback-and/12-04-SUMMARY.md` — deferred defects section (defects 1 & 2 originating documentation)
- `.planning/phases/12-replace-safety-net-reclassification-with-legacy-fallback-and/12-VERIFICATION.md` — gaps section (testSampleByFillNeedFix descope rationale)
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java:5943` — `testSampleByFillNeedFix` in 6-row buggy form
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` — `SampleByFillCursor.toTop()` and iteration state fields (`isGapFilling`, `hasDataForCurrentBucket`, `isEmittingFills`, `simplePrev[]`, `hasSimplePrev[]`, `isInitialized`)

## Re-sorting checklist

When this seed surfaces:

1. Read current rowId design doc / plan.
2. Answer Q1: does rowId rewrite iteration state machine or only the buffer?
3. If Q1 = iteration rewrite → mark Defect 2 "likely absorbed" and verify
   via regression test once rowId lands. Restore
   `testSampleByFillNeedFix` assertion #1 to master's 3-row form; if green,
   close Defect 2.
4. For Defect 1 (CTE corruption): bisect — does it repro with a minimal
   CTE + projection reduction? Is it snapshot-buffer-dependent (test with
   all-DOUBLE aggregates so no PREV state is touched)? Use the repro to
   isolate snapshot-buffer vs factory-plumbing layer.
5. If Defect 1 is factory-plumbing: file as a separate phase
   (investigation-heavy, may span `SqlOptimiser` and
   `SqlCodeGenerator.generateSubQuery`).
6. Regardless: restore `testSampleByFillNeedFix` to master's 3-row form
   once both defects are resolved. Closes Phase 12 CONTEXT.md Success
   Criterion #1 (intentionally deferred).

## Notes

- Planted alongside SEED-001 (bucket 1: plumbing + grammar, independent of rowId).
- These defects pre-existed on the `sm_fill_prev_fast_path` branch before
  Phase 12 started — none of plans 12-01..12-03 introduced them. Phase 12
  only surfaced them by attempting to restore master's expected behavior
  for `testSampleByFillNeedFix`.
- The user's Option A decision ("commit what works, defer
  testSampleByFillNeedFix restoration") was explicitly motivated by the
  expectation that rowId migration would likely absorb these.
