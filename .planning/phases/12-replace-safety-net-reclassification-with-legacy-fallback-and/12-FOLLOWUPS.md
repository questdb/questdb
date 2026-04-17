---
phase: 12
type: followups
status: parked
created: 2026-04-17
trigger: rowId migration phase planning
---

# Phase 12 — Follow-ups (parked pending rowId migration)

Phase 12 shipped with 11 identified follow-up items. The `sm_fill_prev_fast_path`
branch also carries a parallel rowId-migration work stream (moving PREV's
copied-value storage to rowId-based lookup). rowId will land BEFORE PR #6946
merges and its scope is not yet finalized, so these items are parked until
rowId scope is clear.

**Re-sorting trigger:** when rowId migration phase is planned, sort each item
below into `(A) still needed regardless` or `(B) absorbed by rowId / obsolete`.

---

## Bucket 1 — Almost certainly independent of rowId (small follow-up PR)

These are plumbing/parser/UX bugs. rowId work is expected to only change how
PREV values are *stored* (long[] → rowId pointers); these items sit outside
that storage layer.

### WR-01 — Retro-fallback drops FROM/TO/OFFSET/TIMEZONE metadata

**Source:** `12-REVIEW.md`
**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:8043-8056, 8259-8272, 8307-8320` (catch sites) and `core/src/main/java/io/questdb/griffin/SqlOptimiser.java:8467-8469` (stash/clear sites).

`rewriteSampleBy` clears five fields on the nested model:
- `sampleBy` (stashed, restored correctly)
- `sampleByFrom`
- `sampleByTo`
- `sampleByOffset`
- `sampleByTimezoneName`

Retro-fallback restores only `sampleBy`. Queries that specify
`FROM`/`TO`/`OFFSET`/`TIME ZONE` AND trigger retro-fallback (aggregate type
forces legacy dispatch) run the legacy cursor with those bounds null —
unbounded result set, wrong output.

**Latent bug:** no existing retro-fallback test exercises this combination.

**Fix scope:** extend `QueryModel.stashedSampleByNode` to
`stashedSampleByState` capturing all five fields; restore all five in
the catch. ~15 LOC across QueryModel/IQueryModel/SqlOptimiser/SqlCodeGenerator
plus one regression test (`SAMPLE BY 1h FROM ... TO ... FILL(PREV) ALIGN TO CALENDAR TIME ZONE ...` with an aggregate whose output type forces retro-fallback).

**rowId relationship:** independent unless rowId eliminates retro-fallback
entirely. See Bucket 3 for that dependency.

### WR-02 — Walk lands on QueryModelWrapper → UnsupportedOperationException

**Source:** `12-REVIEW.md`
**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:8053, 8054, 8269, 8270, 8317, 8318` (walk sites); `core/src/main/java/io/questdb/griffin/model/QueryModelWrapper.java:1191-1192, 1246-1247` (unconditional throws).

```java
while (curr != null && curr.getStashedSampleByNode() == null) {
    curr = curr.getNestedModel();
}
curr.setSampleBy(stashed);                 // throws UnsupportedOperationException on wrapper
curr.setStashedSampleByNode(null);         // throws UnsupportedOperationException on wrapper
```

`QueryModelWrapper.getStashedSampleByNode()` delegates to its wrapped
model — so the walk can terminate with `curr` being a wrapper whose delegate
holds the stash. The setters throw unconditionally and the retro-fallback
surfaces as a user-facing hard error instead of a legacy-path re-dispatch.

**Fix scope:** unwrap at the end of the walk (~3 LOC):
```java
if (curr instanceof QueryModelWrapper wrapper) {
    curr = wrapper.getDelegate();
}
```

**rowId relationship:** independent.

### WR-03 — Extract common catch helper

**Source:** `12-REVIEW.md`
**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:8043-8056, 8259-8272, 8307-8320`.

Three byte-identical 13-line catch blocks. Any change to WR-01 or WR-02
must be applied three times.

**Fix scope:** extract `fallbackToLegacySampleBy(model, executionContext)`
helper. Each catch becomes a one-liner. ~20 LOC reduction.

**rowId relationship:** independent, but naturally lands with WR-01 and WR-02.

### WR-04 — Chain-rejection position is generic

**Source:** `12-REVIEW.md`
**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:3513`.

D-07 chain error always points at `fillValuesExprs.getQuick(0).position`
(first PREV in the FILL clause) rather than at the specific column whose
chain is malformed. UX degradation vs other grammar rules in phase 12.
Acknowledged in CONTEXT.md D-07 as deliberate trade-off (6-line generic
variant vs 10-line precise variant).

**Fix scope:** track `ExpressionNode` per cross-col mode during the fill-spec
build loop; use that node's position in the chain error. ~10 LOC.

**rowId relationship:** independent.

### Defect 3 — Missing "insufficient fill values" grammar rule

**Source:** `12-04-SUMMARY.md` deferred defects section and `12-VERIFICATION.md`.
**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:generateFill` (position to add grammar check after `fillValuesExprs.size()` resolution).

Master rejects `FILL(PREV, PREV, PREV, PREV, 0)` for 7 aggregate columns
with positioned `SqlException` at pos 554: `"insufficient fill values for SAMPLE BY FILL"`.
Current branch silently pads missing positions with `FILL_CONSTANT(null)` —
query runs, user gets NULLs they did not ask for.

CONTEXT.md grammar list D-05..D-09 did not specify this rule — scope gap.

**Fix scope:** in `generateFill`, after resolving `fillValuesExprs.size()` vs
`columnCount`, throw positioned SqlException when
`fillValuesExprs.size() > 1 AND fillValuesExprs.size() < columnCount`. Position
points at the last fill value. ~5 LOC + 1 regression test.

**rowId relationship:** independent — pure parser rule.

---

## Bucket 2 — Likely absorbed by rowId migration (re-evaluate when rowId scope is known)

These manifest as cursor-level wrong output. If rowId migration rewrites the
iteration state machine and the snapshot buffer simultaneously, both may
vanish for free. If only the buffer encoding changes, they stay orthogonal.

### Defect 1 — CTE-wrap + outer projection corruption

**Source:** `12-04-SUMMARY.md` deferred defects section.

Shape that breaks:
```sql
WITH sq AS (
  SELECT ts, symbol, first(price) AS open, sum(qty) AS cnt
  FROM t WHERE ... SAMPLE BY 1h FILL(PREV, 0) ORDER BY ts DESC
)
SELECT ts, cnt FROM sq   -- outer projects FEWER columns than CTE
```

Output: wrong timestamps (`1970-01-01T00:00:00`), duplicated rows per bucket,
garbled aggregate values. Raw SAMPLE BY (no CTE wrap) is correct. `SELECT *`
from the CTE is also correct. Only reducing the projection via the outer
SELECT triggers it.

**Reproduction:** `SampleByTest#testSampleByFillNeedFix` (still in 6-row
buggy form at `SampleByTest.java:5943`; deferred per user Option A).

**Hypothesis:** outer projection builds a different factory pipeline that
re-reads the CTE cursor in a mode the fast-path cursor does not support
(possibly `recordAt` / random-access / toTop re-use).

**rowId relationship:** UNCLEAR. May be absorbed if rowId rewrites cursor
iteration; may stay if bug is in factory plumbing above the cursor.

### Defect 2 — Fast-path toTop() state corruption

**Source:** `12-04-SUMMARY.md` deferred defects section.

`assertQuery(expected, query, timestampCol, sizeExpected=true)` iterates
cursor twice to verify `size()`. First iteration: correct output (matches
master). Second iteration after `toTop()`: wrong values
(e.g. `cnt=216` instead of `cnt=0` for gap buckets; `high=0.0` instead of
`0.00128`).

The cursor advertises `supportsRandomAccess=false` but `size() >= 0` (advertises
size), so the test framework does the double-iteration to cross-check.
`toTop()` does not fully reset some state — likely `isGapFilling`,
`hasDataForCurrentBucket`, `isEmittingFills`, or the `simplePrev[]` snapshot.

**Fix scope (independent path):** audit `SampleByFillCursor.toTop()` against
every field the cursor mutates during iteration; ensure each is reset.

**rowId relationship:** HIGHLY LIKELY ABSORBED. rowId migration rewrites
how PREV values are tracked. If the rewrite replaces `simplePrev[]` and
the associated `isInitialized`/`hasSimplePrev` flags, `toTop()` reset gets
redesigned in the process.

---

## Bucket 3 — Hypothetical: rowId eliminates retro-fallback entirely

If rowId migration unlocks ALL currently-unsupported PREV types (UUID /
LONG128 / LONG256 / DECIMAL128 / DECIMAL256 / STRING / VARCHAR / BINARY /
arrays / INTERVAL), the retro-fallback mechanism becomes unreachable:

- Delete `FallbackToLegacyException`
- Delete the three catch blocks in `generateSelectGroupBy`
- Delete `QueryModel.stashedSampleByNode` stash plumbing
- Delete the codegen-time unsupported-type check

If this happens, Bucket 1's WR-01, WR-02, WR-03 become moot (they'd be fixing
code we're about to delete). WR-04 and defect 3 remain — they're grammar
rules, not retro-fallback plumbing.

If rowId only unlocks a subset, retro-fallback stays and WR-01/WR-02/WR-03
need fixing.

**Open question until rowId scope is clear.**

---

## Info-tier items (IN-01..IN-06 from 12-REVIEW.md)

Quality notes only. No action required. Kept here for completeness:

- **IN-01** redundant same-package `import io.questdb.griffin.FallbackToLegacyException;` — if retro-fallback is deleted (Bucket 3), so is the import.
- **IN-02** `FallbackToLegacyException.INSTANCE` inherits `SqlException.message`/`position` mutable fields — theoretical thread-safety risk if future code mutates the singleton.
- **IN-03** `testFillPrevRejectNoArg` uses permissive `e.getMessage().contains(...)` disjuncts — tighten to `contains("PREV")` only.
- **IN-04** `hasAnyConstantFill()` walks `fillModes` on every `toPlan` call — invariant, could cache.
- **IN-05** `fillModes` field consistency note — not an issue.
- **IN-06** `Dates.parseOffset` assert message concatenation — correct pattern.

---

## Re-entry checklist (for the agent picking this up)

When rowId migration phase is being planned:

1. Read current rowId design doc / plan.
2. Answer:
   - **Q1:** Does rowId unlock all currently-unsupported PREV types, or a subset?
   - **Q2:** Does rowId rewrite `SampleByFillCursor.toTop()` / iteration state machine?
3. If Q1 = all: move WR-01/WR-02/WR-03/IN-01/IN-02 to "obsolete — delete retro-fallback."
4. If Q1 = subset: fix WR-01/WR-02/WR-03 in a small follow-up PR.
5. If Q2 = yes: mark defects 1 & 2 "likely absorbed — verify with regression tests once rowId lands."
6. If Q2 = no: file defects 1 & 2 as a separate phase (investigation-heavy).
7. WR-04 + defect 3 + IN-03 always land (rowId-independent). Can bundle with item 3 or 4 above.

---

## Related artifacts

- `12-CONTEXT.md` — phase intent, grammar rule table
- `12-VERIFICATION.md` — goal-backward verification, structured gaps
- `12-REVIEW.md` — code review findings (WR-01..WR-04, IN-01..IN-06)
- `12-04-SUMMARY.md` — deferred defects section (Defects 1-3)
