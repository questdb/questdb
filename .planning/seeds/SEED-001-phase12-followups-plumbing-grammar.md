---
id: SEED-001
status: dormant
planted: 2026-04-17
planted_during: Phase 12 completion (sm_fill_prev_fast_path branch)
trigger_when: rowId migration phase is planned
scope: Small
---

# SEED-001: Phase 12 follow-ups (plumbing + grammar, independent of rowId)

## Why This Matters

Phase 12 shipped with five follow-up items that are all real defects or UX
degradations — but whether to fix them depends on what the rowId migration
turns out to be. If rowId eliminates retro-fallback entirely (all PREV types
become fast-path-supported), three of these items (WR-01, WR-02, WR-03)
become obsolete — they'd fix code we're about to delete. If rowId only
unlocks a subset of types, retro-fallback stays and these items are ship
blockers for PR #6946 because WR-01 and WR-02 are real correctness holes.
WR-04 and defect 3 are grammar/UX items that hold regardless of rowId.

"Should not lose them" — the user constraint when this seed was planted.

## When to Surface

**Trigger:** when rowId migration phase is planned.

This seed should be presented during `/gsd-new-milestone` or
`/gsd-new-phase` when the scope mentions:
- rowId migration / PREV rowId storage
- SampleByFillRecordCursorFactory redesign
- Safety-net / retro-fallback changes
- SAMPLE BY FILL(PREV) mechanism rework

## Scope Estimate

**Small** — ~40-50 LOC total across the five items, plus 3-5 regression
tests. Each item is surgical and isolated:

- WR-01: ~15 LOC + 1 test (extend stash to capture FROM/TO/OFFSET/TIMEZONE)
- WR-02: ~3 LOC (unwrap QueryModelWrapper before write-back)
- WR-03: ~20 LOC reduction (extract fallbackToLegacySampleBy helper)
- WR-04: ~10 LOC (per-column ExpressionNode tracking for precise chain error)
- Defect 3: ~5 LOC + 1 test (insufficient fill values grammar rule)

Can ship as one small PR together; no cross-cutting design work required.

## Contents

### WR-01 — Retro-fallback drops FROM/TO/OFFSET/TIMEZONE metadata

**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:8043-8056, 8259-8272, 8307-8320` (catches) + `core/src/main/java/io/questdb/griffin/SqlOptimiser.java:8467-8469` (stash).
`rewriteSampleBy` clears five fields; retro-fallback restores only `sampleBy`.
Queries with `FROM`/`TO`/`OFFSET`/`TIME ZONE` that trigger retro-fallback run
the legacy cursor without those bounds.

### WR-02 — Walk lands on QueryModelWrapper → UnsupportedOperationException

**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:8053-8054, 8269-8270, 8317-8318` + `core/src/main/java/io/questdb/griffin/model/QueryModelWrapper.java:1191-1192, 1246-1247`.
Walk can terminate on a wrapper whose delegate holds the stash. Setters on
the wrapper throw unconditionally. Retro-fallback surfaces as user-facing
hard error.

### WR-03 — Extract common catch helper

**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:8043-8056, 8259-8272, 8307-8320`.
Three byte-identical 13-line catch blocks. Any change to WR-01/WR-02 has to
be applied three times.

### WR-04 — Precise-position chain rejection

**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:3513`.
D-07 chain error always points at first PREV, not the offending column.
Acknowledged in CONTEXT.md D-07 as deliberate trade-off (6-line generic vs
10-line precise).

### Defect 3 — Missing "insufficient fill values" grammar rule

**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:generateFill`.
Master rejects `FILL(PREV,PREV,PREV,PREV,0)` for 7 aggregates at pos 554.
Current branch silently pads with NULL. CONTEXT.md D-05..D-09 did not
specify this rule — scope gap.

## Breadcrumbs

- `.planning/phases/12-replace-safety-net-reclassification-with-legacy-fallback-and/12-FOLLOWUPS.md` — full bucket-1 details with fix scopes
- `.planning/phases/12-replace-safety-net-reclassification-with-legacy-fallback-and/12-REVIEW.md` — WR-01..WR-04 findings with code-review rationale
- `.planning/phases/12-replace-safety-net-reclassification-with-legacy-fallback-and/12-04-SUMMARY.md` — Defect 3 originating decision
- `.planning/phases/12-replace-safety-net-reclassification-with-legacy-fallback-and/12-CONTEXT.md` — D-05..D-09 grammar rule list (missing insufficient-fill rule)
- `.planning/phases/12-replace-safety-net-reclassification-with-legacy-fallback-and/12-VERIFICATION.md` — gaps structured for `/gsd-plan-phase --gaps`

## Re-sorting checklist

When this seed surfaces:

1. Read current rowId design doc / plan.
2. If rowId unlocks ALL currently-unsupported PREV types → retro-fallback is
   deleted. WR-01, WR-02, WR-03 become obsolete. Keep WR-04 + Defect 3 as a
   small PR.
3. If rowId unlocks only a SUBSET → retro-fallback stays. All five items
   ship as a small follow-up PR before PR #6946 merges.
4. Ensure every fix has a regression test. WR-01 in particular has zero
   existing coverage for the retro-fallback + FROM/TO combination.

## Notes

- Planted alongside SEED-002 (bucket 2: defects likely absorbed by rowId).
- Phase 12 shipped with 11 follow-up items total; 6 quality notes (IN-01..IN-06)
  are tracked in `12-FOLLOWUPS.md` but do not warrant a seed.
- The five items in this seed are "should-fix regardless of rowId shape" as
  long as retro-fallback survives in any form.
