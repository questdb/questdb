---
id: SEED-003
status: dormant
planted: 2026-04-17
planted_during: Phase 13 scoping (sm_fill_prev_fast_path branch, while borrowing from sm_fill_prev_fast_all_types)
trigger_when: SAMPLE BY FILL feature scope expands, or ALIGN TO FIRST OBSERVATION is raised by a user report / roadmap review
scope: Medium
---

# SEED-003: Extend fast-path FILL cursor to ALIGN TO FIRST OBSERVATION

## Why This Matters

Today, any `SAMPLE BY ... FILL(...) ALIGN TO FIRST OBSERVATION` query
falls back to the legacy `SampleByFillPrev*RecordCursor` path because
`rewriteSampleBy()` requires a non-null `sampleByOffset`, which FO
does not provide. This is a silent performance regression: users who
write FO queries see single-threaded legacy execution even when their
shape is otherwise fast-path eligible.

The `sm_fill_prev_fast_all_types` branch (commit `e4e3f442a9`,
2026-04-14) already implements this extension. It introduces
`timestamp_floor_first_obs(interval, ts)` — a parallelism-disabled key
function that captures the first timestamp it sees and floors every
subsequent row against that anchor. Rewritten FO models carry a new
`hasFirstObsFloorKey` flag on QueryModel, honored by
`moveWhereInsideSubQueries` to prevent outer-WHERE push-down that
would shift the anchor row.

This seed is planted because:
- The feature is useful in its own right but orthogonal to our
  current PR-6946 milestone.
- It is a candidate for a standalone phase (Phase 14?) once phase 13
  lands.
- The work is already written — borrowing the commit whole is
  plausible, provided we re-verify it on top of our phase-12
  retro-fallback mechanism and phase-13 rowId rewrite.

"Should not lose them" — the user constraint that birthed SEED-001
and SEED-002 applies equally here.

## When to Surface

**Trigger:** when SAMPLE BY FILL scope expands in a future milestone,
or when a user issue requests faster FO+FILL queries, or during
`/gsd-new-milestone` / `/gsd-new-phase` if the scope mentions:

- ALIGN TO FIRST OBSERVATION
- `SampleByFillPrevNotKeyedRecordCursor` / `SampleByFillPrevRecordCursor`
- `timestamp_floor_first_obs`
- FO optimizer gate relaxation
- Legacy `SampleByFillPrev*` cursor deletion

## Scope Estimate

**Medium** — ~1185 LoC on the source commit, spanning optimizer +
function factory + QueryModel + 369 new test lines across 5 suites.
Estimate depends on whether phase 13's rowId rewrite changes the
cursor storage model in ways that interact with FO's
`hasFirstObsFloorKey` flag.

Key open questions to answer during planning:

- **Q1:** Does phase 13's rowId rewrite interact with the FO key
  function `timestamp_floor_first_obs`'s parallelism-disabled
  requirement? FO sends the query through `GroupByRecordCursorFactory`
  (single-threaded) — does rowId add constraints?
- **Q2:** Does FO + retro-fallback (if retro-fallback survives past
  phase 13) have a correctness interaction not covered on the source
  commit, where FO was implemented before retro-fallback existed?
- **Q3:** The month/year stride exclusion on the source commit (due
  to `Micros.floorMM/floorYYYY` vs legacy `addMonths` anchor
  differences) — is this still the right exclusion on current code,
  or has the stride handling changed?

## Contents

### Source

- **Branch:** `sm_fill_prev_fast_all_types`
- **Commit:** `e4e3f442a9` — "Extend fill-cursor fast path to FILL(PREV) ALIGN TO FIRST OBSERVATION"
- **Size:** 9 files, +1185 / -400 LoC

### Code touchpoints (from source commit)

- **NEW:** `core/src/main/java/io/questdb/griffin/engine/functions/date/TimestampFloorFirstObsFunctionFactory.java` — 168 LoC new file; `supportsParallelism()=false`, captures first timestamp in instance field.
- **MOD:** `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` — 137 LoC; relax the `sampleByOffset != null` guard in `rewriteSampleBy` to also accept FO, generate the `timestamp_floor_first_obs` key function when FO is active.
- **MOD:** `core/src/main/java/io/questdb/griffin/model/QueryModel.java` — 13 LoC; add `hasFirstObsFloorKey` boolean flag + setter/getter; nulled in `clear()`.
- **MOD:** `core/src/main/resources/function_list.txt` — register the new function.
- **EXCLUSION:** Month/year strides stay on the legacy path due to
  `Micros.floorMM/floorYYYY` bucket-boundary differences vs the
  legacy sampler's `addMonths` stepping on day-of-month anchors.
  Research whether this exclusion is still needed today.

### Tests (17 on source commit)

`core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` adds:

**Per-type FILL(PREV) + FO:**
- `testFillPrevFirstObsNonKeyed`
- `testFillPrevFirstObsKeyed`
- `testFillPrevFirstObsSymbol`
- `testFillPrevFirstObsString`
- `testFillPrevFirstObsVarchar`
- `testFillPrevFirstObsUuid`
- `testFillPrevFirstObsDecimal128`
- `testFillPrevFirstObsArrayDouble1D`
- `testFillPrevFirstObsEmpty`
- `testFillPrevFirstObsFromOverridesAnchor`
- `testFillPrevFirstObsPlan`
- `testFillPrevFirstObsToTopRewinds`

**Other fill modes + FO:**
- `testFillNoneFirstObsNonKeyed`
- `testFillNullFirstObsKeyed`
- `testFillNullFirstObsNonKeyed`
- `testFillValueFirstObsKeyed`
- `testFillValueFirstObsNonKeyed`

Also refreshed plan text in `ExplainPlanTest`, `SampleByNanoTimestampTest`,
`SampleByTest`, `RecordCursorMemoryUsageTest`.

## Re-sorting checklist

When this seed surfaces:

1. Read commit `e4e3f442a9` in full — including the plan-text
   refreshes that are coupled to the optimizer change.
2. Answer Q1, Q2, Q3 above against the then-current branch state.
3. Decide: (a) cherry-pick the commit whole and adapt any conflicts,
   or (b) plan from scratch using the commit as reference.
4. If (a): verify the commit still applies cleanly on top of the
   target branch. Resolve conflicts from phase 12 (retro-fallback,
   grammar rules) and phase 13 (rowId storage).
5. Run the full FO test suite; cross-check against the legacy cursor
   path to ensure output parity.
6. Decide whether legacy `SampleByFillPrev*RecordCursor` can now be
   deleted (likely if retro-fallback is also gone).

## Breadcrumbs

- `sm_fill_prev_fast_all_types` @ `e4e3f442a9` — full implementation + tests
- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java:rewriteSampleBy` — current FO branch (falls through to legacy)
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillPrevNotKeyedRecordCursor.java` — current legacy cursor that FO routes to
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillPrevRecordCursor.java` — keyed legacy counterpart

## Notes

- Planted alongside SEED-001 (phase 12 plumbing follow-ups) and
  SEED-002 (phase 12 cursor defects).
- Unlike SEED-002, this is not a defect — it is a scope extension.
  Independent of phase 13's rowId work.
- `/gsd-plan-phase 14` (or whichever phase is named next) should
  consider FO as a candidate before starting on other post-13 work.
- Borrowed from the same branch as phase 13's rowId scope.
