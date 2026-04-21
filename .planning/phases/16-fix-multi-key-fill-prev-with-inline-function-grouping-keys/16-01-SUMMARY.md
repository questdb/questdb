---
phase: 16-fix-multi-key-fill-prev-with-inline-function-grouping-keys
plan: 01
subsystem: sql-codegen
tags: [sql, sample-by, fill, classifier, codegen, correctness, regression-tests]

# Dependency graph
requires:
  - phase: 15-address-pr-6946-review-findings-and-retro-fixes
    provides: stable classifier fast-path + defensive -ea lock-in convention
provides:
  - widened generateFill classifier covers non-aggregate FUNCTION/OPERATION grouping keys
  - D-05 aggregate-arm -ea assertion pins the residual arm to genuine aggregate factories
  - 5 regression tests pinning the multi-key cartesian contract across interval / concat / cast / FILL(NULL) variants
affects: [future phases touching SqlCodeGenerator.generateFill classifier, any SAMPLE BY FILL work]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - Four-way classifier partition in generateFill (LITERAL / timestamp_floor / non-aggregate FUNCTION+OPERATION key / aggregate FUNCTION)
    - Same-commit invariant lock-in with -ea assertion at the residual arm

key-files:
  created: []
  modified:
    - core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java

key-decisions:
  - "D-02 predicate: (FUNCTION || OPERATION) && !functionParser.getFunctionFactoryCache().isGroupBy(ast.token), landed verbatim"
  - "D-05 aggregate-arm -ea assertion placed immediately before the `final CharSequence qcAlias = qc.getAlias();` block, in the same commit as the classifier fix"
  - "D-06 cursor-side wiring verified unchanged: SampleByFillRecordCursorFactory.java not modified"
  - "Probe-and-freeze captured Interval.NULL rendering as literal `null` (not empty text), disambiguating RESEARCH.md A2"

patterns-established:
  - "Classifier third-arm insertion: slot between existing continue branches and the aggregate fall-through"
  - "Aggregate-arm -ea lock-in: assert ast.type == FUNCTION && isGroupBy(ast.token) before any aggregate-specific logic runs"

requirements-completed: [COR-01, COR-02, COR-03, COR-04, KEY-01, KEY-02, KEY-03, KEY-04, KEY-05, XPREV-01, FILL-02]

# Metrics
duration: 25min
completed: 2026-04-21
---

# Phase 16 Plan 01: Fix multi-key FILL(PREV) with inline FUNCTION grouping keys Summary

**Widened `SqlCodeGenerator.generateFill`'s classifier to treat non-aggregate FUNCTION and OPERATION grouping keys as factory keys instead of aggregates, restoring cartesian gap-row emission for multi-key SAMPLE BY FILL with `interval(lo, hi)`, `concat(a, b)`, `cast(x AS STRING)`, and `a || b` group expressions.**

## Performance

- **Duration:** ~25 min
- **Started:** 2026-04-21T23:06 UTC
- **Completed:** 2026-04-21T23:22 UTC
- **Tasks:** 5 (all landed in a single commit per D-05 same-commit rule)
- **Files modified:** 2

## Accomplishments

- Fixed silent cartesian-row drop in multi-key SAMPLE BY FILL(PREV/NULL) with inline FUNCTION/OPERATION grouping keys. Pre-fix: 3 rows for the empirical probe (2 keys x 3 buckets). Post-fix: 6 rows.
- Landed D-05 aggregate-arm `-ea` assertion at the residual arm so future AST-shape drift fails CI under surefire `-ea` before reaching users.
- Added 5 regression tests pinning the cartesian contract across every classifier path that previously misbehaved (interval, concat FUNCTION, concat OPERATION, cast, FILL(NULL)).
- Confirmed cursor-side wiring is unchanged: `SampleByFillRecordCursorFactory.java` not modified. `keyColIndices` is derived from the final `fillModes[]` array; fixing the classifier flows through all downstream tiers automatically.

## Task Commits

All 5 tasks landed in a single commit per the plan's D-05 same-commit rule:

1. **Task 1-5 combined:** `82865efbc0` Fix multi-key FILL(PREV) with function keys

The commit contains:

- D-02 classifier widening in `SqlCodeGenerator.java`
- D-05 aggregate-arm -ea assertion in `SqlCodeGenerator.java`
- 5 new regression tests in `SampleByFillTest.java`

## Files Created/Modified

- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` — generateFill classifier widened (+15 lines); third `continue` branch for non-aggregate FUNCTION/OPERATION grouping keys at :3421-3432 and aggregate-arm `-ea` assertion at :3433-3435.
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` — 5 new `@Test` methods (+140 lines):
    - `testFillNullCastMultiKey` at line 467 (FILL(NULL) representative)
    - `testFillPrevCastMultiKey` at line 1229
    - `testFillPrevConcatMultiKey` at line 1255
    - `testFillPrevConcatOperatorMultiKey` at line 1281
    - `testFillPrevIntervalMultiKey` at line 1815 (immediately after single-key `testFillPrevInterval` at line 1678)

All five methods alphabetically placed per CLAUDE.md member ordering; all use `assertQueryNoLeakCheck(..., "ts", false, false)` per Phase 14 D-15 factory contract.

## Decisions Made

See key-decisions in frontmatter. All D-01..D-06 locked in CONTEXT.md; none reopened during execution.

## Deviations from Plan

None - plan executed exactly as written. Probe-and-freeze protocol captured the actual outputs verbatim for each of the 5 new tests; no expected strings guessed.

## Probe-and-Freeze Canonical Example

`testFillPrevIntervalMultiKey` observed output (from the fixed classifier):

```
ts	k	first
2024-01-01T00:00:00.000000Z	('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')	10.0
2024-01-01T00:00:00.000000Z	('2021-01-01T00:00:00.000Z', '2021-02-01T00:00:00.000Z')	null
2024-01-01T01:00:00.000000Z	('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')	10.0
2024-01-01T01:00:00.000000Z	('2021-01-01T00:00:00.000Z', '2021-02-01T00:00:00.000Z')	null
2024-01-01T02:00:00.000000Z	('2021-01-01T00:00:00.000Z', '2021-02-01T00:00:00.000Z')	20.0
2024-01-01T02:00:00.000000Z	('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')	10.0
```

A2 resolution: `Interval.NULL` renders as the literal `null` string (same as DOUBLE null), NOT as empty text. This is the observed rendering across all 5 tests. Assumption A1 (OrderedMap pass-1 discovery order) confirmed: key A renders first at bucket 00:00, but at bucket 02:00 (where key B is discovered as a new data row) the order flips to "new data row first, prior key forward-filled second" - consistent with `testFillPrevKeyedIndependent:1802-1807`.

## Verification

- `mvn -pl core -Dtest=SampleByFillTest test` - 120 tests pass (115 existing + 5 new).
- `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest,SqlOptimiserTest' test` - 1395 tests pass, 0 failures, 0 errors (2 skipped in ExplainPlanTest are pre-existing).
- Single-key anchor `SampleByFillTest#testFillPrevInterval` still passes unchanged under `-ea` (success criterion #4).
- D-05 aggregate-arm assertion did NOT fire during any of the 1395 cross-suite runs - the residual arm's invariant holds across the entire FILL test surface.

## Cursor-Side Wiring Verification (D-06)

`SampleByFillRecordCursorFactory.java` was NOT modified. The end-to-end flow from RESEARCH.md holds verbatim:

1. Classifier post-fix leaves `factoryColToUserFillIdx[col] == -1` for function-key columns.
2. Both the bare-FILL(PREV) branch at :3454-3465 and the per-column branch at :3513-3528 assign `FILL_KEY` when the mapping is `-1`.
3. `keyColIndices` at :3668-3673 iterates `fillModes` and picks up `FILL_KEY` columns automatically.
4. `SampleByFillCursor`'s `outputColToKeyPos` wiring at :358-362 dispatches them through `keysMapRecord`.

No cursor-side adjustment was needed. RESEARCH.md D-06 verdict confirmed.

## Self-Check: PASSED

Verified:

- Commit `82865efbc0` exists in `git log --oneline --all`.
- Title is 43 characters (≤ 50 per CLAUDE.md), no Conventional Commits prefix.
- Commit diff contains exactly 2 files: `SqlCodeGenerator.java` and `SampleByFillTest.java`.
- `grep -n 'functionParser.getFunctionFactoryCache().isGroupBy(ast.token)' SqlCodeGenerator.java` shows 4 matches total: 2 inside generateFill (D-02 predicate + D-05 assertion) plus 2 pre-existing call sites at :5243 and :5268.
- `grep -n 'generateFill aggregate arm: expected aggregate FUNCTION' SqlCodeGenerator.java` shows exactly one match at :3435.
- `grep -n 'non-aggregate FUNCTION/OPERATION in bottomUpCols must resolve' SqlCodeGenerator.java` shows exactly one match at :3430.
- 5 new `testFill*MultiKey` methods exist and land alphabetically.
- Cross-suite `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest,SqlOptimiserTest' test` passes.
