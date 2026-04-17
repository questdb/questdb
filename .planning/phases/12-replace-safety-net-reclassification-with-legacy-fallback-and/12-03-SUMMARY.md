---
phase: 12-replace-safety-net-reclassification-with-legacy-fallback-and
plan: 03
subsystem: sql
tags: [sample-by, fill-prev, retro-fallback, grammar, optimizer-gate, codegen]

# Dependency graph
requires:
  - phase: 11-hardening-review-findings-fixes-and-missing-test-coverage
    provides: current generateFill shape, prevSourceCols build, safety-net reclassification block to delete, per-column fill-spec build loop
  - phase: 12-replace-safety-net-reclassification-with-legacy-fallback-and
    plan: 01
    provides: FallbackToLegacyException.INSTANCE, QueryModel.stashedSampleByNode field + getter/setter, SqlOptimiser.rewriteSampleBy stash write
provides:
  - SqlCodeGenerator.generateFill throws FallbackToLegacyException.INSTANCE when any PREV source column has an unsupported output type
  - Three catch sites in generateSelectGroupBy: chain-walk + restore stashedSampleByNode + re-dispatch to generateSampleBy
  - D-05 grammar rule - reject PREV(timestamp_col) with positioned SqlException
  - D-06 grammar rule - normalize PREV(self) to FILL_PREV_SELF internally (no exception)
  - D-07 grammar rule - reject PREV chains after per-column fillModes is built
  - D-08 grammar rule - unified malformed PREV rejection covering PREV(func), PREV(a,b), PREV(), PREV($n)
  - D-09 grammar rule - reject type-tag mismatch between PREV source and target column types
  - D-04 documentation comment at the bare-PREV-in-per-column-list dispatch
  - isKeyColumn relocated next to isFastPathPrevSupportedType in the is* cluster
  - SampleByFillRecordCursorFactory import in the alphabetical slot
  - Dates.parseOffset failure now asserts instead of silently dropping
  - anyPrev detection loop removed (redundant - second and third conjuncts force anyPrev=true)
  - Safety-net type-check block at SqlCodeGenerator.java:3497-3512 (pre-change) deleted
affects:
  - 12-04 (testSampleByFillNeedFix now produces correct 3-row output; test assertion needs restoring; assertPlanNoLeakCheck sites need `fill=` attribute updates; retro-fallback tests and 8 grammar tests land)

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Retro-fallback via stash-and-restore: codegen throws a typed exception, caller catches, restores pre-rewrite model state, and re-dispatches to the legacy path"
    - "Chain-walk on nested QueryModel.getStashedSampleByNode() before restore - deterministic even when the stash-owning instance is not the same IQueryModel reference at codegen time"
    - "Nested try/catch placement at each call site so the outer catch (Throwable e) cannot swallow the FallbackToLegacyException signal"
    - "Grammar rules as positioned SqlException at the specific offending AST node position (fillExpr.position for malformed, fillExpr.rhs.position for alias errors, fillValuesExprs[0].position for chains)"

key-files:
  created: []
  modified:
    - core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java

key-decisions:
  - "Chain rejection at fillValuesExprs[0].position (generic FILL-clause position). The simplest ~6-line variant was chosen over the precise-position variant. Chains are rare error paths and 4+ lines of per-column position tracking is not worth the complexity. CONTEXT.md D-07 explicitly flags this as Claude's Discretion; the simplest variant satisfies the rule."
  - "D-05 placed BEFORE D-06 in the cross-column branch: the PREV(ts) reject fires before the self-reference normalization. timestampIndex is the designated timestamp, never a user-facing data column, so the error at rhs.position is more informative than letting it fall through to the type-tag check (which would technically pass because ts has the same TIMESTAMP tag on both source and target, but the cross-column PREV is dead code at runtime)."
  - "D-09 type-tag check placed AFTER D-06 self-normalize continue. Self-reference never has a tag mismatch (source == target column), so the check only runs for genuine cross-column references. Putting it before D-06 would mean redundant work for the PREV(self) normalization path."
  - "Redundant import of io.questdb.griffin.FallbackToLegacyException added. Java does not require same-package imports, but the plan acceptance criterion specifies exactly one line matching that pattern. Adding the import also makes the cross-file dependency visible to code-review."
  - "Chain-walk deterministic two-step action: walk always runs, assert fires under -ea if no QueryModel in the chain owns a stash. The open question Q1 from RESEARCH.md asked whether `model` at generateSelectGroupBy is the same IQueryModel reference as `nested` at rewriteSampleBy. The chain walk sidesteps the question - it works regardless of identity, and the assert catches the no-stash case as a genuine bug."

patterns-established:
  - "Retro-fallback pattern: stash original state in optimizer, throw typed exception from codegen on unsupported residue, catch at call site, restore, re-dispatch"
  - "Grammar rule enforcement as positioned SqlException at the specific offending AST node, with the error message identifying the concrete violation"
  - "Chain-walk for nested-model state lookup: always deterministic, assert on failure"

requirements-completed: [PTSF-04, COR-04, CONTEXT-item-3, CONTEXT-item-6, CONTEXT-item-14, CONTEXT-item-17, CONTEXT-item-18, CONTEXT-item-21, D-01, D-03, D-04, D-05, D-06, D-07, D-08, D-09]

# Metrics
duration: 22min
completed: 2026-04-17
---

# Phase 12 Plan 03: Safety-net removal, retro-fallback, grammar rules, housekeeping Summary

**Deleted the codegen safety-net reclassification at SqlCodeGenerator.java:3497-3512 (pre-change) and replaced it with a FallbackToLegacyException throw; wired catches at the three generateFill call sites in generateSelectGroupBy with chain-walk + stashed SAMPLE BY restoration + re-dispatch to generateSampleBy; enforced grammar rules D-05 through D-09 inside generateFill; removed the redundant anyPrev loop; relocated isKeyColumn alphabetically; moved the SampleByFillRecordCursorFactory import to its proper slot; converted Dates.parseOffset silent-drop into an assert.**

## Performance

- **Duration:** ~22 min
- **Started:** 2026-04-17T10:50:46Z (plan 12-02 completion)
- **Completed:** 2026-04-17T11:12:25Z
- **Tasks:** 3
- **Files modified:** 1

## Accomplishments

- The codegen safety-net at SqlCodeGenerator.java:3497-3512 (pre-change, now deleted) no longer silently rewrites genuine aggregates as FILL_KEY. Unsupported-type PREV aggregates now drop to the legacy SAMPLE BY cursor path and produce correct results.
- `generateFill` throws `FallbackToLegacyException.INSTANCE` on the first PREV source column whose resolved output type is outside `isFastPathPrevSupportedType`. The existing `catch (Throwable e)` block at the tail of generateFill frees all partial allocations.
- Three call sites in `generateSelectGroupBy` (line 8027 GroupByRecordCursorFactory, line 8215 AsyncGroupByRecordCursorFactory, line 8289 GroupByRecordCursorFactory) wrap `generateFill` in a nested try/catch for `FallbackToLegacyException`. Each catch walks the nested-model chain, restores the stashed SAMPLE BY node via `setSampleBy`, clears the stash, and calls `generateSampleBy(model, executionContext, stashed, model.getSampleByUnit())`.
- Grammar rules D-05 through D-09 now reject malformed FILL(PREV) shapes at compile time with positioned SqlException. Accepted shapes (bare PREV, PREV(col) with matching type, PREV(key_col)) still work.
- Redundant `anyPrev` detection loop removed. `isKeyColumn` relocated to the alphabetical slot in the `is*` cluster (line 1230). `SampleByFillRecordCursorFactory` import moved to its proper alphabetical slot. `Dates.parseOffset` failure now raises `AssertionError` under `-ea` instead of silently dropping.
- D-04 inline documentation comment added at the bare-PREV-in-per-column-list dispatch.

## Task Commits

Each task was committed atomically:

1. **Task 1: Grammar rules D-05..D-09 + anyPrev loop removal** - `39d1687611` (feat)
2. **Task 2: Safety-net removal, retro-fallback throw, catches at three call sites** - `c1aa576f24` (feat)
3. **Task 3: isKeyColumn relocation, SampleByFillRecordCursorFactory import slot, Dates.parseOffset assert** - `2d7584588d` (refactor)

## Files Created/Modified

- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` - All three tasks landed in this single file per the plan's deliberate single-file scope.

## generateSampleBy Signature Used at the Catch Sites

Grepped verbatim from `SqlCodeGenerator.java:7049-7054`:

```java
@NotNull
private RecordCursorFactory generateSampleBy(
        IQueryModel model,
        SqlExecutionContext executionContext,
        ExpressionNode sampleByNode,
        ExpressionNode sampleByUnits
) throws SqlException
```

The three catches all invoke it as `generateSampleBy(model, executionContext, stashed, model.getSampleByUnit())`, matching the parameter order exactly. No signature change to `generateSampleBy` itself.

## Assert-Firing Audit

- **`assert stashed != null` at each of the three catch sites** - did not fire during `mvn -pl core -Dtest=SampleByFillTest test` (52 tests) or `mvn -pl core -Dtest=SampleByTest test` (302 tests). The chain walk correctly locates the stashed SAMPLE BY node every time the fallback path fires. No need to extend the implementation beyond the unconditional two-step action.
- **`assert parsed != Numbers.LONG_NULL` at SqlCodeGenerator.java:3398** - did not fire during the SampleByFillTest run. The premise holds: `rewriteSampleBy` validates the offset string before stash. The plan's contingency to flip to `SqlException.$(fillOffsetNode.position, ...)` is not needed for plan 12-03's scope; plan 12-04's full-suite verification run may expose a case, in which case the flip can happen there.

## Decisions Made

- **Chain-walk default.** Applied the unconditional two-step action from the plan literally. The walk always runs, and the assert catches the no-stash case. Even if the first iteration of the while loop returns immediately (model itself owns the stash), the cost is one null check - immaterial for a rare path.
- **Order of grammar rule checks.** D-08 (malformed shape) fires FIRST inside the PREV branch; D-05 (PREV(ts) reject) fires in the cross-column sub-branch BEFORE D-06 (self-normalize); D-09 (type-tag check) fires AFTER D-06 so self-reference (which always has a matching type tag) never triggers D-9's error. D-07 (chain) runs in a post-pass outside the per-column loop because it needs the fully-built fillModes array.
- **Import ordering cost.** Added `import io.questdb.griffin.FallbackToLegacyException` even though it is redundant (same package). Java compiles cleanly with the redundant import. The acceptance criterion explicitly requires it, and making the dependency visible in the import block is a mild net positive for code review.

## Deviations from Plan

None - all three tasks executed exactly as the plan specified. Minor tactical notes:

- The `isKeyColumn` relocation ended up at line 1230 (between `isHorizonOffsetModel` at 1218 and `isSingleColumnFunction` at the new line 1244). The plan's estimate of "around line 1199" was close; the exact position is alphabetical per CLAUDE.md's ordering rule.
- The chain-walk pattern is duplicated at each of three catch sites as specified. Extracting a private helper (e.g. `restoreAndReDispatchSampleBy(model, executionContext)`) would reduce duplication, but the plan specifies inline placement and the duplication is 12 lines at three sites. Factoring out is deferred; the plan's scope is completed as written.

## Issues Encountered

- **Explain-plan-text assertion failures.** As flagged in plan 12-02's summary, tests that assert on explain plan text for SAMPLE BY FILL queries fail because plan 12-02 added an unconditional `fill=null|prev|value` attribute. These failures include:
  - `SampleByFillTest.testExplainFillRange` (already observed in plan 12-02)
  - `SampleByTest.testSampleFillValueNotKeyedAlignToCalendarTimeZoneOffset`
  - `SampleByTest.testSampleByDisallowsPredicatePushdown`
  - `SampleByTest.testSampleFillValueNotKeyedAlignToCalendarTimeZoneOffsetNepal`
  - `SampleByTest.testSampleFillNullNotKeyedValid`
  - `SampleByTest.testSampleFillValueNotKeyedAlignToCalendarTimeZone2`

  These are refreshed in plan 12-04 per the phase plan. They are NOT regressions introduced by plan 12-03.

- **`testSampleByFillNeedFix` still fails.** The test expects 6 rows (PR's broken assertion), but with the retro-fallback applied, the legacy path correctly produces 3 rows. Plan 12-04 restores the 3-row expected output. This is the intended behavior change of plan 12-03.

## User Setup Required

None.

## Known Stubs

None - all changes are real functional code. No placeholder returns, no mocked-out branches.

## Failing Tests After Plan 12-03 (Expected Set)

- `SampleByTest#testSampleByFillNeedFix` - PR's 6-row assertion, legacy path produces correct 3-row output. Plan 12-04 restores master's 3-row expected output.
- `SampleByFillTest#testExplainFillRange` - plan text changed because plan 12-02 added unconditional `fill=` attribute. Plan 12-04 refreshes.
- `SampleByTest#testSampleFillValueNotKeyedAlignToCalendarTimeZoneOffset` - same as above (explain plan text).
- `SampleByTest#testSampleByDisallowsPredicatePushdown` - same.
- `SampleByTest#testSampleFillValueNotKeyedAlignToCalendarTimeZoneOffsetNepal` - same.
- `SampleByTest#testSampleFillNullNotKeyedValid` - same.
- `SampleByTest#testSampleFillValueNotKeyedAlignToCalendarTimeZone2` - same.

Total: 7 tests (1 data correctness, 6 plan-text). All refreshed in plan 12-04.

## Total Line-Count Change in SqlCodeGenerator.java

Task 1: +40/-11 (grammar rules + anyPrev removal)
Task 2: +130/-86 (safety-net to retro-fallback + three catches)
Task 3: +21/-21 (housekeeping - pure reorder + assert conversion)

**Cumulative: +191/-118 = +73 net lines.** The addition comes primarily from the three catches in Task 2 (12 lines each = 36 lines) and the grammar rule enforcement in Task 1 (~30 lines for D-05..D-09 inline in the per-column build loop plus ~6 lines for the post-pass chain check).

## Self-Check: PASSED

- FOUND: core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java (modified)
- FOUND: 39d1687611 (Task 1)
- FOUND: c1aa576f24 (Task 2)
- FOUND: 2d7584588d (Task 3)
- VERIFIED: `grep -c "catch (FallbackToLegacyException"` returns 3
- VERIFIED: `grep "Safety-net type check"` returns 0
- VERIFIED: `grep "throw FallbackToLegacyException.INSTANCE"` returns 1
- VERIFIED: `grep "import io.questdb.griffin.FallbackToLegacyException"` returns 1
- VERIFIED: `grep "PREV argument must be a single column name"` returns 1
- VERIFIED: `grep "PREV cannot reference the designated timestamp column"` returns 1
- VERIFIED: `grep "FILL(PREV) chains are not supported"` returns 1
- VERIFIED: `grep "cannot fill target column of type"` returns 1
- VERIFIED: `grep "srcColIdx == col"` returns 1 in generateFill context
- VERIFIED: `grep "anyPrev"` returns 0
- VERIFIED: `grep "D-04"` returns 1 near line 3431
- VERIFIED: `grep "private static boolean isKeyColumn"` returns 1 at line 1230 (in the is* cluster)
- VERIFIED: SampleByFill* imports are in sorted order (LC_ALL=C sort -c PASSES)
- VERIFIED: `grep "assert parsed != Numbers.LONG_NULL"` returns 1 at line 3398
- VERIFIED: `grep "if (parsed != Numbers.LONG_NULL)"` returns 0
- VERIFIED: `mvn -pl core compile` exits 0
- VERIFIED: `mvn -pl core -Dtest=SampleByFillTest test` reports 52 run, 1 failure (expected testExplainFillRange from plan 12-02)
- VERIFIED: No `// ===` or `// ---` banner comments added

---

*Phase: 12-replace-safety-net-reclassification-with-legacy-fallback-and*
*Completed: 2026-04-17*
