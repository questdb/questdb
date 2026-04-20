---
status: all_fixed
phase: 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi
findings_in_scope: 1
fixed: 1
skipped: 0
iteration: 1
scope: critical_warning
---

# Phase 13 Code Review Fix Report

**Fixed at:** 2026-04-20T01:55:38+01:00
**Source review:** .planning/phases/13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi/13-REVIEW.md
**Iteration:** 1

**Summary:**
- Findings in scope: 1
- Fixed: 1
- Skipped: 0
- Scope: critical_warning (Info findings IN-01, IN-02, IN-03 intentionally excluded)

## Fixed Issues

### WR-01: Defect 3 grammar check misses single non-null constant broadcast

**Files modified:**
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java`
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java`
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java`
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java`

**Commit:** 95349b4d4d

**Applied fix:**

1. Widened the Defect 3 grammar guard in `SqlCodeGenerator.java` (previously at
   line 3568). The old condition `fillValuesExprs.size() > 1 &&
   fillValuesExprs.size() < aggNonKeyCount` skipped the `size() == 1` case
   entirely, letting a single non-null constant (e.g. `FILL(0)`) silently pad
   the tail aggregates with NULL. The new condition drops the `size() > 1`
   gate and explicitly carves out the intentional broadcast forms:

   ```java
   if (fillValuesExprs.size() < aggNonKeyCount) {
       final ExpressionNode only = fillValuesExprs.size() == 1 ? fillValuesExprs.getQuick(0) : null;
       final boolean isBroadcastable = only != null
               && (isPrevKeyword(only.token) || isNullKeyword(only.token));
       if (!isBroadcastable) {
           throw SqlException.$(fillValuesExprs.getQuick(0).position, "not enough fill values");
       }
   }
   ```

   Bare `FILL(PREV)` and `FILL(NULL)` continue to broadcast across all
   aggregates; `FILL(0)`, `FILL(42)`, and similar single non-null constants
   now raise `not enough fill values` at the first fill expression position
   (matching master's legacy `SampleByFillValueRecordCursorFactory` behavior).

2. Added `testFillInsufficientFillValuesSingleConstant` in `SampleByFillTest`
   at the alphabetical position immediately after the existing
   `testFillInsufficientFillValues`. The test covers `FILL(0)` with two
   aggregates (`sum(a)`, `sum(b)`) and asserts the positioned `SqlException`
   with the calibrated offset (51, pointing at the `0` token — the first and
   only fill expression).

3. Updated two tests that incidentally depended on the pre-fix broadcast
   behavior of a single non-null constant across multiple aggregates:

   - `SampleByTest.testSampleByFromToIsDisallowedForKeyedQueries`: changed
     `fill(42)` to `fill(42, 42, 42)` to match the three aggregates
     (`avg(x)`, `first(x)`, `last(x)`).
   - `SampleByNanoTimestampTest.testSampleByFromToIsDisallowedForKeyedQueries`:
     same change for its two inner queries.

   Both tests are smoke tests (`printSql` without output assertions); their
   intent was to confirm the queries execute, not to exercise the
   broadcast-one-constant-to-many-aggregates shape.

**Verification:**

- `mvn -pl core -DskipTests -P local-client package 2>&1 | tail -5`:
  BUILD SUCCESS.
- `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest,RecordCursorMemoryUsageTest' test`:
  BUILD SUCCESS, Tests run: 1194, Failures: 0, Errors: 0, Skipped: 2 (pre-existing skips).
- New test `testFillInsufficientFillValuesSingleConstant` passes.
- Existing bare `FILL(PREV)` and `FILL(NULL)` single-element broadcast tests
  (e.g. `testFillNullMultipleAggregates`, `testFillPrevAllNumericTypes`,
  `testFillPrevMixedWithSymbol`, `testFillNullKeyedFromToMultipleAggregates`)
  continue to pass.

**Commit title note:** Per project CLAUDE.md, commit titles do NOT use
Conventional Commits prefixes. Used plain English title
`Reject FILL with fewer values than aggregates for constants` rather than
the `fix(13):` form suggested by the GSD template.

## Skipped Issues

None.

## Out of Scope (not addressed)

The following Info findings were intentionally not addressed under the
`critical_warning` scope:

- IN-01: dead import `FillRangeRecordCursorFactory` in `SqlCodeGenerator.java`.
- IN-02: redundant `fillIdx >= 0` checks in per-column fill loop.
- IN-03: non-ASCII em dashes in Javadoc / `//` comments in
  `SampleByFillRecordCursorFactory.java` and `SqlCodeGenerator.java`
  (project rule text scopes ASCII-only to LOG/error messages).

---

_Fixed: 2026-04-20T01:55:38+01:00_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
