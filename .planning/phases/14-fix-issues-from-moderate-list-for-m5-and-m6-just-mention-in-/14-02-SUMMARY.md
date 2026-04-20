---
phase: 14-fix-issues-from-moderate-list-for-m5-and-m6-just-mention-in-
plan: 02
subsystem: sql
tags: [sample-by, fill, prev, cursor, fillrecord, array, binary, interval, java, questdb]

# Dependency graph
requires:
  - phase: 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi
    provides: FillRecord 4-branch dispatch order, prevRecord/keysMapRecord plumbing, rowId-based PREV snapshots
  - phase: 12-replace-safety-net-reclassification-with-legacy-fallback-and
    provides: FillRecord typed getter skeletons, FQN import sweep, FILL_KEY dispatch for 128/256-bit types
  - phase: 14-01
    provides: Codegen cluster fixes (M-1, M-3, M-9, Mn-13), factoryColToUserFillIdx hoisted, 6 codegen regression tests
provides:
  - "M-2 fix: FillRecord.getArray, getBin, getBinLen each honor the full 4-branch dispatch (non-gap -> FILL_KEY -> cross-col-PREV-to-key -> FILL_PREV_SELF/cross-col-PREV -> FILL_CONSTANT -> default null). Keyed SAMPLE BY FILL queries with ARRAY or BINARY key columns now carry the key through gap rows."
  - "M-8 fix: FillRecord.getInterval override with Interval.NULL default sentinel, inserted alphabetically between getInt and getLong. Queries that project an INTERVAL column through SAMPLE BY FILL no longer crash with UnsupportedOperationException."
  - "FillRecord audit close-out: Javadoc documents that getRecord, getRowId, getUpdateRowId are intentionally not overridden (plumbing, never a valid SAMPLE BY output column)."
  - "11 new @Test methods in SampleByFillTest pinning Tasks 1 and 2: 9 per-type non-keyed FILL(PREV) (BOOLEAN/BYTE/SHORT/INT/LONG/DATE/TIMESTAMP/IPv4/INTERVAL), plus keyed ARRAY and keyed BINARY tests for M-2."
  - "Pre-existing testSampleFillPrevAllTypes and testSampleFillValueAllKeyTypes (SampleByTest + SampleByNanoTimestampTest) expected outputs updated to reflect the corrected post-M-2 BINARY rendering."
affects: [14-03, 14-04]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "4-branch FillRecord dispatch is now complete across all value-returning Record getters (the three plumbing getters getRecord/getRowId/getUpdateRowId remain unoverridden by design; documented in Javadoc)."
    - "INTERVAL is not a persistable DDL type, so a SAMPLE BY FILL test that exercises getInterval must materialize INTERVAL via an inline interval(lo, hi) key expression over stored TIMESTAMP columns."
    - "BINARY has no first(BINARY) aggregate, so non-keyed BINARY FILL(PREV) is infeasible. The only regression path for FillRecord.getBin/getBinLen is a keyed SAMPLE BY where BINARY is the key column."

key-files:
  created: []
  modified:
    - "core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java"
    - "core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java"
    - "core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java"
    - "core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java"

key-decisions:
  - "Keyed SAMPLE BY BINARY tests use a cursor-walk structural assertion (getBin != null, getBinLen > 0) on every row rather than pinning exact hex output. Rationale: rnd_bin output is seed-Rnd deterministic in isolation but subtle harness wiring makes locking exact hex in assertSql fragile, whereas the non-null-on-fill-rows property is precisely the M-2 regression signature."
  - "testFillPrevInterval uses interval(lo, hi) over two TIMESTAMP columns as the GROUP BY key (not a stored INTERVAL column) because INTERVAL is in the non-persisted-types set (ColumnType.java:1137) and QuestDB has no first(INTERVAL) aggregate."
  - "Non-keyed BINARY FILL(PREV) dropped from the D-14 per-type set. No first(BINARY) aggregate exists in core/src/main/java/io/questdb/griffin/engine/functions/groupby/ (verified by listing First*Factory files). BINARY coverage comes solely from testFillPrevKeyedBinary."
  - "testSampleFillPrevAllTypes and testSampleFillValueAllKeyTypes expected outputs updated in place. These existing tests' expected outputs were pinning the pre-fix (M-2 buggy) rendering where BINARY key columns emit empty on fill rows. The M-2 fix changes the correct output to carry the key bytes; updating the expectations is a Rule 1 deviation (fix the buggy assertion that the production fix reveals)."

patterns-established:
  - "FillRecord is now audit-complete for value-returning Record methods: every getter except the three plumbing methods (getRecord, getRowId, getUpdateRowId) has a full 4-branch dispatch. The Javadoc at the FillRecord header documents the scope boundary so future readers know why the plumbing methods use the default UOE."
  - "A SAMPLE BY FILL test that exercises an INTERVAL output column must compute the INTERVAL inline via interval(lo, hi) and treat it as a key (not an aggregate), since there is no first(INTERVAL) aggregate and INTERVAL is non-persistable."

requirements-completed: []

# Metrics
duration: 39min
completed: 2026-04-20
---

# Phase 14 Plan 02: Complete FillRecord Dispatch for ARRAY/BINARY/INTERVAL Summary

**Full 4-branch dispatch for ARRAY/BIN/BinLen getters, new Interval getter with Interval.NULL default, and 11 new regression tests in SampleByFillTest plus four updated expected outputs in SampleByTest / SampleByNanoTimestampTest all-types tests.**

## Performance

- **Duration:** 39 min
- **Started:** 2026-04-20T14:04:06Z
- **Completed:** 2026-04-20T14:43:28Z
- **Tasks:** 3
- **Files modified:** 4 (1 production, 3 test)

## Accomplishments

- M-2 fix: FillRecord.getArray, getBin, getBinLen each adopt the exact Phase 13 4-branch dispatch order. A keyed SAMPLE BY FILL(PREV/NULL) with an ARRAY or BINARY key column now reads the carried key value from keysMapRecord instead of falling through to null (ArrayView/BinarySequence) or -1 (long BinLen). The cross-column-PREV-to-key branch for those three types is also now live.
- M-8 fix: FillRecord.getInterval is added with the same 4-branch shape and Interval.NULL as the default sentinel. Queries that project an INTERVAL output column through SAMPLE BY FILL previously threw UnsupportedOperationException because FillRecord inherited Record's default throw body; they now return the carried value or Interval.NULL.
- FillRecord audit close-out: the inner-class Javadoc documents that getRecord, getRowId, and getUpdateRowId are intentionally not overridden. The default UnsupportedOperationException is the desired signal if an upstream change ever routes one of these plumbing methods through the fill path.
- 11 new @Test methods in SampleByFillTest land per-type non-keyed FILL(PREV) tests for BOOLEAN/BYTE/SHORT/INT/LONG/DATE/TIMESTAMP/IPv4/INTERVAL (9), plus testFillPrevKeyedArray (M-2 ARRAY-key regression via assertSql and the [1.0,2.0] rendering) and testFillPrevKeyedBinary (M-2 BINARY-key regression via a cursor-walk non-null assertion on every row).
- 4 pre-existing SAMPLE BY all-types tests updated (testSampleFillPrevAllTypes in SampleByTest and SampleByNanoTimestampTest; testSampleFillValueAllKeyTypes in the same pair): the expected outputs now show the BINARY key column populated on fill rows, matching the corrected post-M-2 behavior. Their comments documenting the pre-fix "null on fill rows" behavior are replaced with comments describing the post-fix behavior and referencing this plan.

## Task Commits

Each task was committed atomically:

1. **Task 1: M-2 add FILL_KEY + cross-column-PREV-to-key branches to getArray/getBin/getBinLen** - `ea4cf3704b` (fix)
2. **Task 2: M-8 add FillRecord.getInterval + Record audit close-out** - `005e6e74a0` (feat)
3. **Task 3: Per-type and keyed FILL(PREV) regression tests + all-types expected-output updates** - `eb22036989` (test)

## Files Created/Modified

- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` - Three getters (getArray, getBin, getBinLen) extended to the full 4-branch dispatch with FILL_KEY + cross-column-PREV-to-key branches. New getInterval method added alphabetically between getInt and getLong. Javadoc on FillRecord updated to document the three intentionally-unoverridden plumbing methods. One new import: `io.questdb.std.Interval`.
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` - 11 new @Test methods (testFillPrevBoolean, testFillPrevByte, testFillPrevDate, testFillPrevIPv4, testFillPrevInt, testFillPrevInterval, testFillPrevKeyedArray, testFillPrevKeyedBinary, testFillPrevLong, testFillPrevShort, testFillPrevTimestamp). Four new test imports for the cursor-walk BINARY test: `io.questdb.cairo.sql.Record`, `RecordCursor`, `RecordCursorFactory`, `io.questdb.std.BinarySequence`.
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java` - Updated expected outputs in testSampleFillPrevAllTypes and testSampleFillValueAllKeyTypes (BINARY column `m` / `i` now populated on fill rows); updated inline comments from pre-fix "emits null on fill rows" to post-fix "carries the key bytes through keysMapRecord".
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java` - Same expected-output + comment update as SampleByTest for the nano-precision twins.

## Production changes (with line numbers as landed)

All line numbers are after the edits.

- `SampleByFillRecordCursorFactory.java:59` - new `import io.questdb.std.Interval;` inserted in the std-imports alphabetical block.
- `SampleByFillRecordCursorFactory.java:691-699` - Javadoc on FillRecord extended with a new paragraph documenting the three intentionally-unoverridden Record methods (getRecord, getRowId, getUpdateRowId).
- `SampleByFillRecordCursorFactory.java:704-715` - getArray now has the full 4-branch dispatch (FILL_KEY line 708, cross-col-PREV-to-key lines 709-710, FILL_PREV_SELF/cross-col-PREV lines 711-712, FILL_CONSTANT line 713, default null line 714).
- `SampleByFillRecordCursorFactory.java:717-728` - getBin same shape.
- `SampleByFillRecordCursorFactory.java:730-741` - getBinLen same shape.
- `SampleByFillRecordCursorFactory.java:986-997` - new getInterval method with 4-branch dispatch and `return Interval.NULL;` at the tail, placed between getInt (ends line 984) and getLong (starts line 1000).

## Test results

Full SAMPLE BY trio (SampleByFillTest + SampleByTest + SampleByNanoTimestampTest): **680 tests pass, 0 failures, 0 errors.**

Breakdown:
- SampleByFillTest: 89 existing + 11 new = 100 tests
- SampleByTest: 302 tests (2 tests have updated expected outputs; no count change)
- SampleByNanoTimestampTest: 278 tests (2 tests have updated expected outputs; no count change)

**Regression-coverage self-check:**
- Temporarily removed the FILL_KEY and cross-col-PREV-to-key branches from getArray, getBin, getBinLen. testFillPrevKeyedArray and testFillPrevKeyedBinary both failed. Restored Task 1 and both passed.
- Temporarily removed the getInterval override. testFillPrevInterval crashed with UnsupportedOperationException. Restored Task 2 and the test passed.

Both self-checks prove the new tests exercise the actual fix paths rather than coincidentally passing.

## INTERVAL literal syntax used in testFillPrevInterval

`interval(lo, hi)` as a SELECT-list expression, where `lo` and `hi` are stored TIMESTAMP columns. The table DDL stores TIMESTAMPs, not INTERVALs, because INTERVAL is in ColumnType.nonPersistedTypes (lines 1137 in core/src/main/java/io/questdb/cairo/ColumnType.java). Rendering of the INTERVAL value is `('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')` - matching the harness precedent in IntervalFunctionTest.java:85.

## BINARY insertion mechanism used in testFillPrevKeyedBinary

Cursor-walk with a structural non-null assertion. The test creates a table via `CREATE TABLE x AS (SELECT rnd_bin(4,8,0) AS k, ...)`, then inserts a second row with the same key via `INSERT INTO x SELECT k, ... FROM x LIMIT 1`, and walks the cursor of the `SAMPLE BY 1h FILL(PREV)` query asserting `record.getBin(1) != null` and `record.getBinLen(1) > 0` on all 4 rows. The expected output deliberately does not pin exact hex bytes; the regression signal is precisely the "non-null on fill rows" property, which was previously null under the M-2 bug.

## Decisions Made

1. **Keyed BINARY test uses cursor-walk, not assertSql.** The plan explicitly allowed either approach; I chose the cursor-walk for robustness against rnd_bin seed-dependence. Any deterministic hex-literal insert approach would couple the test to rendering minutiae that are orthogonal to the FILL_KEY regression signal.
2. **testFillPrevInterval uses interval() as a computed key.** Per-type D-14 list named INTERVAL as a non-keyed FILL(PREV) test. INTERVAL turned out to be non-persistable AND has no first(INTERVAL) aggregate, so the non-keyed shape is infeasible. The inline interval(lo, hi) key shape still exercises the new FillRecord.getInterval via the FILL_KEY branch, which is itself the crash-path pre-Task-2 fix.
3. **No helper extraction for the 4-branch pattern.** CONTEXT allowed this under Claude's Discretion (14-RESEARCH.md Open Question 2). The recommendation was KEEP the inlined pattern for readability continuity with the existing ~35 getters, and this plan follows it.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] testFillPrevInterval DDL needed to be reshaped.**
- **Found during:** Task 3 first test run
- **Issue:** The plan's initial shape used `CREATE TABLE t (k INTERVAL, v DOUBLE, ts TIMESTAMP)`. INTERVAL is a non-persisted type (`ColumnType.java:1137: nonPersistedTypes.add(INTERVAL)`), so the DDL failed with "non-persisted type: INTERVAL".
- **Fix:** Reshaped the test to store two TIMESTAMP columns (lo, hi) and compute the INTERVAL inline via `interval(lo, hi)` as a GROUP BY key expression. This still exercises the new FillRecord.getInterval under the FILL_KEY branch, which is the M-8 crash path pre-Task-2.
- **Files modified:** `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java`
- **Verification:** testFillPrevInterval now passes; self-check by temporarily removing Task 2's getInterval override confirmed the test crashes with UnsupportedOperationException, so it correctly exercises the fix.
- **Committed in:** `eb22036989` (Task 3)

**2. [Rule 1 - Bug] Pre-existing testSampleFillPrevAllTypes / testSampleFillValueAllKeyTypes were pinning the M-2 buggy rendering.**
- **Found during:** Plan-complete trio test run after Task 3
- **Issue:** These 4 pre-existing tests (2 in SampleByTest, 2 in SampleByNanoTimestampTest) asserted BINARY key columns render empty on fill rows. That is exactly the M-2 bug: pre-fix, FillRecord.getBin/getBinLen had no FILL_KEY branch. Task 1's fix now carries the key bytes forward; these 4 tests failed with the correct new output.
- **Fix:** Updated the expected output in each of the 4 tests to show the BINARY key column populated on fill rows (matching the data row where each key first appears). Replaced the inline comment `"For non-numeric key types (BIN), the fill cursor emits null on fill rows"` with a comment describing the M-2 fix and pointing at this plan.
- **Files modified:** `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java`, `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java`
- **Verification:** All four tests now pass; the trio runs 680/0/0.
- **Committed in:** `eb22036989` (Task 3)

---

**Total deviations:** 2 auto-fixed (both Rule 1 bug fixes in test assertions/DDL that the production fix revealed)
**Impact on plan:** Both deviations are necessary to land the production fixes without stranded failing tests. The plan's scope - lock in M-2 and M-8 behavior with new regression tests - is honored. The 4 pre-existing all-types tests had been implicitly reinforcing the M-2 bug; their expected outputs had to be updated in lockstep with Task 1.

## Issues Encountered

1. **Capturing the actual output for testSampleFillPrev/Value assertion updates was tricky.** Maven's surefire log embeds the "but was:" assertion block inline with thread dump noise, and the BINARY rendering has multi-line rows (a single BINARY value spans two text lines when length > 16). I ended up constructing the corrected expected outputs by hand from the data-row BINARY values (which are unchanged), substituting each fill-row empty BINARY with the data value of the key that first appeared. The correctness check was the assertEquals PASS on the trio run.
2. **No existing helper on ColumnType to distinguish INTERVAL as a valid DDL type.** Tried `CREATE TABLE t (k INTERVAL, ...)` and got "non-persisted type" error; had to switch to inline `interval(lo, hi)` computed key. The error message pointed directly at SqlUtil.toPersistedType, which was helpful.

## User Setup Required

None - no external service configuration required.

## Handoff to Plans 03 and 04

**Test insertion slots taken in `SampleByFillTest.java`:**
- `testFillPrevBoolean` (between `testFillPrevArrayDouble1D` and `testFillPrevByte`)
- `testFillPrevByte` (between `testFillPrevBoolean` and `testFillPrevCrossColumnArrayDimsMismatch`)
- `testFillPrevDate` (between `testFillPrevCrossColumnNonKeyed` and `testFillPrevDecimal128`)
- `testFillPrevIPv4` (before `testFillPrevInt`, after `testFillPrevGeoNoPrevYet`)
- `testFillPrevInt` (between `testFillPrevIPv4` and `testFillPrevInterval`)
- `testFillPrevInterval` (between `testFillPrevInt` and `testFillPrevKeyedArray`)
- `testFillPrevKeyedArray` (before `testFillPrevKeyedBinary`, after `testFillPrevInterval`)
- `testFillPrevKeyedBinary` (between `testFillPrevKeyedArray` and existing `testFillPrevKeyedIndependent`)
- `testFillPrevLong` (before `testFillPrevLong128Fallback`)
- `testFillPrevShort` (between `testFillPrevSelfAlias` and `testFillPrevStringKeyed`)
- `testFillPrevTimestamp` (between `testFillPrevSymbolNull` and `testFillPrevUuidKeyed`)

**BINARY coverage handled as follows:**
- Non-keyed BINARY FILL(PREV) dropped from D-14 - no `first(BINARY)` aggregate exists in QuestDB; infeasible.
- Keyed BINARY FILL(PREV) covered by `testFillPrevKeyedBinary` via cursor-walk (non-null assertion on every row). This is the only route to exercise FillRecord.getBin/getBinLen under FILL_KEY.

**Codebase state:**
- FillRecord is now audit-complete for value-returning Record getters. Plan 03 / Plan 04 should not need to add any new getter overrides.
- The 4-branch dispatch order is preserved exactly (Phase 13 invariant). Future changes must keep FILL_KEY first, then cross-col-PREV-to-key, then FILL_PREV_SELF/cross-col-PREV, then FILL_CONSTANT, then default null.
- `Interval.NULL` is the canonical null sentinel for INTERVAL outputs.

**Regression test suite:**
- 680 tests pass after Plan 14-02 (89+11=100 in SampleByFillTest, 302 in SampleByTest, 278 in SampleByNanoTimestampTest). 0 failures, 0 errors.
- Plan 03 (M-4 - SqlOptimiser.java sub-day + TIME ZONE + FROM) and Plan 04 (M-7 SortedRecordCursorFactory double-free) should preserve this count and add their own regression tests per D-04.

## Self-Check: PASSED

**Created files (none for Plan 14-02 - all edits modify existing files)**

**Modified files verified:**
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` - FOUND (contains `public Interval getInterval(int col)`, 4-branch dispatch for getArray/getBin/getBinLen with FILL_KEY first branch, no `Plan 02 scope` comment)
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` - FOUND (contains all 11 new @Test methods)
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java` - FOUND (testSampleFillPrevAllTypes + testSampleFillValueAllKeyTypes updated)
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java` - FOUND (nano twin tests updated)

**Commits verified:**
- `ea4cf3704b` - FOUND (Task 1)
- `005e6e74a0` - FOUND (Task 2)
- `eb22036989` - FOUND (Task 3)

---

*Phase: 14-fix-issues-from-moderate-list-for-m5-and-m6-just-mention-in-*
*Completed: 2026-04-20*
