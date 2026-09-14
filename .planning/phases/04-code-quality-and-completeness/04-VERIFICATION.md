---
phase: 04-code-quality-and-completeness
verified: 2026-04-13T16:27:53Z
status: human_needed
score: 5/5 must-haves verified
overrides_applied: 0
human_verification:
  - test: "Run EXPLAIN SELECT percentile_disc(value, 0.5) FROM (SELECT 1.0 AS value) GROUP BY value and inspect plan output"
    expected: "Plan shows 'percentile_disc(value,0.5)' with the percentile argument — not just 'percentile_disc(value)'"
    why_human: "toPlan() code is correct but verifying EXPLAIN output requires a live QuestDB server"
  - test: "Run SELECT quantile_disc(value, 0.5) OVER () FROM (SELECT CAST(x AS DOUBLE) AS value FROM long_sequence(5)) and confirm non-error result"
    expected: "Returns 5 rows each containing 3.0 (the median of 1..5)"
    why_human: "Requires running a live QuestDB instance; cannot be verified with static analysis alone"
---

# Phase 4: Code Quality and Completeness Verification Report

**Phase Goal:** Duplicated code is consolidated, all factories are registered, aliases exist for window functions, and test coverage uses correct assertion patterns
**Verified:** 2026-04-13T16:27:53Z
**Status:** human_needed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths (Roadmap Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|---------|
| 1 | quickSort/quickSelect exists in exactly one shared utility class (not 8 duplicate copies across factory files) | VERIFIED | `DoubleSort.java` exists at `core/src/main/java/io/questdb/std/DoubleSort.java` (140 lines, DNF three-way partition + insertion sort). Zero `private.*quickSort`, `private.*partition`, `private.*swap` remain in any window factory. 9 `DoubleSort.sort()` call sites confirmed (2 per each of 4 window factories + 1 in `GroupByDoubleList.java`). `GroupByDoubleList.java` reduced from ~580 to 346 lines. |
| 2 | EXPLAIN on any percentile group-by function shows the percentile argument value in the plan output | VERIFIED (code) | All 5 `toPlan()` methods include the percentile argument: `sink.val("percentile_disc(").val(arg).val(',').val(percentileFunc).val(')')`. Applies to `PercentileDiscDoubleGroupByFunction`, `PercentileDiscLongGroupByFunction`, `PercentileContDoubleGroupByFunction`, `MultiPercentileDiscDoubleGroupByFunction`, `MultiPercentileContDoubleGroupByFunction`. Human spot-check needed to confirm live EXPLAIN output. |
| 3 | quantile_disc() and quantile_cont() work as window functions (not just group-by aggregates) | VERIFIED | 4 new window alias factory files created and wired: `QuantileDiscDoubleWindowFunctionFactory`, `QuantileContDoubleWindowFunctionFactory`, `MultiQuantileDiscDoubleWindowFunctionFactory`, `MultiQuantileContDoubleWindowFunctionFactory`. All extend `AbstractWindowFunctionFactory`, return correct signatures (`quantile_disc(DD)`, `quantile_cont(DD)`, `quantile_disc(DD[])`, `quantile_cont(DD[])`), and delegate to canonical `Percentile*` factories via `delegate.newInstance()`. All 4 registered in `function_list.txt` (lines 833–836). Tests `testQuantileDiscAliasEquivalence` and `testQuantileContAliasEquivalence` verify equivalence in window context. |
| 4 | All 10 previously unregistered group-by factories appear in function_list.txt and are callable from SQL | VERIFIED | `function_list.txt` now contains: 5 percentile group-by (`PercentileDiscDoubleGroupByFunctionFactory`, `PercentileDiscLongGroupByFunctionFactory`, `PercentileContDoubleGroupByFunctionFactory`, `MultiPercentileDiscDoubleGroupByFunctionFactory`, `MultiPercentileContDoubleGroupByFunctionFactory`) at lines 813–817, and 5 quantile group-by aliases (`QuantileDiscDoubleGroupByFunctionFactory`, `QuantileDiscLongGroupByFunctionFactory`, `QuantileContDoubleGroupByFunctionFactory`, `MultiQuantileDiscDoubleGroupByFunctionFactory`, `MultiQuantileContDoubleGroupByFunctionFactory`) at lines 820–824. All factory class files exist on disk. |
| 5 | Percentile window tests cover PARTITION BY paths, ORDER BY error rejection, and use assertQueryNoLeakCheck instead of assertSql | VERIFIED | `PercentileWindowFunctionTest.java`: 0 `assertSql()` calls remain, 36 `assertQueryNoLeakCheck()` calls (6-arg overload with `supportsRandomAccess=true, expectSize=true`), 8 `assertException()` calls. PARTITION BY: `testPercentileDiscOverPartition`, `testPercentileContOverPartition`, `testMultiPercentileDiscOverPartition`, `testMultiPercentileContOverPartition` all present. ORDER BY rejection: all 4 function variants covered (`testPercentileDiscRejectsOrderBy`, `testPercentileContRejectsOrderBy`, `testMultiPercentileDiscRejectsOrderBy`, `testMultiPercentileContRejectsOrderBy`). ROWS frame rejection: `testPercentileDiscRejectsNonWholePartitionFrame`, `testPercentileContRejectsNonWholePartitionFrame`, `testMultiPercentileDiscRejectsRowsFrame`, `testMultiPercentileContRejectsRowsFrame`. 41 total test methods. |

**Score:** 5/5 truths verified (code-level)

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|---------|--------|---------|
| `core/src/main/java/io/questdb/std/DoubleSort.java` | Shared off-heap double quicksort utility | VERIFIED | 140 lines, contains `public static void sort(long ptr, long left, long right)`, DNF three-way partition, insertion sort fallback at threshold 47, `medianOfThree`, `getDouble`, `putDouble`, `swap` private methods. No `quickSelect`. |
| `core/src/main/java/io/questdb/griffin/engine/groupby/GroupByDoubleList.java` | Delegates sort to DoubleSort | VERIFIED | Line 198: `DoubleSort.sort(addressOf(0), lo, hi)`. Dual-pivot sort removed. File now 346 lines. |
| `core/src/main/java/io/questdb/griffin/engine/functions/window/QuantileDiscDoubleWindowFunctionFactory.java` | Window alias factory for quantile_disc | VERIFIED | Extends `AbstractWindowFunctionFactory`, signature `"quantile_disc(DD)"`, delegates to `PercentileDiscDoubleWindowFunctionFactory`. |
| `core/src/main/java/io/questdb/griffin/engine/functions/window/QuantileContDoubleWindowFunctionFactory.java` | Window alias factory for quantile_cont | VERIFIED | Extends `AbstractWindowFunctionFactory`, signature `"quantile_cont(DD)"`, delegates to `PercentileContDoubleWindowFunctionFactory`. |
| `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiQuantileDiscDoubleWindowFunctionFactory.java` | Window alias factory for multi quantile_disc | VERIFIED | Extends `AbstractWindowFunctionFactory`, signature `"quantile_disc(DD[])"`, delegates to `MultiPercentileDiscDoubleWindowFunctionFactory`. |
| `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiQuantileContDoubleWindowFunctionFactory.java` | Window alias factory for multi quantile_cont | VERIFIED | Extends `AbstractWindowFunctionFactory`, signature `"quantile_cont(DD[])"`, delegates to `MultiPercentileContDoubleWindowFunctionFactory`. |
| `core/src/main/resources/function_list.txt` | All 14 new factory registrations | VERIFIED | Lines 813–817: 5 percentile group-by factories. Lines 820–824: 5 quantile group-by aliases. Lines 833–836: 4 quantile window aliases. Total: 14 new registrations with proper comment headers. |
| `core/src/test/java/io/questdb/test/griffin/engine/window/PercentileWindowFunctionTest.java` | Complete test coverage for all percentile window functions | VERIFIED | 0 `assertSql()` calls, 36 `assertQueryNoLeakCheck()` calls, 41 test methods. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `PercentileDiscDoubleWindowFunctionFactory.java` | `DoubleSort.sort()` | `import io.questdb.std.DoubleSort;` + 2 call sites | WIRED | Lines confirmed: `import` at line 52, 2 `DoubleSort.sort(...)` calls in Partitioned and WholeResultSet inner classes. |
| `PercentileContDoubleWindowFunctionFactory.java` | `DoubleSort.sort()` | `import io.questdb.std.DoubleSort;` + 2 call sites | WIRED | Same pattern as disc variant. |
| `MultiPercentileDiscDoubleWindowFunctionFactory.java` | `DoubleSort.sort()` | `import io.questdb.std.DoubleSort;` + 2 call sites | WIRED | Same pattern. |
| `MultiPercentileContDoubleWindowFunctionFactory.java` | `DoubleSort.sort()` | `import io.questdb.std.DoubleSort;` + 2 call sites | WIRED | Same pattern. |
| `GroupByDoubleList.java` | `DoubleSort.sort()` | `import io.questdb.std.DoubleSort;` + 1 call site | WIRED | Line 198: `DoubleSort.sort(addressOf(0), lo, hi)`. |
| `QuantileDiscDoubleWindowFunctionFactory` | `PercentileDiscDoubleWindowFunctionFactory` | `delegate.newInstance()` | WIRED | `private final PercentileDiscDoubleWindowFunctionFactory delegate = new PercentileDiscDoubleWindowFunctionFactory()`. |
| `QuantileContDoubleWindowFunctionFactory` | `PercentileContDoubleWindowFunctionFactory` | `delegate.newInstance()` | WIRED | Same pattern. |
| `MultiQuantileDiscDoubleWindowFunctionFactory` | `MultiPercentileDiscDoubleWindowFunctionFactory` | `delegate.newInstance()` | WIRED | Same pattern. |
| `MultiQuantileContDoubleWindowFunctionFactory` | `MultiPercentileContDoubleWindowFunctionFactory` | `delegate.newInstance()` | WIRED | Same pattern. |
| `function_list.txt` | All 14 factories | FQN registration lines | WIRED | All 14 factory FQNs present at lines 813–836. |
| `PercentileWindowFunctionTest.java` | `assertQueryNoLeakCheck` | Direct method calls | WIRED | 36 calls confirmed, 0 `assertSql()` calls remaining. |

### Data-Flow Trace (Level 4)

Not applicable — phase 4 is a code quality refactoring. No new data-producing components were added. All sort logic is direct delegation to existing verified implementations.

### Behavioral Spot-Checks

Step 7b: SKIPPED — QuestDB requires a running server to execute SQL. Static compilation cannot be verified without the native library environment (native library loading fails in the agent worktree per 04-01-SUMMARY note). The test runner confirms all 41 tests pass per the 04-03-SUMMARY.

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|---------|
| QUAL-01 | 04-01 | Extract duplicated quickSort from 8 window factory locations into shared utility | SATISFIED | `DoubleSort.java` exists; 0 private quickSort/partition/swap in window factories; 9 DoubleSort.sort() call sites. |
| QUAL-02 | 04-02 | toPlan() in all group-by functions must include the percentile argument | SATISFIED (code) | All 5 toPlan() methods include `percentileFunc` argument. Human EXPLAIN verification needed. |
| REG-01 | 04-02 | Register all 10 missing group-by factories in function_list.txt | SATISFIED | 10 group-by factories (5 percentile + 5 quantile aliases) registered at lines 813–824. |
| REG-02 | 04-02 | Add quantile_disc and quantile_cont window function alias factories | SATISFIED | 4 window alias factories created and registered at lines 833–836. |
| TEST-01 | 04-03 | Add tests verifying quantile_disc and quantile_cont work as window functions | SATISFIED | `testQuantileDiscAliasEquivalence` and `testQuantileContAliasEquivalence` in `PercentileWindowFunctionTest`. Note: REQUIREMENTS.md says "as aggregates" but ROADMAP SC 3 specifies "as window functions" — roadmap SC takes precedence and is satisfied. |
| TEST-02 | 04-03 | Add window function tests for percentile_disc/percentile_cont with PARTITION BY | SATISFIED | `testPercentileDiscOverPartition`, `testPercentileContOverPartition` exist (confirmed pre-existing, not newly added). `testMultiPercentileDiscOverPartition`, `testMultiPercentileContOverPartition` also exist. |
| TEST-03 | 04-03 | Add error-path tests verifying ORDER BY in window percentile throws appropriate error | SATISFIED | All 4 variants covered: `testPercentileDiscRejectsOrderBy`, `testPercentileContRejectsOrderBy`, `testMultiPercentileDiscRejectsOrderBy`, `testMultiPercentileContRejectsOrderBy`. ROWS frame rejection also covered for all 4 variants. |
| TEST-04 | 04-03 | Migrate existing percentile tests from assertSql() to assertQueryNoLeakCheck() | SATISFIED | 0 `assertSql()` calls remain; 36 `assertQueryNoLeakCheck()` calls using 6-arg form `(expected, query, null, null, true, true)`. |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|---------|--------|
| None detected | - | - | - | - |

No TODO/FIXME/HACK/placeholder comments found in any of the new or modified files. No stub return patterns detected. All new factory files are fully implemented thin delegating factories.

### Human Verification Required

#### 1. EXPLAIN Output for percentile group-by functions (QUAL-02)

**Test:** Connect to a QuestDB instance with this branch, run:
```sql
CREATE TABLE t AS (SELECT CAST(x AS DOUBLE) AS value FROM long_sequence(5));
EXPLAIN SELECT percentile_disc(value, 0.5) FROM t;
EXPLAIN SELECT percentile_cont(value, 0.5) FROM t;
```
**Expected:** Plan output contains `percentile_disc(value,0.5)` with the comma and percentile value — not just `percentile_disc(value)`.
**Why human:** Running SQL requires a live QuestDB server; cannot verify toPlan() rendering without execution.

#### 2. quantile_disc/quantile_cont as callable window functions (REG-02 + SC-3)

**Test:** Run:
```sql
CREATE TABLE t AS (SELECT CAST(x AS DOUBLE) AS value FROM long_sequence(10));
SELECT quantile_disc(value, 0.5) OVER () FROM t;
SELECT quantile_cont(value, 0.5) OVER () FROM t;
```
**Expected:** Both queries return 10 rows with value 5.0 (disc) and 5.5 (cont) respectively. No "function not found" error.
**Why human:** Requires a live server to exercise the function resolution path and verify the factory is actually invoked through `function_list.txt`.

### Gaps Summary

No blocking gaps found. All 5 roadmap success criteria are verified at the code level. Two human verification items exist to confirm EXPLAIN output rendering and live SQL resolution — these are behavioral spot-checks that cannot be done without a running server, not evidence of incomplete implementation.

---

_Verified: 2026-04-13T16:27:53Z_
_Verifier: Claude (gsd-verifier)_
