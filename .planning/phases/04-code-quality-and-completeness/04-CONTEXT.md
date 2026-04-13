# Phase 4: Code Quality and Completeness - Context

**Gathered:** 2026-04-13
**Status:** Ready for planning

<domain>
## Phase Boundary

Consolidate duplicated sort code into a shared utility, register all 10 missing group-by factories, add quantile_disc/quantile_cont window function aliases, fix toPlan() output, and migrate all window tests to assertQueryNoLeakCheck. Requirements: QUAL-01, QUAL-02, REG-01, REG-02, TEST-01, TEST-02, TEST-03, TEST-04.

</domain>

<decisions>
## Implementation Decisions

### Shared Sort Utility (QUAL-01)
- **D-01:** Create new class `DoubleSort` in `io.questdb.std` package. Follows existing naming convention: `LongSort`, `IntGroupSort`, etc. Contains static `quickSort(long ptr, long left, long right)` operating on raw off-heap double arrays.
- **D-02:** Extract only quickSort + partition + swap from the 8 duplicated window factory methods. Do NOT extract quickSelect — it stays in GroupByDoubleList/GroupByLongList where it uses list-specific accessors.
- **D-03:** `GroupByDoubleList.sort()` must delegate to `DoubleSort.sort(dataPtr(), 0, size - 1)`. This eliminates all duplicate sort code: 8 window copies + 1 GroupByDoubleList copy → 1 DoubleSort implementation.
- **D-04:** All 4 window factories (PercentileDisc, PercentileCont, MultiPercentileDisc, MultiPercentileCont) replace their inline `quickSort` methods with calls to `DoubleSort.sort()`.

### toPlan Output (QUAL-02)
- **D-05:** All percentile/quantile group-by functions must show both the data argument and the percentile value in toPlan(). Format: `percentile_disc(value,0.5)` for single, `percentile_disc(value,[0.25,0.5,0.75])` for multi.
- **D-06:** This applies to: PercentileDiscDoubleGroupByFunction, PercentileDiscLongGroupByFunction, PercentileContDoubleGroupByFunction, MultiPercentileDiscDoubleGroupByFunction, MultiPercentileContDoubleGroupByFunction.

### Factory Registration (REG-01)
- **D-07:** Register all 10 missing group-by factories in `function_list.txt`:
  - PercentileDiscDoubleGroupByFunctionFactory
  - PercentileDiscLongGroupByFunctionFactory
  - PercentileContDoubleGroupByFunctionFactory
  - MultiPercentileDiscDoubleGroupByFunctionFactory
  - MultiPercentileContDoubleGroupByFunctionFactory
  - QuantileDiscDoubleGroupByFunctionFactory
  - QuantileDiscLongGroupByFunctionFactory
  - QuantileContDoubleGroupByFunctionFactory
  - MultiQuantileDiscDoubleGroupByFunctionFactory
  - MultiQuantileContDoubleGroupByFunctionFactory

### Window Function Aliases (REG-02)
- **D-08:** Create thin delegating factory files for window aliases. 4 new files:
  - QuantileDiscDoubleWindowFunctionFactory
  - QuantileContDoubleWindowFunctionFactory
  - MultiQuantileDiscDoubleWindowFunctionFactory
  - MultiQuantileContDoubleWindowFunctionFactory
- **D-09:** Each alias factory overrides `getSignature()` to return `quantile_*` and delegates `newInstance()` to the corresponding `Percentile*` factory. Matches the existing group-by alias pattern (e.g., QuantileDiscDoubleGroupByFunctionFactory).
- **D-10:** Register all 4 new window alias factories in `function_list.txt`.

### Test Migration (TEST-04)
- **D-11:** Migrate all 26 `assertSql()` calls in `PercentileWindowFunctionTest.java` to `assertQueryNoLeakCheck()`. All 26 are data-correctness tests. Error-path tests already use `assertException()`.
- **D-12:** No `assertSql()` calls should remain in the window test file after migration.

### Quantile Alias Tests (TEST-01)
- **D-13:** Test both parsing and result equivalence for quantile aliases. Verify `quantile_disc(col, 0.5)` produces the same result as `percentile_disc(col, 0.5)` for both group-by and window contexts. One test per alias pair (disc/cont), not a full matrix.

### PARTITION BY Tests (TEST-02)
- **D-14:** Add window function tests for percentile_disc and percentile_cont with explicit PARTITION BY. Some PARTITION BY tests already exist — add coverage for single-percentile partitioned paths if missing.

### ORDER BY Error Tests (TEST-03)
- **D-15:** Test all 4 window functions (percentile_disc, percentile_cont, multi-percentile_disc, multi-percentile_cont) with ORDER BY in OVER clause. Verify they throw "only supports whole partition frames" error.
- **D-16:** Also test with ROWS/RANGE frame specs to verify the same error.

### Claude's Discretion
- Exact internal structure of DoubleSort (insertion sort threshold, recursion limit)
- Whether to also port swap/partition as separate public methods or keep them private
- Test data setup for alias equivalence tests (reuse existing test tables or create new ones)
- Whether to add quantile alias error-path tests or rely on the fact that they delegate to the same validation

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Sort utility target
- `core/src/main/java/io/questdb/std/LongSort.java` — Naming convention reference for the new DoubleSort class
- `core/src/main/java/io/questdb/griffin/engine/groupby/GroupByDoubleList.java:186-397` — Source dual-pivot quicksort to extract into DoubleSort

### 8 duplicated quickSort methods to replace
- `core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileDiscDoubleWindowFunctionFactory.java:329` — partitioned variant
- `core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileDiscDoubleWindowFunctionFactory.java:485` — whole-result-set variant
- `core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileContDoubleWindowFunctionFactory.java:340` — partitioned variant
- `core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileContDoubleWindowFunctionFactory.java:506` — whole-result-set variant
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileDiscDoubleWindowFunctionFactory.java:370` — partitioned variant
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileDiscDoubleWindowFunctionFactory.java:568` — whole-result-set variant
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileContDoubleWindowFunctionFactory.java:383` — partitioned variant
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileContDoubleWindowFunctionFactory.java:592` — whole-result-set variant

### toPlan methods to fix
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/PercentileDiscDoubleGroupByFunction.java:200`
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/PercentileDiscLongGroupByFunction.java:199`
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/PercentileContDoubleGroupByFunction.java:94`
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MultiPercentileDiscDoubleGroupByFunction.java:263`
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MultiPercentileContDoubleGroupByFunction.java:153`

### Alias factory pattern reference
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/QuantileDiscDoubleGroupByFunctionFactory.java` — Existing group-by alias factory pattern to follow for window aliases

### Registration target
- `core/src/main/resources/function_list.txt:812` — Section where percentile/quantile factories are registered

### Test file to migrate
- `core/src/test/java/io/questdb/test/griffin/engine/window/PercentileWindowFunctionTest.java` — 26 assertSql → assertQueryNoLeakCheck, plus new tests for aliases, ORDER BY errors, PARTITION BY coverage

### Error validation reference
- `core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileDiscDoubleWindowFunctionFactory.java:99-100` — ORDER BY rejection pattern ("only supports whole partition frames")

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `LongSort` in `io.questdb.std`: Naming convention and package location reference for the new `DoubleSort`
- `QuantileDiscDoubleGroupByFunctionFactory`: Thin alias factory pattern — override signature, delegate newInstance()
- Existing `assertException()` calls in test file: Error-path testing pattern already established

### Established Patterns
- Sort utilities in `io.questdb.std` are stateless static classes (`LongSort`, `IntGroupSort`)
- Alias factories are one-file-per-alias with minimal code: getSignature() + newInstance() delegation
- `function_list.txt` groups related factories together with comment headers
- `toPlan()` outputs function-call-like syntax: `functionName(args)`

### Integration Points
- `GroupByDoubleList.sort()` calls must be redirected to `DoubleSort.sort(dataPtr(), ...)` — dataPtr() already exists
- Window factory inner classes call their private `quickSort()` — replace with `DoubleSort.sort(ptr, left, right)`
- New window alias factories must be registered in `function_list.txt` alongside existing window factories
- `assertQueryNoLeakCheck()` requires the query string and expected result — same interface as `assertSql()` but adds factory property assertions

</code_context>

<specifics>
## Specific Ideas

No specific requirements — standard code quality improvements following established QuestDB patterns.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>

---

*Phase: 04-code-quality-and-completeness*
*Context gathered: 2026-04-13*
