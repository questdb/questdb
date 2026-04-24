# Phase 1: Correctness - Research

**Researched:** 2026-04-13
**Domain:** Zero-GC Java correctness fixes for percentile/quantile aggregate and window functions
**Confidence:** HIGH

## Summary

Phase 1 addresses five correctness bugs in the percentile/quantile function implementations on the `nw_percentile` branch. Every bug produces silent wrong results, hangs, or missing validation -- none cause crashes, making them particularly dangerous. The fixes are isolated, well-scoped, and follow patterns already established in the codebase (`ApproxPercentileDoubleGroupByFunction` for setEmpty/validation, `GroupByDoubleList.sort()` for dual-pivot quicksort, `EmaDoubleWindowFunctionFactory` for constness checks).

All five bugs were independently identified in prior research and confirmed via direct source code inspection. The fix patterns are prescriptive: each bug has a single correct approach dictated by the user's locked decisions (D-01 through D-08). No design choices remain open beyond error message wording and an optional constness check extension for group-by factories.

**Primary recommendation:** Fix each bug in isolation with its own test. Start with COR-05 (setEmpty, smallest change), then COR-01 (DirectArray state), COR-04 (constness validation), COR-03 (range validation), and COR-02 (sort algorithm) last since it touches the most code and feeds into Phase 4 extraction.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- D-01: Reuse GroupByDoubleList's existing dual-pivot quicksort with Dutch National Flag handling for equal elements. Extract it as a shared utility (feeds into QUAL-01 in Phase 4). Do NOT write a new sort -- the existing one already handles the equal-elements case correctly.
- D-02: For window functions, replace the current 8-copy Lomuto quickSort with calls to the extracted shared utility. The window functions operate on off-heap memory via `MemoryARW`, so the utility must work with raw memory addresses (pointer + offset), not just `GroupByDoubleList` internals.
- D-03: Re-apply type/shape on every non-null `getArray()` call: always call `setType()`, `setDimLen()`, `applyShape()` before writing data. This is ~30ns overhead per group -- negligible vs the quickSelect/sort work per group.
- D-04: This applies to all 6 affected classes: `MultiPercentileDiscDoubleGroupByFunction`, `MultiPercentileContDoubleGroupByFunction`, `MultiApproxPercentileDoubleGroupByFunction`, `MultiApproxPercentileDoublePackedGroupByFunction`, `MultiApproxPercentileLongGroupByFunction`, `MultiApproxPercentileLongPackedGroupByFunction`.
- D-05: Percentile constness: validate in window function factory `newInstance()` methods. If `!percentileFunc.isConstant()`, throw `SqlException` with the argument position. This is a compile-time check.
- D-06: Percentile range: validate via `SqlUtil.getPercentileMultiplier()` at computation time (in `computeFirst`/`computeNext` for group-by, in `preparePass2` for window). This catches invalid bind variable values at runtime.
- D-07: Multi-approx_percentile: add per-element validation of the percentile array in `getArray()`, calling `SqlUtil.getPercentileMultiplier()` for each element. Throw `CairoException` on invalid values (runtime, since array contents may come from bind variables). This matches the single-value `ApproxPercentile` pattern.
- D-08: Store `0L` in `setEmpty()`, matching `ApproxPercentileDoubleGroupByFunction` pattern exactly. Add override to `PercentileDiscDoubleGroupByFunction`, `PercentileDiscLongGroupByFunction`, and `MultiPercentileDiscDoubleGroupByFunction`. Also add to `PercentileContDoubleGroupByFunction` and `MultiPercentileContDoubleGroupByFunction` for consistency.

### Claude's Discretion
- Exact error message wording for validation failures
- Whether to add the constness check to group-by factories too (they already validate range at computation time, but constness is not strictly required for group-by since percentile is evaluated per-group)

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| COR-01 | DirectArray reuse in multi-array group-by functions must re-apply type/shape on every non-null path, not only when `out == null` | DirectArray state machine analysis confirmed: `ofNull()` clears type/shape, but non-null path only sets type/shape when `out == null`. Fix: always call `setType()`, `setDimLen()`, `applyShape()` before writing data. Affects 6 classes. |
| COR-02 | Window function quickSort must handle all-equal elements without O(N^2) degradation (three-way partition or quickselect) | Window functions use Lomuto partition with strict `<` comparison. All-equal elements cause pivot to always land at rightmost position, producing O(N^2) time. GroupByDoubleList has correct dual-pivot quicksort with DNF. Must extract and reuse. |
| COR-03 | Multi-approx_percentile must validate each percentile array element and throw SqlException on invalid values | Currently `MultiApproxPercentileDoubleGroupByFunction.getArray()` calls `histogram.getValueAtPercentile(p * 100)` without validating `p` is in [0,1]. Must add per-element `SqlUtil.getPercentileMultiplier()` call. Throw `CairoException.nonCritical()` for runtime validation. |
| COR-04 | Window function factories must validate that the percentile argument is a constant expression | No `isConstant()` check in any of the 4 window function factory `newInstance()` methods. Pattern exists in `EmaDoubleWindowFunctionFactory` (line 121, 131) and `LeadLagWindowFunctionFactoryHelper` (line 80). |
| COR-05 | Disc group-by functions must override setEmpty() to store 0L, consistent with ApproxPercentile pattern | `PercentileDiscDoubleGroupByFunction`, `PercentileDiscLongGroupByFunction`, `PercentileContDoubleGroupByFunction`, `MultiPercentileDiscDoubleGroupByFunction`, `MultiPercentileContDoubleGroupByFunction` lack `setEmpty()` override. Default behavior differs from `ApproxPercentileDoubleGroupByFunction.setEmpty()` which stores `0L`. |
</phase_requirements>

## Project Constraints (from CLAUDE.md)

- **Member ordering:** Class members grouped by kind (static vs. instance) and visibility, sorted alphabetically within each group. New methods must be inserted in correct alphabetical position.
- **Modern Java:** Use enhanced switch, multiline string literals, pattern variables in instanceof checks.
- **NULL handling:** Always consider NULL behavior. Use `Numbers.LONG_NULL` for longs, `Double.NaN` for doubles. Distinguish "not initialized" NULL from actual NULL value.
- **Boolean naming:** Use `is...` or `has...` prefix for boolean variables, fields, and methods.
- **ObjList over T[]:** Use `ObjList<T>` instead of `T[]` for object arrays.
- **Tests:** Use `assertMemoryLeak()`. Use `assertQueryNoLeakCheck()` for query result assertions (asserts factory properties). Use `execute()` for DDL. Use `assertException()` for error-path tests. Prefer UPPERCASE SQL keywords. Use multiline strings for complex SQL. Use underscores for numbers >= 5 digits.
- **Error position:** `SqlException.$(position, msg)` -- position must point at the specific offending character.
- **Resource leaks:** Think carefully about all code paths, especially error paths.
- **Build:** `mvn clean package -DskipTests` for build, `mvn -Dtest=ClassName test` for test. Do NOT run multiple `mvn test` in parallel.

## Architecture Patterns

### COR-01: DirectArray State Machine Fix

**What:** The `DirectArray` field `out` is shared across all group-by groups. When `ofNull()` is called for a null group, it clears `type` to `ColumnType.NULL` and clears `shape`/`strides`. The non-null path then checks `if (out == null)` to decide whether to initialize type/shape. After a null group sets `out.ofNull()`, `out` is no longer null, so the non-null path skips initialization -- and the subsequent `putDouble()` call hits an assertion failure (`assert getElemType() == ColumnType.DOUBLE`). [VERIFIED: nw_percentile branch source code]

**Fix pattern (from D-03):**
```java
// BEFORE (broken) - from MultiPercentileDiscDoubleGroupByFunction.getArray()
if (out == null) {
    out = new DirectArray();
    out.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
    out.setDimLen(0, viewLength);
    out.applyShape();
}

// AFTER (correct) - always re-apply type/shape on non-null path
if (out == null) {
    out = new DirectArray();
}
out.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
out.setDimLen(0, viewLength);
out.applyShape();
```

**Affected classes (D-04):** [VERIFIED: nw_percentile branch source code]
1. `MultiPercentileDiscDoubleGroupByFunction.java` -- lines 138-143
2. `MultiPercentileContDoubleGroupByFunction.java` -- inherits from above, but overrides `getArray()` with same pattern at lines 73-77
3. `MultiApproxPercentileDoubleGroupByFunction.java` -- lines 135-140
4. `MultiApproxPercentileDoublePackedGroupByFunction.java` -- same pattern
5. `MultiApproxPercentileLongGroupByFunction.java` -- same pattern
6. `MultiApproxPercentileLongPackedGroupByFunction.java` -- same pattern

### COR-02: Window Function Sort Algorithm Replacement

**What:** All 4 window function factories have their own `quickSort`/`partition`/`swap` methods that use Lomuto partition with strict `<` comparison. On all-equal elements, the pivot always lands at `right`, shrinking the range by 1 each step -- O(N^2) time and stack depth. [VERIFIED: PercentileDiscDoubleWindowFunctionFactory lines 286-330]

**Correct algorithm:** `GroupByDoubleList.sort()` (lines 186-397) implements Java's DualPivotQuicksort with proper Dutch National Flag partitioning. When all 5 sampled elements are equal, it falls back to single-pivot three-way partitioning that skips equal elements entirely, producing O(N) behavior. [VERIFIED: nw_percentile branch source code, lines 475-548]

**Extraction approach (D-01, D-02):**
The sort must work with raw memory addresses since window functions use `MemoryARW`, not `GroupByDoubleList`. Extract the sort as a static utility that takes a "memory accessor" abstraction (read/write double at index). Two possible approaches:
1. **Interface-based:** Define a `DoubleAccessor` functional interface with `getDouble(long index)` and `putDouble(long index, double value)`. Implement for `GroupByDoubleList` and `MemoryARW`.
2. **Pointer-based:** Pass raw `long ptr` + element stride, use `Unsafe.getUnsafe().getDouble(ptr + index * stride)` directly.

Pointer-based is more idiomatic in QuestDB's zero-GC context (no allocation for interface dispatch). Both work. This is a Phase 1 preparation decision that Phase 4 (QUAL-01) will finalize during extraction. For Phase 1, the minimum viable change is to replace the broken Lomuto sort in window functions with a correct sort inline. Phase 4 extracts the shared utility.

**Files affected:** [VERIFIED: nw_percentile branch source code]
- `PercentileDiscDoubleWindowFunctionFactory.java` -- both inner classes (OverPartition and OverWholeResultSet)
- `PercentileContDoubleWindowFunctionFactory.java` -- both inner classes
- `MultiPercentileDiscDoubleWindowFunctionFactory.java` -- both inner classes
- `MultiPercentileContDoubleWindowFunctionFactory.java` -- both inner classes

Total: 8 locations with duplicate sort code across 4 files.

### COR-03: Multi-Approx Percentile Range Validation

**What:** `MultiApproxPercentileDoubleGroupByFunction.getArray()` and `MultiApproxPercentileLongGroupByFunction.getArray()` iterate over the percentile array and pass each element directly to `histogram.getValueAtPercentile(p * 100)` without validating that `p` is in [0, 1]. Invalid percentiles (e.g., 1.5 or -0.1) silently produce HDR histogram results that may be meaningless. [VERIFIED: nw_percentile branch source code]

**Reference pattern:** `SqlUtil.getPercentileMultiplier()` already validates range and throws `CairoException.nonCritical()` with position info. [VERIFIED: nw_percentile branch SqlUtil.java]

```java
// Source: SqlUtil.java on nw_percentile branch
public static double getPercentileMultiplier(double percentile, int percentilePos) {
    double absPercentile = Math.abs(percentile);
    if (Numbers.isNull(percentile) || absPercentile > 1.0d) {
        throw CairoException.nonCritical().position(percentilePos)
            .put("invalid percentile [expected=range(0.0, 1.0), actual=").put(percentile).put(']');
    }
    return percentile >= 0 ? percentile : 1 - absPercentile;
}
```

**Fix (D-07):** Add per-element validation in `getArray()` before calling `histogram.getValueAtPercentile()`:
```java
for (int i = 0; i < viewLength; i++) {
    double p = view.getDoubleAtAbsIndex(i);
    SqlUtil.getPercentileMultiplier(p, percentilesPos); // validates, throws on invalid
    out.putDouble(i, histogram.getValueAtPercentile(p * 100));
}
```

**Note on exception type (D-07):** This throws `CairoException` (runtime), not `SqlException` (compile-time). This is correct because the percentile array contents may come from bind variables that are only known at execution time.

**Affected classes:**
- `MultiApproxPercentileDoubleGroupByFunction.java`
- `MultiApproxPercentileDoublePackedGroupByFunction.java`
- `MultiApproxPercentileLongGroupByFunction.java`
- `MultiApproxPercentileLongPackedGroupByFunction.java`

### COR-04: Window Function Constness Validation

**What:** None of the 4 window function factories validate that the percentile argument is a constant expression. A non-constant percentile (e.g., `percentile_disc(value, CASE WHEN x > 5 THEN 0.5 ELSE 0.25 END) OVER ()`) compiles and executes without error, but produces wrong results because the percentile is evaluated once during `preparePass2()` and applied to all rows. [VERIFIED: nw_percentile branch source code]

**Reference pattern (from EmaDoubleWindowFunctionFactory):** [VERIFIED: master branch source code]
```java
// Source: EmaDoubleWindowFunctionFactory.java line 131
if (!paramArg.isConstant()) {
    throw SqlException.$(argPositions.getQuick(2), "parameter value must be a constant");
}
```

**Fix (D-05):** Add to each of the 4 window factory `newInstance()` methods, after extracting `percentileFunc`:
```java
if (!percentileFunc.isConstant()) {
    throw SqlException.$(percentilePos, "percentile must be a constant");
}
```

For multi-percentile window factories (signature `DD[]`), the percentile array argument should also be constant:
```java
if (!percentilesFunc.isConstant()) {
    throw SqlException.$(percentilesPos, "percentile array must be a constant");
}
```

**Files affected:**
- `PercentileDiscDoubleWindowFunctionFactory.java` -- in `newInstance()`
- `PercentileContDoubleWindowFunctionFactory.java` -- in `newInstance()`
- `MultiPercentileDiscDoubleWindowFunctionFactory.java` -- in `newInstance()`
- `MultiPercentileContDoubleWindowFunctionFactory.java` -- in `newInstance()`

### COR-05: setEmpty() Override

**What:** The `GroupByFunction` default `setEmpty()` behavior differs from the `ApproxPercentileDoubleGroupByFunction` pattern which explicitly stores `0L`. Without the override, empty groups may store an incorrect sentinel value, causing `getDouble()` / `getArray()` to misinterpret the state. [VERIFIED: ApproxPercentileDoubleGroupByFunction.java line 160-162]

**Reference implementation:**
```java
// Source: ApproxPercentileDoubleGroupByFunction.java line 160-162
@Override
public void setEmpty(MapValue mapValue) {
    mapValue.putLong(valueIndex, 0L);
}
```

**Fix (D-08):** Add this exact `setEmpty()` override to:
1. `PercentileDiscDoubleGroupByFunction.java` -- currently has no `setEmpty()`
2. `PercentileDiscLongGroupByFunction.java` -- currently has no `setEmpty()`
3. `PercentileContDoubleGroupByFunction.java` -- extends `PercentileDiscDoubleGroupByFunction`, inherits its `setEmpty()` once added
4. `MultiPercentileDiscDoubleGroupByFunction.java` -- currently has no `setEmpty()`
5. `MultiPercentileContDoubleGroupByFunction.java` -- extends `MultiPercentileDiscDoubleGroupByFunction`, inherits its `setEmpty()` once added

**Note:** Classes 3 and 5 inherit from classes 1 and 4 respectively, so only classes 1, 2, and 4 need the override added directly. Classes 3 and 5 inherit it automatically. However, D-08 says "also add to PercentileContDoubleGroupByFunction and MultiPercentileContDoubleGroupByFunction for consistency" -- this means add explicit overrides even in subclasses.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Sort with equal-element handling | Custom Lomuto partition sort | GroupByDoubleList's dual-pivot quicksort (lines 186-397) | The existing sort correctly handles all-equal data via DNF partitioning; writing a new sort risks the same Lomuto bug |
| Percentile range validation | Manual `if (p < 0 \|\| p > 1)` checks | `SqlUtil.getPercentileMultiplier()` | Already handles negative percentile mapping, null checks, and correct error message format |
| Constness validation | Custom argument inspection | `Function.isConstant()` check in factory `newInstance()` | Established pattern in EmaDoubleWindowFunctionFactory and LeadLagWindowFunctionFactoryHelper |

## Common Pitfalls

### Pitfall 1: Lomuto operator confusion
**What goes wrong:** Changing `<` to `<=` in Lomuto partition appears to fix the equal-elements case but actually just shifts the degeneration from "all-right" to "all-left". The pivot still lands at an extreme position.
**Why it happens:** Lomuto partition is structurally incapable of three-way partitioning. Equal elements always end up on one side.
**How to avoid:** Use the dual-pivot quicksort from GroupByDoubleList which has proper DNF handling. Do NOT try to fix the Lomuto partition in-place.
**Warning signs:** Timeouts on queries with `WHERE value = constant` columns.

### Pitfall 2: DirectArray type assertion failure
**What goes wrong:** After `ofNull()` clears type to `ColumnType.NULL`, the next `putDouble()` call triggers `assert getElemType() == ColumnType.DOUBLE` assertion failure (in debug) or silent memory corruption (in production).
**Why it happens:** The `if (out == null)` guard conflates "first-time allocation" with "needs type re-initialization". After `ofNull()`, `out` is non-null but has wrong type.
**How to avoid:** Always call `setType()`, `setDimLen()`, `applyShape()` before writing data, regardless of whether `out` is null.
**Warning signs:** Assertion errors in test runs, corrupt array data when null groups alternate with non-null groups.

### Pitfall 3: setEmpty vs setNull confusion
**What goes wrong:** `setEmpty()` is called for empty groups (no rows matched), `setNull()` is called for null groups (all values were null). The default `setEmpty()` may not store the right sentinel, causing the result-retrieval method to misinterpret the group state.
**Why it happens:** The distinction between "empty group" (0L) and "null group" (LONG_NULL) is subtle and easy to miss when implementing new group-by functions.
**How to avoid:** Always override both `setEmpty()` and `setNull()`. Check `ApproxPercentileDoubleGroupByFunction` as the reference pattern.
**Warning signs:** Wrong results for queries with SAMPLE BY where some sample intervals have no data.

### Pitfall 4: CairoException vs SqlException for runtime validation
**What goes wrong:** Using `SqlException` in a `getArray()` method (runtime context) instead of `CairoException`. `SqlException` is for compile-time errors; `getArray()` runs during query execution.
**Why it happens:** Similar intent (validation failure) but different lifecycle stages.
**How to avoid:** Factory `newInstance()` = `SqlException`. Function compute/get methods = `CairoException.nonCritical()`.
**Warning signs:** Unexpected exception types in error-path tests.

### Pitfall 5: Error position must point to the offending argument
**What goes wrong:** Error position points to the function name instead of the specific invalid argument.
**Why it happens:** Using `position` (function position) instead of `argPositions.getQuick(N)` (argument position).
**How to avoid:** Store `percentilePos` from `argPositions.getQuick(1)` and pass it to validation methods.
**Warning signs:** Error messages with incorrect position offsets in test assertions.

### Pitfall 6: Member ordering after adding setEmpty()
**What goes wrong:** Adding `setEmpty()` in the wrong position within the class file, violating QuestDB's alphabetical member ordering convention.
**Why it happens:** Inserting methods at the end of the class instead of in alphabetical position.
**How to avoid:** Insert `setEmpty()` alphabetically among instance methods -- between `setAllocator()`/`getName()` and `setNull()` depending on the class.
**Warning signs:** Code review rejection.

## Code Examples

### COR-05: setEmpty() override (verified pattern)
```java
// Source: ApproxPercentileDoubleGroupByFunction.java lines 160-162 [VERIFIED]
@Override
public void setEmpty(MapValue mapValue) {
    mapValue.putLong(valueIndex, 0L);
}
```

### COR-04: Constness check (verified pattern)
```java
// Source: EmaDoubleWindowFunctionFactory.java lines 130-133 [VERIFIED]
// Parameter must be a constant
if (!paramArg.isConstant()) {
    throw SqlException.$(argPositions.getQuick(2), "parameter value must be a constant");
}
```

### COR-03: Per-element range validation with CairoException (verified pattern)
```java
// Source: SqlUtil.java [VERIFIED: nw_percentile branch]
// Throws CairoException.nonCritical() -- correct for runtime validation
public static double getPercentileMultiplier(double percentile, int percentilePos) {
    double absPercentile = Math.abs(percentile);
    if (Numbers.isNull(percentile) || absPercentile > 1.0d) {
        throw CairoException.nonCritical().position(percentilePos)
            .put("invalid percentile [expected=range(0.0, 1.0), actual=").put(percentile).put(']');
    }
    return percentile >= 0 ? percentile : 1 - absPercentile;
}
```

### COR-01: DirectArray re-initialization (new code pattern)
```java
// Pattern for all 6 multi-array getArray() methods
if (out == null) {
    out = new DirectArray();
}
out.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
out.setDimLen(0, viewLength);
out.applyShape();
```

### Test patterns for error assertions
```java
// Source: ApproxPercentileDoubleGroupByFunctionFactoryTest.java [VERIFIED]
@Test
public void testInvalidPercentile1() throws Exception {
    assertException(
            "select approx_percentile(x::double, 1.1) from long_sequence(1)",
            7,
            "percentile must be between 0.0 and 1.0"
    );
}

// For runtime exceptions in multi-array variants, use try/catch with CairoException
@Test
public void testThrowsOnNegativeValues() throws Exception {
    assertMemoryLeak(() -> {
        execute("create table test (x double)");
        execute("insert into test values (1.0), (-1.0)");
        try {
            assertSql("...", "select approx_percentile(x, 0.5) from test");
            Assert.fail();
        } catch (CairoException ignore) {
        }
    });
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Lomuto `<` partition in window quickSort | Dual-pivot quicksort with DNF | Phase 1 (this phase) | Fixes O(N^2) degeneration on equal elements |
| `out == null` guard for DirectArray type init | Always re-apply type/shape | Phase 1 (this phase) | Fixes corrupt results when null group precedes non-null |
| No percentile validation in multi-approx | Per-element `getPercentileMultiplier()` call | Phase 1 (this phase) | Invalid percentiles now throw instead of silently clamping |
| No constness check for window percentile args | `isConstant()` check in factory `newInstance()` | Phase 1 (this phase) | Non-constant percentiles rejected at compile time |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `PercentileContDoubleGroupByFunction` inherits from `PercentileDiscDoubleGroupByFunction` (the `setEmpty` override propagates via inheritance) | COR-05 | If inheritance chain is different, explicit override needed in more classes. Risk: LOW -- verified from source. |
| A2 | `MultiPercentileContDoubleGroupByFunction` extends `MultiPercentileDiscDoubleGroupByFunction` (same inheritance logic for setEmpty) | COR-05 | Same as A1. [VERIFIED: source code shows `extends MultiPercentileDiscDoubleGroupByFunction`] |

**All other claims are VERIFIED from direct source code inspection on the nw_percentile branch.**

## Open Questions (RESOLVED)

1. **Should constness check be added to group-by factories too?**
   - What we know: Group-by percentile arguments are evaluated per-group at computation time. Non-constant percentile is technically valid for group-by (different percentile per group). However, the current implementation uses `percentileFunc.getDouble(record)` which evaluates at result-retrieval time, not at accumulation time -- so a non-constant percentile actually works correctly for group-by.
   - What's unclear: Whether the user considers this a desirable feature or an accident.
   - Recommendation: Per "Claude's Discretion", skip constness check for group-by factories. It is not strictly required and would restrict valid use cases. Focus constness validation only on window factories where non-constant percentile produces definitively wrong results.

2. **Error message wording for constness check**
   - Recommendation: Use `"percentile must be a constant"` for single-percentile factories and `"percentile array must be a constant"` for multi-percentile factories. This follows the pattern in `EmaDoubleWindowFunctionFactory` ("parameter value must be a constant") and `ApproxPercentileDoubleGroupByFunctionFactory` ("percentile must be a constant").

## Sources

### Primary (HIGH confidence -- direct source code inspection on nw_percentile branch)
- `MultiPercentileDiscDoubleGroupByFunction.java` -- COR-01 bug location (lines 138-143)
- `MultiPercentileContDoubleGroupByFunction.java` -- COR-01 bug location (lines 73-77)
- `MultiApproxPercentileDoubleGroupByFunction.java` -- COR-01/COR-03 bug locations
- `PercentileDiscDoubleWindowFunctionFactory.java` -- COR-02 bug (lines 286-330), COR-04 missing check
- `PercentileContDoubleWindowFunctionFactory.java` -- COR-02/COR-04
- `MultiPercentileDiscDoubleWindowFunctionFactory.java` -- COR-02/COR-04
- `MultiPercentileContDoubleWindowFunctionFactory.java` -- COR-02/COR-04
- `GroupByDoubleList.java` -- correct dual-pivot quicksort (lines 186-548)
- `SqlUtil.java` -- `getPercentileMultiplier()` validation utility
- `ApproxPercentileDoubleGroupByFunction.java` -- reference `setEmpty()`, `init()` validation pattern
- `EmaDoubleWindowFunctionFactory.java` -- reference constness check pattern (lines 121, 131)
- `PercentileDiscDoubleGroupByFunction.java` -- missing `setEmpty()` (COR-05)
- `PercentileDiscLongGroupByFunction.java` -- missing `setEmpty()` (COR-05)

### Secondary (HIGH confidence -- direct source code inspection on master branch)
- `ApproxPercentileDoubleGroupByFunctionFactoryTest.java` -- test patterns for error assertions, `assertException()` usage
- `PercentileWindowFunctionTest.java` -- existing window function test patterns, `assertSql()` usage

## Metadata

**Confidence breakdown:**
- COR-01 fix: HIGH -- bug mechanism confirmed via direct code reading, fix pattern is mechanical
- COR-02 fix: HIGH -- algorithm difference confirmed (Lomuto `<` vs dual-pivot DNF), correct sort verified
- COR-03 fix: HIGH -- missing validation confirmed, `SqlUtil.getPercentileMultiplier()` is the exact utility needed
- COR-04 fix: HIGH -- missing `isConstant()` check confirmed, reference pattern verified in 3 other factories
- COR-05 fix: HIGH -- missing `setEmpty()` confirmed, reference implementation verified in ApproxPercentile

**Research date:** 2026-04-13
**Valid until:** 2026-05-13 (stable -- fixes are against a specific branch with known code)
