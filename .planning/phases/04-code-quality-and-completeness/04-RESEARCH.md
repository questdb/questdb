# Phase 4: Code Quality and Completeness - Research

**Researched:** 2026-04-13
**Domain:** Java refactoring, factory registration, test migration (QuestDB internals)
**Confidence:** HIGH

## Summary

Phase 4 consolidates duplicated sort code, registers missing factories, creates window function aliases, fixes toPlan output, and migrates tests. All changes are mechanical refactoring and boilerplate -- no algorithmic changes, no new data structures, no risky memory management.

The 8 duplicated quickSort methods across 4 window factory files share identical logic (median-of-three pivot, tail-call optimization) and differ only in how they access doubles in memory: partitioned variants use a base offset within MemoryARW, while whole-result-set variants use 0-based offsets. Both resolve to `Unsafe.getUnsafe().getDouble(nativeAddress + index * 8L)` under the hood, which means a single `DoubleSort.sort(long ptr, long left, long right)` method operating on raw off-heap pointers can replace all copies, including the GroupByDoubleList's dual-pivot sort for the simple sort case.

**Primary recommendation:** Execute this phase as three sequential waves: (1) DoubleSort extraction + GroupByDoubleList delegation + window factory refactoring, (2) toPlan fixes + factory registration + window alias creation, (3) test migration + new test coverage.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Create new class `DoubleSort` in `io.questdb.std` package. Follows existing naming convention: `LongSort`, `IntGroupSort`, etc. Contains static `quickSort(long ptr, long left, long right)` operating on raw off-heap double arrays.
- **D-02:** Extract only quickSort + partition + swap from the 8 duplicated window factory methods. Do NOT extract quickSelect -- it stays in GroupByDoubleList/GroupByLongList where it uses list-specific accessors.
- **D-03:** `GroupByDoubleList.sort()` must delegate to `DoubleSort.sort(dataPtr(), 0, size - 1)`. This eliminates all duplicate sort code: 8 window copies + 1 GroupByDoubleList copy -> 1 DoubleSort implementation.
- **D-04:** All 4 window factories (PercentileDisc, PercentileCont, MultiPercentileDisc, MultiPercentileCont) replace their inline `quickSort` methods with calls to `DoubleSort.sort()`.
- **D-05:** All percentile/quantile group-by functions must show both the data argument and the percentile value in toPlan(). Format: `percentile_disc(value,0.5)` for single, `percentile_disc(value,[0.25,0.5,0.75])` for multi.
- **D-06:** toPlan applies to: PercentileDiscDoubleGroupByFunction, PercentileDiscLongGroupByFunction, PercentileContDoubleGroupByFunction, MultiPercentileDiscDoubleGroupByFunction, MultiPercentileContDoubleGroupByFunction.
- **D-07:** Register all 10 missing group-by factories in `function_list.txt`.
- **D-08:** Create 4 thin delegating window alias factory files (QuantileDisc*, QuantileCont*, MultiQuantileDisc*, MultiQuantileCont*).
- **D-09:** Each alias factory overrides `getSignature()` to return `quantile_*` and delegates `newInstance()` to the corresponding `Percentile*` factory.
- **D-10:** Register all 4 new window alias factories in `function_list.txt`.
- **D-11:** Migrate all 26 `assertSql()` calls in `PercentileWindowFunctionTest.java` to `assertQueryNoLeakCheck()`.
- **D-12:** No `assertSql()` calls should remain in the window test file after migration.
- **D-13:** Test quantile alias equivalence (disc/cont) for both group-by and window contexts.
- **D-14:** Add PARTITION BY window function tests if missing for single-percentile partitioned paths.
- **D-15:** Test all 4 window functions with ORDER BY in OVER clause, verify error thrown.
- **D-16:** Also test with ROWS/RANGE frame specs to verify the same error.

### Claude's Discretion
- Exact internal structure of DoubleSort (insertion sort threshold, recursion limit)
- Whether to also port swap/partition as separate public methods or keep them private
- Test data setup for alias equivalence tests (reuse existing test tables or create new ones)
- Whether to add quantile alias error-path tests or rely on delegation to the same validation

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| QUAL-01 | Extract duplicated quickSort/partition/swap from 8 locations across 4 window factories into a shared utility | DoubleSort class in io.questdb.std; all 8 locations verified in codebase; memory access pattern analyzed and confirmed compatible with raw-pointer API |
| QUAL-02 | toPlan() in all group-by functions must include the percentile argument in EXPLAIN output | All 5 toPlan methods identified; current output lacks percentileFunc; fix pattern documented |
| REG-01 | Register all 10 missing group-by factories in function_list.txt | Verified function_list.txt currently has 0 percentile/quantile group-by factories registered; all 10 factory classes exist in codebase |
| REG-02 | Add quantile_disc and quantile_cont window function alias factories | 4 new factory files needed; existing group-by alias pattern documented as template |
| TEST-01 | Add tests verifying quantile_cont and quantile_disc SQL syntax works as aggregates | assertQueryNoLeakCheck pattern for equivalence testing documented |
| TEST-02 | Add window function tests for percentile_disc/percentile_cont with PARTITION BY | Some PARTITION BY tests already exist (testMultiPercentileContOverPartition, etc.); need to verify single-percentile partitioned coverage |
| TEST-03 | Add error-path tests verifying ORDER BY in window percentile throws appropriate error | Error tests exist for percentile_disc/percentile_cont ORDER BY; need to add multi-percentile variants + ROWS/RANGE frame spec tests |
| TEST-04 | Migrate existing percentile tests from assertSql() to assertQueryNoLeakCheck() | 26 assertSql calls identified; simple 2-arg assertQueryNoLeakCheck(expected, query) is sufficient replacement |
</phase_requirements>

## Standard Stack

No new libraries needed. This phase uses only existing QuestDB infrastructure.

### Core Utilities Used
| Class | Package | Purpose | Why Standard |
|-------|---------|---------|--------------|
| `Unsafe` | `io.questdb.std` | Raw off-heap memory access for DoubleSort | QuestDB's standard approach for zero-GC operations |
| `LongSort` | `io.questdb.std` | Naming convention and structural reference for DoubleSort | Existing sort utility in same package |
| `AbstractWindowFunctionFactory` | `...functions.window` | Base class for window alias factories | Provides `isWindow() = true` for factory registration |

[VERIFIED: codebase inspection of nw_percentile branch worktree]

## Architecture Patterns

### Pattern 1: Raw Off-Heap Sort Utility

**What:** A stateless static class `DoubleSort` in `io.questdb.std` that sorts doubles stored contiguously in native memory.

**When to use:** When multiple classes need to sort off-heap double arrays and the sort logic is identical.

**Key insight:** Both MemoryARW-based window factories and Unsafe-based GroupByDoubleList ultimately store doubles at `basePtr + index * 8L`. The MemoryCARWImpl (contiguous memory) implementation means `listMemory.getPageAddress(0) + offset` yields a valid native pointer. [VERIFIED: codebase inspection of MemoryCARWImpl, AbstractMemoryCR.addressOf()]

**API:**
```java
// Source: Decision D-01, verified against LongSort naming convention
package io.questdb.std;

public class DoubleSort {
    public static void sort(long ptr, long left, long right) {
        // ptr = native address of element[0]
        // Access: Unsafe.getUnsafe().getDouble(ptr + index * 8L)
    }
}
```

**Caller mapping:**
- Window partitioned: `DoubleSort.sort(listMemory.getPageAddress(0) + listPtr + DATA_OFFSET, 0, size - 1)` [VERIFIED: codebase inspection]
- Window whole-result-set: `DoubleSort.sort(listMemory.getPageAddress(0), 0, size - 1)` [VERIFIED: codebase inspection]
- GroupByDoubleList: `DoubleSort.sort(addressOf(0), lo, hi)` where `addressOf(0)` = `ptr + HEADER_SIZE` [VERIFIED: GroupByDoubleList.java line 84-86]

### Pattern 2: Thin Delegating Alias Factory

**What:** A factory class that overrides `getSignature()` to register an alias name, delegates `newInstance()` to the canonical factory.

**When to use:** When SQL functions need multiple names (e.g., quantile_disc = percentile_disc).

**Example:**
```java
// Source: QuantileDiscDoubleGroupByFunctionFactory.java (existing pattern)
public class QuantileDiscDoubleWindowFunctionFactory extends AbstractWindowFunctionFactory {
    private final PercentileDiscDoubleWindowFunctionFactory delegate = new PercentileDiscDoubleWindowFunctionFactory();

    @Override
    public String getSignature() {
        return "quantile_disc(DD)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return delegate.newInstance(position, args, argPositions, configuration, sqlExecutionContext);
    }
}
```
[VERIFIED: existing group-by alias pattern in QuantileDiscDoubleGroupByFunctionFactory.java]

### Pattern 3: toPlan with All Arguments

**What:** Group-by function `toPlan()` methods must include all constructor arguments in the plan output.

**Current broken pattern (all 5 functions):**
```java
sink.val("percentile_disc(").val(arg).val(')');
// Output: percentile_disc(value) -- missing percentile parameter
```

**Fixed pattern:**
```java
sink.val("percentile_disc(").val(arg).val(',').val(percentileFunc).val(')');
// Output: percentile_disc(value,0.5) -- includes percentile parameter
```
[VERIFIED: current toPlan methods inspected in codebase; D-05 specifies format]

### Pattern 4: assertQueryNoLeakCheck Migration

**What:** Replace `assertSql(expected, query)` with `assertQueryNoLeakCheck(expected, query)` in window function tests.

**Why:** Per CLAUDE.md, `assertQueryNoLeakCheck()` asserts factory properties (supportsRandomAccess, expectSize, expectedTimestamp) in addition to data correctness. Window function tests are data-correctness tests, not storage tests, so they should use `assertQueryNoLeakCheck()`.

**Migration:** Direct 1:1 replacement -- the 2-arg overload `assertQueryNoLeakCheck(String expected, String query)` exists at AbstractCairoTest line 2070. [VERIFIED: codebase inspection]

### Anti-Patterns to Avoid
- **Copying quickSort for new classes:** Always delegate to DoubleSort.sort() -- never inline sort logic.
- **Using assertSql() for query correctness tests:** Per CLAUDE.md, use assertQueryNoLeakCheck() which also validates factory properties.
- **Modifying quickSelect during extraction:** D-02 explicitly says quickSelect stays in GroupByDoubleList/GroupByLongList. Only quickSort/partition/swap are extracted.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Off-heap double sorting | Inline quickSort in each inner class | `DoubleSort.sort()` static utility | 8 identical copies across 4 files; maintenance nightmare |
| Window function aliasing | Duplicate full window factory logic for quantile_* names | Thin delegating factory + `AbstractWindowFunctionFactory` | Existing pattern; avoid code duplication |
| Factory property validation in tests | Manual cursor property checks | `assertQueryNoLeakCheck()` | Built into AbstractCairoTest; validates supportsRandomAccess, expectSize |

## Common Pitfalls

### Pitfall 1: MemoryARW Offset vs Native Pointer Confusion
**What goes wrong:** Passing a MemoryARW logical offset to DoubleSort instead of a resolved native pointer.
**Why it happens:** Window factory inner classes work with MemoryARW offsets (e.g., `listPtr + DATA_OFFSET`), but DoubleSort needs raw native addresses.
**How to avoid:** Always resolve to native address: `listMemory.getPageAddress(0) + logicalOffset`. For whole-result-set variant, the offset is 0.
**Warning signs:** Segfaults or incorrect sort results.

### Pitfall 2: GroupByDoubleList Sort API Mismatch
**What goes wrong:** DoubleSort uses `long` indices (matching window factory usage), but GroupByDoubleList.sort() uses `int` indices.
**Why it happens:** GroupByDoubleList stores size as `int` (4 bytes), while window factories use `long` for size tracking.
**How to avoid:** DoubleSort.sort(long ptr, long left, long right) accepts `long` parameters. GroupByDoubleList.sort() casts its `int` parameters to `long` when delegating.
**Warning signs:** Compilation errors on parameter types.

### Pitfall 3: Forgetting to Remove Private Methods After Extraction
**What goes wrong:** Leaving dead `quickSort()`, `partition()`, `swap()` methods in window factory inner classes after switching to DoubleSort.
**Why it happens:** The methods are private and won't cause compilation errors if left behind.
**How to avoid:** After replacing call sites, delete all private sort helper methods from every inner class. There are 8 quickSort + 8 partition + 8 swap = 24 private methods to remove across 4 factory files.
**Warning signs:** IDE "unused method" warnings (which may not appear in CLI builds).

### Pitfall 4: Multi-Percentile Array Rendering in toPlan
**What goes wrong:** For multi-percentile functions, `sink.val(percentileFunc)` may not render the array literal in the expected `[0.25,0.5,0.75]` format.
**Why it happens:** The percentileFunc for multi-percentile is an array constant whose toPlan rendering depends on its implementation.
**How to avoid:** Verify toPlan output for multi-percentile functions in tests. The D-05 format specifies `percentile_disc(value,[0.25,0.5,0.75])`.
**Warning signs:** toPlan output shows object reference or unexpected format instead of array literal.

### Pitfall 5: Function Registration Order Matters
**What goes wrong:** SQL parser may not find the correct factory if registrations conflict.
**Why it happens:** function_list.txt entries are processed in order. Factories with overlapping signatures (e.g., `percentile_disc(DD)` group-by vs window) must coexist.
**How to avoid:** Group-by and window factories have separate registration mechanisms (`isGroupBy()` vs `isWindow()` on the factory). Both can share the same signature string. Group related factories together with comment headers per existing convention.
**Warning signs:** "function not found" or wrong function type resolved.

### Pitfall 6: Alphabetical Member Ordering in New DoubleSort Class
**What goes wrong:** CLAUDE.md requires members sorted alphabetically by kind and visibility.
**Why it happens:** When porting code from GroupByDoubleList, easy to forget QuestDB's class layout convention.
**How to avoid:** After writing DoubleSort, verify: public static methods alphabetically, then private static methods alphabetically.
**Warning signs:** Code review rejection.

## Code Examples

### DoubleSort Extraction Pattern

```java
// Source: Decision D-01 + verified memory access patterns from codebase
package io.questdb.std;

public class DoubleSort {
    /**
     * Sorts the specified range of an off-heap double array using quicksort
     * with median-of-three pivot selection and tail-call optimization.
     *
     * @param ptr   native address of the first element (element[0])
     * @param left  the index of the first element, inclusive
     * @param right the index of the last element, inclusive
     */
    public static void sort(long ptr, long left, long right) {
        while (left < right) {
            long pi = partition(ptr, left, right);
            if (pi - left < right - pi) {
                sort(ptr, left, pi - 1);
                left = pi + 1;
            } else {
                sort(ptr, pi + 1, right);
                right = pi - 1;
            }
        }
    }

    private static double getDouble(long ptr, long index) {
        return Unsafe.getUnsafe().getDouble(ptr + index * 8L);
    }

    private static long partition(long ptr, long left, long right) {
        long mid = left + (right - left) / 2;
        if (getDouble(ptr, left) > getDouble(ptr, mid)) {
            swap(ptr, left, mid);
        }
        if (getDouble(ptr, left) > getDouble(ptr, right)) {
            swap(ptr, left, right);
        }
        if (getDouble(ptr, mid) > getDouble(ptr, right)) {
            swap(ptr, mid, right);
        }
        swap(ptr, mid, right);

        double pivot = getDouble(ptr, right);
        long i = left - 1;

        for (long j = left; j < right; j++) {
            if (getDouble(ptr, j) < pivot) {
                i++;
                swap(ptr, i, j);
            }
        }
        swap(ptr, i + 1, right);
        return i + 1;
    }

    private static void putDouble(long ptr, long index, double value) {
        Unsafe.getUnsafe().putDouble(ptr + index * 8L, value);
    }

    private static void swap(long ptr, long i, long j) {
        double temp = getDouble(ptr, i);
        putDouble(ptr, i, getDouble(ptr, j));
        putDouble(ptr, j, temp);
    }
}
```
[ASSUMED: exact internal structure is at Claude's discretion per CONTEXT.md]

### Window Factory Refactoring Pattern

```java
// Before (in PercentileDiscOverPartitionFunction.preparePass2):
quickSort(listPtr + DATA_OFFSET, 0, size - 1);

// After:
DoubleSort.sort(listMemory.getPageAddress(0) + listPtr + DATA_OFFSET, 0, size - 1);

// Before (in PercentileDiscOverWholeResultSetFunction.preparePass2):
quickSort(0, size - 1);

// After:
DoubleSort.sort(listMemory.getPageAddress(0), 0, size - 1);
```
[VERIFIED: codebase inspection of current call sites]

### GroupByDoubleList Delegation Pattern

```java
// Before (in GroupByDoubleList):
public void sort(int lo, int hi) {
    if (lo < 0 || hi >= size()) {
        throw new ArrayIndexOutOfBoundsException(...);
    }
    if (lo < hi) {
        sort(lo, hi, true);  // 400+ lines of dual-pivot quicksort
    }
}

// After:
public void sort(int lo, int hi) {
    if (lo < 0 || hi >= size()) {
        throw new ArrayIndexOutOfBoundsException(...);
    }
    if (lo < hi) {
        DoubleSort.sort(addressOf(0), lo, hi);
    }
}
```
[VERIFIED: GroupByDoubleList.sort() at line 192, addressOf() at line 84]

**Important note on GroupByDoubleList:** The existing sort() in GroupByDoubleList is a dual-pivot quicksort (Java Arrays.sort style, ~380 lines) which is more sophisticated than the simple median-of-three quicksort in the window factories. Decision D-03 says GroupByDoubleList.sort() should delegate to DoubleSort. This means DoubleSort will use the simpler median-of-three quicksort from the window factories. This is acceptable because:
1. GroupByDoubleList.sort() is only called from percentile group-by functions, not hot paths
2. The simpler quicksort is correct and adequate for these use cases
3. Consolidation eliminates ~400 lines of duplicated code
[VERIFIED: GroupByDoubleList.sort implementation spans lines 192-578]

### toPlan Fix Pattern

```java
// Before:
public void toPlan(PlanSink sink) {
    sink.val("percentile_disc(").val(arg).val(')');
}

// After (single percentile):
public void toPlan(PlanSink sink) {
    sink.val("percentile_disc(").val(arg).val(',').val(percentileFunc).val(')');
}

// After (multi percentile -- same pattern, percentileFunc renders as array):
public void toPlan(PlanSink sink) {
    sink.val("percentile_disc(").val(arg).val(',').val(percentileFunc).val(')');
}
```
[VERIFIED: current toPlan implementations in all 5 group-by function classes]

### function_list.txt Registration Pattern

```
# 'percentile_disc' and 'percentile_cont' group-by functions
io.questdb.griffin.engine.functions.groupby.PercentileDiscDoubleGroupByFunctionFactory
io.questdb.griffin.engine.functions.groupby.PercentileDiscLongGroupByFunctionFactory
io.questdb.griffin.engine.functions.groupby.PercentileContDoubleGroupByFunctionFactory
io.questdb.griffin.engine.functions.groupby.MultiPercentileDiscDoubleGroupByFunctionFactory
io.questdb.griffin.engine.functions.groupby.MultiPercentileContDoubleGroupByFunctionFactory

# 'quantile_disc' and 'quantile_cont' group-by aliases
io.questdb.griffin.engine.functions.groupby.QuantileDiscDoubleGroupByFunctionFactory
io.questdb.griffin.engine.functions.groupby.QuantileDiscLongGroupByFunctionFactory
io.questdb.griffin.engine.functions.groupby.QuantileContDoubleGroupByFunctionFactory
io.questdb.griffin.engine.functions.groupby.MultiQuantileDiscDoubleGroupByFunctionFactory
io.questdb.griffin.engine.functions.groupby.MultiQuantileContDoubleGroupByFunctionFactory

# 'quantile_disc' and 'quantile_cont' window aliases
io.questdb.griffin.engine.functions.window.QuantileDiscDoubleWindowFunctionFactory
io.questdb.griffin.engine.functions.window.QuantileContDoubleWindowFunctionFactory
io.questdb.griffin.engine.functions.window.MultiQuantileDiscDoubleWindowFunctionFactory
io.questdb.griffin.engine.functions.window.MultiQuantileContDoubleWindowFunctionFactory
```
[VERIFIED: function_list.txt structure at lines 802-816; all group-by factory classes exist]

### Error Test Pattern (ORDER BY + ROWS/RANGE)

```java
// Source: existing error tests at lines 606-626, 794-815 of PercentileWindowFunctionTest.java
@Test
public void testMultiPercentileDiscRejectsOrderBy() throws Exception {
    assertMemoryLeak(() -> {
        execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) AS value FROM long_sequence(10))");
        assertException(
                "SELECT percentile_disc(value, ARRAY[0.25, 0.5]) OVER (ORDER BY value) FROM test",
                7,
                "percentile_disc window function only supports whole partition frames"
        );
    });
}

@Test
public void testMultiPercentileDiscRejectsRowsFrame() throws Exception {
    assertMemoryLeak(() -> {
        execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) AS value, x % 2 AS category FROM long_sequence(10))");
        assertException(
                "SELECT percentile_disc(value, ARRAY[0.25, 0.5]) OVER (PARTITION BY category ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM test",
                7,
                "percentile_disc window function only supports whole partition frames"
        );
    });
}
```
[VERIFIED: existing error test pattern in PercentileWindowFunctionTest.java]

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Inline quickSort in each window factory inner class | Shared `DoubleSort.sort()` utility | This phase | Reduces 8 identical sort implementations + GroupByDoubleList sort to 1 |
| Group-by percentile factories not registered | Registered in function_list.txt | This phase | Factories become callable from SQL |
| Window-only percentile functions | Both window and group-by + quantile aliases | This phase | Full SQL coverage for all percentile/quantile forms |

## Assumptions Log

> List all claims tagged `[ASSUMED]` in this research. The planner and discuss-phase use this
> section to identify decisions that need user confirmation before execution.

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | DoubleSort internal structure uses simple median-of-three quicksort rather than dual-pivot | Code Examples | Low -- any correct sort works; dual-pivot just has better average-case constants |
| A2 | Replacing GroupByDoubleList's dual-pivot sort with simpler quicksort is acceptable | Code Examples | Low -- only used for percentile group-by, not a hot path. If perf matters, can upgrade later |

## Open Questions

1. **Multi-percentile array rendering in toPlan**
   - What we know: `sink.val(percentileFunc)` calls the Function's toPlan. For single percentile, this renders as a numeric constant (e.g., `0.5`).
   - What's unclear: For multi-percentile, the percentileFunc is an array constant. Its toPlan rendering might produce `[0.25,0.5,0.75]` or something else.
   - Recommendation: Implement the fix and verify the output format in a test. If the rendering doesn't match D-05 format, adjust the toPlan method to explicitly format the array.

2. **PARTITION BY test coverage gaps for single-percentile**
   - What we know: Multi-percentile PARTITION BY tests exist (testMultiPercentileContOverPartition, etc.). Some single-percentile partitioned tests exist too.
   - What's unclear: Whether all single-percentile + PARTITION BY combinations are covered.
   - Recommendation: Audit existing test method names for "Partition" and add any missing single-percentile partitioned tests.

## Project Constraints (from CLAUDE.md)

The following CLAUDE.md directives apply to this phase:

- **Member ordering:** Java class members sorted alphabetically by kind and visibility. New DoubleSort class and modified factory classes must follow this.
- **assertQueryNoLeakCheck:** Use for all query-result assertions in window tests (not assertSql). This is the core of TEST-04.
- **execute() for DDL:** Use `execute()` for CREATE TABLE statements in tests.
- **Multiline strings:** Use for SQL statements and expected results in tests.
- **UPPERCASE SQL keywords:** Prefer UPPERCASE (SELECT, FROM, OVER, PARTITION BY, etc.).
- **ObjList not T[]:** Use ObjList for any new object arrays.
- **Boolean naming:** is.../has... prefix for boolean fields/methods.
- **Active voice:** In commit messages and code comments.
- **Error position convention:** `SqlException.$(position, msg)` points at the offending character.
- **No DELETE support:** Not relevant but should be kept in mind for test data setup.

## Sources

### Primary (HIGH confidence)
- Codebase inspection of `/Users/sminaev/qdbwt/abstract-crafting-sunrise` (nw_percentile branch) -- all source files, factory patterns, test patterns, memory access patterns
- `CLAUDE.md` -- project conventions and testing guidelines
- `04-CONTEXT.md` -- locked decisions D-01 through D-16

### Secondary (MEDIUM confidence)
- None needed -- all findings verified directly in codebase

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no external dependencies, all existing QuestDB infrastructure
- Architecture: HIGH -- all patterns verified in existing codebase (LongSort, QuantileDisc*GroupByFunctionFactory, AbstractWindowFunctionFactory)
- Pitfalls: HIGH -- memory access patterns verified by reading MemoryCARWImpl, AbstractMemoryCR.addressOf(), GroupByDoubleList
- Code examples: HIGH -- derived directly from existing code with verified memory model

**Research date:** 2026-04-13
**Valid until:** 2026-05-13 (stable -- pure refactoring of existing code)
