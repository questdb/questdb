# Codebase Concerns

**Analysis Date:** 2026-04-13

## Critical Bugs

### DirectArray Result Reuse Bug in Multi-Array Group-By Functions

**Issue:** `DirectArray` state management is inconsistent between null and non-null paths in multi-percentile group-by functions. When `ofNull()` is called (lines 121-122, 130-131 in `MultiPercentileDiscDoubleGroupByFunction.java`), it clears type and shape. However, the non-null code path (lines 138-142) re-applies type and shape, but this is only done once and then reused without validation on subsequent calls.

**Files affected:**
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MultiPercentileDiscDoubleGroupByFunction.java` (lines 115-143)
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MultiPercentileContDoubleGroupByFunction.java` (lines 50-139)

**Impact:** Race conditions or incorrect array dimensions when computing results for different groups with varying percentile counts. Array dimensions may not be properly reset between records, causing incorrect indexing.

**Fix approach:** Ensure `out.setDimLen()` and `out.applyShape()` are called every time before populating results, not just during initialization. Check for existing shape/type and reset if needed.

---

### O(N²) Memory + CPU in Partitioned Window Pass1 - Copy-on-Append Pattern

**Issue:** The `pass1` method in window functions (e.g., `PercentileDiscDoubleWindowFunctionFactory.java` lines 178-209) uses a copy-on-append pattern: every time a new value is added to a partition, all existing values are copied to a new allocation. This is O(N²) in memory allocations and CPU cost.

**Files affected:**
- `core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileDiscDoubleWindowFunctionFactory.java` (lines 194-206)
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileDiscDoubleWindowFunctionFactory.java` (lines 224-242)
- Similar patterns in `PercentileContDoubleWindowFunctionFactory.java` and `MultiPercentileContDoubleWindowFunctionFactory.java`

**Code pattern:**
```java
// Line 198-205 in PercentileDiscDoubleWindowFunctionFactory
long newPtr = listMemory.appendAddressFor(8 + (size + 1) * 8) - listMemory.getPageAddress(0);
listMemory.putLong(newPtr, size + 1); // new size
// Copy old values
for (long i = 0; i < size; i++) {
    listMemory.putDouble(newPtr + 8 + i * 8, listMemory.getDouble(listPtr + 8 + i * 8));
}
listMemory.putDouble(newPtr + 8 + size * 8, d);
```

**Impact:** For a partition with N values, memory allocations = N*(N+1)/2. With 10K values in a partition, this is ~50M allocations. CPU cost is similarly quadratic. Severe performance degradation on large partitions.

**Fix approach:** Use an append-friendly data structure like a resizable array (similar to `GroupByDoubleList`) that grows by a factor (e.g., 1.5x or 2x) rather than by a single element each time. Store offset+size, not a full copy.

---

### QuickSort O(N²) Degenerate Case on All-Equal Elements

**Issue:** The custom `quickSort` implementations in window functions use Lomuto partition with strict `<` comparison (lines 304 in `PercentileDiscDoubleWindowFunctionFactory.java`). When all elements are equal, the pivot partitions into one side only, causing O(N²) time and stack depth.

**Files affected:**
- `core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileDiscDoubleWindowFunctionFactory.java` (lines 313-324, partition method lines 286-311)
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileDiscDoubleWindowFunctionFactory.java` (lines 351-362, partition method lines 324-349)
- `core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileContDoubleWindowFunctionFactory.java` (implicitly same)
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileContDoubleWindowFunctionFactory.java` (implicitly same)

**Code pattern in partition (line 304):**
```java
for (long j = left; j < right; j++) {
    if (listMemory.getDouble(listPtr + j * 8) < pivot) {  // strict < causes issue
        i++;
        swap(listPtr, i, j);
    }
}
```

**Impact:** Queries with percentiles on uniformly-valued columns (e.g., all values = 0) degrade to O(N²) time, causing timeouts on large datasets.

**Fix approach:** Use a three-way partition (Dutch National Flag) that handles equal elements separately, or use `<=` in partition and adjust pivot handling. Alternatively, use Dual-Pivot Quicksort (already in `GroupByDoubleList.java`) which handles this case better.

---

### Multi-Approx_Percentile Silently Accepts Invalid Percentiles

**Issue:** `ApproxPercentile` functions perform no validation that percentile arguments are in the range [0.0, 1.0]. Invalid values silently produce incorrect results or undefined behavior.

**Files affected:**
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/ApproxPercentileDoubleGroupByFunction.java` (no validation in constructor or `computeFirst`)
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/ApproxPercentileLongGroupByFunction.java`
- Multi variants: `MultiApproxPercentileDoubleGroupByFunction.java`, `MultiApproxPercentileLongGroupByFunction.java`

**Impact:** User error queries with percentiles like 1.5 or -0.5 silently execute instead of failing fast. Results are meaningless but not obviously wrong.

**Fix approach:** Add validation in factory `newInstance()` methods or `init()` to check that percentile function is constant and within [0.0, 1.0]. Throw `SqlException` if not.

---

### No Validation that Percentile Argument is Constant in Window Functions

**Issue:** Window function percentile arguments must be constant (same value for every row), but there is no validation of this constraint. Non-constant percentile values would produce incorrect results.

**Files affected:**
- `core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileDiscDoubleWindowFunctionFactory.java` (line 79, no check)
- `core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileContDoubleWindowFunctionFactory.java` (similar)
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileDiscDoubleWindowFunctionFactory.java` (line 80, no check)
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileContDoubleWindowFunctionFactory.java` (similar)

**Impact:** Queries like `percentile_disc(value, CASE WHEN x > 5 THEN 0.5 ELSE 0.25 END)` compile and execute incorrectly, returning the same percentile for all rows.

**Fix approach:** Validate that percentile function(s) are constant in `newInstance()`. Use `function.isConstant()` or similar flag.

---

### DirectArray Leak in Multi* Window Reset/Reopen/toTop

**Issue:** `DirectArray result` field (line 123 in `MultiPercentileDiscDoubleWindowFunctionFactory.java`) is not explicitly freed in `reset()` and `reopen()` methods before clearing or re-initializing. If `DirectArray` holds native memory, this causes a leak.

**Files affected:**
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileDiscDoubleWindowFunctionFactory.java` (lines 294-305)
  - `reset()` calls `Misc.free(listMemory)` but not `result`
  - `reopen()` calls `listMemory.close()` but not `result`
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileContDoubleWindowFunctionFactory.java` (similar pattern)

**Impact:** Each reset/reopen leaks DirectArray memory. In long-running queries with repeated window execution, this accumulates.

**Fix approach:** Add `Misc.free(result)` before reassigning in `reset()` and `reopen()`. Set `result = null` after freeing.

---

## Performance Issues

### PercentileDiscLong Uses O(n log n) Sort Instead of O(n) QuickSelect

**Issue:** `PercentileDiscLongGroupByFunction.java` line 115 calls `listA.sort()` (full sort) instead of `quickSelect()` like the Double variant.

**Files affected:**
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/PercentileDiscLongGroupByFunction.java` (line 115)

**Code:**
```java
listA.sort();  // O(n log n) full sort
double percentile = percentileFunc.getDouble(record);
double multiplier = SqlUtil.getPercentileMultiplier(percentile, percentilePos);
int N = (int) Math.max(0, Math.ceil(size * multiplier) - 1);
return listA.getQuick(N);  // Only need one element
```

**Impact:** 2-3x slower than necessary for single percentiles.

**Fix approach:** Check if `GroupByLongList` has `quickSelect()` method. If not, implement it. Change line 115 to `listA.quickSelect(0, size - 1, N)`.

---

### ~320 Lines of QuickSort Duplicated Across 4 Window Factories

**Issue:** The `quickSort()`, `partition()`, and `swap()` methods are duplicated ~320 lines across:
- `PercentileDiscDoubleWindowFunctionFactory.java` (lines 286-330)
- `MultiPercentileDiscDoubleWindowFunctionFactory.java` (lines 324-368)
- `PercentileContDoubleWindowFunctionFactory.java` (implicitly same)
- `MultiPercentileContDoubleWindowFunctionFactory.java` (implicitly same)

**Impact:** Code duplication makes maintenance harder. Bug fixes need to be applied in 4+ places. Increases binary size.

**Fix approach:** Extract into a shared utility class `WindowMemoryQuickSort` with static methods `sort(MemoryARW, long, long, long)` and `sortWithOffset(MemoryARW, long, long, long, long)`. Use in all factories.

---

## Missing Test Coverage

### Quantile Aliases Not Tested

**Issue:** SQL aliases `quantile_disc` and `quantile_cont` (which map to `percentile_disc` and `percentile_cont`) are not explicitly tested.

**Files affected:**
- Test files in `core/src/test/java/io/questdb/test/griffin/engine/functions/groupby/`
- Test files in `core/src/test/java/io/questdb/test/griffin/engine/window/`

**Impact:** Regression risk if aliases are not maintained or are accidentally broken.

**Fix approach:** Add test cases for `quantile_disc(...)` and `quantile_cont(...)` in existing test files, ensuring they produce identical results to `percentile_disc()` and `percentile_cont()`.

---

### PARTITION BY Not Tested in Window Functions

**Issue:** Window function tests focus on `over ()` (whole result set) but lack comprehensive coverage of `over (partition by ...)` cases with multiple partitions.

**Files affected:**
- `core/src/test/java/io/questdb/test/griffin/engine/window/PercentileWindowFunctionTest.java`

**Gaps:**
- Multi-value partitions with different partition sizes
- Empty partitions (NULL or no matching rows)
- Single-value partitions (edge case: size=1)
- Partitions with all equal values

**Impact:** Bugs in partitioned execution (like the copy-on-append O(N²) issue) may not be caught by existing tests.

**Fix approach:** Add test methods:
- `testPercentileDiscPartitionByWithVaryingPartitionSizes()`
- `testPercentileContPartitionByWithSingleValuePartition()`
- `testPercentileDiscPartitionByWithAllEqualValues()`

---

### Error Paths Not Tested

**Issue:** No tests for invalid input errors:
- Percentiles outside [0.0, 1.0]
- Non-constant percentile in window functions
- NULL or empty input arrays

**Files affected:**
- Test files in `core/src/test/java/io/questdb/test/griffin/engine/functions/groupby/`

**Impact:** Regression risk if error handling is changed or removed.

**Fix approach:** Add test methods:
- `testPercentileDiscWithInvalidPercentile()` - expects SqlException
- `testPercentileContWithNullValues()` - expects NaN result
- `testMultiPercentileWithEmptyArray()` - expects null array result

---

## Missing Entries in function_list.txt

**Issue:** Group-by factories for `percentile_disc` and `percentile_cont` are not registered in `core/src/main/resources/function_list.txt`. Window function entries are present (lines 813-816 approximate), but no group-by entries.

**Files affected:**
- `core/src/main/resources/function_list.txt`

**Missing entries (10+ factories):**
- `PercentileDiscDoubleGroupByFunctionFactory`
- `PercentileDiscLongGroupByFunctionFactory`
- `PercentileContDoubleGroupByFunctionFactory`
- `MultiPercentileDiscDoubleGroupByFunctionFactory`
- `MultiPercentileContDoubleGroupByFunctionFactory`

**Impact:** Function discovery system may not work correctly. If function_list.txt is used for reflection-based loading, these functions won't be registered.

**Fix approach:** Add a `# 'percentile_disc' and 'percentile_cont' aggregate functions` section immediately before the window functions section with entries:
```
io.questdb.griffin.engine.functions.groupby.PercentileDiscDoubleGroupByFunctionFactory
io.questdb.griffin.engine.functions.groupby.PercentileDiscLongGroupByFunctionFactory
io.questdb.griffin.engine.functions.groupby.PercentileContDoubleGroupByFunctionFactory
io.questdb.griffin.engine.functions.groupby.MultiPercentileDiscDoubleGroupByFunctionFactory
io.questdb.griffin.engine.functions.groupby.MultiPercentileContDoubleGroupByFunctionFactory
io.questdb.griffin.engine.functions.groupby.PercentileDiscDoubleGroupByFunctionFactory
io.questdb.griffin.engine.functions.groupby.PercentileContDoubleGroupByFunctionFactory
```

---

## Architectural Concerns

### Inconsistent Selection Strategy Between Group-By and Window Functions

**Issue:** Group-by functions use `quickSelect()` (O(n)) for single percentiles and `quickSelectMultiple()` for multi-percentiles. Window functions re-implement custom `quickSort()` (O(n log n) worst case). This inconsistency means window percentiles are slower and less robust.

**Impact:** Performance regression when using window functions vs. group-by aggregates.

**Fix approach:** Refactor window functions to use `GroupByDoubleList.quickSelect()` pattern or extract sorting logic into shared utility.

---

### No Parameterization for Percentile Calculation Method

**Issue:** Both `percentile_disc` and `percentile_cont` hardcode their calculation method (discrete vs. linear interpolation). There's no pluggable strategy for alternative methods (e.g., R7-R9 in statistical packages).

**Impact:** Future enhancement to support alternative methods requires code duplication.

**Fix approach:** Document as design limitation for now. Consider creating `PercentileCalculator` interface for future flexibility.

---

## Fragile Areas

### MultiPercentile Functions Depend on Index Ordering

**Issue:** `MultiPercentileDiscDoubleGroupByFunction.java` lines 146-168 and `MultiPercentileContDoubleGroupByFunction.java` lines 97-114 manually sort indices using insertion sort before calling `quickSelectMultiple()`. The sorting is critical for correctness of `quickSelectMultiple()`, but there's no assertion or documentation enforcing this requirement.

**Files affected:**
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MultiPercentileDiscDoubleGroupByFunction.java` (lines 159-168)
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MultiPercentileContDoubleGroupByFunction.java` (lines 105-114)

**Risk:** If `quickSelectMultiple()` contract is not preserved (e.g., indices must be sorted), silent data corruption.

**Fix approach:** Add assertions before calling `quickSelectMultiple()` to verify indices are sorted:
```java
for (int i = 0; i < viewLength - 1; i++) {
    assert indices[i] <= indices[i + 1];
}
```

---

### Window Memory Layout Not Documented

**Issue:** The memory layout in window functions (size header + values) is not documented. `PercentileDiscDoubleWindowFunctionFactory.java` lines 189-205 assume a specific layout:
- 8 bytes for size at offset 0
- 8 bytes per value starting at offset 8
This is easy to get wrong if modified.

**Fix approach:** Add a comment block documenting the layout at the start of `pass1()`:
```java
// Memory layout for value list:
// [0-7]:     size (long)
// [8+]:      values (doubles)
```

---

## Summary of Severity

| Issue | Severity | Type | Effort |
|-------|----------|------|--------|
| DirectArray reuse bug | **CRITICAL** | Bug | Medium |
| O(N²) copy-on-append in window pass1 | **CRITICAL** | Performance | Large |
| QuickSort O(N²) on equal elements | **HIGH** | Performance | Medium |
| Invalid percentile validation | **MEDIUM** | Bug | Small |
| Non-constant window percentile validation | **MEDIUM** | Bug | Small |
| DirectArray leak in reset/reopen | **MEDIUM** | Leak | Small |
| PercentileDiscLong uses sort not select | **MEDIUM** | Performance | Small |
| QuickSort duplication | **LOW** | Maintenance | Medium |
| Missing test coverage | **MEDIUM** | Testing | Medium |
| Missing function_list.txt entries | **MEDIUM** | Integration | Small |

---

*Concerns audit: 2026-04-13*
