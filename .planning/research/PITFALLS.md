# Domain Pitfalls

**Domain:** Percentile/quantile function hardening in QuestDB (zero-GC, off-heap Java)
**Researched:** 2026-04-13

## Critical Pitfalls

Mistakes that cause wrong results, crashes, or memory corruption.

### Pitfall 1: Quicksort Partition Operator Mismatch (`<` vs `<=`) Causes Silent Wrong Results

**What goes wrong:** The window function's `partition()` method uses strict `<` to compare elements against the pivot, while `GroupByDoubleList.partition()` uses `<=`. With strict `<`, when all elements equal the pivot, every element stays on the right side of the partition. The pivot lands at position `left`, the recursive call processes `[left+1, right]` (still all-equal), and the sort degrades to O(N^2). Worse: when you fix the sort but only partially (e.g., change `<` to `<=` in the window code without also adjusting the swap logic), the partition can place the pivot in the wrong final position, making `quickSelect` or `quickSort` return the wrong element for the requested index.

**Why it happens:** The original Lomuto partition scheme was transcribed from a textbook that assumed distinct elements. The fix seems trivial ("just change `<` to `<=`") but the Lomuto scheme with `<=` has its own degenerate case: it puts every element on the left when all are equal, producing the same O(N^2) behavior in the opposite direction. The correct fix requires a three-way partition (Dutch National Flag) or switching to the Hoare partition scheme, which handles equal elements naturally.

**Consequences:**
- O(N^2) CPU and stack depth on uniform data (all values equal), potential stack overflow on very large partitions.
- If the fix is incomplete (just flipping the operator without restructuring the partition), percentile_disc can return the wrong value for some indices. This is a silent correctness regression that passes tests unless equal-element test cases are added.

**Prevention:**
1. Use the existing `GroupByDoubleList.sort()` dual-pivot quicksort as the reference implementation -- it already has the three-way (Dutch National Flag) fallback for equal elements (the `else` branch in the sort method around the pivot equality check).
2. After extracting the shared utility, test with: (a) all-equal elements, (b) two distinct values, (c) already sorted input, (d) reverse sorted input, (e) single element, (f) two elements.
3. Add a regression test: `percentile_disc(col, 0.5) OVER ()` where col contains 10,000 identical values. Verify it returns the correct value AND completes within a reasonable time (under 1 second). This catches both the correctness and performance regressions.

**Detection:** Performance tests with uniform data. Correctness tests where expected percentile == the uniform value.

**Phase relevance:** Phase where quicksort is fixed (sort algorithm refactoring). Must be done BEFORE the utility extraction phase, so the utility starts life with the correct algorithm.

---

### Pitfall 2: Append-in-Place Offset Arithmetic Silently Corrupts Partition Data

**What goes wrong:** The current copy-on-append pattern in window `pass1()` stores each partition's value list as an offset from `listMemory.getPageAddress(0)`. When switching to append-in-place (where you keep a `(offset, size, capacity)` triple and grow in-place), you must update the offset stored in the `MapValue` after every reallocation. If the reallocation moves the data to a new location in `listMemory` but the old offset in the MapValue is not updated, subsequent reads use the stale offset and read garbage data or data from another partition.

**Why it happens:** The `MemoryARW` abstraction in QuestDB uses pages internally. `appendAddressFor()` returns an absolute address, and the code subtracts `getPageAddress(0)` to convert to a relative offset. After reallocation, the page address can change (especially if the memory crosses a page boundary). The pattern `listMemory.appendAddressFor(newSize) - listMemory.getPageAddress(0)` assumes a single-page layout. Multi-page scenarios silently break the arithmetic because different values in the same partition can end up on different pages, and the relative offset from page 0 becomes meaningless for data on page 1.

**Consequences:** Cross-partition data corruption: one partition's percentile reads another partition's values. The results look plausible (they are valid doubles) but are mathematically wrong. This is extremely difficult to diagnose in production because wrong percentile values are not obviously wrong unless you know the data distribution.

**Prevention:**
1. Consider using `GroupByDoubleList` (which uses `GroupByAllocator` with `Unsafe.malloc` / `Unsafe.realloc` -- single contiguous allocation, no page arithmetic) instead of raw `MemoryARW` offset tricks. The group-by functions already use this pattern successfully.
2. If sticking with `MemoryARW`, ensure you only ever use a single page (configure page size large enough) or switch to absolute addresses instead of relative offsets. Document the memory layout invariant.
3. Add a PARTITION BY test with at least 3 partitions of different sizes (e.g., 1, 100, 10000 values) and verify each partition's percentile is independent. This catches cross-partition corruption.

**Detection:** Test with multiple partitions where data distributions are very different (e.g., partition A has values near 0, partition B has values near 1,000,000). If partition B's percentile returns a value near 0, the offset arithmetic is broken.

**Phase relevance:** Phase where copy-on-append is replaced with append-in-place. This is the single most dangerous refactoring in the project.

---

### Pitfall 3: DirectArray State Machine Violation After ofNull() Produces Corrupt Array Output

**What goes wrong:** `DirectArray.ofNull()` sets `type = ColumnType.NULL`, clears `shape` and `strides`, and resets `flatViewLength = 0`. The non-null code path in `MultiPercentileDiscDoubleGroupByFunction.getArray()` only calls `setType()`, `setDimLen()`, and `applyShape()` during the first call (guarded by `out == null`). On subsequent calls where the previous call returned null but the current call has data, the `out` field is non-null but its state is `{type=NULL, shape=[], strides=[]}`. The code skips the initialization, writes data to a zero-length array, and produces undefined behavior.

**Why it happens:** The `DirectArray` is reused across groups to avoid allocation. The code assumes a linear progression: either it is always null or always non-null. But with GROUP BY, different groups alternate between null and non-null depending on their data. The initialization guard `if (out == null)` is wrong -- it should be `if (out == null || out.getType() == ColumnType.NULL)` or the type/shape should be re-applied unconditionally.

**Consequences:** First non-null group works. Any group that follows a null group gets a corrupt `DirectArray` -- the `putDouble()` call writes to a buffer that has not been resized for the new shape, potentially writing past the allocated memory (native heap corruption) or producing a malformed array result.

**Prevention:**
1. Always call `setType()`, `setDimLen()`, and `applyShape()` before populating `out`, regardless of whether `out` is null. Remove the `if (out == null)` guard for shape setup; keep it only for the `new DirectArray()` allocation.
2. Add a test with three groups: group A has data, group B has all NULLs, group C has data. Verify group C's array is correctly shaped and populated. This is the minimum test to catch the state machine violation.
3. Consider adding an assertion in `DirectArray.putDouble()` that the type is not `ColumnType.NULL` before writing.

**Detection:** Test with mixed NULL/non-NULL groups in a GROUP BY. The second non-null group after a null group is where the bug manifests.

**Phase relevance:** Phase where DirectArray reuse bug is fixed. Must be done BEFORE adding the `setEmpty()` override, because `setEmpty()` interacts with the same state.

---

### Pitfall 4: Memory Leak Cascade in Window Function Lifecycle Methods

**What goes wrong:** The `close()`, `reset()`, `reopen()`, and `toTop()` methods in window functions each have different contracts for what they clean up, but the percentile implementations do not follow all contracts consistently. Specifically:
- `reset()` in `MultiPercentileDiscOverPartitionFunction` calls `Misc.free(listMemory)` and `Misc.free(resultMemory)` but does NOT free the `DirectArray result` field.
- `reopen()` calls `listMemory.close()` and `resultMemory.close()` (not through `Misc.free`, which returns null) so the fields retain stale references to closed objects.
- `toTop()` truncates memories but does not clear the `result` field, leaving it pointing to stale data.

**Why it happens:** The `BasePartitionedWindowFunction` base class handles `map` cleanup in all four methods, but subclass-owned resources must be cleaned up by the subclass. The pattern is easy to get wrong because: (a) `Misc.free(x)` returns null and should be assigned back (`x = Misc.free(x)`), (b) `close()` and `reset()` have subtly different contracts (close = final cleanup; reset = prepare for reuse), (c) `reopen()` must restore the object to a usable state after `reset()`.

**Consequences:** Native memory leaks accumulate during long-running queries or repeated query execution. In QuestDB's zero-GC architecture, there is no finalizer safety net -- leaked native memory is never reclaimed.

**Prevention:**
1. Follow the `Misc.free(field)` pattern consistently: `result = Misc.free(result)` to both close the resource and null the reference.
2. Create a checklist for every window function class: for each native resource field (MemoryARW, DirectArray, Map), verify all four lifecycle methods (close, reset, reopen, toTop) handle it.
3. Tests that call `assertMemoryLeak()` will catch these -- but only if the test exercises the specific lifecycle path (e.g., running the query twice to trigger reopen). Add tests that run the same percentile query twice within the same assertMemoryLeak block.

**Detection:** `assertMemoryLeak()` tests that execute the query at least twice (triggering toTop/reopen cycles). Single-execution tests will not catch reopen/reset leaks.

**Phase relevance:** Phase where DirectArray leak is fixed. Should be fixed BEFORE the utility extraction, so the extracted utility has clean lifecycle semantics.

---

## Moderate Pitfalls

### Pitfall 5: Floating-Point Comparison Edge Cases in Extracted Sort Utility

**What goes wrong:** The window quickSort uses raw `<` / `>` comparisons on doubles. These do not handle NaN correctly: `NaN < x` is false, `NaN > x` is false, `NaN == x` is false. If NaN values leak into the sort input (despite the `Numbers.isFinite(d)` filter in `pass1`), the sort produces an undefined element ordering. Additionally, `-0.0 < +0.0` is false in Java's `<` operator, but `Double.compare(-0.0, +0.0)` returns -1. For percentile_disc this is harmless (both represent zero), but for percentile_cont's linear interpolation, the difference between `-0.0` and `+0.0` in an interpolation formula can produce unexpected sign results.

**Prevention:**
1. Keep the `Numbers.isFinite(d)` filter in `pass1()` as the primary defense -- NaN values should never enter the sort input. Add an assertion in the sort utility that the input contains no NaN.
2. For the extracted utility, document whether it handles NaN (it should not -- callers must filter NaN before sorting). This is safer than trying to handle NaN inside the sort, which makes the code more complex and slower.
3. If percentile_cont ever needs to handle `-0.0` correctly, use `Double.compare()` in the partition method instead of raw `<`. For now, document this as a known limitation.

**Detection:** Test with input containing explicit `-0.0` values mixed with `+0.0`.

**Phase relevance:** Phase where sort utility is extracted. The NaN contract should be documented in the utility's Javadoc.

---

### Pitfall 6: quickSelectMultiple Requires Sorted Indices -- Extraction Can Break Contract

**What goes wrong:** `GroupByDoubleList.quickSelectMultiple()` requires its `indices` array to be sorted in ascending order. The `MultiPercentileDiscDoubleGroupByFunction.getArray()` method sorts the indices using insertion sort before calling `quickSelectMultiple()`. If, during the extraction to a shared utility or during refactoring, the sorting step is accidentally removed, reordered, or applied to a copy that is not passed to `quickSelectMultiple()`, the results are silently wrong -- elements at the requested indices are NOT in their correct positions.

**Why it happens:** The `quickSelectMultiple` algorithm partitions and then decides which sub-array to recurse into based on which indices fall in each partition. If indices are not sorted, the binary split between left/right indices (`splitPoint` tracking) produces incorrect partition boundaries. The data appears sorted-ish but the specific elements at the target indices are wrong.

**Consequences:** Multi-percentile queries (e.g., `percentile_disc(value, [0.25, 0.5, 0.75])`) return wrong values. The values are within the data range so they look plausible, but they are not the correct percentiles.

**Prevention:**
1. Add an assertion at the top of `quickSelectMultiple()` that verifies `indices[i] <= indices[i+1]` for all i.
2. In the extracted utility or wrapper, have the utility sort the indices internally rather than relying on callers. This makes the API harder to misuse.
3. Add a test with percentiles `[0.75, 0.25, 0.5]` (deliberately unsorted) and verify the output matches `[0.25, 0.5, 0.75]` reordered correctly. This catches the case where the result order depends on index sort order.

**Detection:** Test with non-ascending percentile arrays. If results change when you reverse the percentile order, the index sorting is broken.

**Phase relevance:** Phase where shared utility is extracted and multi-percentile functions are refactored.

---

### Pitfall 7: Validation Error Position Points at Wrong Token

**What goes wrong:** QuestDB's `SqlException.$(position, msg)` convention requires the position to point at the specific offending character, not the start of the expression. When adding percentile validation (e.g., "percentile value must be between 0 and 1"), using `argPositions.getQuick(1)` is correct for the percentile argument position. But if the validation is moved to a different method (e.g., a shared validation helper), the position parameter can get lost or point at the function name instead of the argument.

**Why it happens:** In the current code, `percentilePos` is passed through the constructor chain. During refactoring, if a validation method is added that takes only the function reference (not the position), it must fabricate a position, and choosing the wrong one produces confusing error messages for users.

**Consequences:** Error messages like `[5] percentile value must be between 0 and 1` where position 5 points at `percentile_disc` instead of at the `1.5` argument. This is functionally correct (the query fails) but violates QuestDB's error reporting convention and confuses users.

**Prevention:**
1. Always pass both the `Function` and its `int position` together. Consider creating a record or parameter object `(Function func, int pos)` for the validation helper.
2. Add error-path tests that verify the exception message includes the correct position. QuestDB's test pattern is: `Assert.assertEquals("[28] error message", e.getMessage())`.
3. Reference the existing EMA validation pattern in `EmaDoubleWindowFunctionFactory.java` lines 121-138, which correctly uses `argPositions.getQuick(N)`.

**Detection:** Error-path tests that assert the exact error position in the SqlException message.

**Phase relevance:** Phase where factory validation is added. Small effort but easy to get wrong during extraction.

---

### Pitfall 8: Zero-GC Violation from `new double[]` in preparePass2

**What goes wrong:** The `MultiPercentileDiscDoubleWindowFunctionFactory` whole-result-set function allocates `results = new double[percentileCount]` inside `preparePass2()`. This is a Java heap allocation on a data path, violating QuestDB's zero-GC principle. While `preparePass2()` runs once per query (not per row), the allocation size depends on user input (the percentile array length), and in repeated query execution or cursor reopen scenarios, it creates GC pressure.

**Why it happens:** The developer needed a temporary buffer to hold results. In standard Java this is trivially `new double[n]`. In QuestDB's zero-GC world, temporary buffers must be pre-allocated and reused.

**Consequences:** Minor GC pressure per query. Not a correctness issue, but violates a core architectural principle. During code review this will be flagged and require rework.

**Prevention:**
1. Pre-allocate the `double[]` array as a field and grow it lazily: `if (results == null || results.length < percentileCount) results = new double[percentileCount]`. The array then survives across `toTop()` calls.
2. Alternatively, write results directly into `resultMemory` (MemoryARW) and read them back, avoiding the Java array entirely.
3. During the utility extraction phase, ensure the utility does not allocate any Java arrays internally. All buffers should be caller-provided or use off-heap memory.

**Detection:** Code review. QuestDB does not have an automated GC-allocation checker, so this relies on human review and the project convention.

**Phase relevance:** Phase where the zero-GC violation is fixed. Can be done independently of other fixes.

---

## Minor Pitfalls

### Pitfall 9: function_list.txt Registration Order Matters

**What goes wrong:** QuestDB's function registry loads factories from `function_list.txt` in order. If a more specific signature (e.g., `percentile_disc(DD[])` for multi-percentile) is registered before a less specific one (e.g., `percentile_disc(DD)` for single-percentile), or vice versa, the function resolution may pick the wrong factory. The registry uses first-match semantics for ambiguous overloads.

**Prevention:**
1. Register single-argument variants before multi-argument variants.
2. Add a test that calls both `percentile_disc(value, 0.5)` and `percentile_disc(value, [0.25, 0.5, 0.75])` in the same test to verify both resolve correctly.
3. Check existing patterns: `ApproxPercentileDoubleGroupByFunction` and its multi variant for registration order.

**Detection:** Integration tests that exercise both overloads in the same session.

**Phase relevance:** Phase where missing function_list.txt entries are added.

---

### Pitfall 10: Alias Registration for quantile_disc/quantile_cont Window Functions Misses Factory

**What goes wrong:** Adding SQL aliases (quantile_disc -> percentile_disc) requires either: (a) registering the same factory class twice with different names, or (b) having the factory respond to multiple signatures. If the alias factory is a new class that delegates to the original but has a bug in delegation (e.g., forgetting to pass through `percentilePos`), the alias produces different results than the original.

**Prevention:**
1. Follow the existing alias pattern in QuestDB. Check how other aliases are implemented (e.g., search for factories that delegate to other factories).
2. Add a test that runs the exact same query with `percentile_disc` and `quantile_disc` and asserts identical results.

**Detection:** Pairwise tests comparing alias results to canonical function results.

**Phase relevance:** Phase where quantile aliases are added for window functions.

---

### Pitfall 11: Test Migration from assertSql to assertQueryNoLeakCheck Changes Assertion Semantics

**What goes wrong:** Switching tests from `assertSql()` to `assertQueryNoLeakCheck()` adds factory property assertions (`supportsRandomAccess`, `expectSize`, `expectedTimestamp`). If the percentile functions return incorrect metadata for these properties, previously passing tests will fail -- not because of a data regression, but because of incorrect factory metadata.

**Prevention:**
1. When migrating each test, first run it with `assertQueryNoLeakCheck()` and observe which factory property assertions fail. Fix the function factory metadata, not the test parameters.
2. Specifically check: percentile window functions should support random access (they compute the full result in preparePass2), should report expected size (row count is known), and should NOT have an expected timestamp column.

**Detection:** Run the migrated tests and observe failure messages -- they will explicitly say which factory property failed.

**Phase relevance:** Phase where tests are migrated. Low risk but annoying if done without understanding the semantic difference.

---

## Phase-Specific Warnings

| Phase Topic | Likely Pitfall | Mitigation |
|-------------|---------------|------------|
| QuickSort fix | Pitfall 1 (operator mismatch) | Use three-way partition or adopt GroupByDoubleList's dual-pivot sort. Test all-equal-elements case explicitly. |
| Copy-on-append -> append-in-place | Pitfall 2 (offset corruption) | Prefer GroupByDoubleList pattern over raw MemoryARW offset arithmetic. Test multi-partition with different sizes. |
| DirectArray reuse bug | Pitfall 3 (state machine) | Always re-apply type/shape before populating. Test null-then-non-null group sequence. |
| DirectArray leak fix | Pitfall 4 (lifecycle methods) | Audit all four lifecycle methods per resource. Run queries twice in assertMemoryLeak. |
| Shared sort utility extraction | Pitfalls 1, 5, 6 (algorithm, NaN, index contract) | Start with correct algorithm (not current broken one). Document NaN contract. Sort indices internally. |
| Factory validation | Pitfall 7 (error position) | Pass position alongside Function. Assert exact error position in tests. |
| Zero-GC fix | Pitfall 8 (heap allocation) | Pre-allocate and reuse buffers. No new arrays in data-path methods. |
| function_list.txt registration | Pitfall 9 (order) | Test both overloads in same session. |
| Quantile aliases | Pitfall 10 (delegation) | Pairwise result comparison tests. |
| Test migration | Pitfall 11 (assertion semantics) | Understand factory property semantics before migrating. |

## Ordering Recommendation

The pitfalls have dependency chains that constrain phase ordering:

1. **Fix correctness bugs first** (Pitfalls 1, 3, 4) -- the sort algorithm, DirectArray state, and lifecycle leaks must be correct before extracting utilities. Otherwise the utility codifies bugs.
2. **Fix performance** (Pitfall 2) -- the append-in-place refactoring is the riskiest change and benefits from having correct tests already in place.
3. **Extract utilities** (Pitfalls 5, 6) -- only after the algorithm is correct and tested.
4. **Add validation** (Pitfalls 7, 8) -- lower risk, can be done in parallel with utility extraction.
5. **Registration and aliases** (Pitfalls 9, 10, 11) -- mechanical changes, lowest risk, do last.

## Sources

- Direct code analysis of `nw_percentile` branch (QuestDB PR #6680)
- `PercentileDiscDoubleWindowFunctionFactory.java` -- window sort implementation with strict `<` partition
- `GroupByDoubleList.java` -- reference dual-pivot quicksort and quickSelect with `<=` partition
- `MultiPercentileDiscDoubleGroupByFunction.java` -- DirectArray reuse pattern and quickSelectMultiple usage
- `MultiPercentileDiscDoubleWindowFunctionFactory.java` -- copy-on-append pattern and lifecycle methods
- `BasePartitionedWindowFunction.java` -- lifecycle contract (close/reset/reopen/toTop)
- `DirectArray.java` -- ofNull/setType/applyShape state transitions
- `EmaDoubleWindowFunctionFactory.java` -- reference validation pattern for constant arguments
- `Misc.java` -- `free()` returns null pattern
- `AbstractStdDevDoubleWindowFunctionFactory.java` -- reference window function structure

---

*Pitfalls audit: 2026-04-13*
