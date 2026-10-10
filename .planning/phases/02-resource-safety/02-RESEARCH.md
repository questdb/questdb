# Phase 2: Resource Safety - Research

**Researched:** 2026-04-13
**Domain:** Native memory lifecycle and zero-GC compliance in QuestDB Multi* window percentile functions
**Confidence:** HIGH

## Summary

Phase 2 fixes two resource safety bugs in 4 inner classes across 2 factory files (MultiPercentileDiscDoubleWindowFunctionFactory and MultiPercentileContDoubleWindowFunctionFactory). RES-01 addresses DirectArray `result` field leaking native memory when lifecycle methods (reset/reopen/toTop) fail to free it. RES-02 addresses a zero-GC violation where `preparePass2()` heap-allocates a `new double[percentileCount]` on every call in the OverWholeResultSet variants.

Both fixes are mechanical, following established QuestDB patterns. The `Misc.free(result)` idiom frees native memory and returns null in one call. The `double[] results` pre-allocation is straightforward because Phase 1 (COR-04) now guarantees percentile arguments are constant, meaning `percentileCount` never changes between calls. The 4 affected inner classes are: MultiPercentileDiscOverPartitionFunction, MultiPercentileDiscOverWholeResultSetFunction, MultiPercentileContOverPartitionFunction, MultiPercentileContOverWholeResultSetFunction.

**Primary recommendation:** Fix RES-01 first (lifecycle leak) across all 4 inner classes, then RES-02 (zero-GC) in the 2 OverWholeResultSet classes. Both fixes can be in a single plan since they are small and independent.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- D-01: Use `result = Misc.free(result)` pattern in `reset()`, `reopen()`, and `toTop()` of all Multi* window function inner classes. This frees native memory and nulls the reference in one call. Matches QuestDB convention (BasePartitionedWindowFunction, etc.).
- D-02: Apply to both MultiPercentileDisc and MultiPercentileCont factories, in both OverPartitionFunction and OverWholeResultSetFunction inner classes (4 total classes affected).
- D-03: Also verify `close()` already frees correctly (it does per review, but confirm during implementation).
- D-04: Pre-allocate `double[] results` as a persistent field. In preparePass2(): only reallocate if `results == null || results.length < percentileCount`. Since percentileCount is now guaranteed constant (COR-04 from Phase 1 enforces constness), the array is allocated exactly once per function lifetime and reused.
- D-05: On `reopen()`/`reset()`: do NOT null the results array -- let it be reused. Only `close()` should release it (set to null).
- D-06: This applies to MultiPercentileDiscOverWholeResultSetFunction and MultiPercentileContOverWholeResultSetFunction (and their partitioned variants if they have the same pattern).
- D-07: Verify fixes using assertMemoryLeak() with reopen cycles: run a Multi* window query, call cursor.toTop(), re-iterate to trigger reopen/toTop paths. Any native memory leak fails the test.

### Claude's Discretion
- Whether to also fix similar patterns in the partitioned variants (OverPartitionFunction) if they have the same issue
- Exact placement of Misc.free() calls relative to other cleanup in reset/reopen/toTop

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| RES-01 | Multi* window functions must free DirectArray `result` in reset(), reopen(), and toTop() via Misc.free() | Confirmed: all 4 inner classes have a `DirectArray result` field that `close()` frees but `reset()`/`reopen()`/`toTop()` do not. The `Misc.free()` pattern returns null, preventing double-free. See Architecture Patterns below for exact current code and fix patterns. |
| RES-02 | Multi* window preparePass2 must pre-allocate and reuse double[] rather than heap-allocating per call | Confirmed: both OverWholeResultSet classes allocate `results = new double[percentileCount]` on every `preparePass2()` call. Since COR-04 (Phase 1) guarantees percentileCount is constant, the array can be allocated once and reused. The partitioned variants (OverPartitionFunction) do NOT have this issue -- they compute results into `resultMemory` (MemoryARW), not a `double[]` field. |
</phase_requirements>

## Project Constraints (from CLAUDE.md)

- **Member ordering:** Class members grouped by kind (static vs. instance) and visibility, sorted alphabetically. New methods inserted in correct alphabetical position.
- **Modern Java:** Use enhanced switch, multiline string literals, pattern variables.
- **NULL handling:** Always consider NULL paths. Distinguish uninitialized from actual NULL.
- **ObjList over T[]:** For object arrays, but `double[]` is a primitive array so this does not apply.
- **Tests:** Use `assertMemoryLeak()`. Use `assertQueryNoLeakCheck()` for query result assertions. Use `execute()` for DDL. Prefer UPPERCASE SQL keywords. Use multiline strings. Use underscores for numbers >= 5 digits.
- **Resource leaks:** Think carefully about all code paths, especially error paths.
- **Build:** `mvn -Dtest=ClassName test` for test. Do NOT run multiple `mvn test` in parallel.

## Architecture Patterns

### RES-01: DirectArray Lifecycle Leak Fix

**What:** The `DirectArray result` field in all 4 Multi* inner classes owns native memory (allocated via `Unsafe.malloc` in DirectArray). The `close()` method correctly calls `Misc.free(result)`, but `reset()`, `reopen()`, and `toTop()` do not. When the window function framework calls these lifecycle methods (e.g., when a cursor is reopened for a second iteration), the old DirectArray's native memory leaks because a new one is allocated without freeing the old one. [VERIFIED: nw_percentile branch source code]

**Current buggy code in all 4 inner classes:**

Partitioned variants (`MultiPercentileDiscOverPartitionFunction`, `MultiPercentileContOverPartitionFunction`):
```java
// Current -- result is ONLY freed in close(), not in reset/reopen/toTop
@Override
public void close() {
    super.close();
    Misc.free(percentilesFunc);
    Misc.free(listMemory);
    Misc.free(resultMemory);
    Misc.free(result);              // <-- only here
}

@Override
public void reopen() {
    super.reopen();                 // calls map.reopen()
    listMemory.close();
    resultMemory.close();
    // result NOT freed -- LEAK
}

@Override
public void reset() {
    super.reset();                  // calls Misc.free(map)
    Misc.free(listMemory);
    Misc.free(resultMemory);
    // result NOT freed -- LEAK
}

@Override
public void toTop() {
    super.toTop();                  // calls Misc.clear(map) + arg.toTop()
    listMemory.truncate();
    resultMemory.truncate();
    // result NOT freed -- LEAK
}
```

WholeResultSet variants (`MultiPercentileDiscOverWholeResultSetFunction`, `MultiPercentileContOverWholeResultSetFunction`):
```java
// Current -- result freed only in close()
@Override
public void close() {
    super.close();
    Misc.free(percentilesFunc);
    Misc.free(listMemory);
    Misc.free(result);              // <-- only here
}

@Override
public void reopen() {
    listMemory.close();
    size = 0;
    results = null;
    // result NOT freed -- LEAK
}

@Override
public void reset() {
    super.reset();
    Misc.free(listMemory);
    size = 0;
    results = null;
    // result NOT freed -- LEAK
}

@Override
public void toTop() {
    super.toTop();
    listMemory.truncate();
    size = 0;
    results = null;
    // result NOT freed -- LEAK
}
```

**Fix pattern (D-01):** Add `result = Misc.free(result)` to all three lifecycle methods in all 4 classes.

```java
// Partitioned variant fix:
@Override
public void reopen() {
    super.reopen();
    listMemory.close();
    resultMemory.close();
    result = Misc.free(result);
}

@Override
public void reset() {
    super.reset();
    Misc.free(listMemory);
    Misc.free(resultMemory);
    result = Misc.free(result);
}

@Override
public void toTop() {
    super.toTop();
    listMemory.truncate();
    resultMemory.truncate();
    result = Misc.free(result);
}
```

```java
// WholeResultSet variant fix:
@Override
public void reopen() {
    listMemory.close();
    size = 0;
    result = Misc.free(result);
    // Do NOT null results -- per D-05
}

@Override
public void reset() {
    super.reset();
    Misc.free(listMemory);
    size = 0;
    result = Misc.free(result);
    // Do NOT null results -- per D-05
}

@Override
public void toTop() {
    super.toTop();
    listMemory.truncate();
    size = 0;
    result = Misc.free(result);
    // Do NOT null results -- per D-05
}
```

**Important nuance for toTop():** `toTop()` semantically resets iteration position without necessarily freeing resources. However, for DirectArray `result`, the array is lazily re-created in `getArray()` when `result == null`, so freeing it in `toTop()` is safe. The next `getArray()` call allocates a fresh DirectArray. This matches the existing pattern in `PercentileDiscDoubleWindowFunctionFactory` where `result = Double.NaN` resets the scalar result in `toTop()`. [VERIFIED: PercentileDiscDoubleWindowFunctionFactory.java lines 435-439]

**D-03 verification:** `close()` already frees `result` correctly in all 4 classes: `Misc.free(result)` is present in every `close()` method. [VERIFIED: nw_percentile branch source code]

### RES-02: Zero-GC double[] Allocation Fix

**What:** The `OverWholeResultSet` variants allocate `results = new double[percentileCount]` inside `preparePass2()` on every call. This violates QuestDB's zero-GC principle for data paths. The allocation triggers GC pressure and is unnecessary because `percentileCount` is constant (guaranteed by COR-04 from Phase 1). [VERIFIED: nw_percentile branch source code]

**Current buggy code (both Disc and Cont WholeResultSet):**
```java
@Override
public void preparePass2() {
    if (size == 0) {
        results = null;     // <-- sets to null, forcing re-allocation
        return;
    }
    // ...
    results = new double[percentileCount];  // <-- heap allocation every call
    for (int i = 0; i < percentileCount; i++) {
        // ... compute results[i]
    }
}
```

**Fix (D-04, D-05):** Make `results` a persistent field. Reallocate only if the array is null or too small. Do NOT null it in `reopen()`/`reset()` (only in `close()`).

```java
@Override
public void preparePass2() {
    if (size == 0) {
        return;     // <-- do NOT null results; leave previous array allocated
    }

    ArrayView percentiles = percentilesFunc.getArray(null);
    FlatArrayView view = percentiles.flatView();
    int percentileCount = view.length();

    // Sort the list
    quickSort(0, size - 1);

    // Pre-allocate or reuse results array
    if (results == null || results.length < percentileCount) {
        results = new double[percentileCount];
    }

    for (int i = 0; i < percentileCount; i++) {
        // ... compute results[i] (unchanged)
    }
}
```

**Key behavioral change for empty-data case:** When `size == 0`, the current code sets `results = null`. The `getArray()` method checks `if (results != null)` to decide whether to write data or call `ofNull()`. After the fix, when `size == 0`, `results` is NOT nulled. This means `getArray()` would return stale data from a previous execution. The fix must handle this:

Option A: Keep a boolean flag `isResultValid` instead of checking `results != null`:
```java
private boolean isResultValid;

@Override
public void preparePass2() {
    if (size == 0) {
        isResultValid = false;
        return;
    }
    // ... compute ...
    isResultValid = true;
}

@Override
public ArrayView getArray(Record rec) {
    if (result == null) {
        result = new DirectArray();
        result.setType(type);
    }
    if (isResultValid) {
        result.setDimLen(0, results.length);
        result.applyShape();
        for (int i = 0; i < results.length; i++) {
            result.putDouble(i, results[i]);
        }
        return result;
    }
    result.ofNull();
    return result;
}
```

Option B: Track the valid count separately from array capacity:
```java
private int resultCount;

@Override
public void preparePass2() {
    if (size == 0) {
        resultCount = 0;
        return;
    }
    // ... allocate/reuse results ...
    resultCount = percentileCount;
    // ... compute ...
}

@Override
public ArrayView getArray(Record rec) {
    // ... use resultCount > 0 instead of results != null
}
```

**Recommendation:** Option A (boolean flag) is simpler and more explicit. It does not change the semantics of `getArray()` except the condition check. [ASSUMED]

**Scope for OverPartitionFunction:** The partitioned variants do NOT have the `double[] results` field. They store results in `resultMemory` (MemoryARW). So RES-02 only applies to the 2 OverWholeResultSet classes. [VERIFIED: nw_percentile branch source code]

### RES-01 + RES-02 interaction for WholeResultSet reopen/reset/toTop

The current code nulls `results` in `reopen()`, `reset()`, and `toTop()`. Per D-05, we stop nulling it. Combined with the empty-data fix above, the updated lifecycle for WholeResultSet variants looks like:

```java
@Override
public void reopen() {
    listMemory.close();
    size = 0;
    result = Misc.free(result);
    isResultValid = false;         // or resultCount = 0
    // results NOT nulled -- reused
}

@Override
public void reset() {
    super.reset();
    Misc.free(listMemory);
    size = 0;
    result = Misc.free(result);
    isResultValid = false;
    // results NOT nulled -- reused
}

@Override
public void toTop() {
    super.toTop();
    listMemory.truncate();
    size = 0;
    result = Misc.free(result);
    isResultValid = false;
    // results NOT nulled -- reused
}

@Override
public void close() {
    super.close();
    Misc.free(percentilesFunc);
    Misc.free(listMemory);
    Misc.free(result);
    results = null;                 // only null here
}
```

### Test pattern: assertMemoryLeak with reopen cycles (D-07)

The test infrastructure already exercises reopen cycles: `assertQueryNoLeakCheck()` (called from `assertFactoryCursor`) calls `assertCursor()` twice, which calls `factory.getCursor()` twice. The second `getCursor()` call triggers `reopen()` on the window function. The `assertMemoryLeak()` wrapper checks for native memory leaks by comparing memory counters before and after the test lambda. [VERIFIED: AbstractCairoTest.java lines 1053-1078, 1888-1906]

So the simplest test approach is:

```java
@Test
public void testMultiPercentileDiscMemoryLeakOnReopen() throws Exception {
    assertMemoryLeak(() -> {
        execute("CREATE TABLE test AS (SELECT cast(x AS double) value FROM long_sequence(10))");
        assertQueryNoLeakCheck(
                """
                        value\tpercentile_disc
                        1.0\t[3.0,5.0,8.0]
                        2.0\t[3.0,5.0,8.0]
                        ...""",
                "SELECT value, percentile_disc(value, ARRAY[0.25, 0.5, 0.75]) OVER () FROM test",
                null
        );
    });
}
```

The `assertMemoryLeak` wrapper detects the leak: the first `getCursor()` allocates a DirectArray. The second `getCursor()` calls `reopen()`. Without the fix, `reopen()` does not free the old DirectArray -- native memory counter increases. At the end of the test, `assertMemoryLeak` sees the discrepancy and fails. With the fix, `reopen()` calls `Misc.free(result)`, the native memory is properly tracked, and the test passes.

**Existing tests already cover reopen cycles** because they use `assertMemoryLeak()`:
- `testMultiPercentileContOverPartition` [VERIFIED: PercentileWindowFunctionTest.java]
- `testMultiPercentileContOverWholeResultSet` [VERIFIED: PercentileWindowFunctionTest.java]
- `testMultiPercentileDiscOverPartition` (if it exists) or similar tests

However, the existing tests use `assertSql()` which does NOT call the factory twice. The test must use `assertQueryNoLeakCheck()` which calls the factory twice (triggering reopen). This is the key difference.

**Important: existing tests use assertSql(), not assertQueryNoLeakCheck()**. The existing tests in `PercentileWindowFunctionTest` use `assertSql()`, which creates a cursor only once. This means the existing tests do NOT exercise the reopen path and do NOT catch the DirectArray leak. The Phase 2 tests MUST use `assertQueryNoLeakCheck()` to trigger the factory reopen cycle that exposes the leak. [VERIFIED: PercentileWindowFunctionTest.java uses assertSql throughout]

### Affected Files Summary

| File | Inner Class | RES-01 | RES-02 |
|------|-------------|--------|--------|
| `MultiPercentileDiscDoubleWindowFunctionFactory.java` | MultiPercentileDiscOverPartitionFunction | YES: add `result = Misc.free(result)` to reopen/reset/toTop | NO: uses resultMemory, no double[] |
| `MultiPercentileDiscDoubleWindowFunctionFactory.java` | MultiPercentileDiscOverWholeResultSetFunction | YES: add `result = Misc.free(result)` to reopen/reset/toTop | YES: pre-allocate double[] results |
| `MultiPercentileContDoubleWindowFunctionFactory.java` | MultiPercentileContOverPartitionFunction | YES: add `result = Misc.free(result)` to reopen/reset/toTop | NO: uses resultMemory, no double[] |
| `MultiPercentileContDoubleWindowFunctionFactory.java` | MultiPercentileContOverWholeResultSetFunction | YES: add `result = Misc.free(result)` to reopen/reset/toTop | YES: pre-allocate double[] results |

### Anti-Patterns to Avoid
- **Calling `result.close()` instead of `result = Misc.free(result)`:** Leaves a dangling non-null reference. If `getArray()` checks `if (result == null)`, it skips allocation but the closed DirectArray has `ptr == 0` and cannot be used.
- **Nulling `results` in `reopen()`/`toTop()`:** Violates D-05 and forces heap re-allocation on the next preparePass2().
- **Freeing `results` (the double[] array) in `close()`:** Java GC handles `double[]`; setting `results = null` is sufficient. `Misc.free()` is for `Closeable` objects that own native memory.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Native memory cleanup + null | Manual `result.close(); result = null;` | `result = Misc.free(result)` | One-liner idiom, null-safe, consistent with all QuestDB resource patterns |
| Memory leak detection in tests | Custom memory counting | `assertMemoryLeak()` wrapper | Built into test framework, checks all native memory tags automatically |
| Reopen cycle testing | Manual cursor open/close/reopen calls | `assertQueryNoLeakCheck()` | Calls factory.getCursor() twice automatically, exercising reopen path |

## Common Pitfalls

### Pitfall 1: Using assertSql instead of assertQueryNoLeakCheck for leak tests
**What goes wrong:** Tests pass but do not detect the memory leak because assertSql creates a cursor only once, never triggering reopen().
**Why it happens:** assertSql is simpler and all existing Multi* window tests use it.
**How to avoid:** Phase 2 tests MUST use assertQueryNoLeakCheck() inside assertMemoryLeak(). The assertQueryNoLeakCheck call triggers two getCursor() calls, exercising reopen().
**Warning signs:** Tests pass before the fix is applied -- the test is not testing what it claims.

### Pitfall 2: Forgetting the empty-data path after removing results = null
**What goes wrong:** After size == 0 in preparePass2(), getArray() returns stale data from a previous execution because results is no longer nulled.
**Why it happens:** D-05 says don't null results on reopen/reset, and the natural extension is to not null it in preparePass2's empty case either.
**How to avoid:** Introduce a validity flag (boolean or int count) to track whether results contains valid data. Check the flag in getArray() instead of checking `results != null`.
**Warning signs:** Incorrect percentile values returned for empty tables or tables with all-null values.

### Pitfall 3: Adding Misc.free(result) but not using the return value
**What goes wrong:** Writing `Misc.free(result)` without `result =` leaves `result` pointing to a closed DirectArray.
**Why it happens:** The `Misc.free()` call looks like a void side-effect call. Easy to forget it returns null.
**How to avoid:** Always write `result = Misc.free(result)`. Never use `Misc.free(result)` as a standalone statement for a field.
**Warning signs:** NullPointerException or assertion failure in DirectArray when getArray() tries to use the closed array.

### Pitfall 4: Member ordering when adding new fields
**What goes wrong:** Adding `isResultValid` or `resultCount` field in the wrong position violates QuestDB's alphabetical member ordering convention.
**Why it happens:** Adding fields at the end of field declarations instead of in alphabetical position among same-kind members.
**How to avoid:** Instance fields are sorted alphabetically. `isResultValid` goes between `listMemory` and `percentilesFunc`. `resultCount` goes between `result` and `results`.
**Warning signs:** Code review rejection.

## Code Examples

### Misc.free() idiom (verified pattern)
```java
// Source: Misc.java [VERIFIED: nw_percentile branch]
public static <T extends Closeable> T free(T object) {
    if (object != null) {
        try {
            object.close();
        } catch (IOException e) {
            throw new FatalError(e);
        }
    }
    return null;
}
```

### DirectArray.close() frees native memory (verified)
```java
// Source: DirectArray.java [VERIFIED: nw_percentile branch]
public void close() {
    type = ColumnType.UNDEFINED;
    ptr = Unsafe.free(ptr, size, MEM_TAG);  // frees native memory
    flatViewLength = 0;
    size = 0;
    shape.clear();
    strides.clear();
}
```

### BasePartitionedWindowFunction lifecycle (reference for super calls)
```java
// Source: BasePartitionedWindowFunction.java [VERIFIED: nw_percentile branch]
@Override
public void reopen() {
    if (map != null) {
        map.reopen();
    }
}

@Override
public void reset() {
    Misc.free(map);
}

@Override
public void toTop() {
    super.toTop();    // calls arg.toTop()
    Misc.clear(map);
}
```

### Test pattern for memory leak detection with reopen (verified)
```java
// Source: AbstractCairoTest.java [VERIFIED: master branch]
// assertFactoryCursor calls assertCursor twice, triggering factory reopen:
protected void assertFactoryCursor(...) throws SqlException {
    assertCursor(expected, factory, ...);    // first cursor
    assertTimestamp(expectedTimestamp, factory, executionContext);
    assertCursor(expected, factory, ...);    // second cursor (triggers reopen)
    assertVariableColumns(factory, executionContext);
}

// assertCursor opens a cursor from factory:
protected static void assertCursor(...) throws SqlException {
    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
        // ... iterate and verify
    }
    // cursor.close() triggers function lifecycle
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Allocate double[] on every preparePass2 call | Pre-allocate and reuse double[] across calls | Phase 2 (this phase) | Eliminates heap allocation on query data path |
| DirectArray leaked through reopen/reset/toTop | `result = Misc.free(result)` in all lifecycle methods | Phase 2 (this phase) | Zero native memory leaks under reopen cycles |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Boolean flag `isResultValid` is the cleanest approach for the empty-data path after removing `results = null` from preparePass2 | Architecture Patterns (RES-02) | LOW -- alternative approaches (resultCount int, or sentinel array values) also work. Implementation detail within Claude's discretion. |

## Open Questions

1. **Should the partitioned variants also get the isResultValid flag?**
   - What we know: The partitioned variants store results in `resultMemory` (MemoryARW), not `double[]`. They do not have the empty-data problem because they iterate the map cursor in preparePass2 and only write results for partitions with data.
   - What's unclear: Nothing -- this is resolved.
   - Recommendation: No flag needed for partitioned variants. They only need RES-01 (DirectArray leak fix).

## Sources

### Primary (HIGH confidence -- direct source code inspection on nw_percentile branch)
- `MultiPercentileDiscDoubleWindowFunctionFactory.java` -- both inner classes' lifecycle methods (reopen/reset/toTop/close) inspected for result leak
- `MultiPercentileContDoubleWindowFunctionFactory.java` -- same inspection
- `PercentileDiscDoubleWindowFunctionFactory.java` -- reference for single-percentile lifecycle (result = Double.NaN in toTop)
- `BasePartitionedWindowFunction.java` -- super.reopen/reset/toTop behavior confirmed
- `BaseWindowFunction.java` -- super.close/reset/toTop behavior confirmed
- `DirectArray.java` -- close() frees native memory via Unsafe.free, implements QuietCloseable
- `Misc.java` -- free() returns null after closing, null-safe
- `AbstractCairoTest.java` -- assertFactoryCursor calls assertCursor twice (reopen cycle), assertMemoryLeak wraps native memory tracking

### Secondary (HIGH confidence -- Phase 1 summaries and verified execution)
- Phase 1 Plan 02 Summary -- confirmed COR-04 constness validation applied to all 4 window factories
- PercentileWindowFunctionTest.java -- confirmed existing tests use assertSql (no reopen cycle)

## Metadata

**Confidence breakdown:**
- RES-01 fix: HIGH -- bug mechanism confirmed (DirectArray result not freed in 4 inner classes), fix pattern is mechanical Misc.free(result) idiom
- RES-02 fix: HIGH -- heap allocation in preparePass2 confirmed (2 OverWholeResultSet classes), pre-allocation pattern is straightforward given COR-04 constness guarantee
- Test strategy: HIGH -- assertMemoryLeak + assertQueryNoLeakCheck pattern verified in AbstractCairoTest.java, guarantees reopen cycle triggers leak detection
- Pitfalls: HIGH -- empty-data edge case identified from code inspection, stale-data risk documented

**Research date:** 2026-04-13
**Valid until:** 2026-05-13 (stable -- fixes are against a specific branch with known code)
