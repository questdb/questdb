# Technology Stack

**Project:** Harden Percentile/Quantile Functions in QuestDB
**Researched:** 2026-04-13

## Recommended Algorithms

This is not a greenfield stack selection -- QuestDB exists, is zero-GC Java with native C/C++, and has no third-party Java dependencies. The "stack" here is the set of algorithms and data structure patterns to use for exact percentile computation.

### Core Algorithm: Quickselect with Three-Way Partitioning

| Algorithm | Purpose | Avg Complexity | Worst Complexity | Why |
|-----------|---------|---------------|-----------------|-----|
| Quickselect (Hoare) | Find k-th element for percentile_disc | O(n) | O(n^2) | Standard approach. PostgreSQL uses full sort, but quickselect is strictly better when you only need one percentile value. DuckDB uses std::nth_element which is introselect under the hood. |
| Three-way partition (Dutch National Flag) | Partition step within quickselect | O(n) per pass | O(n) per pass | Fixes the critical all-equal-elements O(n^2) bug. Lomuto with strict `<` degenerates because equal elements pile on one side. Three-way partitioning puts equals in the center and never recurses into them. |
| Median-of-three pivot | Pivot selection for quickselect | - | - | Sufficient for practical inputs. Avoids worst-case on sorted/reverse-sorted data. The PROJECT.md marks introselect as out of scope, and median-of-three is a pragmatic middle ground. |
| Insertion sort (small N) | Sub-arrays below threshold | O(n^2) but tiny n | O(n^2) but tiny n | For partitions of ~16 elements, insertion sort has lower constant factors than quickselect due to no recursion overhead and good cache behavior. |

**Confidence: HIGH** -- These are textbook algorithms with decades of production use. The specific three-way partitioning fix directly addresses the identified O(n^2) bug.

### Percentile Interpolation: Linear Interpolation for percentile_cont

| Method | Purpose | Notes |
|--------|---------|-------|
| Nearest-rank (floor) | percentile_disc | `index = ceil(p * N) - 1`, return element at that index. SQL standard for discrete percentile. |
| Linear interpolation (C=1, R-7) | percentile_cont | `h = (N-1)*p`, `lower = floor(h)`, `upper = ceil(h)`, `result = x[lower] + (h - lower) * (x[upper] - x[lower])`. This is PostgreSQL's method and the SQL standard. |

**Confidence: HIGH** -- SQL standard behavior. PostgreSQL, DuckDB, and ClickHouse (quantileExact) all use these formulas.

### Data Structure: Off-Heap Resizable Double Array

| Structure | Version | Purpose | Why |
|-----------|---------|---------|-----|
| Off-heap resizable array (native memory) | N/A - custom | Accumulate values for sorting/selecting | Zero-GC requirement. QuestDB already has `DirectIntList`, `DirectLongList` patterns. The percentile functions need a `DirectDoubleList` equivalent (or store doubles as raw 8-byte values in a DirectLongList via Double.doubleToRawLongBits). |
| GroupByLongList (existing) | N/A | Accumulate long values for percentile on longs | Already exists in QuestDB. Needs quickselect added as a method, operating on the native memory region. |

**Confidence: HIGH** -- Aligns with existing QuestDB patterns.

### Window Function Strategy: Per-Partition Materialization

| Strategy | Purpose | Why |
|----------|---------|-----|
| Collect-then-select per partition | Window percentile over PARTITION BY | Exact percentile requires seeing all values in a partition. No shortcut -- must accumulate all values, then apply quickselect. This is what PostgreSQL and DuckDB do. |
| Append-in-place (not copy-on-append) | Avoid O(n^2) memory in pass1 | The PROJECT.md identifies a copy-on-append bug causing O(n^2) memory+CPU. Each new value should append to the existing buffer, not copy the entire buffer each time. |

**Confidence: HIGH** -- Fundamental requirement of exact percentile computation.

## Algorithm Details

### Quickselect with Three-Way Partitioning (Recommended Implementation)

```
quickSelect(arr, lo, hi, k):
    while lo < hi:
        pivot = medianOfThree(arr, lo, lo + (hi-lo)/2, hi)
        (lt, gt) = threeWayPartition(arr, lo, hi, pivot)
        // After partition: arr[lo..lt-1] < pivot, arr[lt..gt] == pivot, arr[gt+1..hi] > pivot
        if k < lt:
            hi = lt - 1
        elif k > gt:
            lo = gt + 1
        else:
            return arr[k]  // k is in the equal-to-pivot region
    return arr[lo]
```

**Why three-way partition matters for QuestDB specifically:**
- Time-series data frequently has repeated values (e.g., sensor readings that plateau, status codes, bucketed metrics)
- The existing Lomuto partition with `compareGroups(j, pivotIndex) < 0` puts ALL elements equal to the pivot on the right side
- With all-equal input, every partition step produces an (N-1, 0) split -> O(n^2) total
- Three-way partition produces an (0, N, 0) split on all-equal input -> O(n) total

### Why NOT Full Sort

| Approach | Time | Space | When to Use |
|----------|------|-------|-------------|
| Full sort (quicksort/mergesort) | O(n log n) | O(1) in-place or O(n) | When you need MULTIPLE different percentiles from the same dataset in one pass |
| Quickselect | O(n) avg | O(1) in-place | When you need ONE percentile value |
| Quickselect + partition reuse | O(n) avg | O(1) in-place | When you need a SMALL fixed number of percentiles (each subsequent select reuses partition information) |

**Decision:** Use quickselect for single-percentile functions (percentile_disc, percentile_cont). Use full sort for multi-percentile array variants (where the caller requests percentiles at [0.25, 0.5, 0.75, 0.99] simultaneously) because after sorting, all k-th element lookups are O(1).

**Confidence: HIGH** -- This is the standard approach in every database that distinguishes single vs. multi-percentile.

### Why NOT Introselect

Introselect (quickselect with fallback to median-of-medians when recursion depth exceeds a threshold) guarantees O(n) worst-case. The PROJECT.md explicitly marks this as out of scope:

> "Recursive quickSort depth limit (introsort) -- median-of-three is sufficient in practice"

This is a reasonable engineering decision because:
1. Three-way partitioning already eliminates the most common worst case (repeated values)
2. Median-of-three pivot selection eliminates the second most common worst case (sorted/reverse-sorted input)
3. The remaining adversarial inputs require a deliberately crafted dataset -- unlikely in time-series workloads
4. Median-of-medians has a large constant factor (~5x slower in practice) that makes the guaranteed O(n) slower than quickselect's average O(n) for all realistic inputs

**Confidence: HIGH** -- Verified by both theory and the project's explicit scoping decision.

## How Production Databases Handle This

### PostgreSQL

- **Aggregate percentile_disc/percentile_cont:** Ordered-set aggregate. Collects all values into a tuplesort, performs a full sort, then indexes into the sorted array. Uses tuplesort's internal quicksort (with median-of-three and insertion sort for small partitions). Always O(n log n).
- **Window percentile:** Not natively supported as a window function in standard PostgreSQL. Users work around it with subqueries or extensions.
- **Multi-percentile:** `percentile_disc(array[0.25, 0.5, 0.75])` sorts once, then does multiple lookups. Efficient.
- **NULL handling:** NULLs are filtered before sorting (excluded from the sorted set and from the count N).

**Confidence: MEDIUM** -- Based on training data knowledge of PostgreSQL internals. The implementation lives in `orderedsetaggs.c`. Could not verify against current source due to tool restrictions, but PostgreSQL's percentile implementation has been stable since PostgreSQL 9.4 (2014).

### DuckDB

- **Aggregate quantile:** Uses `std::nth_element` (introselect in libstdc++/libc++) for single quantile. This is O(n) average, O(n) worst-case guaranteed.
- **Window quantile:** For exact window quantiles, DuckDB materializes each partition's values and applies nth_element. For moving window quantiles with a sliding frame, DuckDB uses a more sophisticated approach with a segment tree, but for whole-partition frames it falls back to materialization.
- **Multi-quantile:** Sorts once, indexes multiple times.
- **NULL handling:** NULLs excluded from computation.

**Confidence: MEDIUM** -- Based on training data knowledge of DuckDB's holistic aggregate implementation. DuckDB's quantile code lives in `quantile.cpp`.

### ClickHouse

- **quantileExact:** Collects all values into a vector, then uses `std::nth_element` for the selection. O(n) average.
- **quantileExactWeighted:** Uses a hash map to count occurrences, then iterates in sorted order. Efficient for low-cardinality data.
- **quantileExactHigh/Low:** Variations that control rounding direction at partition boundaries.
- **NULL handling:** NULLs excluded.

**Confidence: MEDIUM** -- Based on training data knowledge. ClickHouse's quantile implementations are in `AggregateFunctionQuantile.h`.

### Summary of Industry Practice

| Database | Single Percentile Algorithm | Multi-Percentile | Worst-Case Guarantee |
|----------|---------------------------|-------------------|---------------------|
| PostgreSQL | Full sort O(n log n) | Sort once, index N times | O(n log n) guaranteed |
| DuckDB | nth_element / introselect O(n) | Sort once, index N times | O(n) guaranteed |
| ClickHouse | nth_element O(n) | Sort once, index N times | O(n) guaranteed (via introselect in STL) |
| **QuestDB (recommended)** | **Quickselect + 3-way O(n)** | **Full sort, index N times** | **O(n^2) theoretical but eliminated for practical inputs by 3-way + median-of-3** |

## Specific Recommendations for QuestDB Implementation

### 1. Shared QuickSelect Utility

Extract the 8 duplicated quickSort copies into a single shared utility class. Add both `quickSort` and `quickSelect` as methods.

```java
// Recommended API shape
public class DirectMemorySort {
    // For multi-percentile: full sort, then index
    public static void quickSort(long address, int count);
    
    // For single percentile: O(n) selection
    public static double quickSelectDouble(long address, int count, int k);
    public static long quickSelectLong(long address, int count, int k);
}
```

Operate directly on native memory pointers (off-heap) to maintain zero-GC. The existing `LongGroupSort` pattern of using Lomuto partition should be replaced with Hoare-style three-way partition.

### 2. Three-Way Partition Implementation

```
threeWayPartition(arr, lo, hi, pivot):
    lt = lo      // arr[lo..lt-1] < pivot
    i  = lo      // arr[lt..i-1] == pivot
    gt = hi      // arr[gt+1..hi] > pivot
    while i <= gt:
        if arr[i] < pivot:
            swap(arr, lt, i)
            lt++; i++
        elif arr[i] > pivot:
            swap(arr, i, gt)
            gt--
        else:
            i++
    return (lt, gt)
```

This is Dijkstra's Dutch National Flag algorithm. It handles equal elements in O(n) single pass.

### 3. NaN/NULL Handling

For doubles, QuestDB uses `Double.NaN` as NULL sentinel. Before sorting/selecting:
1. Partition NaN values to the end of the array (single pass, O(n))
2. Record `validCount = total - nanCount`
3. Apply quickselect only on `arr[0..validCount-1]`
4. Compute percentile index using `validCount`, not `total`

This two-phase approach avoids contaminating the sort/select with NaN comparisons (which are tricky: `NaN < x` is false, `NaN > x` is false, `NaN == x` is false).

### 4. Double Comparison Considerations

Use `Double.compare(a, b)` rather than `a < b` / `a > b` for the partition comparisons. `Double.compare` handles:
- NaN (sorts after positive infinity)
- -0.0 vs +0.0 (treats them as distinct, -0.0 < +0.0)

Since NaN values should already be partitioned out before selection, the main concern is -0.0 vs +0.0 ordering consistency.

## Alternatives Considered

| Category | Recommended | Alternative | Why Not |
|----------|-------------|-------------|---------|
| Selection algorithm | Quickselect + 3-way | Introselect (quickselect + median-of-medians fallback) | Out of scope per PROJECT.md. Three-way + median-of-three already eliminates practical worst cases. Median-of-medians has 5x constant factor overhead. |
| Selection algorithm | Quickselect + 3-way | Floyd-Rivest algorithm | O(n) with smaller constants than quickselect on average, but more complex to implement and debug. Not used by any major database. Overkill for the hardening scope. |
| Partition scheme | Three-way (Dutch National Flag) | Lomuto single-pivot (current) | Lomuto degenerates to O(n^2) on all-equal elements -- this is the bug being fixed. |
| Partition scheme | Three-way (Dutch National Flag) | Dual-pivot (Java's DualPivotQuicksort) | QuestDB's LongSort already uses dual-pivot for full sorts, but for quickselect the three-way single-pivot is simpler and sufficient. Dual-pivot quickselect exists but adds implementation complexity without meaningful benefit for selection. |
| Pivot selection | Median-of-three | Random pivot | Random pivot has the same expected O(n) but requires a PRNG call per recursion. Median-of-three is deterministic and slightly better in practice on partially sorted data. |
| Pivot selection | Median-of-three | Ninther (median of medians of three) | Better pivot quality but more overhead. Only worth it for very large arrays. Not needed here. |
| Sort for multi-percentile | Quicksort (in-place) | Radix sort | QuestDB has radix sort in native code (Vect.radixSortABLongIndexAsc), but it requires O(n) extra space and is only faster for large arrays of integers/longs. For double arrays of typical partition sizes, quicksort is simpler and sufficient. |
| Data accumulation | Off-heap resizable array | On-heap ArrayList<Double> | Violates zero-GC constraint. Absolutely not. |
| Approximate alternative | N/A (exact required) | t-digest, HDR Histogram | Already have approx_percentile via HDR Histogram. The exact functions are the whole point of this work. |

## Implementation Notes (QuestDB-Specific)

### Memory Layout

Store accumulated double values as raw bytes in native memory:
- Use `Unsafe.getDouble(address + index * 8)` / `Unsafe.putDouble(address + index * 8, value)` for access
- Track `count` and `capacity` separately
- Growth strategy: double capacity when full (standard amortized O(1) append)
- Free with `Unsafe.free(address, capacity * 8, memoryTag)` using appropriate `MemoryTag`

### Resource Cleanup Pattern

Follow QuestDB's `QuietCloseable` pattern:
- Implement `Closeable` on any class holding native memory
- Release memory in `close()`, set pointer to 0
- Use `Misc.free()` in cleanup paths
- Test with `assertMemoryLeak()` to catch leaks

### Window Function Integration

For window `percentile_disc() OVER (PARTITION BY ...)`:
1. **pass1:** Accumulate values into a per-partition native buffer (append-in-place, NOT copy-on-append)
2. **pass2:** For each partition, apply quickselect on its buffer, emit result for every row in that partition
3. **Reuse buffers:** Between partitions, reset the count but keep the allocated memory (avoid free/malloc churn)

## Sources

- QuestDB source: `core/src/main/java/io/questdb/std/LongGroupSort.java` -- existing Lomuto partition quicksort
- QuestDB source: `core/src/main/java/io/questdb/std/LongSort.java` -- dual-pivot quicksort with Dutch National Flag (reference for three-way partition)
- QuestDB source: `core/src/main/java/io/questdb/griffin/engine/functions/groupby/ApproxPercentileDoubleGroupByFunction.java` -- HDR Histogram-based approximate percentile (reference pattern)
- Textbook: Cormen et al., "Introduction to Algorithms" (CLRS), Chapter 9 "Medians and Order Statistics" -- quickselect analysis
- Textbook: Sedgewick & Wayne, "Algorithms" (4th ed), Section 2.3 -- three-way quicksort / Dijkstra's Dutch National Flag
- PostgreSQL: `src/backend/utils/adt/orderedsetaggs.c` -- ordered-set aggregate implementation (MEDIUM confidence, training data)
- DuckDB: `src/core_functions/aggregate/holistic/quantile.cpp` -- quantile implementation (MEDIUM confidence, training data)
- ClickHouse: `src/AggregateFunctions/AggregateFunctionQuantile.h` -- quantile implementation (MEDIUM confidence, training data)
