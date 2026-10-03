# Phase 3: Performance - Research

**Researched:** 2026-04-13
**Domain:** Off-heap memory layout optimization and selection algorithm porting (zero-GC Java)
**Confidence:** HIGH

## Summary

Phase 3 addresses two independent performance pathologies in QuestDB's percentile function implementations. The first (PERF-01) is an O(N^2) copy-on-append pattern in four partitioned window inner classes where every new value triggers a full block copy to a newly allocated region in MemoryARW. The fix replaces this with a capacity-tracked block layout that grows in-place via 2x doubling, achieving amortized O(1) per append. The second (PERF-02) is the use of O(n log n) full sort in `PercentileDiscLongGroupByFunction.getLong()` when only a single order statistic is needed; the fix ports the existing `quickSelect()` and `quickSelectMultiple()` from `GroupByDoubleList` to `GroupByLongList`, changing double comparisons to long comparisons.

Both changes are structurally well-understood. PERF-01 carries the higher risk due to offset arithmetic changes that affect four classes and their corresponding preparePass2 readers. PERF-02 is a mechanical port with a proven reference implementation already in the codebase.

**Primary recommendation:** Implement PERF-01 and PERF-02 as two independent tasks within a single plan. PERF-02 should be done first since it is lower risk and self-contained. PERF-01 should be done second with careful attention to offset arithmetic and thorough testing of all four affected factories.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

#### Append-in-Place Strategy (PERF-01)
- **D-01:** Use capacity-tracking per partition. Change the per-partition block layout from `[size|val0|val1|...|valN]` to `[capacity|size|val0|val1|...|valN]` (add 8-byte capacity header).
- **D-02:** Growth policy: when `size >= capacity`, allocate a new block at `2 * capacity` and copy once. This gives amortized O(1) per row, O(N) total. Only O(log N) copies per partition instead of O(N).
- **D-03:** Old blocks become garbage in MemoryARW — they are freed when the query ends (MemoryARW.close()). This is acceptable since the total wasted space is bounded by 2x the live data.
- **D-04:** Apply to all 4 partitioned window inner classes: PercentileDiscOverPartitionFunction, PercentileContOverPartitionFunction, MultiPercentileDiscOverPartitionFunction, MultiPercentileContOverPartitionFunction.
- **D-05:** The whole-result-set variants already use O(1) append (`listMemory.putDouble(size * 8, d)`) — no changes needed there.
- **D-06:** In `preparePass2()`, read values from the capacity-tracked block: skip the capacity header, read `size` from offset 0 of the data, then read values from offset 8 onward. Adjust all offset arithmetic accordingly.

#### GroupByLongList quickSelect (PERF-02)
- **D-07:** Direct port of `quickSelect()` and `quickSelectMultiple()` from `GroupByDoubleList` to `GroupByLongList`. Change `double` comparisons to `long` comparisons. Use `getQuick()`/`setValueAt()` which already exist in GroupByLongList.
- **D-08:** Add bounds validation to `quickSelect()` (lo >= 0, hi < size(), k within range) — matching GroupByDoubleList's quickSelect. Also add bounds validation to `quickSelectMultiple()` which GroupByDoubleList is missing.
- **D-09:** Update `PercentileDiscLongGroupByFunction.getLong()` to call `listA.quickSelect(0, size - 1, N)` instead of `listA.sort()`.

### Claude's Discretion
- Initial capacity for new partition blocks (suggested: 16 elements = 128 bytes + 16 byte header)
- Whether to add a `quickSelectMultiple()` to GroupByLongList too (for potential future multi-percentile long support)
- Exact offset arithmetic in the capacity-tracked block layout

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| PERF-01 | Partitioned window pass1 must use append-in-place with capacity tracking, not copy-on-append O(N^2) | Block layout change from `[size\|data...]` to `[capacity\|size\|data...]` across 4 inner classes; preparePass2 offset adjustment; MemoryARW arena semantics verified |
| PERF-02 | PercentileDiscLongGroupByFunction must use quickSelect O(n) via new GroupByLongList.quickSelect() | Direct port from GroupByDoubleList.quickSelect() (lines 593-717); mechanical long-for-double substitution; caller update at PercentileDiscLongGroupByFunction line 115 |
</phase_requirements>

## Project Constraints (from CLAUDE.md)

- **Zero-GC on data paths:** No Java heap allocations during query execution. The capacity-tracking approach uses MemoryARW (off-heap), consistent with this. [VERIFIED: codebase inspection]
- **No third-party dependencies:** All algorithms implemented from first principles. quickSelect is already implemented in-house. [VERIFIED: codebase inspection]
- **Member ordering:** Java class members grouped by kind (static vs instance) and visibility, sorted alphabetically. New quickSelect/quickSelectMultiple methods must be inserted in correct alphabetical position. [CITED: CLAUDE.md]
- **Boolean naming:** Use `is...`/`has...` prefix for booleans. [CITED: CLAUDE.md]
- **Tests use assertMemoryLeak():** Performance changes must be tested within assertMemoryLeak to detect native memory regressions. [CITED: CLAUDE.md]
- **Use assertQueryNoLeakCheck() for query result assertions.** [CITED: CLAUDE.md]
- **Numbers >= 10_000 use underscore separators.** [CITED: CLAUDE.md]
- **Commit titles:** Plain English, no conventional commits prefix, max 50 chars. [CITED: CLAUDE.md]

## Architecture Patterns

### Current Block Layout (PERF-01 target)

The four partitioned window inner classes store per-partition data in `MemoryARW` (specifically `MemoryCARWImpl`, a single contiguous memory region). Each partition's data block is stored at an offset from `getPageAddress(0)`.

**Current layout per partition:**
```
Offset:  [0..7]     [8..15]    [16..23]    ...    [8+N*8..8+N*8+7]
Content: size(long)  val_0      val_1       ...    val_N-1
```

**Problem:** On each new value, the code allocates a new block of `8 + (size+1)*8` bytes via `appendAddressFor()`, copies all old values element-by-element, appends the new value, and updates the MapValue to point to the new block. The old block becomes dead space. This is O(N) per row, O(N^2) total per partition.

**New layout per partition (decision D-01):**
```
Offset:  [0..7]        [8..15]     [16..23]   [24..31]    ...    [16+N*8..16+N*8+7]
Content: capacity(long) size(long)  val_0      val_1       ...    val_N-1
```

**Growth strategy (decision D-02):** When `size >= capacity`, allocate a new block at `2 * capacity` elements, copy once, update MapValue pointer. Amortized O(1) per row. Old block is abandoned in the MemoryARW arena and reclaimed when the query ends (MemoryARW.close()). [VERIFIED: codebase inspection of MemoryCARWImpl.close()]

### MemoryARW Offset Convention

All pointers stored in MapValue are **relative offsets** from `getPageAddress(0)`:
- Write: `long allocPtr = listMemory.appendAddressFor(bytes) - listMemory.getPageAddress(0)` [VERIFIED: codebase inspection]
- Read: `listMemory.getLong(listPtr)` internally does `pageAddress + listPtr` [VERIFIED: AbstractMemoryCR.addressOf()]

**Critical:** When MemoryCARW extends its backing region (realloc), `pageAddress` may change. All previously stored offsets remain valid because they are relative. The code must never store absolute pointers across append operations. [VERIFIED: MemoryCARWImpl.checkAndExtend()]

### quickSelect Algorithm Structure (GroupByDoubleList)

The existing `quickSelect()` in GroupByDoubleList (lines 593-717 on the nw_percentile branch) uses:
1. **Median-of-three pivot selection** in `partition()` -- sorts lo/mid/hi, uses hi as pivot [VERIFIED: codebase inspection]
2. **Iterative quickSelectImpl** -- avoids StackOverflowError on degenerate inputs [VERIFIED: codebase inspection]
3. **Bounds validation** in the public `quickSelect()` entry point [VERIFIED: codebase inspection]
4. **quickSelectMultiple** for selecting multiple order statistics in one pass, with tail-call optimization [VERIFIED: codebase inspection]

### Pattern: GroupByDoubleList vs GroupByLongList Structural Similarity

Both classes share identical structure: [VERIFIED: codebase inspection]
- Same `HEADER_SIZE = 2 * Integer.BYTES` (8 bytes)
- Same `SIZE_OFFSET = Integer.BYTES` (4 bytes)
- Same buffer layout: `[capacity(4)|size(4)|data...]`
- Same accessor pattern: `getQuick(index)`, `setValueAt(index, value)`, `swap(a, b)`
- Same memory management via `GroupByAllocator`

**Key difference for porting:** GroupByDoubleList uses `double` comparisons and `getQuick(int index)` returns `double`. GroupByLongList uses `long` comparisons and `getQuick(long index)` returns `long`. Note the index parameter type difference: `int` vs `long`. The quickSelect port must use `long` indices for consistency with GroupByLongList's existing API. [VERIFIED: codebase inspection]

### Files Requiring Modification

**PERF-01 (4 files, same pattern in each):**
1. `PercentileDiscDoubleWindowFunctionFactory.java` -- `PercentileDiscOverPartitionFunction` inner class
2. `PercentileContDoubleWindowFunctionFactory.java` -- `PercentileContOverPartitionFunction` inner class
3. `MultiPercentileDiscDoubleWindowFunctionFactory.java` -- `MultiPercentileDiscOverPartitionFunction` inner class
4. `MultiPercentileContDoubleWindowFunctionFactory.java` -- `MultiPercentileContOverPartitionFunction` inner class

In each file, two methods change:
- `pass1()`: Replace copy-on-append with capacity-tracked append-in-place
- `preparePass2()`: Adjust offset arithmetic to account for the new capacity header

**PERF-02 (2 files):**
1. `GroupByLongList.java` -- Add `quickSelect()`, `quickSelectMultiple()`, `quickSelectImpl()`, `partition()` methods
2. `PercentileDiscLongGroupByFunction.java` -- Change `listA.sort()` to `listA.quickSelect(0, size - 1, N)` at line 115

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Selection algorithm | New quickSelect for longs | Port from GroupByDoubleList.quickSelect() | Already proven, handles degenerate inputs, iterative to avoid stack overflow |
| Capacity growth logic | Custom growth factor or linked list | Standard 2x doubling with arena allocation | Well-understood amortized O(1), bounded 2x waste, no fragmentation |
| Memory management | Manual free of old blocks | MemoryARW arena semantics (freed on close/truncate) | MemoryCARW already manages the contiguous region; old blocks are safely abandoned |

## Common Pitfalls

### Pitfall 1: Off-by-One in Capacity Header Offset
**What goes wrong:** After adding the 8-byte capacity header, all value offsets shift by 8. Forgetting to adjust even one read/write location causes silent data corruption.
**Why it happens:** The old layout starts values at offset 8 (after size). The new layout starts values at offset 16 (after capacity + size). Every `listPtr + 8 + i * 8` must become `listPtr + 16 + i * 8`.
**How to avoid:** Define named constants for the offsets (e.g., `CAPACITY_OFFSET = 0`, `SIZE_OFFSET = 8`, `DATA_OFFSET = 16`) and use them consistently. Search for every occurrence of `listPtr + 8` in the modified files.
**Warning signs:** Tests producing wrong percentile values but not crashing (shifted reads still land within the block, just at wrong positions).

### Pitfall 2: MapValue Not Updated After Block Relocation
**What goes wrong:** When the block grows, a new region is allocated at a different offset. If the MapValue is not updated, subsequent pass1 calls for the same partition read the old (abandoned) block.
**Why it happens:** The growth path allocates a new block and must store the new offset back into the MapValue. The existing code already does this (`value.putLong(0, listPtr)`), but the capacity-growth path must do it too.
**How to avoid:** In the growth path, always update `value.putLong(0, newListPtr)` after copying data to the new block.
**Warning signs:** Partition sizes stop growing after the first capacity overflow, or data appears duplicated.

### Pitfall 3: long vs int Index Types in GroupByLongList
**What goes wrong:** GroupByLongList uses `long` for index parameters (e.g., `getQuick(long index)`, `setValueAt(long index, value)`), while GroupByDoubleList uses `int`. A direct copy-paste without adjusting types causes compilation errors or truncation.
**Why it happens:** GroupByLongList was designed for larger datasets and uses long indices throughout.
**How to avoid:** Use `int` parameters for quickSelect's lo/hi/k (matching the int size() return type), but call `getQuick(long)`, `setValueAt(long, long)` etc. with the appropriate widening. Check every method signature against the existing GroupByLongList API.
**Warning signs:** Compilation errors, or silent truncation if int is cast to long incorrectly.

### Pitfall 4: quickSelect Modifies Data In-Place
**What goes wrong:** quickSelect partially rearranges the array. If the same list is used for multiple percentile calculations, the second call operates on partially-rearranged data and produces wrong results.
**Why it happens:** quickSelect is an in-place partial sort. After `quickSelect(0, n-1, k)`, element at index k is correct but other elements are only partitioned (not sorted).
**How to avoid:** For PERF-02, `PercentileDiscLongGroupByFunction` only computes a single percentile per call, so this is safe. For multi-percentile, use `quickSelectMultiple()` which handles multiple indices in one pass. The window function `preparePass2` iterates partitions independently, so no issue there.
**Warning signs:** Wrong results only when multiple percentiles are requested from the same data.

### Pitfall 5: Initial Block Allocation Must Include Capacity Header
**What goes wrong:** The "new partition" path (when `value.isNew()`) must allocate `capacity_header + size_header + initial_capacity * 8` bytes, not just `size_header + 1 * 8`.
**Why it happens:** The initial allocation must pre-allocate space for future growth. Allocating only enough for one element defeats the purpose of capacity tracking.
**How to avoid:** Initial allocation: `appendAddressFor(8 + 8 + initialCapacity * 8)` where first 8 is capacity, second 8 is size, rest is data. Set capacity to initialCapacity, size to 1, write first value.
**Warning signs:** Every second value triggers a growth copy, negating the performance benefit.

## Code Examples

### PERF-01: New pass1() Pattern (Capacity-Tracked Append)

```java
// Source: Derived from CONTEXT.md D-01, D-02, D-03 + codebase inspection
// Constants (define at class level or as static final)
private static final int INITIAL_CAPACITY = 16;
private static final long CAPACITY_OFFSET = 0;   // relative to block start
private static final long SIZE_OFFSET_BLOCK = 8;  // relative to block start
private static final long DATA_OFFSET = 16;        // relative to block start

@Override
public void pass1(Record record, long recordOffset, WindowSPI spi) {
    double d = arg.getDouble(record);
    if (Numbers.isFinite(d)) {
        partitionByRecord.of(record);
        MapKey key = map.withKey();
        key.put(partitionByRecord, partitionBySink);
        MapValue value = key.createValue();

        long listPtr;

        if (value.isNew()) {
            // Allocate: [capacity(8) | size(8) | data(initialCap * 8)]
            long bytes = DATA_OFFSET + INITIAL_CAPACITY * 8L;
            listPtr = listMemory.appendAddressFor(bytes) - listMemory.getPageAddress(0);
            listMemory.putLong(listPtr + CAPACITY_OFFSET, INITIAL_CAPACITY);
            listMemory.putLong(listPtr + SIZE_OFFSET_BLOCK, 1);
            listMemory.putDouble(listPtr + DATA_OFFSET, d);
        } else {
            listPtr = value.getLong(0);
            long size = listMemory.getLong(listPtr + SIZE_OFFSET_BLOCK);
            long capacity = listMemory.getLong(listPtr + CAPACITY_OFFSET);

            if (size >= capacity) {
                // Grow: allocate 2x, copy, abandon old block
                long newCapacity = capacity * 2;
                long bytes = DATA_OFFSET + newCapacity * 8L;
                long newPtr = listMemory.appendAddressFor(bytes) - listMemory.getPageAddress(0);
                listMemory.putLong(newPtr + CAPACITY_OFFSET, newCapacity);
                listMemory.putLong(newPtr + SIZE_OFFSET_BLOCK, size + 1);
                // Copy old values
                for (long i = 0; i < size; i++) {
                    listMemory.putDouble(newPtr + DATA_OFFSET + i * 8, listMemory.getDouble(listPtr + DATA_OFFSET + i * 8));
                }
                listMemory.putDouble(newPtr + DATA_OFFSET + size * 8, d);
                listPtr = newPtr;
            } else {
                // Append in-place: capacity allows it
                listMemory.putDouble(listPtr + DATA_OFFSET + size * 8, d);
                listMemory.putLong(listPtr + SIZE_OFFSET_BLOCK, size + 1);
            }
        }

        value.putLong(0, listPtr);
    }
}
```

### PERF-01: Updated preparePass2() Offset Arithmetic

```java
// Source: Derived from CONTEXT.md D-06 + codebase inspection
@Override
public void preparePass2() {
    RecordCursor cursor = map.getCursor();
    MapRecord record = map.getRecord();

    while (cursor.hasNext()) {
        MapValue value = record.getValue();
        long listPtr = value.getLong(0);
        long size = listMemory.getLong(listPtr + SIZE_OFFSET_BLOCK); // was: listPtr

        if (size > 0) {
            double percentile = percentileFunc.getDouble(record);
            double multiplier = SqlUtil.getPercentileMultiplier(percentile, percentilePos);

            // Sort the list (values start at listPtr + DATA_OFFSET)
            quickSort(listPtr + DATA_OFFSET, 0, size - 1); // was: listPtr + 8

            int N = (int) Math.max(0, Math.ceil(size * multiplier) - 1);
            double result = listMemory.getDouble(listPtr + DATA_OFFSET + N * 8L); // was: listPtr + 8 + N * 8L

            // Store result back at DATA_OFFSET
            listMemory.putDouble(listPtr + DATA_OFFSET, result); // was: listPtr + 8
        }
    }
}
```

### PERF-02: quickSelect Port to GroupByLongList

```java
// Source: Port from GroupByDoubleList.java lines 593-717 on nw_percentile branch
// Change: double -> long comparisons, int index params with long accessor calls

public void quickSelect(int lo, int hi, int k) {
    if (lo < 0 || hi >= size() || k < lo || k > hi) {
        throw new ArrayIndexOutOfBoundsException("lo=" + lo + ", hi=" + hi + ", k=" + k + ", size=" + size());
    }
    if (lo < hi) {
        quickSelectImpl(lo, hi, k);
    }
}

public void quickSelectMultiple(int lo, int hi, int[] indices, int from, int to) {
    // Bounds validation (D-08: added for GroupByLongList, missing in GroupByDoubleList)
    if (lo < 0 || hi >= size()) {
        throw new ArrayIndexOutOfBoundsException("lo=" + lo + ", hi=" + hi + ", size=" + size());
    }
    quickSelectMultipleImpl(lo, hi, indices, from, to);
}

private void quickSelectImpl(int lo, int hi, int k) {
    while (lo < hi) {
        int pivotIndex = partition(lo, hi);
        if (k < pivotIndex) {
            hi = pivotIndex - 1;
        } else if (k > pivotIndex) {
            lo = pivotIndex + 1;
        } else {
            break;
        }
    }
}

private int partition(int lo, int hi) {
    int mid = lo + (hi - lo) / 2;
    // Median-of-three
    if (getQuick(mid) < getQuick(lo)) {
        swap(lo, mid);
    }
    if (getQuick(hi) < getQuick(lo)) {
        swap(lo, hi);
    }
    if (getQuick(mid) < getQuick(hi)) {
        swap(mid, hi);
    }
    long pivot = getQuick(hi);
    int i = lo - 1;
    for (int j = lo; j < hi; j++) {
        if (getQuick(j) <= pivot) {
            i++;
            swap(i, j);
        }
    }
    swap(i + 1, hi);
    return i + 1;
}
```

### PERF-02: Caller Update in PercentileDiscLongGroupByFunction

```java
// Source: CONTEXT.md D-09 + codebase inspection of line 115
// Before:
//     listA.sort();
//     ...
//     return listA.getQuick(N);

// After:
listA.quickSelect(0, size - 1, N);
return listA.getQuick(N);
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Copy-on-append per partition (O(N^2) total) | Capacity-tracked blocks with 2x growth (O(N) total) | This phase | Removes quadratic bottleneck on partitioned window percentile for large partitions |
| Full dual-pivot sort for single percentile (O(n log n)) | quickSelect with median-of-three (O(n) average) | Already done for double in this branch; this phase ports to long | Faster single-percentile group-by on long columns |

**Already modernized (no changes needed):**
- GroupByDoubleList.quickSelect() -- already on nw_percentile branch [VERIFIED: codebase inspection]
- PercentileDiscDoubleGroupByFunction.getLong() -- already uses quickSelect [VERIFIED: codebase inspection]
- Whole-result-set window variants -- already O(1) append [VERIFIED: codebase inspection]

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Initial capacity of 16 elements is a good default for new partition blocks | Code Examples (PERF-01) | Low -- too small wastes a few extra copies on first few rows; too large wastes a small amount of memory per partition. 16 is a reasonable middle ground. Marked as Claude's Discretion. |
| A2 | The `quickSelectMultiple` method should be added to GroupByLongList for future-proofing | Architecture Patterns | Low -- marked as Claude's Discretion. Could skip if not needed now, but the port is trivial. |

## Open Questions

1. **Should the `partition()` method in GroupByLongList use the same Lomuto scheme as GroupByDoubleList's quickSelect, or the Hoare scheme?**
   - What we know: GroupByDoubleList's quickSelect uses Lomuto with median-of-three. The dual-pivot sort uses a different scheme.
   - What's unclear: Nothing -- the decision D-07 says "direct port", so use the same Lomuto scheme.
   - Recommendation: Direct port, no algorithmic changes.

2. **Will copy-on-grow for large partitions cause memory spikes in MemoryCARW?**
   - What we know: When a partition block at capacity C grows, a new 2C block is allocated while the old C block remains abandoned. Peak memory is ~3x the live data (old block + new block + future growth headroom).
   - What's unclear: Whether MemoryCARW's max pages configuration could be hit for very large partitions.
   - Recommendation: This is within the 2x waste bound stated in D-03, and MemoryCARW extends by remapping. The existing window function tests with large datasets will validate this. Not a blocking concern.

## Sources

### Primary (HIGH confidence)
- GroupByDoubleList.java (nw_percentile branch) -- quickSelect implementation at lines 593-717, partition at 689-717 [VERIFIED: codebase inspection]
- GroupByLongList.java (nw_percentile branch) -- current sort implementation, API surface [VERIFIED: codebase inspection]
- PercentileDiscDoubleWindowFunctionFactory.java (nw_percentile branch) -- pass1 O(N^2) pattern at lines 186-209, preparePass2 at 230-256 [VERIFIED: codebase inspection]
- PercentileContDoubleWindowFunctionFactory.java (nw_percentile branch) -- identical pass1 pattern at lines 178-209 [VERIFIED: codebase inspection]
- MultiPercentileDiscDoubleWindowFunctionFactory.java (nw_percentile branch) -- identical pass1 pattern at lines 214-244 [VERIFIED: codebase inspection]
- MultiPercentileContDoubleWindowFunctionFactory.java (nw_percentile branch) -- identical pass1 pattern at lines 214-244 [VERIFIED: codebase inspection]
- PercentileDiscLongGroupByFunction.java (nw_percentile branch) -- sort() call at line 115 [VERIFIED: codebase inspection]
- PercentileDiscDoubleGroupByFunction.java (nw_percentile branch) -- quickSelect() call at line 119 (reference pattern) [VERIFIED: codebase inspection]
- MemoryCARWImpl.java -- appendAddressFor(), close(), contiguous memory semantics [VERIFIED: codebase inspection]
- AbstractMemoryCR.java -- addressOf(offset) = pageAddress + offset [VERIFIED: codebase inspection]

### Secondary (MEDIUM confidence)
- CONTEXT.md decisions D-01 through D-09 -- locked implementation choices [VERIFIED: user decisions]

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new libraries, pure internal refactoring
- Architecture: HIGH -- all patterns verified against current codebase on nw_percentile branch
- Pitfalls: HIGH -- identified from direct code analysis of offset arithmetic and API differences

**Research date:** 2026-04-13
**Valid until:** 2026-05-13 (stable -- internal codebase, no external dependencies)
