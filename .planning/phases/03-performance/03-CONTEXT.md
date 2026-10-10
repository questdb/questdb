# Phase 3: Performance - Context

**Gathered:** 2026-04-13
**Status:** Ready for planning

<domain>
## Phase Boundary

Fix two performance pathologies: (1) O(N²) copy-on-append in partitioned window pass1 → amortized O(1) via capacity-tracking, and (2) O(n log n) full sort in PercentileDiscLongGroupByFunction → O(n) quickSelect. Requirements: PERF-01, PERF-02.

</domain>

<decisions>
## Implementation Decisions

### Append-in-Place Strategy (PERF-01)
- **D-01:** Use capacity-tracking per partition. Change the per-partition block layout from `[size|val0|val1|...|valN]` to `[capacity|size|val0|val1|...|valN]` (add 8-byte capacity header).
- **D-02:** Growth policy: when `size >= capacity`, allocate a new block at `2 * capacity` and copy once. This gives amortized O(1) per row, O(N) total. Only O(log N) copies per partition instead of O(N).
- **D-03:** Old blocks become garbage in MemoryARW — they are freed when the query ends (MemoryARW.close()). This is acceptable since the total wasted space is bounded by 2x the live data.
- **D-04:** Apply to all 4 partitioned window inner classes: PercentileDiscOverPartitionFunction, PercentileContOverPartitionFunction, MultiPercentileDiscOverPartitionFunction, MultiPercentileContOverPartitionFunction.
- **D-05:** The whole-result-set variants already use O(1) append (`listMemory.putDouble(size * 8, d)`) — no changes needed there.
- **D-06:** In `preparePass2()`, read values from the capacity-tracked block: skip the capacity header, read `size` from offset 0 of the data, then read values from offset 8 onward. Adjust all offset arithmetic accordingly.

### GroupByLongList quickSelect (PERF-02)
- **D-07:** Direct port of `quickSelect()` and `quickSelectMultiple()` from `GroupByDoubleList` to `GroupByLongList`. Change `double` comparisons to `long` comparisons. Use `getQuick()`/`setValueAt()` which already exist in GroupByLongList.
- **D-08:** Add bounds validation to `quickSelect()` (lo >= 0, hi < size(), k within range) — matching GroupByDoubleList's quickSelect. Also add bounds validation to `quickSelectMultiple()` which GroupByDoubleList is missing.
- **D-09:** Update `PercentileDiscLongGroupByFunction.getLong()` to call `listA.quickSelect(0, size - 1, N)` instead of `listA.sort()`.

### Claude's Discretion
- Initial capacity for new partition blocks (suggested: 16 elements = 128 bytes + 16 byte header)
- Whether to add a `quickSelectMultiple()` to GroupByLongList too (for potential future multi-percentile long support)
- Exact offset arithmetic in the capacity-tracked block layout

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Current O(N²) code to replace
- `core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileDiscDoubleWindowFunctionFactory.java:185-209` — Current copy-on-append pattern in PercentileDiscOverPartitionFunction.pass1()

### Reference for correct O(1) append
- `core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileDiscDoubleWindowFunctionFactory.java:380-387` — Whole-result-set pass1 already does O(1) append

### quickSelect source to port
- `core/src/main/java/io/questdb/griffin/engine/groupby/GroupByDoubleList.java` — Contains quickSelect (line ~560) and quickSelectMultiple (line ~617) to port to GroupByLongList

### Target for quickSelect addition
- `core/src/main/java/io/questdb/griffin/engine/groupby/GroupByLongList.java` — Add quickSelect and quickSelectMultiple methods

### Caller to update
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/PercentileDiscLongGroupByFunction.java:115` — Change `listA.sort()` to `listA.quickSelect()`

### Other partitioned variants to update (PERF-01)
- `core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileContDoubleWindowFunctionFactory.java`
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileDiscDoubleWindowFunctionFactory.java`
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileContDoubleWindowFunctionFactory.java`

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `GroupByDoubleList.quickSelect()`: Proven O(n) algorithm with median-of-three pivot. Direct port target.
- `MemoryARW`: Handles page growth internally — `appendAddressFor()` grows as needed. The capacity-tracking approach builds on top of this.

### Established Patterns
- Partitioned window functions store per-partition state via Map key → MapValue (long pointer into listMemory)
- Block layout in listMemory: first 8 bytes = metadata, rest = data array
- Whole-result-set variants use simple sequential append (no block structure)

### Integration Points
- `pass1()` writes blocks, `preparePass2()` reads and sorts them, `pass2()` emits results
- MapValue stores the offset pointer to the current partition's block in listMemory
- When block is relocated (capacity growth), MapValue must be updated with new offset

</code_context>

<specifics>
## Specific Ideas

No specific requirements — standard performance optimization following established patterns.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>

---

*Phase: 03-performance*
*Context gathered: 2026-04-13*
