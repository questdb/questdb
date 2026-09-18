# Phase 1: Correctness - Context

**Gathered:** 2026-04-13
**Status:** Ready for planning

<domain>
## Phase Boundary

Fix 5 correctness bugs in percentile/quantile function implementations so that every function returns correct results on all inputs — including all-equal data, mixed null/non-null groups, single-element partitions, and invalid arguments.

Requirements: COR-01, COR-02, COR-03, COR-04, COR-05.

</domain>

<decisions>
## Implementation Decisions

### Sort Algorithm (COR-02)
- **D-01:** Reuse GroupByDoubleList's existing dual-pivot quicksort with Dutch National Flag handling for equal elements. Extract it as a shared utility (feeds into QUAL-01 in Phase 4). Do NOT write a new sort — the existing one already handles the equal-elements case correctly.
- **D-02:** For window functions, replace the current 8-copy Lomuto quickSort with calls to the extracted shared utility. The window functions operate on off-heap memory via `MemoryARW`, so the utility must work with raw memory addresses (pointer + offset), not just `GroupByDoubleList` internals.

### DirectArray State Management (COR-01)
- **D-03:** Re-apply type/shape on every non-null `getArray()` call: always call `setType()`, `setDimLen()`, `applyShape()` before writing data. This is ~30ns overhead per group — negligible vs the quickSelect/sort work per group.
- **D-04:** This applies to all 6 affected classes: `MultiPercentileDiscDoubleGroupByFunction`, `MultiPercentileContDoubleGroupByFunction`, `MultiApproxPercentileDoubleGroupByFunction`, `MultiApproxPercentileDoublePackedGroupByFunction`, `MultiApproxPercentileLongGroupByFunction`, `MultiApproxPercentileLongPackedGroupByFunction`.

### Validation Strategy (COR-03, COR-04)
- **D-05:** Percentile constness: validate in window function factory `newInstance()` methods. If `!percentileFunc.isConstant()`, throw `SqlException` with the argument position. This is a compile-time check.
- **D-06:** Percentile range: validate via `SqlUtil.getPercentileMultiplier()` at computation time (in `computeFirst`/`computeNext` for group-by, in `preparePass2` for window). This catches invalid bind variable values at runtime.
- **D-07:** Multi-approx_percentile: add per-element validation of the percentile array in `getArray()`, calling `SqlUtil.getPercentileMultiplier()` for each element. Throw `CairoException` on invalid values (runtime, since array contents may come from bind variables). This matches the single-value `ApproxPercentile` pattern.

### setEmpty() Override (COR-05)
- **D-08:** Store `0L` in `setEmpty()`, matching `ApproxPercentileDoubleGroupByFunction` pattern exactly. Add override to `PercentileDiscDoubleGroupByFunction`, `PercentileDiscLongGroupByFunction`, and `MultiPercentileDiscDoubleGroupByFunction`. Also add to `PercentileContDoubleGroupByFunction` and `MultiPercentileContDoubleGroupByFunction` for consistency.

### Claude's Discretion
- Exact error message wording for validation failures
- Whether to add the constness check to group-by factories too (they already validate range at computation time, but constness is not strictly required for group-by since percentile is evaluated per-group)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Existing reference implementations
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/ApproxPercentileDoubleGroupByFunction.java` — Reference for setEmpty(), validation pattern, lifecycle
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/ApproxPercentileDoubleGroupByFunctionFactory.java` — Reference for factory validation pattern
- `core/src/main/java/io/questdb/griffin/engine/groupby/GroupByDoubleList.java` — Contains the correct dual-pivot quicksort with Dutch National Flag (lines 186-397) to be extracted as shared utility

### Files to modify (COR-01: DirectArray reuse)
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MultiPercentileDiscDoubleGroupByFunction.java:138-143`
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MultiPercentileContDoubleGroupByFunction.java` — similar pattern
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MultiApproxPercentileDoubleGroupByFunction.java:135-140`
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MultiApproxPercentileDoublePackedGroupByFunction.java`
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MultiApproxPercentileLongGroupByFunction.java`
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MultiApproxPercentileLongPackedGroupByFunction.java`

### Files to modify (COR-02: Sort algorithm)
- `core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileDiscDoubleWindowFunctionFactory.java:286-330`
- `core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileContDoubleWindowFunctionFactory.java`
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileDiscDoubleWindowFunctionFactory.java`
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileContDoubleWindowFunctionFactory.java`

### Files to modify (COR-03, COR-04: Validation)
- `core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileDiscDoubleWindowFunctionFactory.java` — add constness check in newInstance()
- `core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileContDoubleWindowFunctionFactory.java`
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileDiscDoubleWindowFunctionFactory.java`
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileContDoubleWindowFunctionFactory.java`
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MultiApproxPercentileDoubleGroupByFunction.java:142` — add per-element validation
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MultiApproxPercentileLongGroupByFunction.java`

### Files to modify (COR-05: setEmpty)
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/PercentileDiscDoubleGroupByFunction.java`
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/PercentileDiscLongGroupByFunction.java`
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/PercentileContDoubleGroupByFunction.java`
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MultiPercentileDiscDoubleGroupByFunction.java`
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MultiPercentileContDoubleGroupByFunction.java`

### Array type system
- `core/src/main/java/io/questdb/cairo/arr/DirectArray.java:119` — ofNull() clears type/shape
- `core/src/main/java/io/questdb/cairo/arr/DirectArray.java:136` — putDouble() asserts type == DOUBLE
- `core/src/main/java/io/questdb/cairo/arr/MutableArray.java:100` — setType() implementation

### Validation utility
- `core/src/main/java/io/questdb/griffin/SqlUtil.java:98-103` — getPercentileMultiplier() range validation

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `GroupByDoubleList.sort()` (lines 186-397): Correct dual-pivot quicksort with Dutch National Flag. Must be extracted as a static utility that operates on raw double pointers.
- `SqlUtil.getPercentileMultiplier()`: Already validates range and handles negative percentiles. Reuse for all validation.
- `ApproxPercentileDoubleGroupByFunction.setEmpty()`: Pattern to follow — stores 0L.

### Established Patterns
- Group-by functions use `setEmpty()` for empty groups (0L) and `setNull()` for null groups (LONG_NULL)
- Factory `newInstance()` validates arguments at compile time
- `Numbers.isFinite(value)` filters both NaN and Infinity during computation
- `CairoException.nonCritical()` for runtime validation errors

### Integration Points
- Window function sort operates on `MemoryARW` (off-heap append-only memory), not `GroupByDoubleList`. The shared utility must accept raw `long ptr` + offset parameters.
- `FunctionParser` resolves overloads — constness check must happen in factory `newInstance()`, not in FunctionParser.

</code_context>

<specifics>
## Specific Ideas

No specific requirements — standard correctness fixes following established QuestDB patterns.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>

---

*Phase: 01-correctness*
*Context gathered: 2026-04-13*
