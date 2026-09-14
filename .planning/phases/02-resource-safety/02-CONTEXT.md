# Phase 2: Resource Safety - Context

**Gathered:** 2026-04-13
**Status:** Ready for planning

<domain>
## Phase Boundary

Fix native memory leaks and eliminate heap allocations on the query data path in Multi* window percentile functions. Two requirements: RES-01 (DirectArray leak in lifecycle methods) and RES-02 (zero-GC violation in preparePass2).

</domain>

<decisions>
## Implementation Decisions

### DirectArray Lifecycle (RES-01)
- **D-01:** Use `result = Misc.free(result)` pattern in `reset()`, `reopen()`, and `toTop()` of all Multi* window function inner classes. This frees native memory and nulls the reference in one call. Matches QuestDB convention (BasePartitionedWindowFunction, etc.).
- **D-02:** Apply to both MultiPercentileDisc and MultiPercentileCont factories, in both OverPartitionFunction and OverWholeResultSetFunction inner classes (4 total classes affected).
- **D-03:** Also verify `close()` already frees correctly (it does per review, but confirm during implementation).

### Zero-GC Allocation Fix (RES-02)
- **D-04:** Pre-allocate `double[] results` as a persistent field. In preparePass2(): only reallocate if `results == null || results.length < percentileCount`. Since percentileCount is now guaranteed constant (COR-04 from Phase 1 enforces constness), the array is allocated exactly once per function lifetime and reused.
- **D-05:** On `reopen()`/`reset()`: do NOT null the results array — let it be reused. Only `close()` should release it (set to null).
- **D-06:** This applies to MultiPercentileDiscOverWholeResultSetFunction and MultiPercentileContOverWholeResultSetFunction (and their partitioned variants if they have the same pattern).

### Testing Strategy
- **D-07:** Verify fixes using assertMemoryLeak() with reopen cycles: run a Multi* window query, call cursor.toTop(), re-iterate to trigger reopen/toTop paths. Any native memory leak fails the test.

### Claude's Discretion
- Whether to also fix similar patterns in the partitioned variants (OverPartitionFunction) if they have the same issue
- Exact placement of Misc.free() calls relative to other cleanup in reset/reopen/toTop

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Lifecycle reference
- `core/src/main/java/io/questdb/griffin/engine/functions/window/PercentileDiscDoubleWindowFunctionFactory.java` — Contains both OverPartitionFunction and OverWholeResultSetFunction inner classes with reset/reopen/toTop/close methods
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileDiscDoubleWindowFunctionFactory.java` — Primary target: lines 472 (new double[]), 483-487 (reopen), 490-495 (reset)
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiPercentileContDoubleWindowFunctionFactory.java` — Same pattern

### Existing patterns to follow
- `core/src/main/java/io/questdb/std/Misc.java` — `Misc.free()` and `Misc.freeIfCloseable()` patterns
- `core/src/main/java/io/questdb/griffin/engine/functions/window/AbstractStdDevDoubleWindowFunctionFactory.java` — Reference for correct window function lifecycle (from stddev/variance PR #6922)

### Phase 1 context (already applied)
- `.planning/phases/01-correctness/01-CONTEXT.md` — DirectArray re-init pattern (D-03/D-04) already implemented in Phase 1

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `Misc.free(Closeable)`: Returns null after closing — standard QuestDB cleanup idiom
- Phase 1 already fixed DirectArray state re-init; this phase fixes the lifecycle leak

### Established Patterns
- `reset()` frees owned resources (Misc.free), resets counters to 0
- `reopen()` closes and re-creates mutable state, preserves configuration
- `toTop()` resets iteration position without freeing resources
- `close()` frees everything

### Integration Points
- The `results` field in MultiPercentile* is read by `pass2()` and `getArray()` — must remain valid between preparePass2() and pass2()

</code_context>

<specifics>
## Specific Ideas

No specific requirements — standard lifecycle fixes following established QuestDB patterns.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>

---

*Phase: 02-resource-safety*
*Context gathered: 2026-04-13*
