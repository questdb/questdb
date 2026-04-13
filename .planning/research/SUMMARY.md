# Project Research Summary

**Project:** Harden Percentile/Quantile Functions in QuestDB
**Domain:** Zero-GC Java database function hardening (algorithms, memory safety, lifecycle correctness)
**Researched:** 2026-04-13
**Confidence:** HIGH

## Executive Summary

This project hardens the exact percentile/quantile aggregate and window functions on QuestDB's `nw_percentile` branch. The code under review has the right structural idea — TWO_PASS window functions, off-heap group-by accumulators, per-partition maps — but contains a cluster of interconnected correctness, memory safety, and performance bugs introduced when the algorithms were first written. No new architectural components are required. Every fix follows patterns already established and working in QuestDB (StdDev window functions, SumDouble group-by, GroupByDoubleList). The hardening work is primarily about replacing broken algorithm choices with correct ones and enforcing resource lifecycle discipline consistently.

The algorithm decisions are settled: three-way (Dutch National Flag) partitioning is mandatory for correctness on equal-element inputs (the current strict-`<` Lomuto partition is O(N^2) on uniform data); quickselect is preferred over full sort for single-percentile functions (O(n) vs O(n log n)); append-in-place with capacity doubling replaces copy-on-append to avoid O(N^2) memory allocation. The multi-percentile array path uses a full sort plus indexed lookup, which is already efficient, but has a `DirectArray` state machine bug that corrupts results when a null group precedes a non-null group. These bugs are independently identified by all four research threads.

The primary risk is the copy-on-append to append-in-place refactoring in the window `pass1()` method. It involves replacing `MemoryARW`-relative offset arithmetic with a capacity-tracking scheme, and a reallocation can silently move data to a different `MemoryARW` page, corrupting cross-partition results with plausible-looking but wrong values. The mitigation is to adopt the `GroupByDoubleList` / `GroupByAllocator` pattern (single contiguous allocation via `Unsafe.malloc/realloc`) instead of raw page-relative offsets, which avoids page-boundary arithmetic entirely. All four research files independently converge on the same four-phase ordering: correctness first, resource safety second, performance third, polish and completeness last.

## Key Findings

### Recommended Stack

QuestDB's zero-GC, no-third-party-dependencies constraint fixes the implementation language and data structure choices. All computation happens on native (off-heap) memory managed via `Unsafe`, with `GroupByAllocator` for group-by state and `MemoryARW` for window function state. The only algorithm choices are: which selection/sort algorithm to use, and how to accumulate values without violating the zero-GC constraint.

**Core algorithms and data structures:**
- **Three-way (Dutch National Flag) quicksort partition** — replaces Lomuto strict-`<` partition; the sole correct fix for O(N^2) behavior on equal-element inputs. `LongSort.java` in QuestDB already uses this pattern and serves as the reference.
- **Quickselect with median-of-three pivot** — O(n) average for single-percentile selection; DuckDB and ClickHouse use `std::nth_element` (introselect) for the same purpose. Introselect is explicitly out of scope per PROJECT.md; median-of-three + three-way covers practical worst cases.
- **Full sort + indexed lookup** — for multi-percentile array variants where the caller requests multiple percentiles simultaneously; O(n log n) sort once, O(1) per lookup thereafter.
- **Off-heap resizable array (GroupByDoubleList / GroupByAllocator)** — zero-GC accumulation of doubles for group-by; the `nw_percentile` branch already introduced `GroupByDoubleList` and this should be used as the model for the window function append-in-place fix.
- **Linear interpolation (SQL standard R-7 method)** — for `percentile_cont`: `h = (N-1)*p`, result = `x[floor(h)] + frac(h) * (x[ceil(h)] - x[floor(h)])`. Matches PostgreSQL and SQL standard.

### Expected Features

**Must have (table stakes — exist but are broken or incomplete):**
- `percentile_disc(p, expr)` group-by on double and long — already exists, needs sort fix
- `percentile_cont(p, expr)` group-by on double — already exists, needs sort fix
- Window `percentile_disc/cont` over whole result set — already exists, O(N^2) partition bug
- Window `percentile_disc/cont` with PARTITION BY — already exists, O(N^2) memory + partition bug
- Multi-percentile array variants (`percentile_disc(DOUBLE[], expr)`) — already exists, DirectArray state machine bug
- NULL/NaN exclusion — already implemented, edge cases need verification
- Percentile argument validation (0 <= p <= 1) — missing in multi-array variants
- Resource cleanup on all lifecycle paths — broken in Multi* window functions

**Should have (correctness differentiators and maintainability):**
- O(n) single percentile via quickselect — currently O(n log n); major win for large partitions
- Append-in-place window accumulation — eliminates O(N^2) memory; architecturally important
- Shared quickSort/quickSelect utility — eliminates 8 copies across 4 factory files
- Pre-allocated `double[]` in `preparePass2` — eliminates zero-GC violation on query data path
- `quantile_disc` / `quantile_cont` window function aliases — missing for window variants
- `toPlan()` fix to show percentile argument — currently absent from EXPLAIN output

**Defer (out of scope per PROJECT.md):**
- Introselect (guaranteed O(n) worst-case) — median-of-three + three-way covers practical cases
- JMH micro-benchmarks — separate effort
- New approximate algorithms (t-digest, KLL sketch) — `approx_percentile` via HDR already exists
- Parallel/SIMD percentile — future performance work
- Weighted percentile — not in SQL standard

### Architecture Approach

The percentile functions fit entirely within QuestDB's established group-by and window function frameworks. The group-by path uses the `GroupByFunction` lifecycle (computeFirst / computeNext / merge / setNull / setEmpty) with `GroupByDoubleList` flyweight over `GroupByAllocator`-managed memory. The window path uses the TWO_PASS `WindowFunction` lifecycle (pass1 accumulates, preparePass2 sorts and selects, pass2 writes results) orchestrated by `CachedWindowRecordCursorFactory`. `StdDevOverPartitionFunction` is the canonical reference for a correct TWO_PASS partitioned window function and should be followed exactly for lifecycle method signatures and memory cleanup patterns.

**Major components (no new ones needed):**
1. **GroupByDoubleList / GroupByAllocator** — off-heap accumulation for group-by percentile; already on nw_percentile branch; needs `quickSelect` method added
2. **MemoryARW per-partition buffer** — off-heap storage for window function value lists; current copy-on-append must be replaced with capacity-tracking append-in-place
3. **DirectArray (multi-percentile result)** — owns its native memory; has state machine bug (ofNull does not free memory, but initialization guard uses `out == null`) and lifecycle leak (not freed in reset/reopen)
4. **Shared sort/select utility** — to be extracted from 8 existing duplicate copies; must start with the correct three-way partition algorithm
5. **FunctionFactory + function_list.txt** — 10 missing group-by factory registrations; window alias factories for quantile_disc/quantile_cont

### Critical Pitfalls

1. **Lomuto `<` partition is wrong for equal elements** — changing `<` to `<=` in Lomuto does not fix it (equal-left degeneration replaces equal-right degeneration). Must restructure to three-way partition. Test with 10,000 identical values.

2. **Copy-on-append to append-in-place introduces silent data corruption** — after reallocation, `MemoryARW` page address can change, making stored relative offsets stale. Fix: adopt `GroupByAllocator` + `Unsafe.malloc/realloc` (single contiguous allocation, no page arithmetic) rather than patching MemoryARW offsets.

3. **DirectArray state machine: ofNull() does not free memory** — the `if (out == null)` initialization guard in `getArray()` misses re-initialization after a null group. Always re-apply `setType/setDimLen/applyShape` before populating, regardless of whether `out` is null.

4. **Memory leak cascade in window lifecycle methods** — `reset()` does not free `DirectArray result`; `reopen()` calls `close()` without nulling the reference; `toTop()` leaves stale result pointers. Pattern: `result = Misc.free(result)` assigns null back. Tests must run the query twice within `assertMemoryLeak()` to trigger reopen cycles.

5. **quickSelectMultiple requires sorted indices** — the multi-percentile select algorithm fails silently if indices are not ascending. Sort indices before calling, or enforce the contract with an assertion inside the utility.

## Implications for Roadmap

All four research files independently converge on the same phase ordering. The rationale is:
- bugs must be corrected before utilities are extracted (otherwise the utility codifies the bug)
- resource safety must follow correctness (memory patterns change when copy-on-append is replaced)
- performance and polish depend on a stable, correct foundation

### Phase 1: Correctness

**Rationale:** The most dangerous bugs produce silent wrong results. Wrong sort behavior on equal elements, DirectArray state machine violations, and missing validation corrupt query output without errors. These must be fixed first, before any extraction or refactoring, so that test coverage can verify correctness of later phases.

**Delivers:** Percentile functions that return correct results on all inputs: all-equal data, single-element partitions, all-NULL partitions, mixed null/non-null groups, and unsorted percentile index arrays.

**Addresses:** correct results on all-equal data, DirectArray reuse fix, percentile argument validation in multi-array variants, `setEmpty` vs `setNull` distinction in group-by functions.

**Avoids:** Pitfall 1 (Lomuto operator mismatch), Pitfall 3 (DirectArray state machine), Pitfall 6 (unsorted quickSelectMultiple indices).

**Research flag:** No additional phase research needed. Patterns are clear from `GroupByDoubleList.sort()` (three-way partition reference) and PITFALLS.md analysis.

### Phase 2: Resource Safety

**Rationale:** After correctness is established and verified by tests, fix memory lifecycle. The lifecycle bugs (missing `Misc.free` in reset/reopen, zero-GC violation in preparePass2) are independent of the algorithm fixes but require the algorithm to be stable before the memory patterns are restructured.

**Delivers:** No native memory leaks under any execution path. Zero heap allocations on the query data path. `assertMemoryLeak()` tests pass including multi-execution (toTop/reopen) scenarios.

**Addresses:** Resource cleanup on all code paths, pre-allocated `double[]` in Multi* preparePass2, Reopenable contract in `PercentileContOverPartitionFunction.reopen()`.

**Avoids:** Pitfall 4 (lifecycle method cascade), Pitfall 8 (zero-GC violation from `new double[]`).

**Research flag:** No additional research needed. Reference pattern is `StdDevOverPartitionFunction` lifecycle methods, documented in ARCHITECTURE.md Pattern 4.

### Phase 3: Performance

**Rationale:** The append-in-place refactoring (replacing copy-on-append in window pass1) is the single riskiest structural change. It must come after correctness tests are in place so that any data corruption introduced by offset arithmetic errors is immediately caught. The quickselect optimization is lower risk but logically belongs in the same phase.

**Delivers:** O(n) memory for window accumulation (down from O(N^2)), O(n) average selection for single-percentile functions (down from O(n log n)), and linear-time behavior on uniform/repeated-value data.

**Addresses:** O(n) single percentile via quickselect, append-in-place window accumulation.

**Avoids:** Pitfall 2 (offset arithmetic corruption during append-in-place). Mitigate by adopting `GroupByAllocator` pattern rather than raw MemoryARW offset arithmetic.

**Research flag:** The append-in-place refactoring warrants a short design spike before coding. Key decision: whether to use `GroupByAllocator` for window functions (currently only used for group-by) or introduce a window-specific capacity-tracking scheme over MemoryARW. This is an architectural decision with meaningful tradeoffs — confirm which approach to use before writing code.

### Phase 4: Code Quality and Completeness

**Rationale:** Extract the shared utility, add missing registrations, add aliases, and fix EXPLAIN output after the algorithm is correct and the lifecycle is clean. Extracting an incorrect algorithm into a shared utility is worse than leaving the duplication.

**Delivers:** 8 duplicate quickSort/quickSelect copies reduced to one shared utility, 10 missing factory registrations added, `quantile_disc`/`quantile_cont` window aliases, toPlan fix, migrated tests using `assertQueryNoLeakCheck`.

**Addresses:** Shared sort utility, `quantile_disc`/`quantile_cont` window aliases, toPlan showing percentile argument, test migration from assertSql to assertQueryNoLeakCheck.

**Avoids:** Pitfall 5 (NaN contract in extracted utility — document it), Pitfall 7 (error position in validation), Pitfall 9 (function_list.txt registration order), Pitfall 10 (alias delegation), Pitfall 11 (assertSql vs assertQueryNoLeakCheck semantic difference).

**Research flag:** No additional research needed. All patterns are established; this is mechanical work.

### Phase Ordering Rationale

- Correctness before extraction: extracting 8 broken copies into one shared broken utility creates a single point of failure for all 8 callers instead of isolated failures.
- Resource safety before performance: the append-in-place refactoring changes how partition memory is managed; doing it on top of already-leaking lifecycle code compounds debugging difficulty.
- Performance before polish: the append-in-place change is structural enough that extracting the utility first and then restructuring memory management would require touching the extracted utility a second time.
- The `DirectArray` state machine fix and the `setEmpty` override fix are both Phase 1 work because both interact with the same `getArray()` code path.

### Research Flags

Phases needing deeper design work before coding:
- **Phase 3 (append-in-place):** Decision required on GroupByAllocator vs MemoryARW with capacity tracking for window function buffers. The MemoryARW multi-page arithmetic is the identified source of Pitfall 2 (cross-partition corruption). Recommend a short design spike to confirm which approach to use before writing code.

Phases with well-documented patterns (no additional research needed):
- **Phase 1 (correctness):** Three-way partition from `LongSort.java`; DirectArray fix from PITFALLS.md Pitfall 3; quickSelectMultiple index sort is a one-liner.
- **Phase 2 (resource safety):** Exact lifecycle pattern from `StdDevOverPartitionFunction` in ARCHITECTURE.md Pattern 4. Checklist: enumerate every native field, verify all four lifecycle methods.
- **Phase 4 (code quality):** Mechanical extraction and registration. Only subtlety is NaN contract documentation in the shared utility Javadoc.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack (algorithms) | HIGH | Three-way partition, quickselect, linear interpolation are textbook with decades of production use. Industry comparison across PostgreSQL, DuckDB, ClickHouse confirms approach. |
| Features | HIGH | Based on direct source code inspection of nw_percentile branch. All 17 findings from PROJECT.md verified in research. |
| Architecture | HIGH | All patterns verified via direct source code inspection on master and nw_percentile branches. Reference implementations (StdDev, SumDouble) confirmed in code. |
| Pitfalls | HIGH | All 11 pitfalls derived from direct code analysis of nw_percentile branch. Root causes are specific line-level issues, not inferences. |

**Overall confidence: HIGH**

### Gaps to Address

- **PostgreSQL/DuckDB/ClickHouse source verification** — industry comparison is based on training-data knowledge of those codebases (MEDIUM confidence). The algorithm recommendations do not depend on this comparison — they follow from the QuestDB constraints and are correct independently — but the comparative table in STACK.md should be treated as illustrative rather than normative.

- **GroupByAllocator for window functions** — currently `GroupByAllocator` is only used for group-by functions. Using it in window functions would simplify the append-in-place implementation significantly. This cross-boundary use needs design validation: confirm that `GroupByAllocator`'s lifecycle (owned by the group-by engine) does not conflict with window function lifecycle (owned by `CachedWindowRecordCursorFactory`). If it conflicts, a window-specific allocator or a raw `Unsafe.malloc/realloc` wrapper is needed.

- **quickSelectMultiple algorithm correctness** — PITFALLS.md identifies that unsorted indices break the algorithm. The existing `GroupByDoubleList.quickSelectMultiple()` must be audited to confirm it is otherwise correct, since it is not one of the 8 duplicate copies being replaced.

## Sources

### Primary (HIGH confidence — direct source code inspection)

- `core/src/main/java/io/questdb/std/LongSort.java` — dual-pivot quicksort with Dutch National Flag (three-way partition reference)
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/ApproxPercentileDoubleGroupByFunction.java` — reference group-by percentile pattern
- `core/src/main/java/io/questdb/griffin/engine/functions/window/AbstractStdDevDoubleWindowFunctionFactory.java` — TWO_PASS window function reference implementation
- `core/src/main/java/io/questdb/griffin/engine/window/CachedWindowRecordCursorFactory.java` — two-pass orchestration
- `core/src/main/java/io/questdb/griffin/engine/functions/window/BasePartitionedWindowFunction.java` — lifecycle contract
- `core/src/main/java/io/questdb/cairo/arr/DirectArray.java` — DirectArray lifecycle (ofNull/clear/close)
- nw_percentile branch: `PercentileContDoubleWindowFunctionFactory.java`, `MultiPercentileDiscDoubleWindowFunctionFactory.java`, `GroupByDoubleList.java` — implementation under review

### Secondary (MEDIUM confidence — training data knowledge of external systems)

- PostgreSQL `src/backend/utils/adt/orderedsetaggs.c` — ordered-set aggregate: full sort, NULL exclusion
- DuckDB `src/core_functions/aggregate/holistic/quantile.cpp` — nth_element (introselect) for single quantile
- ClickHouse `src/AggregateFunctions/AggregateFunctionQuantile.h` — quantileExact via nth_element

### Tertiary (reference)

- Cormen et al., "Introduction to Algorithms" (CLRS), Chapter 9 — quickselect O(n) analysis
- Sedgewick & Wayne, "Algorithms" (4th ed), Section 2.3 — three-way quicksort / Dutch National Flag
- ISO/IEC 9075 SQL standard — percentile_disc / percentile_cont semantics and interpolation formula

---
*Research completed: 2026-04-13*
*Ready for roadmap: yes*
