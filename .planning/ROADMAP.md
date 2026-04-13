# Roadmap: Harden Percentile/Quantile Functions

## Overview

This roadmap hardens QuestDB's percentile/quantile aggregate and window functions across four phases ordered by dependency: correctness bugs first (so we extract correct code, not buggy code), resource safety second (memory patterns depend on corrected algorithms), performance third (the riskiest structural change, guarded by correctness tests), and code quality last (mechanical extraction and registration on a stable foundation).

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3, 4): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [ ] **Phase 1: Correctness** - Fix algorithms and validation so every percentile query returns correct results on all inputs
- [ ] **Phase 2: Resource Safety** - Eliminate memory leaks and zero-GC violations in window function lifecycle
- [ ] **Phase 3: Performance** - Replace O(N^2) accumulation with append-in-place and add O(n) quickselect
- [ ] **Phase 4: Code Quality and Completeness** - Extract shared utilities, register factories, add aliases, migrate tests

## Phase Details

### Phase 1: Correctness
**Goal**: Every percentile/quantile function returns correct results on all inputs including all-equal data, mixed null/non-null groups, single-element partitions, and invalid arguments
**Depends on**: Nothing (first phase)
**Requirements**: COR-01, COR-02, COR-03, COR-04, COR-05
**Success Criteria** (what must be TRUE):
  1. percentile_disc and percentile_cont on a column of 10,000 identical values return that value (not hang or produce wrong result)
  2. Multi-percentile array group-by returns correct results when a null group precedes a non-null group in the same query
  3. Multi-approx_percentile with an out-of-range percentile value (e.g., 1.5 or -0.1) throws a SqlException instead of silently clamping
  4. Window percentile with a non-constant expression as the percentile argument is rejected at compile time
  5. Disc group-by functions return correct empty-group values (0L sentinel) matching ApproxPercentile pattern
**Plans:** 2 plans
Plans:
- [ ] 01-01-PLAN.md — Fix group-by correctness: setEmpty, DirectArray state, multi-approx validation
- [ ] 01-02-PLAN.md — Fix window factory correctness: constness validation, sort algorithm replacement

### Phase 2: Resource Safety
**Goal**: No native memory leaks under any execution path, and no heap allocations on the query data path
**Depends on**: Phase 1
**Requirements**: RES-01, RES-02
**Success Criteria** (what must be TRUE):
  1. Running a Multi* window percentile query twice within assertMemoryLeak (triggering reopen/toTop cycles) reports zero leaked bytes
  2. Multi* window preparePass2 executes without allocating on the Java heap (pre-allocated double[] reused across calls)
**Plans:** 1 plan
Plans:
- [ ] 02-01-PLAN.md — Fix DirectArray lifecycle leak and double[] zero-GC compliance in Multi* window functions

### Phase 3: Performance
**Goal**: Window percentile accumulation is O(N) memory and single-percentile group-by selection is O(n) average time
**Depends on**: Phase 2
**Requirements**: PERF-01, PERF-02
**Success Criteria** (what must be TRUE):
  1. Partitioned window percentile on a table with 100K+ rows completes without O(N^2) memory growth (append-in-place with capacity tracking replaces copy-on-append)
  2. PercentileDiscLongGroupByFunction uses quickSelect (O(n) average) instead of full sort (O(n log n)) for single-percentile computation
  3. All existing percentile tests continue to pass after the structural refactoring (no data corruption from offset arithmetic changes)
**Plans:** 2 plans
Plans:
- [ ] 03-01-PLAN.md — Port quickSelect to GroupByLongList and update PercentileDiscLongGroupByFunction caller
- [ ] 03-02-PLAN.md — Replace O(N^2) copy-on-append with capacity-tracked append-in-place in 4 window factories

### Phase 4: Code Quality and Completeness
**Goal**: Duplicated code is consolidated, all factories are registered, aliases exist for window functions, and test coverage uses correct assertion patterns
**Depends on**: Phase 3
**Requirements**: QUAL-01, QUAL-02, REG-01, REG-02, TEST-01, TEST-02, TEST-03, TEST-04
**Success Criteria** (what must be TRUE):
  1. quickSort/quickSelect exists in exactly one shared utility class (not 8 duplicate copies across factory files)
  2. EXPLAIN on any percentile group-by function shows the percentile argument value in the plan output
  3. quantile_disc() and quantile_cont() work as window functions (not just group-by aggregates)
  4. All 10 previously unregistered group-by factories appear in function_list.txt and are callable from SQL
  5. Percentile window tests cover PARTITION BY paths, ORDER BY error rejection, and use assertQueryNoLeakCheck instead of assertSql
**Plans:** 3 plans
Plans:
- [x] 04-01-PLAN.md — Extract DoubleSort utility and replace 9 duplicate sort implementations
- [x] 04-02-PLAN.md — Fix toPlan output, register 10 group-by factories, create 4 window alias factories
- [x] 04-03-PLAN.md — Migrate assertSql to assertQueryNoLeakCheck, add alias/error/partition tests

## Progress

**Execution Order:**
Phases execute in numeric order: 1 -> 2 -> 3 -> 4

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Correctness | 0/2 | Planning complete | - |
| 2. Resource Safety | 0/1 | Planning complete | - |
| 3. Performance | 0/2 | Planning complete | - |
| 4. Code Quality and Completeness | 0/3 | Planning complete | - |
