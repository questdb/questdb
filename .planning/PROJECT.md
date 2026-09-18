# Harden Percentile/Quantile Functions

## What This Is

Hardening pass on QuestDB's percentile_disc, percentile_cont, quantile_disc, and quantile_cont aggregate and window functions (PR #6680, `nw_percentile` branch). The functions exist and pass basic tests but have correctness bugs, performance pathologies, resource leaks, and test gaps identified by code review.

## Core Value

Every percentile/quantile query must return correct results on all inputs — including edge cases like null groups, all-equal values, large partitions, and invalid percentile arguments — without crashing, leaking memory, or producing silent wrong answers.

## Requirements

### Validated

- ✓ percentile_disc/percentile_cont work as group-by aggregates on double data — existing tests pass
- ✓ percentile_disc works on long data as group-by aggregate — existing tests pass
- ✓ Multi-percentile array variants return DOUBLE[] — existing tests pass
- ✓ Window function variants work for whole-result-set frames — existing tests pass
- ✓ approx_percentile multi-array variant reuses HDR Histogram — existing tests pass
- ✓ quantile_disc/quantile_cont are aliases of percentile functions — sqllogictest pass
- ✓ NULL/NaN values are filtered during computation — existing tests verify

### Active

- [ ] Fix DirectArray reuse bug in multi-array group-by functions (ofNull clears type/shape, non-null path skips re-init)
- [ ] Fix O(N²) memory+CPU in partitioned window pass1 (copy-on-append → append-in-place)
- [ ] Fix quickSort O(N²) on all-equal elements (Lomuto strict < → three-way partition or quickselect)
- [ ] Add percentile validation in multi-approx_percentile (silent clamping → throw on invalid)
- [ ] Validate percentile argument is constant in window function factories
- [ ] Fix DirectArray leak in Multi* window reset/reopen/toTop
- [ ] Add quickSelect to GroupByLongList for O(n) percentile on longs
- [ ] Register 10 missing group-by factories in function_list.txt
- [ ] Extract duplicated quickSort into shared utility (8 copies → 1)
- [ ] Fix zero-GC violation: pre-allocate double[] in Multi* preparePass2
- [ ] Add quantile_disc/quantile_cont window function aliases
- [ ] Add setEmpty() override in disc group-by functions
- [ ] Fix toPlan() to show percentile argument in EXPLAIN output
- [ ] Add tests for quantile_cont/quantile_disc SQL syntax
- [ ] Add window function tests for PARTITION BY paths
- [ ] Add error-path tests (ORDER BY in window should fail)
- [ ] Switch tests from assertSql to assertQueryNoLeakCheck

### Out of Scope

- Adding new percentile algorithms (e.g. t-digest) — not part of this PR
- Performance benchmarking / JMH conversion — separate effort
- Negative percentile documentation — low priority, can be a follow-up
- Recursive quickSort depth limit (introsort) — median-of-three is sufficient in practice

## Context

- **Branch:** `nw_percentile` (PR #6680), 40 commits ahead of master
- **Author:** Nick Woolmer (@nwoolmer)
- **CI status:** "Other tests (A)" jobs were failing due to sqllogictest bpchar crash — fixed by rebuilding Rust native libraries
- **Reviews:** Consolidated review posted on PR with 17 findings (4 critical, 4 high, 4 medium, 5 low)
- **Codebase:** QuestDB is zero-GC Java with native C/C++/Rust. No third-party Java dependencies. Column-oriented storage with SIMD vectorization.
- **Existing patterns:** ApproxPercentileDoubleGroupByFunction is the reference implementation for group-by percentile functions. Statistical window functions (stddev, variance, covariance, corr) from PR #6922 are the reference for window function patterns.

## Constraints

- **Zero-GC:** No Java heap allocations on data paths (query execution, aggregation)
- **No third-party deps:** All algorithms implemented from first principles
- **Backward compat:** Function signatures and SQL syntax must not change
- **Test conventions:** assertMemoryLeak(), assertQueryNoLeakCheck(), UPPERCASE SQL keywords
- **Pre-compiled Rust:** sqllogictest runner uses pre-compiled native binaries; Rust source changes require rebuild workflow

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Correctness before performance | Bugs that produce wrong results or crash are more urgent than O(N²) perf | — Pending |
| Extract shared quickSort utility | 8 duplicated copies across 4 factories is a maintenance and bug-surface risk | — Pending |
| Use quickselect instead of full sort where possible | O(n) avg vs O(n log n) for finding k-th element | — Pending |
| Rebuild Rust binaries rather than work around bpchar | Proper fix in the runner, not casts in every test file | ✓ Good |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-04-13 after initialization*
