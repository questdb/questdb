# Feature Landscape: Percentile/Quantile Function Hardening

**Domain:** Time-series database aggregate and window functions
**Researched:** 2026-04-13
**Focus:** Correctness behaviors that production databases guarantee

## Table Stakes

Features users expect. Missing = product feels incomplete or buggy.

### 1. SQL Standard Semantics for percentile_disc

| Behavior | PostgreSQL | DuckDB | QuestDB (current) | Status |
|----------|-----------|--------|-------------------|--------|
| Returns actual input value (no interpolation) | Yes | Yes | Yes | OK |
| NULL inputs excluded from computation | Yes | Yes | Yes (Numbers.isFinite) | OK |
| Empty set returns NULL | Yes | Yes | Yes (ptr <= 0 or size == 0) | OK |
| fraction=0.0 returns minimum | Yes | Yes | Yes: ceil(0)-1=-1, clamped to 0 | NEEDS TEST |
| fraction=1.0 returns maximum | Yes | Yes | Yes: ceil(size)-1=size-1 | NEEDS TEST |
| Single value returns that value for any fraction | Yes | Yes | Yes: ceil(1*p)-1=0 for p in (0,1] | NEEDS TEST |
| All-equal values returns that value | Yes | Yes | OK if sort works | NEEDS TEST |
| fraction must be in [0, 1] | Yes (error) | Yes (error) | Yes (CairoException) | OK |
| NULL fraction returns NULL | Yes | DuckDB: error | Throws CairoException via Numbers.isNull | DIVERGENCE |
| Works with LONG/INT types | Yes (anyelement) | Yes | Yes (PercentileDiscLong) | OK |
| GROUP BY support | Yes | Yes | Factory exists but NOT REGISTERED | BUG |
| SAMPLE BY support | N/A | N/A | getSampleByFlags returns FILL_ALL | NEEDS VALIDATION |

**Complexity:** Low per item, but coverage gaps are numerous.

**Notes on percentile_disc index formula:**
- PostgreSQL: "first value whose position in the ordering equals or exceeds the specified fraction"
- QuestDB current formula: `N = max(0, ceil(size * multiplier) - 1)` then quickSelect to index N
- For fraction=0.0: `ceil(0) - 1 = -1`, clamped to 0 -- returns minimum. Correct.
- For fraction=1.0: `ceil(size) - 1 = size - 1` -- returns maximum. Correct.
- For single element: `ceil(1 * p) - 1`, for any p in (0,1]: `ceil(p) - 1 = 0`. Correct.
- For fraction in (0, 1/size): `ceil(frac) - 1 = 0` -- returns minimum. Matches PostgreSQL.

### 2. SQL Standard Semantics for percentile_cont

| Behavior | PostgreSQL | DuckDB | QuestDB (current) | Status |
|----------|-----------|--------|-------------------|--------|
| Linear interpolation between adjacent values | Yes | Yes | Yes | OK |
| NULL inputs excluded | Yes | Yes | Yes (Numbers.isFinite) | OK |
| Empty set returns NULL | Yes | Yes | Yes | OK |
| fraction=0.0 returns minimum | Yes | Yes | position=0, lower==upper | OK |
| fraction=1.0 returns maximum | Yes | Yes | position=size-1, lower==upper | OK |
| Single value returns that value | Yes | Yes | position=0, lower==upper | OK |
| All-equal values returns that value | Yes | Yes | Interpolation of equal values | OK |
| fraction must be in [0, 1] | Yes (error) | Yes (error) | Yes (CairoException) | OK |
| NULL fraction returns NULL | Yes | DuckDB: error | Throws CairoException | DIVERGENCE |
| Returns DOUBLE even for integer input | Yes (double precision) | Yes | Yes (DoubleFunction) | OK |
| GROUP BY support | Yes | Yes | Factory exists but NOT REGISTERED | BUG |

**Interpolation formula (matches PostgreSQL and DuckDB):**
```
position = fraction * (N - 1)
lowerIndex = floor(position)
upperIndex = ceil(position)
if lowerIndex == upperIndex: return values[lowerIndex]
else: return values[lowerIndex] + (values[upperIndex] - values[lowerIndex]) * (position - lowerIndex)
```

**Complexity:** Low -- formulas are already correct; main gap is test coverage and factory registration.

### 3. Window Function Support

| Behavior | PostgreSQL | DuckDB | QuestDB (current) | Status |
|----------|-----------|--------|-------------------|--------|
| OVER () whole result set | N/A (not a window fn) | Yes | Yes | OK |
| OVER (PARTITION BY ...) | N/A | Yes | Yes | OK |
| OVER (ORDER BY ...) with frames | N/A | DuckDB allows it | Explicitly rejected (error) | ACCEPTABLE |
| Same value for all rows in partition | Expected | Yes | Yes (two-pass) | OK |
| Empty partition returns NULL | Expected | Yes | Verify | NEEDS TEST |
| Single-row partition correct | Expected | Yes | Verify | NEEDS TEST |
| All-NULL partition returns NULL | Expected | Yes | Verify (no values added) | NEEDS TEST |
| Mixed NULL/non-NULL partition | Expected | Yes | Verify NULL filtering | NEEDS TEST |
| quantile_disc/quantile_cont as window aliases | Expected | Yes | NOT IMPLEMENTED | MISSING |

**Key PostgreSQL difference:** PostgreSQL does NOT support percentile_disc/percentile_cont as window functions at all. They are ordered-set aggregates only, using `WITHIN GROUP (ORDER BY ...)` syntax. DuckDB supports both styles. QuestDB's approach of supporting them as window functions is closer to DuckDB's model.

**Complexity:** Medium -- the window function variants exist but lack alias registration and partition edge-case testing.

### 4. Quantile Aliases

| Feature | PostgreSQL | DuckDB | QuestDB (current) | Status |
|---------|-----------|--------|-------------------|--------|
| quantile_disc = percentile_disc alias | No (PG has no quantile_*) | Yes | Group-by: factory exists. Window: NOT REGISTERED | PARTIAL |
| quantile_cont = percentile_cont alias | No | Yes | Group-by: factory exists. Window: NOT REGISTERED | PARTIAL |
| Consistent behavior between aliases | N/A | Yes | Verify | NEEDS VALIDATION |

**Complexity:** Low -- factory classes exist but are not registered in function_list.txt.

### 5. Multi-Percentile Array Variants

| Feature | PostgreSQL | DuckDB | QuestDB (current) | Status |
|---------|-----------|--------|-------------------|--------|
| percentile_disc(DOUBLE[]) returns DOUBLE[] | Yes (anyarray) | Yes (LIST) | Yes (DirectArray) | OK |
| percentile_cont(DOUBLE[]) returns DOUBLE[] | Yes | Yes | Yes | OK |
| Empty input returns NULL array | Yes | Yes | Returns null via ofNull() | OK |
| Individual NULL percentile in array | PG: NULL in output position | DuckDB: error | Verify | NEEDS TEST |
| Multi-percentile window variants | N/A in PG | Yes | Yes (Multi* factories) | OK |

**Complexity:** Medium -- the DirectArray reuse bug (ofNull clears type/shape) must be fixed.

### 6. Error Handling

| Behavior | PostgreSQL | DuckDB | QuestDB (current) | Status |
|----------|-----------|--------|-------------------|--------|
| percentile < -1.0 raises error | Yes | Yes | Yes | OK |
| percentile > 1.0 raises error | Yes | Yes | Yes | OK |
| Non-numeric percentile raises error | Yes | Yes | Yes (type mismatch) | OK |
| Wrong arg count raises error | Yes | Yes | Yes | OK |
| NULL percentile handling | PG: returns NULL | DuckDB: error | Throws CairoException via Numbers.isNull | DIVERGENCE |
| NaN percentile handling | PG: undefined | DuckDB: error | Numbers.isNull catches NaN -- error | OK |
| Window + ORDER BY raises error | N/A | DuckDB allows it | Explicit error message | ACCEPTABLE |
| EXPLAIN shows percentile argument | Yes | Yes | toPlan() missing percentile arg | BUG |
| Non-constant percentile in window | Should error | Should error | NOT VALIDATED | NEEDS CHECK |
| Multi-approx_percentile invalid validation | Error expected | Error expected | Silent clamping | BUG |

**NULL percentile divergence detail:**
- PostgreSQL: `percentile_disc(NULL) WITHIN GROUP (ORDER BY x)` returns NULL.
- DuckDB: `quantile_disc(x, NULL)` raises an error.
- QuestDB current: `Numbers.isNull(percentile)` in `getPercentileMultiplier` throws CairoException.
- Recommendation: QuestDB's behavior matches DuckDB. This is acceptable. Document it.

**Complexity:** Low per item. The toPlan() bug and multi-approx validation are straightforward fixes.

### 7. Resource Management

| Behavior | Expected | QuestDB (current) | Status |
|----------|----------|-------------------|--------|
| No memory leaks on close/reset/reopen | Required | DirectArray leak in Multi* window reset/reopen/toTop | BUG |
| No memory leaks on error paths | Required | Verify | NEEDS TEST |
| Zero-GC on data paths | QuestDB requirement | double[] allocation in Multi* preparePass2 | BUG |
| Correct GroupByAllocator lifecycle | Required | Verify | NEEDS VALIDATION |
| setEmpty() on disc group-by functions | Required for SAMPLE BY fill | Missing override | BUG |

**Complexity:** High -- resource leaks are QuestDB's #1 pain point per CLAUDE.md.

## Edge Cases: Specific and Testable

### NULL Handling Edge Cases

| # | Edge Case | PostgreSQL Behavior | DuckDB Behavior | QuestDB Expected | Test Exists? |
|---|-----------|-------------------|-----------------|-----------------|--------------|
| N1 | All values NULL | Returns NULL | Returns NULL | Returns NULL (NaN) | Partial (sqllogictest) |
| N2 | Mix of NULL and non-NULL | NULLs excluded, percentile of non-NULLs | Same | Same (Numbers.isFinite filter) | Partial |
| N3 | NULL percentile argument | Returns NULL | Error | Error (CairoException) | Commented out in sqllogictest |
| N4 | NaN in double column | NaN sorts to end, included in computation | Excluded | Excluded (isFinite) | Yes (sqllogictest) |
| N5 | +Infinity in double column | Included, sorts to end | Included | EXCLUDED (isFinite returns false) | NO |
| N6 | -Infinity in double column | Included, sorts to start | Included | EXCLUDED (isFinite returns false) | NO |
| N7 | Negative zero (-0.0) | Equals +0.0 | Equals +0.0 | Should equal +0.0 (Java semantics) | NO |
| N8 | NULL in GROUP BY key | Separate NULL group | Separate NULL group | Separate NULL group | Partial (sqllogictest) |

**IMPORTANT: N4/N5/N6 (NaN and Infinity handling)** -- QuestDB excludes NaN AND Infinity values because `Numbers.isFinite()` returns false for all of them (checks exponent bits == all-ones). PostgreSQL includes Infinity in computation but NaN behavior varies. DuckDB excludes NaN but includes Infinity. QuestDB excludes both.

This is consistent with QuestDB's convention where `Numbers.isNull(double)` returns true for NaN and Infinity -- they are treated as NULL throughout the database. However, users porting queries from PostgreSQL that contain Infinity values in percentile columns will get different results. This should be documented but is acceptable as a deliberate design choice.

### Empty/Small Partition Edge Cases

| # | Edge Case | PostgreSQL Behavior | DuckDB Behavior | QuestDB Expected | Test Exists? |
|---|-----------|-------------------|-----------------|-----------------|--------------|
| E1 | Empty table | NULL | NULL | NULL | Yes (sqllogictest) |
| E2 | WHERE 1=0 (empty result) | NULL | NULL | NULL | Yes (sqllogictest) |
| E3 | Single row, any fraction | That value | That value | That value | NO (dedicated test) |
| E4 | Single non-NULL among NULLs | That value | That value | That value | NO |
| E5 | Two rows, fraction=0.5 disc | Lower of the two | Lower of the two | Verify: ceil(2*0.5)-1=0 -- min | NO |
| E6 | Two rows, fraction=0.5 cont | Average of the two | Average | Verify: 0.5*(2-1)=0.5, interp | NO |
| E7 | Empty window partition (PARTITION BY with no matching rows) | NULL for that partition | NULL | Verify pass2 with no values | NO |
| E8 | Single-row window partition | That value | That value | Verify | NO |
| E9 | All-NULL window partition | NULL | NULL | Verify (no values in pass1 list) | NO |

### All-Equal Values Edge Cases

| # | Edge Case | PostgreSQL Behavior | DuckDB Behavior | QuestDB Expected | Test Exists? |
|---|-----------|-------------------|-----------------|-----------------|--------------|
| A1 | All values identical, disc | Returns that value | Returns that value | Returns that value | NO |
| A2 | All values identical, cont | Returns that value | Returns that value | Returns that value (interpolation of equals) | NO |
| A3 | Large all-equal set (1000+ rows) | Correct, no perf issue | Correct | PERFORMANCE BUG in quickSelect partition() | KNOWN BUG |

**CRITICAL: A3 details** -- The `partition()` method in `GroupByDoubleList` used by quickSelect uses Lomuto-style partitioning with `<=` comparison. For all-equal elements, every element satisfies `getQuick(j) <= pivot`, so `i` advances to `hi-1` and pivotIndex lands at `hi`. Each partition step only eliminates 1 element from the range, producing O(N) partition steps each doing O(N) work = O(N^2) total. The `sort()` method uses dual-pivot quicksort with Dutch National Flag 3-way partitioning that handles equal elements in O(N) -- but quickSelect does not use this sort method.

The window function factories have their own quickSort implementations (8 copies duplicated across 4 factories) that also use Lomuto-style partitioning with `<` strict comparison. These are even worse on all-equal data: with `<` comparison, no elements move past the pivot, so pivotIndex is always `lo`, and the recursion makes no progress. This causes infinite recursion (stack overflow) on all-equal data. The tail-call optimization (`while (left < right)`) prevents stack overflow but results in O(N^2) time.

### Boundary Percentile Edge Cases

| # | Edge Case | Expected Behavior | QuestDB Verified? |
|---|-----------|-------------------|-------------------|
| B1 | fraction = 0.0 exactly | disc: minimum, cont: minimum | Yes (sqllogictest) |
| B2 | fraction = 1.0 exactly | disc: maximum, cont: maximum | Yes (sqllogictest) |
| B3 | fraction = 0.5 (median) | disc: ceil(N/2)th, cont: linear interp | Yes (sqllogictest) |
| B4 | fraction very close to 0 (1e-15) | Minimum or near-minimum | NO |
| B5 | fraction very close to 1 (1-1e-15) | Maximum or near-maximum | NO |
| B6 | Negative fraction (-0.5) | QuestDB-specific: maps to 1 - 0.5 = 0.5 (descending shorthand) | Yes (sqllogictest) |
| B7 | Negative fraction (-1.0) | QuestDB-specific: maps to 0.0 (minimum) | NO |
| B8 | Negative fraction (-0.0) | Should equal +0.0 per IEEE 754 | NO |

**QuestDB-specific: negative percentile** -- QuestDB supports negative percentiles as a descending-order shortcut: `percentile_disc(x, -0.3)` is equivalent to `percentile_disc(x, 0.7)`. This is a DuckDB-inherited feature. PostgreSQL does not support negative fractions (errors).

### Numeric Precision Edge Cases

| # | Edge Case | Risk | Test Exists? |
|---|-----------|------|--------------|
| P1 | Very large doubles (1e308) | percentile_cont interpolation: `lower + (upper - lower) * frac` could overflow if upper - lower is large | NO |
| P2 | Very small doubles (1e-308) | Underflow in interpolation -- unlikely but untested | NO |
| P3 | Mixed sign large doubles (-1e308 and +1e308) | Subtraction in interpolation could overflow to Infinity | NO |
| P4 | Integer type extremes in percentile_cont | long values near Long.MAX_VALUE cast to double lose precision | Partial (sqllogictest tests 9223372036854775794) |
| P5 | Size near Integer.MAX_VALUE | GroupByDoubleList uses int for size -- capped at ~2 billion doubles (~16 GB per group) | DESIGN LIMIT |

### Window Function Specific Edge Cases

| # | Edge Case | Expected | QuestDB Status |
|---|-----------|----------|----------------|
| W1 | PARTITION BY with some empty partitions | NULL for empty partitions | NEEDS TEST |
| W2 | Multiple cursor reuse (reopen) | Correct results, no leaks | DirectArray leak in reopen | BUG |
| W3 | Window fn in subquery | Correct propagation | NEEDS TEST |
| W4 | Multiple percentile window fns in same query | Each computes independently | NEEDS TEST |
| W5 | Partitioned window with all-NULL partition | NULL for that partition | NEEDS TEST |
| W6 | O(N^2) memory in partitioned pass1 | Efficient append | KNOWN BUG: copy-on-append pattern |
| W7 | Large number of partitions (10K+) | Bounded memory per partition | Verify map cleanup | NEEDS TEST |

## Differentiators

Features that set QuestDB's implementation apart from competitors.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Negative percentile (descending) | Convenient shorthand for `1 - p` | Low | Already implemented via getPercentileMultiplier. DuckDB has this; PostgreSQL does not. |
| SAMPLE BY integration | Time-series native: percentile per time bucket without subqueries | Medium | getSampleByFlags=FILL_ALL present but needs validation with setEmpty(). Unique to QuestDB. |
| O(n) quickSelect instead of O(n log n) sort | Faster single-percentile computation | Medium | Already implemented in GroupByDoubleList. Important for large partitions in time-series workloads. |
| Multi-percentile quickSelectMultiple | Efficient batch computation of multiple percentiles | Medium | Already implemented. Avoids redundant full sorting. |
| approx_percentile (HDR Histogram) | Bounded memory for streaming/large datasets | Low (exists) | Already production-ready. Unique performance tier alongside exact computation. |
| Zero-GC percentile computation | No GC pauses during percentile queries | High | Core QuestDB value prop. Requires fixing double[] allocation in Multi* preparePass2. |
| Window function percentile | Percentile computed per-partition without subquery | Medium | DuckDB supports this too. PostgreSQL does NOT support percentile as window function. |
| Parallel group-by support | supportsParallelism() for percentile_disc | Medium | merge() method exists for combining partial results. Enables multi-threaded aggregation. |

## Anti-Features

Features to explicitly NOT build in this hardening pass.

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| WITHIN GROUP (ORDER BY) syntax | Different SQL dialect; QuestDB uses `percentile_disc(expr, fraction)` call syntax | Document QuestDB's syntax. The function-call syntax is simpler and avoids ordered-set aggregate complexity. |
| percentile_cont on non-numeric types (interval, timestamp) | Interpolation on timestamps is ambiguous; QuestDB stores timestamps as LONG internally | Users can cast to LONG, compute percentile, cast back. Already demonstrated in sqllogictest. |
| ORDER BY within window frame for percentile | Running percentile adds massive complexity (incremental sort maintenance) | Explicit error message already implemented. Could be a future feature. |
| New algorithms (t-digest, KLL sketch) | Out of scope for hardening pass; approx_percentile already exists via HDR Histogram | Track as separate feature request if users need different accuracy/memory tradeoffs. |
| RESPECT NULLS / IGNORE NULLS syntax | PostgreSQL does not implement it either; QuestDB always ignores NULLs in aggregates | Document that NULLs (and NaN/Infinity) are always excluded. Consistent behavior. |
| percentile_disc on STRING/SYMBOL types | PostgreSQL supports anyelement, but string percentile is rarely useful in time-series | Only support numeric types (DOUBLE, LONG, INT, SHORT, BYTE, FLOAT). |
| Weighted percentile | Not in SQL standard, not requested by users | Future feature if demand arises. |
| Introselect (guaranteed O(n) worst-case) | Three-way partition in quickSelect addresses the practical concern; introselect adds complexity | Document as potential future optimization if adversarial inputs become a concern. |
| JMH benchmarks | Separate effort per PROJECT.md out-of-scope | Track as follow-up work. |

## Feature Dependencies

```
Registration in function_list.txt
  --> Group-by percentile_disc/percentile_cont available in SQL
  --> quantile_disc/quantile_cont group-by aliases available
  --> Window quantile_disc/quantile_cont aliases available

Fix DirectArray reuse bug (ofNull clears type/shape)
  --> Multi-percentile array variants return correct results
  --> Multi-percentile window variants return correct results

Fix quickSelect O(N^2) on all-equal (three-way partition or use sort())
  --> percentile correct AND fast for all-equal partitions
  --> Large partition performance acceptable

Fix O(N^2) memory in partitioned window pass1 (copy-on-append pattern)
  --> Partitioned window function scalable to large datasets

Fix DirectArray leak in Multi* window reset/reopen/toTop
  --> No memory leaks on cursor reuse
  --> assertMemoryLeak tests pass

Fix zero-GC violation (double[] in Multi* preparePass2)
  --> No GC pressure on hot path
  --> Consistent with QuestDB zero-GC guarantee

Add setEmpty() override in disc group-by functions
  --> SAMPLE BY fill works correctly for percentile
  --> Empty groups return NULL not undefined

Extract shared quickSort utility (8 copies --> 1)
  --> Single source of truth for sorting
  --> Bug fixes in sort (e.g., three-way partition) apply everywhere
  --> Reduces code surface for future maintenance

Fix toPlan() to show percentile argument
  --> EXPLAIN output useful for debugging percentile queries
```

## MVP Recommendation

Priority order for the hardening pass:

1. **Register missing group-by factories** (10 factories in function_list.txt) -- unblocks all group-by percentile/quantile queries. Without this, the functions literally cannot be called via SQL.
2. **Fix DirectArray reuse bug** -- correctness bug producing wrong results in multi-percentile array variants.
3. **Fix quickSelect O(N^2) on all-equal** -- performance correctness. O(N^2) on common time-series patterns (plateaus, constant metrics) can look like a query hang to users.
4. **Fix DirectArray leak in Multi* window** -- memory leak on cursor reuse.
5. **Fix zero-GC violation** -- consistency with QuestDB's core guarantee.
6. **Add setEmpty() override** -- correctness for SAMPLE BY fill.
7. **Fix O(N^2) memory in partitioned window pass1** -- scalability for partitioned windows.
8. **Add window quantile_disc/quantile_cont aliases** -- feature completeness.
9. **Extract shared quickSort utility** -- maintenance improvement, reduces 8 copies to 1.
10. **Fix toPlan() to show percentile argument** -- observability.
11. **Validate percentile argument is constant in window factories** -- error path correctness.
12. **Add comprehensive edge-case tests** -- validation of all the above.

Defer:
- **Negative percentile documentation** -- low priority, existing behavior is acceptable
- **New percentile algorithms** -- separate feature, not hardening
- **WITHIN GROUP syntax** -- different SQL dialect, not needed
- **Introselect depth limit** -- three-way partition addresses the practical concern

## Sources

- PostgreSQL 16 aggregate functions documentation: https://www.postgresql.org/docs/16/functions-aggregate.html (HIGH confidence -- verified via WebFetch)
- PostgreSQL 16 ordered-set aggregate internals: https://www.postgresql.org/docs/16/xaggr.html (HIGH confidence -- verified via WebFetch)
- PostgreSQL 16 window functions: https://www.postgresql.org/docs/16/functions-window.html (HIGH confidence -- verified, confirms percentile NOT available as window function)
- DuckDB behavior: inferred from sqllogictest files adapted from DuckDB test suite on nw_percentile branch (MEDIUM confidence -- tests clearly adapted from DuckDB patterns)
- DuckDB quantile semantics: training data knowledge (MEDIUM confidence -- could not access DuckDB docs directly due to WebFetch restrictions)
- QuestDB implementation: direct code review of nw_percentile branch (HIGH confidence -- read all relevant source files)
- QuestDB Numbers.isFinite/isNull: direct code review of Numbers.java on master (HIGH confidence)

### Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| PostgreSQL semantics | HIGH | Verified via official docs; interpolation formula not explicitly stated in docs but matches standard linear interpolation |
| DuckDB semantics | MEDIUM | Inferred from adapted sqllogictest files; could not access DuckDB docs directly |
| QuestDB current behavior | HIGH | Direct code review of all implementation files on nw_percentile branch |
| Edge case completeness | MEDIUM | 30+ specific testable edge cases identified; may miss corner cases in integer overflow or concurrent access paths |
| Performance pathology analysis | HIGH | Confirmed by reading partition() method -- Lomuto scheme with <= degenerates on equal elements |
| NULL/NaN/Infinity semantics | HIGH | Verified Numbers.isFinite implementation against EXP_BIT_MASK check |
