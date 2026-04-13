# Requirements: Harden Percentile/Quantile Functions

**Defined:** 2026-04-13
**Core Value:** Every percentile/quantile query must return correct results on all inputs without crashing, leaking memory, or producing silent wrong answers.

## v1 Requirements

### Correctness

- [ ] **COR-01**: DirectArray reuse in multi-array group-by functions must re-apply type/shape on every non-null path, not only when `out == null`
- [ ] **COR-02**: Window function quickSort must handle all-equal elements without O(N^2) degradation (three-way partition or quickselect)
- [ ] **COR-03**: Multi-approx_percentile must validate each percentile array element and throw SqlException on invalid values
- [ ] **COR-04**: Window function factories must validate that the percentile argument is a constant expression
- [ ] **COR-05**: Disc group-by functions must override setEmpty() to store 0L, consistent with ApproxPercentile pattern

### Resource Safety

- [ ] **RES-01**: Multi* window functions must free DirectArray `result` in reset(), reopen(), and toTop() via Misc.free()
- [ ] **RES-02**: Multi* window preparePass2 must pre-allocate and reuse double[] rather than heap-allocating per call

### Performance

- [ ] **PERF-01**: Partitioned window pass1 must use append-in-place with capacity tracking, not copy-on-append O(N^2)
- [ ] **PERF-02**: PercentileDiscLongGroupByFunction must use quickSelect O(n) via new GroupByLongList.quickSelect()

### Code Quality

- [ ] **QUAL-01**: Extract duplicated quickSort/partition/swap from 8 locations across 4 window factories into a shared utility
- [ ] **QUAL-02**: toPlan() in all group-by functions must include the percentile argument in EXPLAIN output

### Registration & Aliases

- [ ] **REG-01**: Register all 10 missing group-by factories in function_list.txt
- [ ] **REG-02**: Add quantile_disc and quantile_cont window function alias factories

### Tests

- [ ] **TEST-01**: Add tests verifying quantile_cont and quantile_disc SQL syntax works as aggregates
- [ ] **TEST-02**: Add window function tests for percentile_disc/percentile_cont with PARTITION BY
- [ ] **TEST-03**: Add error-path tests verifying ORDER BY in window percentile throws appropriate error
- [ ] **TEST-04**: Migrate existing percentile tests from assertSql() to assertQueryNoLeakCheck()

## v2 Requirements

### Future Enhancements

- **FUT-01**: Add introselect (quickselect + heapselect fallback) for guaranteed O(n) worst-case
- **FUT-02**: Convert standalone benchmark to JMH micro-benchmark
- **FUT-03**: Document negative percentile semantics (mapping -p to 1-|p|)

## Out of Scope

| Feature | Reason |
|---------|--------|
| New percentile algorithms (t-digest) | Not part of this PR, separate effort |
| WITHIN GROUP (ORDER BY) syntax | PostgreSQL ordered-set aggregate syntax, QuestDB uses different approach |
| Infinity handling change | QuestDB convention: Infinity treated as NULL, consistent with other functions |
| Parallel merge stress tests | Would require concurrent test infrastructure, defer to CI |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| COR-01 | Phase 1 | Pending |
| COR-02 | Phase 1 | Pending |
| COR-03 | Phase 1 | Pending |
| COR-04 | Phase 1 | Pending |
| COR-05 | Phase 1 | Pending |
| RES-01 | Phase 2 | Pending |
| RES-02 | Phase 2 | Pending |
| PERF-01 | Phase 3 | Pending |
| PERF-02 | Phase 3 | Pending |
| QUAL-01 | Phase 4 | Pending |
| QUAL-02 | Phase 4 | Pending |
| REG-01 | Phase 4 | Pending |
| REG-02 | Phase 4 | Pending |
| TEST-01 | Phase 4 | Pending |
| TEST-02 | Phase 4 | Pending |
| TEST-03 | Phase 4 | Pending |
| TEST-04 | Phase 4 | Pending |

**Coverage:**
- v1 requirements: 17 total
- Mapped to phases: 17
- Unmapped: 0

---
*Requirements defined: 2026-04-13*
*Last updated: 2026-04-13 after roadmap creation*
