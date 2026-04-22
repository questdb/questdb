---
created: 2026-04-22T17:10:07.665Z
completed_at: 2026-04-22
completed_in: 17-05-PLAN.md
resolved_by_commits:
  - 6bdb014f2d
title: Upgrade SqlOptimiserTest#testSampleByFromToKeyedQuery from printSql smoke test to plan assertion (or delete)
area: testing
files:
  - core/src/test/java/io/questdb/test/griffin/SqlOptimiserTest.java:4256
  - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java:7015
---

## Problem

Surfaced during /review-pr 6946 test-only review (2026-04-22), finding 4.1 — not
covered by phase 17 (checked 17-CONTEXT.md, 17-DISCUSSION-LOG.md, 17-RESEARCH.md,
17-VALIDATION.md, all four 17-0N-PLAN.md files; zero hits for the test name or
for `printSql` handling in SqlOptimiserTest).

On master the test was `testSampleByFromToDisallowedQueryWithKey` and asserted
an exception via `assertException(..., 0, "are not supported for keyed SAMPLE BY")`.
PR #6946 renamed it to `testSampleByFromToKeyedQuery` and replaced the body with
a bare `printSql(...)` call inside `assertMemoryLeak`. `printSql` compiles and
runs the SQL, writes rows into the shared `sink`, and returns void. Nothing
inspects `sink`.

The test now only guarantees "doesn't throw, doesn't leak". It would pass under:

1. Silent regression to the legacy cursor path (no plan assertion).
2. `LIMIT 6` being dropped by a plan rewrite (no row-count check).
3. Cartesian collapse to 0 rows (no row-shape check).
4. Plan losing `Async Group By` parallelism (no plan string).
5. Arbitrary silent correctness bugs that don't throw.

Signals:

- It is the **only** `printSql(...)`-only test in SqlOptimiserTest.java (1 of 1
  vs. 167 `assertPlanNoLeakCheck` + 75 `assertSql` + 12 `assertModel`).
- It sits inside the `testSampleByFromTo*` cluster where every neighbour asserts
  either a plan or a result.
- Precedent from 17-RESEARCH.md:445 calls this exact pattern "silent buggy
  behavior": *"Phase 13 Plan 06 restored `testSampleFillValueNotEnough` to
  `assertException` form ... restored from `printSql` (silent buggy behavior)
  to `assertException`."*
- `SampleByTest#testSampleByFromToIsAllowedForKeyedQueries`
  ([SampleByTest.java:7015](core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java:7015))
  is the real positive-case test (asserts `9 buckets x 479 keys = 4311 rows`
  via `count(*) rows, count_distinct(x) keys` wrapper). So the SqlOptimiserTest
  version is strictly a subset of what already exists elsewhere.

## Solution

Two options, either is acceptable:

**Option A (preferred):** Convert to `assertPlanNoLeakCheck` pinning the new
fast-path plan. Consistent with neighbouring `testSampleByFromTo*` tests. Form:

```java
@Test
public void testSampleByFromToKeyedQuery() throws Exception {
    assertMemoryLeak(() -> {
        execute(SampleByTest.FROM_TO_DDL);
        final String query = """
                SELECT ts, count, s
                FROM fromto
                SAMPLE BY 5d FROM '2018-01-01' TO '2019-01-01'
                LIMIT 6""";
        assertPlanNoLeakCheck(query, """
                Limit lo: 6 skip-over-rows: 0 limit: 6
                    Sample By Fill
                      range: ('2018-01-01','2019-01-01')
                      stride: '5d'
                      fill: none
                        Sort
                          keys: [ts]
                            Async Group By workers: 1
                              keys: [ts,count,s]
                              ...
                """);
    });
}
```

Run the query with a probe `printSql` first to capture the exact plan string
per Phase 15 D-02 probe-and-freeze pattern.

**Option B:** Delete the method. SampleByTest already covers correctness; this
SqlOptimiserTest variant adds no signal over `assertMemoryLeak` alone.

Either way, the current middle ground — keep the method, strip its teeth — is
worse than both options because the method name suggests coverage it does not
provide.

Recommended slot: fold into phase 17 Plan 03 as an m8d-style test-only tweak.
The plan already modifies `SampleByFillTest` / `SampleByTest` / `SampleByNano...`
and is scoped to lax-assertion cleanup (m8c tightens `testFillPrevRejectNoArg`
in the same spirit). If phase 17 has already closed, open as a standalone
test-only phase.
