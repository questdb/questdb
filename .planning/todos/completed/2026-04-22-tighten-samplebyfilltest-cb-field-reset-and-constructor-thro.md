---
created: 2026-04-22T17:10:07.665Z
completed_at: 2026-04-22
completed_in: 17-05-PLAN.md
resolved_by_commits:
  - d6d4b1628b
title: Tighten SampleByFillTest CB field reset and constructor-throw exception assertion
area: testing
files:
  - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java:350
  - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java:3122
  - core/src/main/java/io/questdb/cairo/vm/MemoryPARWImpl.java:1266
  - core/src/main/java/io/questdb/cairo/vm/MemoryCARWImpl.java:193
---

## Problem

Two related test-quality findings surfaced during /review-pr 6946 test-only
review (2026-04-22), bundled because both are inside SampleByFillTest and fit
the same "lax assertion" theme as phase 17 D-23 (m8c). Neither is covered by
phase 17 (verified: zero hits in 17-CONTEXT / 17-DISCUSSION-LOG / 17-RESEARCH /
17-VALIDATION / 17-0N-PLAN).

### 4.6 — `circuitBreakerConfiguration` field not reset

`testFillKeyedRespectsCircuitBreaker` at `SampleByFillTest.java:350` assigns a
custom `circuitBreakerConfiguration` to the `protected static` field on
`AbstractCairoTest` and never restores it.

- `staticOverrides.reset()` runs only in `@AfterClass`; the `@Before setUp()`
  in `AbstractCairoTest` does not touch this field.
- No other test in SampleByFillTest reads the field today, so no observable
  bleed in the current state.
- Any future test in the same class that constructs a
  `NetworkSqlExecutionCircuitBreaker(engine, circuitBreakerConfiguration, ...)`
  would inherit the tick-counting mock clock and misbehave silently. This is a
  latent foot-gun.

Caveat: the exact same pattern exists in other test classes
(`ParallelFilterTest:828`, `OrderByTimeoutTest`, `CheckpointTest`,
`ParallelGroupByFuzzTest`). The project-wide norm is to not reset. Fixing this
one test diverges from precedent; that divergence is the only judgment call
here.

### 4.7 — `testSortedRecordCursorFactoryConstructorThrow` lax catch

`testSortedRecordCursorFactoryConstructorThrow` at `SampleByFillTest.java:3122`
catches `Throwable` (not `Exception` / `CairoException`) and asserts the
message contains one of six substrings:

```java
msg.contains("max pages") || msg.contains("maxPages")
    || msg.contains("Maximum number of pages") || msg.contains("limit")
    || msg.contains("overflow") || msg.contains("breached")
```

Comment justifies laxity with *"the LimitOverflowException may wrap inside a
CairoException or SqlException depending on the surface that catches it"*.
In practice:

- All allocation-exhaustion sites that fire for `sqlSortKeyMaxPages = -1`
  raise `LimitOverflowException extends CairoException` with stable text.
- `MemoryPARWImpl.java:1266` / `MemoryCARWImpl.java:193` both produce
  `"Maximum number of pages (-1) breached in VirtualMemory"` — these back
  `RecordTreeChain`'s allocation path.
- All six `LimitOverflowException` sites share either `"breached"` or
  `"memory exceeded"` as stable substrings.

So the broad matcher accepts essentially any exception that mentions a limit,
when a single canonical substring suffices.

## Solution

Both fixes fit in the current test structure, no harness changes:

### 4.6 fix (4-line try/finally)

```java
assertMemoryLeak(() -> {
    try {
        circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            // ...existing body unchanged...
        };
        final WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
            // ...existing body unchanged...
        }, configuration, LOG);
    } finally {
        circuitBreakerConfiguration = null;
    }
});
```

Or skip if we decide matching project precedent is preferable to one-off
divergence. Note the decision either way.

### 4.7 fix (3-line swap)

```java
try {
    assertSql("", "SELECT ts, k, sum(x) FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR");
    fail("expected LimitOverflowException from pathological sqlSortKeyMaxPages");
} catch (CairoException ex) {
    TestUtils.assertContains(ex.getFlyweightMessage(), "Maximum number of pages");
}
```

Gains:

1. Typed catch (`CairoException`, the LimitOverflowException superclass) —
   JVM-level `Error`s propagate instead of being swallowed.
2. Single canonical substring, not 6-way disjunction.
3. If the exception is re-wrapped to `SqlException` somewhere on the construct
   path (the lax comment implies uncertainty), the typed catch fails loud
   instead of hiding the signal.

### Routing

Recommended slot: phase 17 Plan 03 under an m8d / m8e pair alongside D-23
(m8c). Same file, same theme (tighten lax rejection tests), zero new
structural change. If phase 17 has closed, fold into a standalone test-only
plan together with the SqlOptimiserTest printSql finding tracked in sibling
todo `2026-04-22-upgrade-sqloptimisertest-testsamplebyfromtokeyedquery-from-p.md`.
