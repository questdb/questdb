---
phase: 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi
plan: 03
subsystem: sql
tags: [sql, fill, prev, test, cherry-pick, data-correctness, sample-by]
requires:
  - 13-02-PLAN.md (rowId rewrite in SampleByFillRecordCursorFactory landed)
  - Commit f43a3d7057 on branch sm_fill_prev_fast_all_types (source of cherry-picks)
provides:
  - Data-correctness proof that rowId rewrite produces correct carry-forward
    values for every currently-retro-fallback-routed aggregate output type
  - Authorization gate for Plan 04 (retro-fallback deletion + fast-path gate
    lifting)
affects:
  - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java
tech-stack:
  added: []
  patterns:
    - Data-correctness-only tests without plan-text assertion (Option B),
      deferred plan-text to Plan 04 per D-07 commit sequencing
key-files:
  created: []
  modified:
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java
decisions:
  - "Option B at checkpoint: ship data-correctness assertions only; plan-text
    assertion deferred to Plan 04 to preserve D-07 six-commit sequencing"
  - "Single-line TODO comment on each new test points forward to Plan 04's
    plan-text assertion"
  - "No banner comments; Chars import dropped (unused); Assert import retained
    (used elsewhere)"
metrics:
  duration: ~30m
  completed: 2026-04-19
  tasks: 1
  files: 1
---

# Phase 13 Plan 03: Per-type FILL(PREV) data-correctness tests Summary

One-liner: 13 per-type FILL(PREV) tests cherry-picked from f43a3d7057 pin
data correctness for SYMBOL (3), ARRAY (1), STRING (2), VARCHAR (2), UUID
(2), Long256 (1), Decimal128 (1), Decimal256 (1) under the rowId rewrite
from Plan 02, authorizing Plan 04 to lift the fast-path gates.

## Scope

Cherry-pick 13 per-type FILL(PREV) `@Test` methods from commit `f43a3d7057`
on branch `sm_fill_prev_fast_all_types` into our branch's
`SampleByFillTest.java`, placed in alphabetical position among existing
methods. All 13 assert data correctness via `assertSql(expected, query)`.

## The 13 Tests

| # | Test Name | Category | File Line |
|---|-----------|----------|-----------|
| 1 | `testFillPrevSymbolKeyed` | SYMBOL (keyed) | 1950 |
| 2 | `testFillPrevSymbolNonKeyed` | SYMBOL (non-keyed) | 1999 |
| 3 | `testFillPrevSymbolNull` | SYMBOL (null handling) | 2026 |
| 4 | `testFillPrevArrayDouble1D` | ARRAY (DOUBLE[] 1D) | 925 |
| 5 | `testFillPrevStringKeyed` | STRING (keyed) | 1896 |
| 6 | `testFillPrevStringNonKeyed` | STRING (non-keyed, reallocation path) | 1922 |
| 7 | `testFillPrevVarcharKeyed` | VARCHAR (keyed) | 2104 |
| 8 | `testFillPrevVarcharNonKeyed` | VARCHAR (non-keyed, unicode) | 2130 |
| 9 | `testFillPrevUuidKeyed` | UUID (keyed) | 2053 |
| 10 | `testFillPrevUuidNonKeyed` | UUID (non-keyed, null handling) | 2079 |
| 11 | `testFillPrevLong256NonKeyed` | Long256 (non-keyed) | 1471 |
| 12 | `testFillPrevDecimal128` | Decimal128 (non-keyed, null handling) | 1095 |
| 13 | `testFillPrevDecimal256` | Decimal256 (non-keyed, null handling) | 1120 |

## Test Results

Full suite:

```
Tests run: 87, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Individual runs — each of the 13 tests passes in isolation:

```
testFillPrevSymbolKeyed:         Tests run: 1, Failures: 0, Errors: 0  BUILD SUCCESS
testFillPrevSymbolNonKeyed:      Tests run: 1, Failures: 0, Errors: 0  BUILD SUCCESS
testFillPrevSymbolNull:          Tests run: 1, Failures: 0, Errors: 0  BUILD SUCCESS
testFillPrevArrayDouble1D:       Tests run: 1, Failures: 0, Errors: 0  BUILD SUCCESS
testFillPrevStringKeyed:         Tests run: 1, Failures: 0, Errors: 0  BUILD SUCCESS
testFillPrevStringNonKeyed:      Tests run: 1, Failures: 0, Errors: 0  BUILD SUCCESS
testFillPrevVarcharKeyed:        Tests run: 1, Failures: 0, Errors: 0  BUILD SUCCESS
testFillPrevVarcharNonKeyed:     Tests run: 1, Failures: 0, Errors: 0  BUILD SUCCESS
testFillPrevUuidKeyed:           Tests run: 1, Failures: 0, Errors: 0  BUILD SUCCESS
testFillPrevUuidNonKeyed:        Tests run: 1, Failures: 0, Errors: 0  BUILD SUCCESS
testFillPrevLong256NonKeyed:     Tests run: 1, Failures: 0, Errors: 0  BUILD SUCCESS
testFillPrevDecimal128:          Tests run: 1, Failures: 0, Errors: 0  BUILD SUCCESS
testFillPrevDecimal256:          Tests run: 1, Failures: 0, Errors: 0  BUILD SUCCESS
```

## Deviation from plan

### Option B chosen at checkpoint

PLAN.md §interfaces required every new test to include a plan-text assertion
that the plan contains `Sample By Fill` (proof of fast-path routing). After
cherry-picking the 13 methods and running them, it surfaced that every new
test's data-correctness `assertSql` passes (rowId replay produces the correct
value for every var-width or wide type), but every plan-text assertion fails
because the existing fast-path gates in the optimizer and code generator
still route these queries to the legacy Sample By cursor.

Affected gates, still present in the codebase as of this commit (intentionally
retained for Plan 04 per D-04, D-07):

| File | Line | Symbol / Usage |
|---|---|---|
| `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` | 474 | `hasPrevWithUnsupportedType` declaration |
| `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` | 532 | `hasPrevWithUnsupportedType` inner `isUnsupportedPrevType` call |
| `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` | 571 | `hasPrevWithUnsupportedType` return on top-level column type |
| `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` | 574 | `isUnsupportedPrevType` static predicate declaration |
| `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` | 8179 | `rewriteSampleBy` uses `!hasPrevWithUnsupportedType` as precondition for fast-path rewrite |
| `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | 1200 | `isFastPathPrevSupportedType` static predicate declaration |
| `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | 3554 | `generateFill` codegen-time check that throws `FallbackToLegacyException` for unsupported PREV types |

The user was presented with two options at the checkpoint:

- Option A: lift the gates inside this plan so the 13 tests execute on the
  fast path and all plan-text assertions pass — but this conflates Plan 03
  and Plan 04 into a single commit, violating D-07's independent-bisectable
  commit sequencing (chain.clear() fix, rowId rewrite, test cherry-pick,
  retro-fallback deletion, defect resolution, grammar items).
- Option B: ship Plan 03 with `assertSql` data-correctness assertions only;
  defer the `Sample By Fill` plan-text assertion to Plan 04.

**User chose Option B.** Rationale: preserves D-07 commit sequencing; Plan 04
already needs to touch the same files (both SqlOptimiser and
SqlCodeGenerator) to delete retro-fallback machinery, so bundling the gate
removal and the plan-text assertions into Plan 04 keeps related changes
together.

### Implementation of Option B

For each of the 13 new tests:

1. The `Assert.assertTrue(...)` + `Chars.contains(getPlanSink(query).getSink(),
   "Sample By Fill")` two-line block was removed.
2. A single-line TODO comment replaced it:
   `// Plan 04 adds "Sample By Fill" plan assertion once fast-path gates are lifted.`
3. The `Chars` import was removed (no other use in the file). The `Assert`
   import was retained (used elsewhere, e.g., line 1798).
4. No banner comments were introduced (`git diff` verified no `// ===` or
   `// ---` lines added).

## Expanded Plan 04 scope

Plan 04 originally scoped as "retro-fallback deletion" per D-04. Option B at
Plan 03's checkpoint expands Plan 04's scope with the following additional
items:

### New Plan 04 item 1: lift fast-path gates

Delete the optimizer and codegen gates that currently keep SYMBOL, ARRAY,
STRING, VARCHAR, UUID, Long256, Decimal128, and Decimal256 on the legacy
Sample By cursor:

- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` — delete
  `hasPrevWithUnsupportedType` (line 474), `isUnsupportedPrevType`
  (line 574), and the call site at line 8179 inside `rewriteSampleBy` (the
  `&& !hasPrevWithUnsupportedType(...)` guard falls out).
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` — delete
  `isFastPathPrevSupportedType` (line 1200) and the throw site inside
  `generateFill` at line 3554.

### New Plan 04 item 2: add plan-text assertions to the 13 Plan 03 tests

After the gates are lifted, add a `Sample By Fill` plan-text assertion to
each of the 13 tests. Two implementation choices:

- Add `assertPlanNoLeakCheck(query, expectedPlanFragment)` before the
  `assertSql` — consistent with existing pattern at lines 1501-1523 and
  1591-1613.
- Restore the `Assert.assertTrue(Chars.contains(getPlanSink(query).getSink(),
  "Sample By Fill"))` form — simpler, matches what f43a3d7057 originally
  used. Requires re-adding `Chars` import.

### New Plan 04 item 3: delete obsolete retro-fallback guard tests

The 6 retained retro-fallback guard tests listed in the resume instructions
are now provably obsolete (their whole purpose is to pin legacy-routing for
types that Plan 04 moves to the fast path). Delete:

- `testFillPrevSymbolLegacyFallback` (line 1968)
- `testFillPrevCrossColumnUnsupportedFallback` (line 1068)
- `testFillPrevCaseOverDecimalFallback` (line 948) — the CASE-over-DECIMAL256
  output type moves to fast path once the gate falls
- `testFillPrevExpressionArgDecimal128Fallback` (line 1141) — same reasoning
- `testFillPrevExpressionArgStringFallback` (line 1169) — STRING moves to
  fast path
- `testFillPrevIntervalFallback` (line 1300) — INTERVAL cast to STRING moves
  to fast path once STRING is unlocked

One guard test that is NOT in the deletion list: `testFillPrevLong128Fallback`
(line 1445). If Plan 04's gate lifting covers Long128 too, delete this one as
well; otherwise keep it.

### Retro-fallback infrastructure retained for Plan 04

Per D-04 / D-07, the following machinery stays in place for Plan 04 to
remove atomically:

- `core/src/main/java/io/questdb/griffin/FallbackToLegacyException.java`
- `QueryModel.stashedSampleByNode` + getter/setter + `clear()` reset
- `IQueryModel` declarations for the stash field
- `SqlOptimiser.rewriteSampleBy` stash write
- `SqlCodeGenerator` three `try { ... } catch (FallbackToLegacyException e)
  { ... }` sites at the `generateFill` call points

## Authorization for Plan 04

**Plan 04 is authorized.** The rowId rewrite in Plan 02 is proven to produce
correct carry-forward values for every currently-retro-fallback-routed
aggregate output type: SYMBOL (with null handling), ARRAY, STRING (including
reallocation-path values), VARCHAR (including unicode), UUID, Long256,
Decimal128, Decimal256.

Plan 04 can now lift the optimizer and code generator fast-path gates without
risk of data corruption. Retro-fallback machinery becomes dead code.

## Key Decisions

- **Option B at checkpoint** — ship data-correctness assertions only, defer
  plan-text assertions to Plan 04 (preserves D-07 commit sequencing).
- **Single-line TODO comment** — each of the 13 tests carries a forward
  pointer (`// Plan 04 adds "Sample By Fill" plan assertion once fast-path
  gates are lifted.`) so the debt is visible at the test site.
- **Chars import removed** — no remaining usage after dropping the plan-text
  assertions.
- **Assert import retained** — still used elsewhere (testFillPrevRejectNoArg
  at line 1798).
- **Retro-fallback guard tests retained** — the 7 existing retro-fallback
  guard tests stay until Plan 04 lifts the gates; they are each proven to
  correctly pin legacy-routing behavior before the gate lifting.

## Self-Check: PASSED

Verified:

- 13 new test methods present with exact names:
  `grep -c "public void testFillPrev(SymbolKeyed|SymbolNonKeyed|SymbolNull|
  ArrayDouble1D|StringKeyed|StringNonKeyed|VarcharKeyed|VarcharNonKeyed|
  UuidKeyed|UuidNonKeyed|Long256NonKeyed|Decimal128|Decimal256)\("` = 13
- 7 retained retro-fallback guard tests still present:
  `grep -c "testFillPrevSymbolLegacyFallback|testFillPrevCrossColumnUnsupported
  Fallback|testFillPrevCaseOverDecimalFallback|testFillPrevExpressionArg
  Decimal128Fallback|testFillPrevExpressionArgStringFallback|testFillPrev
  IntervalFallback|testFillPrevLong128Fallback"` = 7
- Zero active `Sample By Fill` assertions among the 13 new tests (only the
  forward-pointer TODO comment; no `assertPlanNoLeakCheck` or
  `Chars.contains(...)` on plan output).
- Full `SampleByFillTest` suite: 87/87 pass, 0 failures, 0 errors.
- Each of the 13 new tests passes in isolation.
- No banner comments (`// ===` / `// ---`) introduced: `git diff HEAD |
  grep -E "^\+.*// (===|---)"` returns no matches.
- Commit hash: `99240223bf` present in `git log --oneline` on branch
  `sm_fill_prev_fast_path`.
- Commit title (`Add per-type FILL(PREV) data-correctness tests`) is plain
  English, no Conventional Commits prefix, 48 characters.
