---
phase: 13
type: validation-inputs
status: parked
created: 2026-04-17
trigger: consumed during /gsd-plan-phase 13
---

# Phase 13 — Validation Inputs (borrowed from `sm_fill_prev_fast_all_types`)

Phase 13 migrates FILL(PREV) snapshot storage from per-type materialization
to a single chain rowId per key. These tests exist on the
`sm_fill_prev_fast_all_types` branch and assert the goal state: every
per-type FILL(PREV) variant produces correct output on the fast path
(no retro-fallback).

**Rule:** Phase 13 is not complete until all 13 tests pass on the fast
path (plan shows `Sample By Fill`, not `Sample By`).

## Source

- Branch: `sm_fill_prev_fast_all_types`
- Commit: `f43a3d7057` — "Extend fill-cursor fast path to all column types for FILL(PREV)"
- File: `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java`

Fetch the tests with:
```bash
git show f43a3d7057:core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java
```

## Tests to borrow (13 total)

Each name matches the `@Test` method on the source commit. All use
`assertMemoryLeak` + `assertSql` or `assertQueryNoLeakCheck`.

### Symbol (3)
- `testFillPrevSymbolKeyed`
- `testFillPrevSymbolNonKeyed`
- `testFillPrevSymbolNull`

### Array (1)
- `testFillPrevArrayDouble1D`

### String / Varchar (4)
- `testFillPrevStringKeyed`
- `testFillPrevStringNonKeyed`
- `testFillPrevVarcharKeyed`
- `testFillPrevVarcharNonKeyed`

### UUID (2)
- `testFillPrevUuidKeyed`
- `testFillPrevUuidNonKeyed`

### Long256 (1)
- `testFillPrevLong256NonKeyed`

### Decimal128/256 (2)
- `testFillPrevDecimal128`
- `testFillPrevDecimal256`

## Tests to NOT borrow (deliberately retained on our branch)

The source commit deletes these — do NOT mirror the deletion. Our phase
12 deliberately added equivalent retro-fallback guard tests that must
stay until phase 13's rowId rewrite demonstrably unlocks all types.

- `testFillPrevSymbolLegacyFallback` — asserts retro-fallback plan for
  SYMBOL first(). If rowId unlocks SYMBOL, this test becomes obsolete and
  can be removed *alongside* the retro-fallback deletion (not before).
- `testFillPrevCrossColumnUnsupportedFallback` — same rationale.

See SEED-001 bucket 1 for the retro-fallback handling decision matrix.

## Expected status progression

| Stage | Expected behavior on our branch |
|-------|---------------------------------|
| **Before phase 13** | Tests do not exist on our branch. |
| **After chain.clear() fix (commit 1 of phase 13)** | Tests still not present. |
| **After rowId rewrite (commit 2 of phase 13)** | Cherry-pick tests. All 13 must pass. Plan output must show `Sample By Fill` (fast path), not `Sample By` (legacy). |
| **After retro-fallback cleanup (if Bucket 3 of SEED-001 fires)** | Legacy-fallback guard tests (`testFillPrevSymbolLegacyFallback`, `testFillPrevCrossColumnUnsupportedFallback`) are deleted alongside the retro-fallback machinery. |

## Planner consumption

When `/gsd-plan-phase 13` is invoked, the planner must:

1. Read this file.
2. Add a validation task to one of the plans that:
   - Cherry-picks the 13 tests from `f43a3d7057` into
     `SampleByFillTest.java`.
   - Runs them and confirms all 13 pass on the fast path.
   - Verifies plan output for each is `Sample By Fill` (fast path), not
     `Sample By` (legacy).
3. Use success of this validation as a gate on phase 13 completion.

## Related artifacts

- `13-CONTEXT.md` (TBD) — consume this file during context gathering
- `.planning/seeds/SEED-001-phase12-followups-plumbing-grammar.md` — retro-fallback handling decisions
- `.planning/seeds/SEED-002-phase12-cursor-defects-rowid-dependent.md` — cursor defects that rowId may absorb
