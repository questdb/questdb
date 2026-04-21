# Phase 15: Address PR #6946 review findings and retro-document post-phase-14 fixes - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-21
**Phase:** 15-address-pr-6946-review-findings-and-retro-fixes
**Areas discussed:** C-1 fix location, C-2 rejection approach, Plan grouping, Retro-doc format, C-3 test harness

---

## C-1 fix location (unified with C-2)

### First pass — broad comparison

| Option | Description | Selected |
|--------|-------------|----------|
| Cursor-side, cache at init | In SampleByFillCursor.initialize(), pre-compute converted long[] constantFillTimestamps for TIMESTAMP columns. FillRecord.getTimestamp FILL_CONSTANT branch reads cached value. | |
| Cursor-side, per-row driver.from | FillRecord.getTimestamp FILL_CONSTANT branch applies driver.from(...) per read. Minimal diff, O(rows) cost. | (user rejected as inefficient) |
| Codegen-side | In generateFill, parse quoted literal via TimestampConstant.newInstance(driver.parseQuotedLiteral(...), targetType). Cursor reads raw. | |

**User's initial ask:** compare options 1 and 3 in depth; option 2 (per-row) rejected upfront.

### Second pass — after reading coerceRuntimeConstantType + createPlaceHolderFunction

Key discovery: `coerceRuntimeConstantType` is a validator, not a converter. The legacy cursor path (`SampleByFillValueRecordCursorFactory.createPlaceHolderFunction` at `:144-173`) already implements the codegen-side fix pattern verbatim, including master's exact error message for unquoted numerics.

| Option | Description | Selected |
|--------|-------------|----------|
| Yes, codegen-side unified | Mirror createPlaceHolderFunction's TIMESTAMP branch in generateFill. Chars.isQuoted check throws master's error (closes C-2); TimestampConstant.newInstance(driver.parseQuotedLiteral(token), targetType) parses in column's native unit (closes C-1). | ✓ |
| Split: C-1 cursor-side + C-2 codegen-side | Fix C-1 in cursor via cached long[] + separate codegen C-2 check. Two files; doesn't adopt legacy pattern. | |
| Expand scope: codegen-side for all fill types | Generalize to INT/LONG/FLOAT/DOUBLE/SHORT/BYTE/IPv4. Broader blast radius. Scope creep. | |

**User's choice:** Yes, codegen-side unified (Recommended).
**Notes:** Unifies C-1 + C-2 into a single fix mechanism that matches the legacy cursor path's proven approach.

---

## C-2 scope (strictness of non-quoted rejection)

| Option | Description | Selected |
|--------|-------------|----------|
| Match legacy broadly | Chars.isQuoted(fillExpr.token) rejects 1236, now(), to_timestamp('...') alike. Matches master behavior. One unified check. | ✓ |
| Narrow: reject only unquoted numerics | Only reject numeric-literal CONSTANT nodes. Allow TIMESTAMP-returning functions. More permissive than master. | |
| Planner's discretion | Planner picks after reading test coverage. | |

**User's choice:** Match legacy broadly.
**Notes:** Minimal divergence from master + inherits legacy's proven pattern.

---

## Plan grouping

| Option | Description | Selected |
|--------|-------------|----------|
| File-clustered | Plan 01 Codegen (C-1+C-2 + M-5); Plan 02 Cursor (C-3 + M-4); Plan 03 Test-only (M-7); Plan 04 Retro-doc. 4 plans. | ✓ |
| Severity-clustered | Plan 01 Critical batch (C-1+C-2 + C-3); Plan 02 Moderate batch (M-4 + M-5 + M-7); Plan 03 Retro-doc. 3 plans, cross-file commits. | |
| One-per-finding | 6 fix plans + 1 retro plan = 7 plans. Maximum bisectability, most planning overhead. | |
| Pair Critical fixes, cluster rest | Plan 01 C-1+C-2; Plan 02 C-3; Plan 03 Moderate cluster; Plan 04 Retro-doc. 4 plans, Critical-per-commit visibility. | |

**User's choice:** File-clustered.
**Notes:** Matches Phase 14's D-01 clustering; keeps same-file changes in same commit.

---

## Retro-doc format

| Option | Description | Selected |
|--------|-------------|----------|
| One plan, three SUMMARY files | 15-04-PLAN.md + 15-04-A/B/C-SUMMARY.md per commit. Per-commit audit granularity. | |
| One plan, one consolidated SUMMARY | 15-04-PLAN.md + single 15-04-SUMMARY.md with three sections. | ✓ |
| Three separate plans | 15-04/05/06-PLAN.md each with own SUMMARY. Matches one-plan-per-commit convention but heavy for zero-code artifacts. | |
| Fold into codegen/cursor SUMMARYs | Mention commits in existing Plan 01/02 SUMMARYs. Loses discoverability. | |

**User's choice:** One plan, one consolidated SUMMARY.

---

## C-3 circuit breaker test harness

| Option | Description | Selected |
|--------|-------------|----------|
| Reuse ParallelGroupByFuzzTest pattern | DefaultSqlExecutionCircuitBreakerConfiguration + tick-counting MillisecondClock + NetworkSqlExecutionCircuitBreaker + WorkerPool. Production CB code, deterministic, proven in-repo. | ✓ |
| Custom stub SqlExecutionCircuitBreaker | Subclass throws on Nth statefulThrowExceptionIfTripped() call. Simpler but loses real CB wiring coverage. | |
| Planner's discretion | Default to reuse unless harness mismatch. | |

**User's choice:** Reuse ParallelGroupByFuzzTest pattern.

---

## Claude's Discretion

- Exact placement of TIMESTAMP-target detection inside the FILL_CONSTANT branch at `SqlCodeGenerator.java:3620-3628` (before or after existing isNull/isPrev branches).
- Whether C-3's CB field is captured inside `of()` (criterion #3 default) or `initialize()` if cleaner.
- Test placement within `SampleByFillTest.java` (alphabetical per CLAUDE.md).
- Whether Plan 04's `15-04-PLAN.md` has an empty task list or a single "write SUMMARY" task.
- Whether C-3's test lives in `SampleByFillTest` or a new file based on harness fit.

## Deferred Ideas

- M-6 CairoException.critical guard test (Future Work per ROADMAP.md).
- M-6b multi-worker parallel test (Future Work per ROADMAP.md).
- Dead `isKeyColumn` removal, em-dash cleanup, PR title fix, `.planning/` diff noise (merge-time via `/gsd-pr-branch`).
- Expansion of D-03 codegen-side coercion to non-TIMESTAMP fill-value types (future phase if analogous bugs surface).
