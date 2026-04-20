---
phase: 14
slug: fix-issues-from-moderate-list-for-m5-and-m6-just-mention-in
status: draft
nyquist_compliant: false
wave_0_complete: true
created: 2026-04-20
---

# Phase 14 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | JUnit 4 + QuestDB harness (`AbstractCairoTest`, `AbstractSqlParserTest`) |
| **Config file** | none — harness base classes provide `CairoConfiguration` |
| **Quick run command** | `mvn -pl core -Dtest=SampleByFillTest test` |
| **Full suite command** | `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,RecordCursorMemoryUsageTest,ExplainPlanTest' test` |
| **Estimated runtime** | ~180 seconds (full suite); ~60 seconds (quick run) |

---

## Sampling Rate

- **After every task commit:** Run `mvn -pl core -Dtest=<single-class> test` for the class affected by that task.
- **After every plan wave:** Run combined SAMPLE BY trio: `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest' test`.
- **Before `/gsd-verify-work`:** Full suite (including `RecordCursorMemoryUsageTest,ExplainPlanTest`) must be green.
- **Max feedback latency:** ~180 seconds.

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Finding | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|---------|-----------------|-----------|-------------------|-------------|--------|
| 14-01-M1 | 01 | 1 | M-1 reorder-safe classification | Outer projection reorder does not misclassify bare FILL(PREV) columns | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevOuterProjectionReorder test` | file exists (new test method) | pending |
| 14-01-M3 | 01 | 1 | M-3 broadcast tightening | `PREV(colX)` single-arg does not silently broadcast across all aggregates | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevCrossColumnBroadcastRejection test` | file exists (new) | pending |
| 14-01-M9 | 01 | 1 | M-9 cross-column full-type match | DECIMAL/GEOHASH/ARRAY precision/width/dim mismatches reject with clear error | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevCrossColumnDecimalPrecisionMismatch test` | file exists (new) | pending |
| 14-01-MN13 | 01 | 1 | Mn-13 fillValues leak | No native-memory leak on the `generateFill` success path | unit | transitively covered by `assertMemoryLeak`-wrapped SAMPLE BY FILL tests | existing | pending |
| 14-02-M2 | 02 | 2 | M-2 ARRAY/BIN getters | Keyed SAMPLE BY FILL(PREV) with ARRAY/BINARY columns returns correct key-carried values | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevKeyedArray test`, `…testFillPrevKeyedBinary test` | file exists (new) | pending |
| 14-02-M8 | 02 | 2 | M-8 INTERVAL getter + audit | FILL(PREV) on INTERVAL columns returns correct values; all typed getters covered | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevInterval test` | file exists (new) | pending |
| 14-02-D14 | 02 | 2 | D-14 per-type coverage | BOOLEAN/BYTE/SHORT/INT/LONG/DATE/TIMESTAMP/IPv4/BINARY/INTERVAL all FILL(PREV)-correct | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillPrev{Boolean,Byte,…} test` | file exists (new, 10 methods) | pending |
| 14-03-M4A | 03 | 3 | M-4 Case A (empty base + TIME ZONE + FROM) | Fill rows land at the correct UTC grid derived from the local-time FROM | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillSubDayTimezoneFromEmpty test` | file exists (new) | pending |
| 14-03-M4B | 03 | 3 | M-4 Case B (sparse) | Leading fills align with local-time grid, not UTC grid | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillSubDayTimezoneFromSparse test` | file exists (new) | pending |
| 14-03-M4C | 03 | 3 | M-4 Case C (dense) | Behavior locked in (was already correct, masked by `firstTs` anchor) | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillSubDayTimezoneFromDense test` | file exists (new) | pending |
| 14-04-M7 | 04 | 4 | M-7 double-free defensive fix | `SortedRecordCursorFactory` constructor throw does not propagate double-free | unit | `mvn -pl core -Dtest=SampleByFillTest#testSortedRecordCursorFactoryConstructorThrow test` | file exists (new; harness location Plan 04 discretion) | pending |
| 14-XX-D16 | 01 or dedicated | 1 | D-16 ghost-test cleanup | Renamed `testSampleByFromToIsDisallowedForKeyedQueries` (and nano twin) assert correct passing behavior | unit | `mvn -pl core -Dtest=SampleByTest test`, `…SampleByNanoTimestampTest test` | existing (rename + assert) | pending |
| 14-XX-D17 | 01 or 02 | 1–2 | D-17 Decimal zero-vs-null | FILL(PREV) distinguishes legitimate `0.00` from pre-data NULL | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevDecimalZeroVsNull test` | file exists (new) | pending |
| 14-XX-D18 | 01 or 04 | 1–4 | D-18 RCM direct factory assertion | `RecordCursorMemoryUsageTest` asserts `SampleByFillRecordCursorFactory` in base-factory chain | unit | `mvn -pl core -Dtest=RecordCursorMemoryUsageTest test` | existing (modify) | pending |
| 14-XX-D19 | 03 or final | 3 | D-19 PR description Trade-offs update | PR #6946 body's `## Trade-offs` section contains M-5 memory envelope + M-6 3-scan multiplier bullets | manual + `gh pr edit` | `gh pr view 6946 --json body \| jq -r .body \| grep -c 'O(K keys'` | N/A | pending |
| 14-gate | all | final | Phase gate: full SAMPLE BY trio + RCM + ExplainPlan green | All added/modified tests pass in the combined run | unit | `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,RecordCursorMemoryUsageTest,ExplainPlanTest' test` | existing | pending |

*Status: pending · green · red · flaky*

---

## Wave 0 Requirements

Existing infrastructure covers all phase requirements. All target test classes
(`SampleByFillTest`, `SampleByTest`, `SampleByNanoTimestampTest`,
`RecordCursorMemoryUsageTest`, `ExplainPlanTest`) already exist in
`core/src/test/java/io/questdb/test/griffin/...`. Plan work is additive (new
test methods) and modify-in-place (ghost-test rename, RCM factory-type
assertion restore).

- [x] Target test classes present
- [x] `assertMemoryLeak`/`assertQueryNoLeakCheck` helpers available
- [x] `AbstractCairoTest` harness supports `CairoConfiguration` overrides for M-7 trigger (`sqlSortKeyMaxPages = -1`)
- [x] No new framework, no new dependencies

---

## Manual-Only Verifications

| Behavior | Finding | Why Manual | Test Instructions |
|----------|---------|------------|-------------------|
| PR #6946 description update | D-19, D-20 | GitHub PR body is outside the codebase; bullets must land in the existing `## Trade-offs` section | 1. Run `gh pr view 6946 --json body -q .body` to fetch current body. 2. Insert two bullets into `## Trade-offs`: memory envelope (O(K×B)), 3-pass scan. 3. `gh pr edit 6946 --body-file <path>`. 4. Verify with `gh pr view 6946 --json body -q .body \| grep -E 'O\(K keys\|three times'`. |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 180s
- [ ] `nyquist_compliant: true` set in frontmatter (after plan-checker approval)

**Approval:** pending
