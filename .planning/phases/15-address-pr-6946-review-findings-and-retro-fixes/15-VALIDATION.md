---
phase: 15
slug: address-pr-6946-review-findings-and-retro-fixes
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-04-21
---

# Phase 15 -- Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | JUnit 4 + QuestDB AbstractCairoTest (Maven) |
| **Config file** | `core/pom.xml` |
| **Quick run command** | `mvn -pl core -Dtest=SampleByFillTest test` |
| **Full suite command** | `mvn -pl core -Dtest='SampleByTest,SampleByFillTest,SampleByNanoTimestampTest,SqlOptimiserTest,ExplainPlanTest' test` |
| **Estimated runtime** | ~90 seconds quick, ~6-8 minutes full |

---

## Sampling Rate

- **After every task commit:** Run `mvn -pl core -Dtest=<affected-class> test` (single class).
- **After every plan wave:** Run the quick run command for that plan's primary test class.
- **Before `/gsd-verify-work`:** Full suite command must be green.
- **Max feedback latency:** ~90 seconds per task commit, ~8 minutes per wave.

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 15-01-01 | 01 | 1 | Criterion 1 (C-1) | - | TIMESTAMP fill constants unit-convert to column's native unit at codegen | integration | `mvn -pl core -Dtest=SampleByTest#testTimestampFillNullAndValue test` | YES | pending |
| 15-01-02 | 01 | 1 | Criterion 2 (C-2) | - | Unquoted numeric fill values for TIMESTAMP rejected at codegen | integration | `mvn -pl core -Dtest=SampleByTest#testTimestampFillValueUnquoted test` | YES | pending |
| 15-01-03 | 01 | 1 | Criterion 1 (nano twin) | - | Nano TIMESTAMP fill constants unchanged (already correct) | integration | `mvn -pl core -Dtest=SampleByNanoTimestampTest#testTimestampFillNullAndValue test` | YES | pending |
| 15-01-04 | 01 | 1 | Criterion 2 (nano twin) | - | Unquoted numeric rejected for nano TIMESTAMP target | integration | `mvn -pl core -Dtest=SampleByNanoTimestampTest#testTimestampFillValueUnquoted test` | YES | pending |
| 15-01-05 | 01 | 1 | Criterion 5 (M-5) | - | Non-TIMESTAMP timestamp-index fallback skips fill instead of crashing | integration | `mvn -pl core -Dtest=SampleByFillTest test` | YES | pending |
| 15-02-01 | 02 | 1 | Criterion 3 (C-3) | T-15-01 | Keyed SAMPLE BY FILL respects SqlExecutionCircuitBreaker cancellation | integration | `mvn -pl core -Dtest=SampleByFillTest#testFillKeyedRespectsCircuitBreaker test` | Wave 0 (new test) | pending |
| 15-02-02 | 02 | 1 | Criterion 4 (M-4) | - | FillRecord.getLong256 sink resets to null on FILL_PREV_SELF miss | integration | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevLong256NoPrevYet test` | Wave 0 (new test) | pending |
| 15-03-01 | 03 | 1 | Criterion 6 (M-7) | - | testSampleByFromToParallelSampleByRewriteWithKeys asserts bounded-TO output | integration | `mvn -pl core -Dtest=SqlOptimiserTest#testSampleByFromToParallelSampleByRewriteWithKeys test` | YES | pending |
| 15-04-01 | 04 | 2 | Criterion 7 (retro-doc) | - | 15-04-SUMMARY.md exists with three commit hashes | doc | `test -f .planning/phases/15-address-pr-6946-review-findings-and-retro-fixes/15-04-SUMMARY.md` | Wave 0 (new doc) | pending |

*Status: pending / green / red / flaky*

---

## Wave 0 Requirements

- [ ] `SampleByFillTest.testFillKeyedRespectsCircuitBreaker` - new test (Plan 02, C-3)
- [ ] `SampleByFillTest.testFillPrevLong256NoPrevYet` - new test (Plan 02, M-4)
- [ ] `15-04-SUMMARY.md` - retro-doc artifact (Plan 04)

*Restoration (not new):*
- `SampleByTest.testTimestampFillNullAndValue` - restore expected output to master form
- `SampleByTest.testTimestampFillValueUnquoted` - restore assertException form
- `SampleByNanoTimestampTest.testTimestampFillValueUnquoted` - restore assertException form (nano twin; null-and-value nano already correct)
- `SqlOptimiserTest.testSampleByFromToParallelSampleByRewriteWithKeys` - upgrade compile-only to output-assertion

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| `/review-pr 6946` shows 0 Critical findings | Criterion 8 | Requires external tool invocation against the branch head, not an in-JVM test | Run `/review-pr 6946` after phase completes; verify Moderate list reduced to deferred items only |

---

## Validation Sign-Off

- [ ] All tasks have automated verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (3 new tests/docs in the task map above)
- [ ] No watch-mode flags
- [ ] Feedback latency < 90s per task commit
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
