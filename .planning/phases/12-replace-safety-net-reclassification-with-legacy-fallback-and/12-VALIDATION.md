---
phase: 12
slug: replace-safety-net-reclassification-with-legacy-fallback-and
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-04-17
---

# Phase 12 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | JUnit 4 + QuestDB test harness (assertMemoryLeak, assertQueryNoLeakCheck, assertPlanNoLeakCheck) |
| **Config file** | core/pom.xml (Maven surefire) |
| **Quick run command** | `mvn -pl core -Dtest=SampleByFillTest test` |
| **Full suite command** | `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest' test` |
| **Estimated runtime** | ~120 seconds quick / ~600 seconds full |

---

## Sampling Rate

- **After every task commit:** Run `mvn -pl core -Dtest=SampleByFillTest test`
- **After every plan wave:** Run the full suite command
- **Before `/gsd-verify-work`:** Full suite must be green across SampleByFillTest + SampleByTest + SampleByNanoTimestampTest + ExplainPlanTest
- **Max feedback latency:** ~120 seconds (quick)

---

## Per-Task Verification Map

Filled by the planner. Expected task-to-test mapping:

| Task Area | Requirement | Test File | Test Type | Automated Command |
|---|---|---|---|---|
| QueryModel stash field | Retro-fallback state plumbing | (unit — no dedicated test; exercised indirectly) | indirect | full suite |
| Optimizer gate Tier 1 (cross-col, LONG128, INTERVAL) | CONTEXT item 1, 2 | SampleByFillTest (new retro-fallback tests) | integration | `mvn -pl core -Dtest=SampleByFillTest test` |
| Safety-net removal + retro-fallback | CONTEXT item 3 | testFillPrevExpressionArgDecimal128Fallback, ...StringFallback, ...CaseOverDecimalFallback, testFillPrevLong128Fallback, testFillPrevIntervalFallback | integration | `mvn -pl core -Dtest=SampleByFillTest#testFillPrev*Fallback test` |
| hasExplicitTo LONG_NULL guard | CONTEXT item 4 | testFillToNullTimestamp | integration | `mvn -pl core -Dtest=SampleByFillTest#testFillToNullTimestamp test` |
| testSampleByFillNeedFix restore | CONTEXT item 5 | SampleByTest#testSampleByFillNeedFix | integration | `mvn -pl core -Dtest=SampleByTest#testSampleByFillNeedFix test` |
| Grammar — PREV(ts) reject | CONTEXT D-05 | testFillPrevRejectTimestamp | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevRejectTimestamp test` |
| Grammar — PREV(self) alias | CONTEXT D-06 | testFillPrevSelfAlias | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevSelfAlias test` |
| Grammar — chain reject | CONTEXT D-07 | testFillPrevRejectMutualChain, testFillPrevRejectThreeHopChain, testFillPrevAcceptPrevToSelfPrev, testFillPrevAcceptPrevToConstant | unit | `mvn -pl core -Dtest='SampleByFillTest#testFillPrev*Chain,SampleByFillTest#testFillPrevAccept*' test` |
| Grammar — malformed reject | CONTEXT D-08 | testFillPrevRejectFuncArg, testFillPrevRejectMultiArg, testFillPrevRejectNoArg, testFillPrevRejectBindVar | unit | `mvn -pl core -Dtest='SampleByFillTest#testFillPrevReject*' test` |
| Grammar — type mismatch reject | CONTEXT D-09 | testFillPrevRejectTypeMismatch | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevRejectTypeMismatch test` |
| FILL_KEY coverage — UUID/Long256/Decimal128/256 | CONTEXT items 8-11 | testFillKeyedUuid, testFillKeyedLong256, testFillKeyedDecimal128, testFillKeyedDecimal256 | integration | `mvn -pl core -Dtest='SampleByFillTest#testFillKeyed*' test` |
| Geo no-prev-yet sentinels | CONTEXT item 12 | testFillPrevGeoNoPrevYet | integration | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevGeoNoPrevYet test` |
| assertSql → assertQueryNoLeakCheck conversion | CONTEXT item 13 | ~15 tests in SampleByFillTest | factory-property check | full SampleByFillTest run |
| anyPrev loop removal | CONTEXT item 14 | (exercised by existing tests) | indirect | full suite |
| FillRecord getter alphabetization | CONTEXT item 15 | (compile-time correctness) | compile | `mvn -pl core compile` |
| SampleByFillCursor member alphabetization | CONTEXT item 16 | (compile-time) | compile | `mvn -pl core compile` |
| isKeyColumn relocation | CONTEXT item 17 | (compile-time) | compile | `mvn -pl core compile` |
| Import alphabetization | CONTEXT item 18 | (compile-time) | compile | `mvn -pl core compile` |
| FQN → plain imports | CONTEXT item 19 | (compile-time) | compile | `mvn -pl core compile` |
| toPlan fill= unconditional | CONTEXT item 20 | Plan-text assertions in SampleByFillTest, ExplainPlanTest | integration | `mvn -pl core -Dtest='SampleByFillTest,ExplainPlanTest' test` |
| Dates.parseOffset assert/throw | CONTEXT item 21 | (exercised by existing offset tests) | indirect | full suite |

Planner fills the exact Task IDs when creating PLAN.md files.

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] No framework install — JUnit 4 and QuestDB test harness are already in core/pom.xml.
- [ ] No new test file needed — all new tests go into existing SampleByFillTest.java and SampleByTest.java.

*Existing infrastructure covers all phase requirements.*

---

## Manual-Only Verifications

All phase behaviors have automated verification via the test commands above. No manual verifications required.

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (N/A — existing infrastructure)
- [ ] No watch-mode flags
- [ ] Feedback latency < 120s quick, < 600s full
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
