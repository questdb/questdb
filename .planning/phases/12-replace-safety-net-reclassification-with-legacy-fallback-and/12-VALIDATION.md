---
phase: 12
slug: replace-safety-net-reclassification-with-legacy-fallback-and
status: approved
nyquist_compliant: true
wave_0_complete: true
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

Phase 12 has **no traditional Wave 0**: no new framework to install and no
test-scaffolding file to create ahead of the production tasks. The three
points below explain why `wave_0_complete: true` is the correct state for
this phase:

- **(a) No framework install needed.** JUnit 4 and the QuestDB test harness
  (AbstractGriffinTest, assertMemoryLeak, assertQueryNoLeakCheck,
  assertPlanNoLeakCheck, assertExceptionNoLeakCheck) are already present
  in `core/pom.xml`. No `mvn install` of a new dependency is required; no
  test runner to install; no CI configuration changes.
- **(b) Scaffolding files are authored by plan 12-01 as its Wave 1 tasks.**
  The only two "scaffolding" artifacts new to phase 12 are
  `core/src/main/java/io/questdb/griffin/FallbackToLegacyException.java`
  (new file) and the `QueryModel.stashedSampleByNode` field block in
  `QueryModel.java` + `IQueryModel.java` (new members). Both are produced
  by plan 12-01 Task 1 — the first task of the first plan in Wave 1.
  **This project treats Wave 1 as the effective Wave 0 for test-enabled
  scaffolding**: the scaffolding is committed before any consumer (plans
  12-03 and 12-04) reads it, which is the same invariant a classical
  Wave 0 would guarantee.
- **(c) All 19 new tests land in existing test files in plan 12-04.**
  `SampleByFillTest.java` and `SampleByTest.java` already exist on master.
  Plan 12-04 adds 19 `@Test` methods (5 retro-fallback + 8 grammar
  negative + 3 grammar positive + 5 FILL_KEY + 1 TO-null; note the
  internal delta of 11 vs 8 on the grammar count — see plan 12-04
  `must_haves.truths`) into the existing files. No new test class is
  created; no test runner changes are required.

*Existing infrastructure covers all phase requirements; plan 12-01 supplies
the per-plan scaffolding within Wave 1.*

---

## Manual-Only Verifications

All phase behaviors have automated verification via the test commands above. No manual verifications required.

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies.
      Every task in plans 12-01..12-04 carries an `<automated>` block
      pointing at either `mvn -pl core compile` or a targeted
      `mvn -pl core -Dtest=...` command. No manual-only verifications.
- [x] Sampling continuity: no 3 consecutive tasks without automated verify.
      The longest run of tasks touching the same file is plan 12-02 (3
      tasks) — each with its own automated verify.
- [x] Wave 0 covers all MISSING references.
      N/A in the classical sense; see "Wave 0 Requirements" above for the
      Wave-1-as-effective-Wave-0 explanation. No `MISSING — Wave 0 must
      create {...}` sentinels appear in any `<automated>` block.
- [x] No watch-mode flags.
      No `-Dtest=... -Dmaven.surefire.rerunFailingTestsCount=...` or other
      watch-mode options in any verify command. All runs are single-shot.
- [x] Feedback latency < 120s quick, < 600s full.
      Quick run: `mvn -pl core -Dtest=SampleByFillTest test` — measured
      ~60-120s on a warm cache per plan 11 benchmarks. Full run across
      four test classes — measured ~400-600s. Both within budget.
- [x] `nyquist_compliant: true` set in frontmatter.

**Approval:** approved 2026-04-17
