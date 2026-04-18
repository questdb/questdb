---
phase: 13
slug: migrate-fill-prev-snapshots-from-materialized-values-to-rowi
status: ready
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-18
last_updated: 2026-04-18
---

# Phase 13 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | JUnit 4 + QuestDB `AbstractCairoTest` (Maven Surefire) |
| **Config file** | `core/pom.xml` |
| **Quick run command** | `mvn -pl core -Dtest=SampleByFillTest test` |
| **Full suite command** | `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest,RecordCursorMemoryUsageTest' test` |
| **Combined-suite (ordering-sensitive)** | `mvn -pl core -Dtest='SampleByFillTest,SampleByTest' test` AND `mvn -pl core -Dtest='SampleByTest,SampleByFillTest' test` |
| **Estimated runtime** | ~180 seconds (full phase suite) |

---

## Sampling Rate

- **After every task commit:** Run `mvn -pl core -Dtest=SampleByFillTest test`.
- **After every plan wave:** Run the full suite command.
- **Before `/gsd-verify-work`:** Full suite must be green AND combined-suite ordering check (both orderings) must exit 0.
- **Max feedback latency:** 180 seconds.

---

## Per-Task Verification Map

Every task in every PLAN.md maps to an `<automated>` verify command (or a Wave 0 gating deliverable). No 3 consecutive tasks without automated verify.

| Plan | Task | Automated Verify |
|------|------|-----------------|
| 13-01 | Task 1: D-02 investigation report | `test -f .planning/phases/13-.../13-INVESTIGATION.md && wc -l .planning/phases/13-.../13-INVESTIGATION.md` returns >= 40 lines, with verdict (a) |
| 13-01 | Task 2: chain.clear() in SortedRecordCursor.of() | `grep -n "chain.clear()" SortedRecordCursor.java` + `mvn -pl core -Dtest='SampleByFillTest,SampleByTest' test` exits 0 |
| 13-02 | Task 1: Field/constant/helper rewrite in SampleByFillRecordCursorFactory | `grep -c "simplePrevRowId\|PREV_ROWID_SLOT\|updateKeyPrevRowId\|saveSimplePrevRowId\|prevRecord = baseCursor.getRecordB" SampleByFillRecordCursorFactory.java` >= 5 (field declaration reset + constant + both renamed helpers + prevRecord init) |
| 13-02 | Task 2: FillRecord getters + emit-path recordAt + mapValueTypes resize | `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest' test` exits 0 |
| 13-03 | Task 1: Cherry-pick 13 per-type tests | `mvn -pl core -Dtest=SampleByFillTest test` exits 0 AND `grep -c "public void testFillPrev\(SymbolKeyed\|SymbolNonKeyed\|SymbolNull\|ArrayDouble1D\|StringKeyed\|StringNonKeyed\|VarcharKeyed\|VarcharNonKeyed\|UuidKeyed\|UuidNonKeyed\|Long256NonKeyed\|Decimal128\|Decimal256\)\b" SampleByFillTest.java` = 13 |
| 13-04 | Task 1: Delete retro-fallback machinery + 7 tests | `test ! -f FallbackToLegacyException.java` AND `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest,RecordCursorMemoryUsageTest' test` exits 0 |
| 13-05 | Task 1: Restore assertion #1 to master's 3-row form | `mvn -pl core -Dtest=SampleByTest#testSampleByFillNeedFix test` — assertion #1 passes |
| 13-05 | Task 2: Restore assertion #2 + Defect 1 disposition (autonomous: false) | `mvn -pl core -Dtest=SampleByTest#testSampleByFillNeedFix test` exits 0 AND plan for CTE query contains `Sample By Fill` |
| 13-06 | Task 1: WR-04 precise chain-rejection position + update 2 chain-rejection tests | `mvn -pl core -Dtest='SampleByFillTest#testFillPrevRejectMutualChain+testFillPrevRejectThreeHopChain' test` exits 0 |
| 13-06 | Task 2: Defect 3 grammar + testFillInsufficientFillValues + testSampleByFillNeedFix assertion #3 restoration | `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest,RecordCursorMemoryUsageTest' test` exits 0 |

---

## Wave 0 Requirements

- No framework installation required — QuestDB test harness already in place.
- Wave 0 artifact: `13-INVESTIGATION.md` (D-02 reproduction outcome). Planned as Plan 13-01 Task 1 — the first task of the first wave, explicitly gating all downstream work. `autonomous: false`; verdicts (b) and (c) surface to user before any rowId implementation starts.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| SEED-002 Defect 1 post-rewrite disposition | SC-8 | Requires judgement: if `testSampleByFillNeedFix` assertion #2 passes after rowId rewrite, absorbed (Branch A); if factory-plumbing bug surfaces, Branch B/C requires locus decision | Plan 13-05 Task 2 is `autonomous: false` and encodes the branching logic + escalation to user when fix scope is non-trivial |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies.
- [x] Sampling continuity: no 3 consecutive tasks without automated verify (every plan's tasks have per-task or per-plan verify commands).
- [x] Wave 0 covers MISSING references (D-02 investigation report — Plan 13-01 Task 1).
- [x] No watch-mode flags.
- [x] Feedback latency < 180s for quick run.
- [x] `nyquist_compliant: true` set.

**Approval:** ready for `/gsd-execute-phase 13`.
