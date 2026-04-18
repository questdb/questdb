---
phase: 13
slug: migrate-fill-prev-snapshots-from-materialized-values-to-rowi
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-04-18
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
| **Estimated runtime** | ~180 seconds (full phase suite) |

---

## Sampling Rate

- **After every task commit:** Run `mvn -pl core -Dtest=SampleByFillTest test`
- **After every plan wave:** Run the full suite command
- **Before `/gsd-verify-work`:** Full suite must be green AND combined-suite ordering check must exit 0
- **Max feedback latency:** 180 seconds

---

## Per-Task Verification Map

*Populated by the planner. Every task in every PLAN.md must map to either an `<automated>` verify command or a Wave 0 dependency that provides one.*

---

## Wave 0 Requirements

- No framework installation required — QuestDB test harness already in place.
- Wave 0 artifact: D-02 investigation report (chain.clear() reproduction outcome). This is a gating task, not a test file; the planner must schedule it as the first task before any rowId implementation begins.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| SEED-002 Defect 1 post-rewrite disposition | SC-8 | Requires judgement: if `testSampleByFillNeedFix` assertion #2 passes after rowId rewrite, absorbed; if it fails, planner-flagged task to fix in `generateFill()` plumbing | Run `mvn -pl core -Dtest=SampleByTest#testSampleByFillNeedFix test` after Commit 2; inspect plan output via `EXPLAIN` to confirm `Sample By Fill` path is selected |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (D-02 investigation report)
- [ ] No watch-mode flags
- [ ] Feedback latency < 180s for quick run
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
