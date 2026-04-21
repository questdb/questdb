---
phase: 16
slug: fix-multi-key-fill-prev-with-inline-function-grouping-keys
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-04-21
---

# Phase 16 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | JUnit 4 via `AbstractCairoTest` superclass harness |
| **Config file** | `core/pom.xml` (surefire plugin, existing; runs with `-ea`) |
| **Quick run command** | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevIntervalMultiKey test` |
| **Full suite command** | `mvn -pl core -Dtest=SampleByFillTest test` |
| **Estimated runtime** | ~2-3 minutes per quick run; ~3 minutes full `SampleByFillTest`; ~10-15 minutes cross-suite |

---

## Sampling Rate

- **After every task commit:** Run the full `SampleByFillTest` class — `mvn -pl core -Dtest=SampleByFillTest test`. The 5 new tests plus every existing FILL regression land in ~3 minutes.
- **After every plan wave:** Run cross-suite — `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest,SqlOptimiserTest' test` to catch plan-text or nano-timestamp drift.
- **Before `/gsd-verify-work`:** Full `mvn -pl core test` suite must be green.
- **Max feedback latency:** ~3 minutes (single-class quick run).

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 16-01-01 | 01 | 1 | COR-01..04, KEY-01..05, XPREV-01 | — | Classifier treats non-aggregate FUNCTION/OPERATION grouping keys as FILL_KEY instead of FILL_PREV_SELF. D-05 aggregate-arm assertion fires only on invariant drift. | integration | `mvn -pl core -Dtest=SampleByFillTest test` | ❌ W0 | ⬜ pending |
| 16-01-02 | 01 | 1 | COR-01, KEY-01..05, XPREV-01 | — | Multi-key `interval(lo, hi)` FILL(PREV) produces 6-row cartesian output. | integration | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevIntervalMultiKey test` | ❌ W0 | ⬜ pending |
| 16-01-03 | 01 | 1 | COR-01, KEY-01..05 | — | Multi-key `concat(a, b)` FILL(PREV) produces 6-row cartesian output. | integration | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevConcatMultiKey test` | ❌ W0 | ⬜ pending |
| 16-01-04 | 01 | 1 | COR-01, KEY-01..05 | — | Multi-key `a \|\| b` (OPERATION) FILL(PREV) produces 6-row cartesian output. | integration | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevConcatOperatorMultiKey test` | ❌ W0 | ⬜ pending |
| 16-01-05 | 01 | 1 | COR-01, KEY-01..05 | — | Multi-key `cast(x AS STRING)` FILL(PREV) produces 6-row cartesian output. | integration | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevCastMultiKey test` | ❌ W0 | ⬜ pending |
| 16-01-06 | 01 | 1 | COR-01, KEY-01..05, FILL-02 | — | Multi-key `cast(x AS STRING)` FILL(NULL) produces 6-row cartesian output with null aggregates in gaps. | integration | `mvn -pl core -Dtest=SampleByFillTest#testFillNullCastMultiKey test` | ❌ W0 | ⬜ pending |
| 16-01-07 | 01 | 1 | Regression anchor | — | Single-key `testFillPrevInterval` still passes unchanged (success criterion #4). | integration | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevInterval test` | ✅ at `SampleByFillTest.java:1678` | ⬜ pending |
| 16-01-08 | 01 | 1 | Plan-text stability | — | ExplainPlanTest / SqlOptimiserTest unchanged (classifier is codegen-only; zero existing plan-text assertions exercise inline-function grouping keys on the fast path). | integration | `mvn -pl core -Dtest='ExplainPlanTest,SqlOptimiserTest' test` | ✅ existing suites | ⬜ pending |
| 16-01-09 | 01 | 1 | Cross-suite | — | Full SAMPLE BY family passes (SampleByTest + SampleByNanoTimestampTest). | integration | `mvn -pl core -Dtest='SampleByTest,SampleByNanoTimestampTest' test` | ✅ existing suites | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` — 5 new test methods land alphabetically per CLAUDE.md member ordering: `testFillNullCastMultiKey`, `testFillPrevCastMultiKey`, `testFillPrevConcatMultiKey`, `testFillPrevConcatOperatorMultiKey`, `testFillPrevIntervalMultiKey`.
- No new framework install needed; JUnit 4 + `AbstractCairoTest` harness already in place.
- No shared fixtures or conftest-equivalent needed; each test is self-contained with its own DDL + INSERT inside `assertMemoryLeak`.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| `Interval.NULL` rendering in multi-key FILL(PREV) "no prev yet" cell | A2 in RESEARCH.md assumptions | `Interval.NULL` may render as empty text rather than the `null` literal used for DOUBLE columns (Phase 14 D-15 convention). Row-order is also OrderedMap-hash-dependent (A1). | During task execution, run the fixture once against the fixed code, capture the exact observed output (including row ordering), and freeze it as the expected string. Precedent: Phase 15 Plan 03 "probe-and-freeze" pattern. |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (5 new tests)
- [ ] No watch-mode flags (surefire runs one-shot)
- [ ] Feedback latency < 180s per task commit
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
