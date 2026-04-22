---
phase: 17
slug: verify-pr-6946-body-drift-against-landed-commits-decide-code
status: approved
nyquist_compliant: true
wave_0_complete: false
created: 2026-04-22
---

# Phase 17 - Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `17-RESEARCH.md` "## Validation Architecture" section (lines 580-650).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | JUnit 4 (existing QuestDB convention) + JMH for m3 benchmark |
| **Config file** | `core/pom.xml` (maven-surefire-plugin) |
| **Quick run command** | `mvn -Dtest=SampleByFillTest test -pl core` |
| **Full suite command** | `mvn -Dtest=SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest,SqlOptimiserTest,SqlParserTest,RecordCursorMemoryUsageTest,FillRecordDispatchTest test -pl core` |
| **Estimated runtime** | Quick: ~30-60s; Full: ~5-8m |

---

## Sampling Rate

- **After every task commit:** `mvn -Dtest=SampleByFillTest test -pl core` (catches the regression-test-belongs-next-to-fix invariant)
- **After every plan wave:** full suite command above + `mvn -Dtest=SampleByFillKeyedResetBenchmark ...` when m3 benchmark landed
- **Before `/gsd-verify-work`:** full suite green + `/review-pr 6946` re-run shows all items closed or explicitly deferred under D-01
- **Max feedback latency:** ~60 seconds (quick run)

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Decision | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|----------|-----------------|-----------|-------------------|-------------|--------|
| 17-01-01 | 01 | 1 | D-07/D-08/D-09/D-10 | Cancellation CB honoured during keyed pass-1 | unit + regression self-check | `mvn -Dtest=SampleByFillTest#testFillKeyedPass1RespectsCircuitBreaker test -pl core` | existing file, new method | pending |
| 17-01-02 | 01 | 1 | D-26/D-27 | Cross-column FILL(PREV) rejects TIMESTAMP_MICRO<->NANO + INTERVAL variant | unit | `mvn -Dtest=SampleByFillTest#testFillPrevCrossColumnTimestampUnitMismatch test -pl core` | existing file, new method | pending |
| 17-02-01 | 02 | 2 | D-14/D-15/D-17/D-18/D-19 | Minor code hygiene + comment clarity | compile-only | `mvn -pl core compile` + `mvn -Dtest=SampleByFillTest#testTimestampFillNullAndValue test -pl core` | existing files | pending |
| 17-02-02 | 02 | 2 | D-16 | IntList/BitSet refactor with no measurable cost | full SampleBy* suite + JMH benchmark gate | `mvn -Dtest=SampleByFillTest,SampleByTest,SampleByNanoTimestampTest test -pl core` + JMH harness | W0 new `SampleByFillKeyedResetBenchmark.java` | pending |
| 17-02-03 | 02 | 2 | D-20 | FillRecord dispatch covers 4 branches across 30 getters | property test (synthetic) | `mvn -Dtest=FillRecordDispatchTest test -pl core` | W0 new `FillRecordDispatchTest.java` | pending |
| 17-03-01 | 03 | 2 | D-12/D-13 | Predicate pushed into inner Async Group By `filter:` slot | unit + EXPLAIN plan | `mvn -Dtest=SampleByFillTest#testFillNullPushdownEliminatesFilteredKeyFills test -pl core` | existing file, new method + comments in SampleByTest/SampleByNanoTimestampTest | pending |
| 17-03-02 | 03 | 2 | D-21/D-22/D-23 | DST spring-forward + single-row keyed FROM/TO + exact-position rejection | unit | `mvn -Dtest=SampleByFillTest#testFillWithOffsetAndTimezoneAcrossDstSpringForward,SampleByFillTest#testFillKeyedSingleRowFromTo,SampleByFillTest#testFillPrevRejectNoArg test -pl core` | existing file, new methods | pending |
| 17-04-01 | 04 | 3 | D-03/D-05/D-06/D-11/D-24/D-28 | PR #6946 body + title reflect shipped state; D-02/D-04 ledgered as no-ops | shell verification | `gh pr view 6946 --json body,title` + grep assertions | external artefact | pending |

*Status: pending / green / red / flaky*

---

## Wave 0 Requirements

- [ ] `core/src/test/java/io/questdb/test/griffin/engine/groupby/FillRecordDispatchTest.java` - new file for m4 D-20. Scope: 30 typed getters x 4 dispatch branches = ~120 assertions with a synthetic FillRecord.
- [ ] `benchmarks/src/main/java/org/questdb/SampleByFillKeyedResetBenchmark.java` - new JMH harness for m3 D-16 per-bucket reset comparison.

*(No framework install needed - JUnit 4 and JMH both already in use.)*

---

## Manual-Only Verifications

| Behavior | Decision | Why Manual | Test Instructions |
|----------|----------|------------|-------------------|
| PR #6946 body contains the rewritten K x B bullet | D-03 | PR body is an external GitHub artefact; no CI path to assert on it | `gh pr view 6946 --json body --jq '.body' \| grep -c "Unrelated to the cancellation"` returns 1 |
| PR #6946 body contains the M1 pre-1970 row | D-11 | same | `gh pr view 6946 --json body --jq '.body' \| grep -c "pre-1970"` returns >= 1 |
| PR #6946 body does NOT contain the stale M3.4 sentences | D-05 | same | `gh pr view 6946 --json body --jq '.body' \| grep -c "offset applied only when"` returns 0 |
| PR #6946 body does NOT contain test-count lines | D-06 | same | `gh pr view 6946 --json body --jq '.body' \| grep -cE "SampleByFillTest: [0-9]+ tests?\|SampleByNanoTimestampTest: [0-9]+ tests?"` returns 0 |
| PR #6946 title is the renamed form | D-24 | GitHub artefact | `gh pr view 6946 --json title --jq '.title'` returns `feat(sql): add cross-column FILL(PREV) and move SAMPLE BY FILL onto fast path` |
| `/review-pr 6946` re-run shows all items closed or closed-under-D-01 | ROADMAP SC#8 | Integrates with Claude Code review workflow; runs as a slash command | Run `/review-pr 6946` in a fresh session; confirm M1/M2/M3.2-bullet/M3.4-sentences/M3.5/m9 closed; M3.1/M3.3/M-new/M-unit closed-under-D-01 |
| PR body contains only ASCII | D-03/D-05/D-11 generalized | GitHub rendering / CLAUDE.md extended ASCII rule | `gh pr view 6946 --json body --jq '.body' \| LC_ALL=C grep -P '[^\x00-\x7F]' \| wc -l` returns 0 |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies (PR-body tasks in Plan 04 are inherently manual per the map above)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references (FillRecordDispatchTest, SampleByFillKeyedResetBenchmark)
- [x] No watch-mode flags
- [x] Feedback latency < 60s (quick run)
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved 2026-04-22 (orchestrator-authored from RESEARCH.md Validation Architecture section)
