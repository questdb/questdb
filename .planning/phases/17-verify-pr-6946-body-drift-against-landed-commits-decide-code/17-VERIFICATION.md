---
phase: 17-verify-pr-6946-body-drift-against-landed-commits-decide-code
verified: 2026-04-22T17:36:22Z
status: human_needed
score: 8/8 ROADMAP Success Criteria verified (SC#8 requires human execution)
overrides_applied: 2
overrides:
  - must_have: "Plan 01 Task 1 `testFillKeyedPass1RespectsCircuitBreaker` regression test"
    reason: "Differential test structurally infeasible: upstream `SortedRecordCursor.buildChain()` -> `AsyncGroupByRecordCursor.buildMap():237` polls the cancellation CB before the :611 poll executes in every reachable base-cursor-materialized shape. Under both fixed and reverted production code, the test would pass identically, hiding the coverage gap. Production CB poll IS landed at :611 matching master cursor-path witnesses; master-parity + defense-in-depth is the justification. User explicitly chose Option B at a documented checkpoint."
    accepted_by: "sergey@questdb.com (user checkpoint decision during Plan 01 execution)"
    accepted_at: "2026-04-22"
  - must_have: "Plan 03 m8c error-message assertion string 'too few arguments for 'PREV' [found=0,expected=1]'"
    reason: "Research prediction error. Actual production rejection message is 'PREV argument must be a single column name' at position 43 (= sql.indexOf('PREV(')), thrown from `SqlCodeGenerator.generateFill`'s grammar validation layer which fires before ExpressionParser's arity-check layer. Test uses the real production string; intent (exact-substring + exact-position replacing contains-any-of) is fully preserved per D-23."
    accepted_by: "sergey@questdb.com (via Plan 03 commit 5d1d12451c)"
    accepted_at: "2026-04-22"
gaps: []
human_verification:
  - test: "Run `/review-pr 6946` in Claude Code and verify all 11 Phase 17 findings resolve as closed or explicitly-deferred-under-D-01."
    expected: "M1 closed by Plan 04 Edit A (M1 row). M2 closed by Plan 03 Task 1. M3.1 closed-under-D-01 (regression recovery, no body row per D-02). M3.2 closed by Plan 04 Edit B (K x B bullet rewrite). M3.3 closed-under-D-01 (delivered by Phase 16 per D-04). M3.4 closed by Plan 04 Edits C+D. M3.5 closed by Plan 04 Edit E. M-new closed by Plan 01 Task 1 (production pass-1 CB poll at :611). M-unit closed by Plan 01 Task 2. m1-m9 all closed by Plans 02+03. If any finding surfaces as 'not closed', Phase 17 must be re-opened."
    why_human: "ROADMAP SC#8 mandates a /review-pr 6946 re-run as the phase-exit check. The review-pr skill requires parallel-agent spawning outside the verifier's single-agent scope; SUMMARY explicitly defers this to a post-commit manual step. Automated checks cannot substitute for a reviewer agent's semantic reading of the PR body + branch state."
---

# Phase 17: Address `/review-pr 6946` follow-ups Verification Report

**Phase Goal:** Close all 11 verified findings from the second `/review-pr 6946` pass (2 Moderate + 9 Minor) plus 1 during-discuss finding (M-new pass-1 CB gap) plus 1 surfaced-via-todo finding (M-unit TIMESTAMP/INTERVAL unit mismatch). Each finding resolves as a code fix, a new regression test, a PR-body edit, or an explicit no-op per D-01 regression-recovery principle. ROADMAP Success Criteria 1-8.

**Verified:** 2026-04-22T17:36:22Z
**Status:** human_needed (all automated checks pass; SC#8 explicitly requires human /review-pr 6946 re-run)
**Re-verification:** No -- initial verification.

## Goal Achievement

### Observable Truths (ROADMAP Success Criteria)

| # | Truth (ROADMAP SC) | Status | Evidence |
|---|--------------------|--------|----------|
| 1 | Each finding has a landed resolution (code / body / test / explicit no-op). | VERIFIED | 11 findings + M-new + M-unit: all mapped to code commits (Plans 01+02+03) or body edits (Plan 04) or explicit no-op under D-01 (M3.1 per D-02, M3.3 per D-04). See D-ID Coverage Matrix below. |
| 2 | M3 four-commit verdict: scope / landed / corner-cases / follow-up for `9df205bac5`, `c1deb9b14d`, `82865efbc0`, `289d43090a`. | VERIFIED | CONTEXT.md D-02 / D-03 / D-04 / D-05 document verdicts; RESEARCH.md provides per-commit evidence. `git log --oneline` confirms all four M3 commits are on the branch: `9df205bac5`, `c1deb9b14d` (prior-phase), `82865efbc0` (prior-phase), `289d43090a`. |
| 3 | Decision log per M3 sub-item splits gap into (a) body edits, (b) code fixes, (c) tests. | VERIFIED | CONTEXT.md D-IDs partition: D-02 (M3.1: code-only, no body / no test), D-03 (M3.2: bullet rewrite only), D-04 (M3.3: Phase 16 already closed), D-05 (M3.4: two sentence rewrites, no new test), D-06 (M3.5: drop count lines). |
| 4 | PR #6946 body updated with M1 row, rewritten sentences, bullet, dropped count lines. | VERIFIED (scope-collapsed per D-01 + D-05) | Live `gh pr view` returns: M1 row (1 hit), "Unrelated to the cancellation" K x B rewrite (1 hit), M3.4 sentence 1 "stripped TZ from timestamp_floor_utc" (1 hit), M3.4 sentence 2 "also when sub-day + TZ + FROM requires it" (1 hit), stale "offset applied only when" (0 hits), count lines (0 hits). ROADMAP listed 4 new rows but collapsed to 1 (M1 only) per D-01 regression-recovery principle + RESEARCH D-05 master-support verdict -- the three omitted rows are explicit no-ops documented in Plan 04's `deliberate_noops` / `key-decisions`. |
| 5 | All minor code-fix findings land: m1-m7. | VERIFIED | grep evidence: m1 inline `Misc.free(fillValues.getQuick(fillIdx))` at SqlCodeGenerator.java:3657 (1 hit); m2 alphabetical field block in SampleByFillCursor (visually confirmed); m3 IntList for outputColToKeyPos landed at :314 (BitSet for keyPresent reverted per benchmark verdict); m5 rationale comment at :427; m6 Record.getLong256(int, CharSink) contract comment at :1093; m7 `fillOffset != null` + `putAscii(" offset ")` at QueryModel.java:2326-2327. |
| 6 | All test findings land: M2, m4, m8a, m8b, m8c. | VERIFIED | M2 `testFillNullPushdownEliminatesFilteredKeyFills` (1 hit in SampleByFillTest, includes assertPlanNoLeakCheck); m4 `FillRecordDispatchTest.java` exists with 30 @Test methods; m8a `testFillWithOffsetAndTimezoneAcrossDstSpringForward` (1 hit); m8b `testFillKeyedSingleRowFromTo` (1 hit); m8c `testFillPrevRejectNoArg` tightened to exact-substring + position (assertion uses "PREV argument must be a single column name" per override). |
| 7 | PR title renamed per m9 (D-24). | VERIFIED | `gh pr view 6946 --json title --jq '.title'` returns exactly `feat(sql): add cross-column FILL(PREV) and move SAMPLE BY FILL onto fast path`. |
| 8 | `/review-pr 6946` re-run shows all items closed. | NEEDS HUMAN | Explicitly deferred to post-commit manual step per Plan 04 SUMMARY (Rule 4 deviation; recorded in `next_phase_readiness`). All 11 findings mapped to landed artifacts in code/tests/body; automated checks cannot substitute for the review-pr agent's semantic reading. |

**Score:** 8 / 8 ROADMAP Success Criteria verified (SC#8 requires human action to formally discharge; all automated preconditions satisfied).

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` | Pass-1 CB poll at :611 + m2 alphabetical block + m3 IntList + m5 / m6 comments | VERIFIED | `grep -c statefulThrowExceptionIfTripped` returns 3 (was 2; new poll at :611 as first statement inside `while (baseCursor.hasNext())` at :610). `IntList outputColToKeyPos` at :314. `boolean[] keyPresent` at :307 (benchmark-gated revert). m5 comment at :427. m6 comment at :1093. |
| `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | Widened `needsExactTypeMatch` for TIMESTAMP + INTERVAL; m1 slot-null inline form | VERIFIED | `targetTag == ColumnType.TIMESTAMP` at :3609; `targetTag == ColumnType.INTERVAL` at :3610. `fillValues.setQuick(fillIdx, Misc.free(fillValues.getQuick(fillIdx)))` at :3657. |
| `core/src/main/java/io/questdb/griffin/model/QueryModel.java` | m7 fillOffset emission symmetric with fillFrom/fillTo | VERIFIED | `if (fillOffset != null)` at :2326; `sink.putAscii(" offset ")` at :2327. |
| `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` | M-unit test + M2 pushdown + m8a + m8b + m8c tightened | VERIFIED | 127 @Test methods (was 123). `testFillPrevCrossColumnTimestampUnitMismatch`, `testFillNullPushdownEliminatesFilteredKeyFills`, `testFillWithOffsetAndTimezoneAcrossDstSpringForward`, `testFillKeyedSingleRowFromTo` present. `testFillPrevRejectNoArg` uses `assertExceptionNoLeakCheck` + exact substring "PREV argument must be a single column name" at position 43. `testFillKeyedPass1RespectsCircuitBreaker` ABSENT (user-approved drop; override applied). |
| `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java` | 2 D-13 cross-reference comments above testSampleByAlignToCalendarFillNullWithKey1/2 | VERIFIED | 303 @Test methods (unchanged). `grep -c "Predicate pushdown past SAMPLE BY"` returns 2. |
| `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java` | 2 nano-mirror D-13 comments (RESEARCH D-13 4-site correction) | VERIFIED | 279 @Test methods (delta +1 from pre-Phase-17 is owed to commit `289d43090a` -- Phase 17 itself added 0 tests here, only comments). `grep -c "Predicate pushdown past SAMPLE BY"` returns 2. |
| `core/src/test/java/io/questdb/test/griffin/engine/groupby/FillRecordDispatchTest.java` | New file, m4 property test, 30 @Test methods | VERIFIED | File exists; 30 @Test methods; all 35 typed-getter names covered (exceeds 30 threshold in Plan 02 acceptance). |
| `benchmarks/src/main/java/org/questdb/SampleByFillKeyedResetBenchmark.java` | JMH harness gating m3 refactor | VERIFIED | File exists; contains `@Benchmark` + `@Param({"10", "100", "1000", "10000"})` plus resetPrimitiveArrays / resetIntListSetAll / resetBitSet methods. |
| `.planning/todos/completed/2026-04-22-reject-cross-column-fill-prev-timestamp-unit-mismatch.md` | D-28 retired todo with completed_at + completed_in | VERIFIED | File exists at completed/ path; `completed_at: 2026-04-22` + `completed_in: 17-01-PLAN.md` present in YAML frontmatter. Pending-path file removed. |
| PR #6946 body (external) | M1 row + K x B bullet + 2 M3.4 sentences + no count lines + ASCII only | VERIFIED | All greps pass (see Truth #4 evidence); non-ASCII byte count via Python = 0. |
| PR #6946 title (external) | "feat(sql): add cross-column FILL(PREV) and move SAMPLE BY FILL onto fast path" | VERIFIED | `gh pr view 6946 --json title` returns exact match. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|----|--------|---------|
| SampleByFillRecordCursorFactory.initialize() pass-1 loop | SqlExecutionCircuitBreaker.statefulThrowExceptionIfTripped() | First-statement CB poll inside `while (baseCursor.hasNext())` body at :610-611 | WIRED | Verified: :611 poll is first statement inside :610 while body; matches master `SampleByFillPrevRecordCursor.java:171` verbatim. Defense-in-depth: upstream `AsyncGroupByRecordCursor.buildMap():237` also polls; production path is correct even though differential test is infeasible. |
| SqlCodeGenerator.generateFill per-column branch | Existing `!isTypeCompatible` throw path | Widened `needsExactTypeMatch` with TIMESTAMP + INTERVAL tags | WIRED | Verified: :3605-3610 predicate widening; :3614-3620 throw path unchanged. `testFillPrevCrossColumnTimestampUnitMismatch` green confirms rejection fires for TIMESTAMP_MICRO -> TIMESTAMP_NANO. |
| testFillNullPushdownEliminatesFilteredKeyFills | Inner Async JIT Group By `filter:` slot | `assertPlanNoLeakCheck` pins `filter: s='s2'` inside `Async JIT Group By` node | WIRED | Verified: `grep -c assertPlanNoLeakCheck` increased by 1; test body asserts both data (per-key-domain cartesian) and plan (filter inside inner Group By). |
| PR body K x B bullet | Plan 01 pass-1 CB fix + Phase 15 Plan 02 emission CB fixes | Parenthetical "Unrelated to the cancellation SqlExecutionCircuitBreaker, which is honoured by the fill cursor." | WIRED | Body contains exact disambiguation. Three CB polls in factory (hasNext :382, emitNextFillRow :537, initialize pass-1 :611) match the "honoured by the fill cursor" claim. |
| PR body M3.4 sentences | Commit `289d43090a` widened `setFillOffset` | Sentences describe `sampleByTimezoneName == null || sub-day + TZ + FROM` predicate | WIRED | Both replacement sentences present in body; stale "only when" wording absent (0 hits). |
| Completed todo | Plan 01 Task 2 commit `889a4676b9` | `completed_in: 17-01-PLAN.md` frontmatter | WIRED | Confirmed via grep. |
| D-13 comments (4 sites) | PR body "Predicate pushdown past SAMPLE BY" Trade-off | Exact substring cross-reference | WIRED | SampleByTest and SampleByNanoTimestampTest both contain 2 hits each (4 total). |

### Data-Flow Trace (Level 4)

Not applicable -- Phase 17 produces (1) backend Java code that is invoked via SQL queries exercised by test suite, not user-rendered UI components; (2) external PR metadata artifacts. The test suite passing (739/739) is the canonical data-flow gate; the test counts and green status confirm real data flows through every new production change. No hollow-prop / disconnected-source pattern is possible at this layer.

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Pass-1 CB poll present at :611 | `grep -c statefulThrowExceptionIfTripped SampleByFillRecordCursorFactory.java` | 3 (was 2) | PASS |
| Widened predicate covers TIMESTAMP | `grep "targetTag == ColumnType.TIMESTAMP" SqlCodeGenerator.java` | 1 hit at :3609 | PASS |
| Widened predicate covers INTERVAL | `grep "targetTag == ColumnType.INTERVAL" SqlCodeGenerator.java` | 1 hit at :3610 | PASS |
| m1 slot-null inline form | `grep "Misc.free(fillValues.getQuick(fillIdx))" SqlCodeGenerator.java` | 1 hit | PASS |
| m7 fillOffset emission | `grep "putAscii(\" offset \")" QueryModel.java` | 1 hit | PASS |
| PR title correct | `gh pr view 6946 --json title --jq '.title'` | exact match | PASS |
| PR body M1 row | `grep "SAMPLE BY FILL on a table with pre-1970 timestamps"` | 1 | PASS |
| PR body K x B disambiguation | `grep "Unrelated to the cancellation"` | 1 | PASS |
| PR body M3.4 sentence 1 | `grep "stripped TZ from \`timestamp_floor_utc\`"` | 1 | PASS |
| PR body M3.4 sentence 2 | `grep "also when sub-day + TZ + FROM requires it"` | 1 | PASS |
| PR body stale wording removed | `grep "offset applied only when"` | 0 | PASS |
| PR body count lines removed | `grep -E "SampleByFillTest: [0-9]+ tests\|SampleByNanoTimestampTest: [0-9]+ tests"` | 0 | PASS |
| PR body ASCII only | Python non-ASCII byte count | 0 | PASS |
| Todo retired | `test -f .planning/todos/completed/... && test ! -f .planning/todos/pending/...` | both satisfied | PASS |
| Completed todo back-reference | `grep "completed_in: 17-01-PLAN.md"` | 1 | PASS |
| Test suite: SampleByFillTest | `mvn -Dtest=SampleByFillTest test -pl core` | 127 / 0 failures | PASS |
| Test suite: SampleByTest | `mvn -Dtest=SampleByTest test -pl core` | 303 / 0 failures | PASS |
| Test suite: SampleByNanoTimestampTest | `mvn -Dtest=SampleByNanoTimestampTest test -pl core` | 279 / 0 failures | PASS |
| Test suite: FillRecordDispatchTest | `mvn -Dtest=FillRecordDispatchTest test -pl core` | 30 / 0 failures | PASS |
| Test suite: ExplainPlanTest (plan-assertion infrastructure) | `mvn -Dtest=ExplainPlanTest test -pl core` | 522 passed, 0 failed, 2 skipped | PASS |
| All commits present | `git log --oneline` | 9 commits present: f05fa2eb25, 889a4676b9, 2a4070b851, 3dbbbde82d, 8838de6801, b2408afc9b, 5d1d12451c, 4529631666 + SUMMARY docs commits | PASS |

### D-ID Coverage Matrix (CONTEXT.md decisions D-01..D-28)

Every decision D-01..D-28 accounted for:

| D-ID | Concern | Addressed In | Status |
|------|---------|--------------|--------|
| D-01 | Regression-recovery-does-not-warrant-body-edit principle | Structural; shapes Plans 01-04 | ADDRESSED (process) |
| D-02 | M3.1 TIMESTAMP unit drift + alias guard no-op | Plan 04 frontmatter `deliberate_noops` | EXPLICIT NO-OP |
| D-03 | M3.2 K x B bullet rewrite | Plan 04 Edit B (body) | ADDRESSED |
| D-04 | M3.3 FUNCTION/OPERATION classifier no-op (Phase 16) | Plan 04 frontmatter `deliberate_noops` | EXPLICIT NO-OP |
| D-05 | M3.4 two stale sentences rewrite; optional row SKIPPED per RESEARCH master-support verdict | Plan 04 Edits C + D | ADDRESSED |
| D-06 | M3.5 drop test-count lines | Plan 04 Edit E | ADDRESSED |
| D-07 | M-new pass-1 CB gap confirmed | Plan 01 Task 1 (finding) | ADDRESSED |
| D-08 | Add pass-1 CB poll at :604 | Plan 01 Task 1, commit f05fa2eb25 (landed at :611) | ADDRESSED |
| D-09 | testFillKeyedPass1RespectsCircuitBreaker | Plan 01 Task 1 (test dropped per user Option B; override applied) | ADDRESSED (override) |
| D-10 | No PR body row for M-new | Plan 04 (no row added) | ADDRESSED |
| D-11 | M1 pre-1970 FILL row | Plan 04 Edit A | ADDRESSED |
| D-12 | M2 pushdown test + plan assertion | Plan 03 Task 1 (testFillNullPushdownEliminatesFilteredKeyFills), commit b2408afc9b | ADDRESSED |
| D-13 | 4 comment sites in SampleByTest + SampleByNanoTimestampTest | Plan 03 Task 1, commit b2408afc9b | ADDRESSED |
| D-14 | m1 slot-null fix | Plan 02 Task 1, commit 2a4070b851 | ADDRESSED |
| D-15 | m2 field ordering | Plan 02 Task 1, commit 2a4070b851 | ADDRESSED |
| D-16 | m3 IntList/BitSet refactor + benchmark gate | Plan 02 Task 2, commit 3dbbbde82d (IntList lands, BitSet reverts) | ADDRESSED |
| D-17 | m5 assert rationale comment | Plan 02 Task 1, commit 2a4070b851 (at :427) | ADDRESSED |
| D-18 | m6 getLong256 Record contract comment | Plan 02 Task 1, commit 2a4070b851 (at :1093) | ADDRESSED |
| D-19 | m7 fillOffset emission in toSink0 | Plan 02 Task 1, commit 2a4070b851 | ADDRESSED |
| D-20 | m4 FillRecord dispatch property test | Plan 02 Task 3, commit 8838de6801 (FillRecordDispatchTest.java, 30 @Test) | ADDRESSED |
| D-21 | m8a DST spring-forward | Plan 03 Task 2, commit 5d1d12451c | ADDRESSED |
| D-22 | m8b single-row keyed FROM/TO | Plan 03 Task 2, commit 5d1d12451c | ADDRESSED |
| D-23 | m8c tighten testFillPrevRejectNoArg | Plan 03 Task 2, commit 5d1d12451c (RESEARCH prediction corrected; override applied) | ADDRESSED (override) |
| D-24 | m9 PR title rename | Plan 04, `gh pr edit 6946 --title` | ADDRESSED |
| D-25 | Planner decides plan structure | Structural; Plans 01-04 materialize the Cluster A + Cluster B split | ADDRESSED (process) |
| D-26 | Widen needsExactTypeMatch TIMESTAMP + INTERVAL | Plan 01 Task 2, commit 889a4676b9 | ADDRESSED |
| D-27 | testFillPrevCrossColumnTimestampUnitMismatch | Plan 01 Task 2, commit 889a4676b9 (Variant A lands; Variant B dropped per DDL spike) | ADDRESSED |
| D-28 | Todo retirement | Plan 04, commit 4529631666 | ADDRESSED |

**Coverage summary:** 28 / 28 decisions accounted for. 2 explicit no-ops (D-02, D-04 per Plan 04 frontmatter). 3 process-structural decisions (D-01, D-25 and sub-clusters). 23 substantive deliveries (code / tests / body / title / todo). 2 deliberate deviations accepted via override (D-09 user-approved test drop; D-23 research-prediction correction).

### Requirements Coverage

| Requirement | Source Plan(s) | Description (from REQUIREMENTS.md) | Status | Evidence |
|-------------|---------------|------------------------------------|--------|----------|
| COR-02 | 17-01, 17-02 | Correctness: fast-path cursor emits correct values across edge cases | SATISFIED (strengthened) | M-unit rejection test (Plan 01 Task 2) + FillRecordDispatchTest (30 @Test across 4 branches) + m1 slot-null guard |
| COR-03 | 17-03 | Correctness: SAMPLE BY FILL regression behavior | SATISFIED (strengthened) | M2 pushdown test + 4 D-13 cross-ref comments |
| COR-04 | 17-01, 17-02, 17-03, 17-04 | Correctness: SAMPLE BY FILL implementation integrity | SATISFIED (strengthened) | Pass-1 CB poll + needsExactTypeMatch widening + m1/m5/m6/m7 + m8a/m8b/m8c |
| XPREV-01 | 17-01, 17-04 | Cross-column PREV support | SATISFIED (strengthened) | Unit-mismatch rejection + D-28 todo retirement |
| FILL-05 | 17-03 | FILL semantics correctness | SATISFIED (strengthened) | m8a (DST), m8b (single-row keyed FROM/TO) |
| KFTR-02 | 17-03 | Keyed fill transition semantics | SATISFIED (strengthened) | m8b hasKeyPrev false->true transition pinned |
| KFTR-04 | 17-03 | Keyed fill reject-no-arg grammar | SATISFIED (strengthened) | m8c tightened to exact-substring + position |

No new requirement IDs introduced (per CONTEXT phase charter). No orphaned requirements.

### Anti-Patterns Found

No anti-patterns detected in modified files.

- No `TODO` / `FIXME` / `placeholder` markers introduced in Phase 17 commits (verified by the plans' own grep-based acceptance criteria; no new stub patterns surfaced during verification).
- Test suite is 739 / 739 green; no `@Ignore` / `@Skip` added.
- All new comments are ASCII-only per CLAUDE.md (Plan 04 normalized 41 non-ASCII bytes in PR body to 0; no ASCII-violations in source code per spot checks).
- Commit hygiene: all 9 commits follow CLAUDE.md rules (no Conventional Commits prefixes on commit titles; <= 50 chars; long-form bodies).

### Human Verification Required

#### 1. Run `/review-pr 6946` re-run (ROADMAP SC#8)

**Test:** In Claude Code, run `/review-pr 6946`. Compare surfaced findings against Phase 17's closed list.

**Expected:** All 11 original findings (M1, M2, M3.1-5, m1-m9) plus M-new + M-unit resolve as:
- **M1** closed by Plan 04 Edit A (PR body M1 row).
- **M2** closed by Plan 03 Task 1 (testFillNullPushdownEliminatesFilteredKeyFills + 4 D-13 comments).
- **M3.1** closed-under-D-01 / D-02 (pure mid-term regression recovery; no body row, no test).
- **M3.2** closed by Plan 04 Edit B (K x B bullet rewrite disambiguating cancellation CB from memory CB).
- **M3.3** closed-under-D-01 / D-04 (delivered by Phase 16 Plan 01).
- **M3.4** closed by Plan 04 Edits C + D (two stale sentence rewrites).
- **M3.5** closed by Plan 04 Edit E (count lines dropped).
- **m1-m7** code-fix findings closed by Plan 02 Task 1 + Plan 02 Task 2 (m3 benchmark-gated partial).
- **m4** closed by Plan 02 Task 3 (FillRecordDispatchTest).
- **m8a/m8b/m8c** closed by Plan 03 Task 2.
- **m9** closed by Plan 04 (title renamed).
- **M-new** closed by Plan 01 Task 1 (production pass-1 CB poll landed; paired test dropped with user-approved override).
- **M-unit** closed by Plan 01 Task 2 (widened needsExactTypeMatch + Variant A test).

If `/review-pr 6946` surfaces any finding not in the above list, or flags any "still open" status, Phase 17 has NOT delivered ROADMAP SC#8 and must be re-opened.

**Why human:** The `/review-pr` skill requires parallel-agent spawning outside the verifier's single-agent scope. Plan 04 SUMMARY explicitly defers SC#8 to a post-commit manual step (Rule 4 deviation). Automated grep checks cannot substitute for a reviewer agent's semantic reading of the PR body + branch state.

### Gaps Summary

No gaps. All 11 ROADMAP findings + M-new + M-unit addressed across Plans 01-04 (28 / 28 D-IDs accounted for per coverage matrix). Two deliberate deviations accepted via override (D-09 user-approved test drop; D-23 research-prediction correction). Automated verification of ROADMAP SC#1-7 is complete. SC#8 requires a human-initiated `/review-pr 6946` re-run as the formal phase-exit check.

---

*Verified: 2026-04-22T17:36:22Z*
*Verifier: Claude (gsd-verifier)*

## Addendum -- 2026-04-23 follow-up

Re-running the full `SqlOptimiserTest` class during a follow-up review surfaced
7 failures in `testSampleByFromToBasicWhereOptimisation*`. Plan 05's
`deferred-items.md` had labelled these "pre-existing", but that diagnosis was
incorrect: the `git stash`-based confirmation only reverted Plan 05's edit, not
the full Phase 17 stack.

Bisection traced the regression to Plan 02 commit `2a4070b851` (m7 symmetry
fix in `QueryModel.toSink0()` -- now emits `offset '...'` when `fillOffset`
is non-null). The production behaviour was already correct via
`setFillOffset(sampleByOffset)` wired in by `6558835d0d` pre-Phase-17; only
the serialized model text widened. Pure test-expectation staleness, not a
runtime regression.

**Closed by follow-up commit** updating the 11 stale expected strings in
`SqlOptimiserTest.java:4106-4219` to include ` offset '10:00'` before
` stride 5d`. The timezone variant at `SqlOptimiserTest.java:5418` remains
untouched (m7 guard correctly suppresses emission when
`sampleByTimezoneName != null` without the sub-day-wrap + FROM combination).

`deferred-items.md` amended to reflect the correct disposition.
