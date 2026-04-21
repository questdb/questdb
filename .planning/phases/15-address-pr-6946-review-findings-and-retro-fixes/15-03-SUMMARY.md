---
phase: 15-address-pr-6946-review-findings-and-retro-fixes
plan: 03
subsystem: test-only
tags: [sample-by, fill, optimizer, test-upgrade, m-7]

requires:
  - phase: 14-fix-issues-from-moderate-list-for-m5-and-m6-just-mention-in-
    provides: "Phase 14 D-15 assertQueryNoLeakCheck(..., ts, false, false) contract for SAMPLE BY FILL factories"
  - phase: 15-address-pr-6946-review-findings-and-retro-fixes
    plan: 01
    provides: "Codegen-cluster clean baseline"
  - phase: 15-address-pr-6946-review-findings-and-retro-fixes
    plan: 02
    provides: "Cursor-cluster clean baseline"
provides:
  - "Output-level regression lock on testSampleByFromToParallelSampleByRewriteWithKeys for the two literal-key variants (keyed, keyed with offset)"
  - "Documentation inside the test body for the pre-existing FUNCTION-typed key + FILL defensive guard trip"
affects: [plan-04-retro-doc]

tech-stack:
  added: []
  patterns:
    - "One-time printSql probe + frozen multiline expected-output literal for deterministic DDL shapes (extends Phase 14 D-16 ghost-test rename precedent)"
    - "Partial M-7 upgrade: two of four variants bounded + output-asserted; remaining two stay compile-only with explanatory comment anchored to a named defect"

key-files:
  created: []
  modified:
    - "core/src/test/java/io/questdb/test/griffin/SqlOptimiserTest.java"

key-decisions:
  - "Probe captured via a temporary probeM7Outputs test method running printSql on each of the four bounded candidate queries; Q1 and Q2 succeeded and their outputs were frozen; Q3 and Q4 (concat('1', s) computed-key expressions) failed with CairoException.critical defensive guard regardless of TO, FROM, or cardinality-cap placement, so they stay compile-only per Rule 4 (architectural issue outside Plan 03's test-only boundary)"
  - "Cardinality cap WHERE x <= 4 chosen over the default full cardinality (480) or WHERE x <= 2 to match the plan's recommended shape and produce a 36-row assertion (9 buckets between 2017-12-20 and 2018-01-29 x 4 keys) - readable yet thorough, and matches the RESEARCH.md M-7 recommended shape at section 7 and the explicit example in RESEARCH.md 'M-7 recommended test upgrade shape'"
  - "TO terminator '2018-01-31' matches the non-keyed parallel reference at :4918 and the existing testSampleByFromToParallelSampleByRewriteWithUnion fixture at :4957-5034 - single shared anchor across the file's FROM-TO test suite"
  - "Kept the existing plan-text assertions at :5013-5053 byte-identical per plan acceptance criteria (non-keyed shouldSucceedParallel + shouldSucceedWithOffset); no plan-text assertions added for the bounded keyed variants per RESEARCH Open Question 3 recommendation (full data assertion per D-11, no plan-text scope expansion)"

patterns-established:
  - "probe-and-freeze output capture for deterministic DDL (FROM_TO_DDL uses timestamp_sequence + long_sequence, hence the outputs are stable across runs and platforms)"
  - "Partial-upgrade-with-deferred-work pattern: when a test upgrade surfaces a pre-existing defect, keep the pre-upgrade coverage for affected variants and document the deferred defect inline"

requirements-completed: [COR-01, COR-04]

duration: 20min
completed: 2026-04-21
---

# Phase 15 Plan 03: M-7 test upgrade Summary

**testSampleByFromToParallelSampleByRewriteWithKeys now pins bounded-output correctness for the two literal-key FROM-TO variants via assertQueryNoLeakCheck on 36-row frozen tables; two computed-key variants stay compile-only with an inline comment documenting the pre-existing SampleByFillCursor defensive-guard trip that blocks their upgrade.**

## Performance

- **Duration:** 20 min
- **Started:** 2026-04-21T14:45:38Z
- **Completed:** 2026-04-21T14:56:25Z
- **Tasks:** 1 (test body rewrite with probe-and-freeze)
- **Files modified:** 1

## Accomplishments

- Replaced two of the four compile-only `select(...).close()` calls (keyed, keyed-with-offset) with `assertQueryNoLeakCheck(expected, query, "ts", false, false)` over bounded TO-specified variants. Each bounded variant adds `where x <= 4` to cap key cardinality and `to '2018-01-31'` to bound the grid at the same terminator used by the existing `testSampleByFromToParallelSampleByRewriteWithUnion` fixture at `:4957-5034`.
- Froze the expected-output multiline literals via a one-time probe in a temporary `probeM7Outputs` test method that ran `printSql` on each of the four candidate bounded queries. Q1 and Q2 produced 36-row tables (9 buckets from 2017-12-20 to 2018-01-29 at 5-day stride, 4 keys) with a tidy staircase of `null` fill rows and `1.0`..`4.0` avg values in the data-bearing bucket 2017-12-30. The probe method was removed before commit.
- Kept the two computed-key variants (`concat('1', s)` projection) as compile-only with an inline comment anchored to the specific defect they trip: the `CairoException.critical` defensive guard at `SampleByFillCursor.hasNext():486` fires as soon as iteration starts, regardless of TO, FROM, or cap shape. This is a pre-existing fast-path defect orthogonal to Plan 03's test-only scope.
- Updated the narrative comment at the head of the method to record why the test originally compiled-only and what Plan 03 changed, preserving the historical rationale for future readers.

## Task Commits

Plan 03 lands as a single atomic commit:

1. **Assert output of keyed FROM-TO SAMPLE BY test** - `18d4a91e85`
   - SqlOptimiserTest.java `testSampleByFromToParallelSampleByRewriteWithKeys`: two bounded-variant `assertQueryNoLeakCheck` blocks at `:4976-4979`, two compile-only `select(...).close()` calls at `:4995-4996` with explanatory comment at `:4981-4990`, updated narrative comment at `:4882-4887`. Non-keyed assertions below (formerly `:4918-4953`, now shifted to `:5013-5053` after insertion) byte-identical.

## Files Created/Modified

- `core/src/test/java/io/questdb/test/griffin/SqlOptimiserTest.java` - 107 additions, 12 deletions. Four compile-only calls replaced by two `assertQueryNoLeakCheck` blocks + two doc-commented compile-only calls. Two frozen multiline expected-output strings (~36 lines each) added inline.

## Decisions Made

- **Probe method shape.** Added a temporary `@Test probeM7Outputs` right above the target test, used `printSql(q)` + `System.err.println(sink)` with BEGIN/END sentinel lines per query, ran via `mvn -pl core -Dtest=SqlOptimiserTest#probeM7Outputs test`, captured Q1 and Q2 output from the interleaved stderr stream, and deleted the method before commit. The BEGIN/END sentinels were essential because stdout/stderr interleaved with QuestDB's LOG output during `printSql`.
- **Cardinality cap x <= 4.** Matches RESEARCH.md recommendation and balances coverage vs. readability. With 9 buckets and 4 keys, 36 rows per variant is tractable and asserts both data-row fill (`2017-12-30` bucket: `1.0`..`4.0`) and leading/trailing null fill shape. Alternatives (x <= 2 for compactness, x <= 10 for more data-row samples) were evaluated and rejected - `x <= 4` is the sweet spot. No iteration was needed (probe ran once, outputs used directly).
- **Computed-key variants stay compile-only.** The probe revealed that `concat('1', s)` computed-key projections trip `SampleByFillCursor.hasNext()`'s defensive guard at `:486` the moment iteration starts, emitting `CairoException.critical "sample by fill: data row timestamp ... precedes next bucket"`. I tested multiple FROM/TO combinations: `FROM '2018-01-01' TO '2018-01-31'` (fails), `TO '2018-01-31'` bare (fails), no FROM no TO bare-bounds (fails), `FROM '2017-12-20' TO '2018-02-28'` wider-bounds (fails). This is not a Plan 03 bug; it is a pre-existing fast-path defect for FUNCTION-typed projections that the original compile-only test shape hid. Plan 03 is explicitly test-only per CONTEXT D-11, so I applied **Rule 4 (architectural)**: document the trip inline and keep the variants as compile-only. The alternative (upgrade to `assertException` with the guard error) would pin broken behavior as "expected", which is worse than preserving compile-only coverage.
- **Commit title.** `Assert output of keyed FROM-TO SAMPLE BY test` - 46 chars, fits under the 50-char CLAUDE.md cap with headroom. The body follows the active-voice "component-as-subject" convention per CLAUDE.md PR guidelines.

## Deviations from Plan

### Rule 4 deviation - Computed-key variants

**1. [Rule 4 - Architectural] shouldSucceedKeyedExpr and shouldSucceedKeyedExprWithOffset remain compile-only**

- **Found during:** Probe (Task 1)
- **Issue:** The plan prescribed upgrading all four variants to bounded `assertQueryNoLeakCheck` with `to '2018-01-31'`. Probing the two computed-key variants surfaced a pre-existing defect in `SampleByFillCursor.hasNext()`: the defensive guard at line 486 (`"sample by fill: data row timestamp ... precedes next bucket"`) fires on iteration regardless of FROM/TO/cap placement. The guard is there to catch upstream contract violations (per CONTEXT `<deferred> M-6 missing test`); the fact that computed-key projections trip it indicates a real bucket-grid-drift bug for FUNCTION-typed keys on the keyed FROM-TO fast path.
- **Why this is Rule 4:** The fix requires codegen changes in the SAMPLE BY rewrite for FUNCTION-typed projection columns - specifically, how the fast path computes bucket boundaries when the projection pipeline includes a computed-key expression. This is architectural work (new codegen logic, not a one-line bug fix) and is explicitly out of Plan 03's test-only scope. CONTEXT defers M-6 (the guard's own missing test) to a post-merge phase; this discovery broadens that deferral to "plus fix the guard's underlying trigger for FUNCTION-typed keys".
- **Fix applied:** Preserved the two computed-key variants as compile-only (same as before) and added an inline comment at `:4981-4990` explaining the trip, the guard location, and the deferred status. The narrative comment at the method head records that only the literal-key variants got bounded output assertions.
- **Files modified:** core/src/test/java/io/questdb/test/griffin/SqlOptimiserTest.java (comment-only vs. the plan's full bounded-variant upgrade for these two variants)
- **Commit:** 18d4a91e85

Otherwise the plan executed as written: Q1 and Q2 got bounded TO + cardinality cap + `assertQueryNoLeakCheck` exactly as prescribed, with `(false, false)` matching Phase 14 D-15 for the SAMPLE BY FILL factory contract.

## Issues Encountered

- **Probe Q3/Q4 failures.** The probe of the computed-key variants returned `CairoException.critical` from `SampleByFillCursor.hasNext()`. Diagnosis above under Deviations. The four fallback probes (different FROM/TO shapes) all returned the same guard trip, confirming the failure is not sensitive to TO/FROM placement.
- **Workspace had pre-existing unrelated edits** (`SqlCodeGenerator.java` and `SqlOptimiser.java` had dead-code removals from a previous session). These are CONTEXT's deferred housekeeping items and explicitly out of Plan 03 scope, so I staged only the test file for this plan's commit.

## Verification Matrix (per VALIDATION.md task 15-03-01)

| Task ID  | Test                                                                       | Result         |
| -------- | -------------------------------------------------------------------------- | -------------- |
| 15-03-01 | SqlOptimiserTest#testSampleByFromToParallelSampleByRewriteWithKeys         | PASS           |
| Full     | `mvn -pl core -Dtest=SqlOptimiserTest test`                                 | PASS (172/172) |
| Adjacent | `mvn -pl core -Dtest='SampleByFillTest,SampleByTest' test`                  | PASS (418/418) |

## User Setup Required

None - pure test-only change. No external service configuration.

## Threat Flags

| Flag | File | Description |
|------|------|-------------|
| threat_flag: latent-bug | core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java | SampleByFillCursor.hasNext():486 defensive guard trips on computed-key (FUNCTION-typed projection like `concat('1', s)`) SAMPLE BY FILL queries regardless of FROM/TO placement. Surfaces only when iteration occurs (the original compile-only test shape hid it). Indicates bucket-grid computation drift between the sampler and the fast-path cursor for FUNCTION-typed projections. Out of Plan 03 scope; should be picked up as a successor to CONTEXT M-6 "missing test for the defensive guard". Two compile-only test variants (shouldSucceedKeyedExpr, shouldSucceedKeyedExprWithOffset) preserve the pre-Phase-15 coverage envelope until this is fixed. |

## Next Phase Readiness

- Plan 04 (Retro-doc) unblocked - paper trail only, no code dependency on Plan 03.

## Handoff to Plan 04

Plan 03 touches only SqlOptimiserTest.java; Plan 04 writes no code (retro-doc commits). No handoff artifacts needed.

## Self-Check: PASSED

- Commit `18d4a91e85` exists (verified via `git log --oneline -3`).
- `core/src/test/java/io/questdb/test/griffin/SqlOptimiserTest.java` contains:
  - 0 matches for `select\(shouldSucceedKeyed\)\.close\(\)` (verified via grep).
  - 0 matches for `select\(shouldSucceedKeyedWithOffset\)\.close\(\)` (verified via grep).
  - 0 matches for `FROZEN_OUTPUT_FROM_PROBE` (verified via grep).
  - 0 matches for `probeM7Outputs` (probe method removed; verified via grep).
  - 2 matches for `where x <= 4` (at :4889 and :4893, only inside this method body; verified via grep).
  - 2 matches for `assertQueryNoLeakCheck(shouldSucceedKeyed` prefix (my 2 new calls).
  - 1 match for `printSql` in the file (at :4259 inside an unrelated pre-existing test; 0 inside `testSampleByFromToParallelSampleByRewriteWithKeys`).
  - 0 new banner comments (`// ===` / `// ---`).
- `core/src/test/java/io/questdb/test/griffin/SqlOptimiserTest.java` preserves the non-keyed plan-text and output assertions at the end of the method body unchanged (verified via Read at :5013-5053).
- `mvn -pl core -Dtest=SqlOptimiserTest#testSampleByFromToParallelSampleByRewriteWithKeys test` exits 0 (Tests run: 1, Failures: 0, Errors: 0, Skipped: 0).
- `mvn -pl core -Dtest=SqlOptimiserTest test` exits 0 (Tests run: 172, Failures: 0, Errors: 0, Skipped: 0).
- `mvn -pl core -Dtest='SampleByFillTest,SampleByTest' test` exits 0 (Tests run: 418, Failures: 0, Errors: 0, Skipped: 0).

---
*Phase: 15-address-pr-6946-review-findings-and-retro-fixes*
*Completed: 2026-04-21*
