---
phase: 17-verify-pr-6946-body-drift-against-landed-commits-decide-code
plan: 03
subsystem: sql
tags: [sample-by, fill-prev, fill-null, predicate-pushdown, dst, keyed-from-to, grammar-rejection, tests-only]

# Dependency graph
requires:
  - phase: 17-verify-pr-6946-body-drift-against-landed-commits-decide-code
    provides: "Plan 01 (pass-1 CB + cross-column unit rejection) + Plan 02 (minor code hygiene + dispatch property test) must land first so the test surface this plan pins is authoritative"
provides:
  - "testFillNullPushdownEliminatesFilteredKeyFills pins the per-key-domain cartesian contract introduced by predicate pushdown into the inner Async JIT Group By (both data and EXPLAIN plan assertions)"
  - "Four comment blocks (2 in SampleByTest + 2 in SampleByNanoTimestampTest) cross-reference the 'Predicate pushdown past SAMPLE BY' Trade-off bullet Plan 04 adds to the PR body"
  - "testFillWithOffsetAndTimezoneAcrossDstSpringForward companions the existing fall-back fixture for DST coverage across both transition directions"
  - "testFillKeyedSingleRowFromTo pins the hasKeyPrev() false->true transition in a single-row keyed fixture"
  - "testFillPrevRejectNoArg tightened from contains-any-of to exact-substring + exact-position assertion"
affects: ["17-04 PR body bullet wording ('Predicate pushdown past SAMPLE BY' Trade-off grep-matches the test comments)"]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "EXPLAIN plan assertion alongside data assertion to make the test self-documenting about the mechanism, not just the output"
    - "Probe-and-freeze per Phase 15 D-02 applied to both m8a (DST spring-forward expected output) and m8b (single-row keyed FROM/TO expected output)"
    - "Exact-substring + exact-position assertions (assertExceptionNoLeakCheck) replacing loose contains-any-of checks at grammar-rejection sites"
    - "Cross-file comment cross-reference via unique substring grep anchor ('Predicate pushdown past SAMPLE BY')"

key-files:
  created: []
  modified:
    - "core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java"
    - "core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java"
    - "core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java"

key-decisions:
  - "m8c exact-message deviation: actual PREV() rejection is 'PREV argument must be a single column name' at position 43 (sql.indexOf('PREV(')), not the 'too few arguments for PREV [found=0,expected=1]' predicted by RESEARCH.md D-23. Grammar rule in SqlCodeGenerator.generateFill fires before ExpressionParser's arity-check layer because PREV is classified as a fill-spec keyword whose argument shape is validated by generateFill. m8c intent preserved: exact substring + exact position replacing fuzzy contains-any-of."
  - "M2 pushdown EXPLAIN plan shape: inner Group By node is Async JIT Group By (not plain Async Group By) because the pushed filter's s='s2' comparison is JIT-compilable; the 'filter: s=s2' slot sits inside the inner node nested below outer Sample By Fill. Asserted verbatim via assertPlanNoLeakCheck."
  - "Nano-mirror decisions per test: M2 pushdown (EXPLAIN plan shape, unit-agnostic) -- skip. m8a DST (UTC grid alignment, unit-agnostic) -- skip; existing testFillWithOffsetAndTimezoneAcrossDst also has no nano mirror. m8b single-row keyed (unit-sensitive but mechanism is shared dispatch path in FillRecord, not a timestamp-driver specific invariant) -- skip. m8c grammar rejection (unit-agnostic) -- skip. D-13 comments land on SampleByNanoTimestampTest's Key1/Key2 directly because the nano mirrors of those tests already exist independently."
  - "Alphabetical placement confirmed: M2 test between testFillNullNonKeyedNoToClause and testFillNullSparseDataLargeRange; m8a after testFillWithOffsetAndTimezoneAcrossDst (fall-back companion); m8b between testFillKeyedRespectsCircuitBreaker and testFillKeyedUuid; m8c stays at existing position (tightened in place)."

patterns-established:
  - "Probe-and-freeze: run the test with placeholder expected output, capture actual via the test-framework's assertion diff, freeze as the expected string in the final commit (used for both m8a bucket alignment and m8b null-rendering)"
  - "EXPLAIN plan + data assertion pairing for semantic-change tests: the data assertion proves the output changed, the plan assertion proves the mechanism that drove the change"

requirements-completed: [COR-03, COR-04, FILL-05, KFTR-02, KFTR-04]

# Metrics
duration: ~35min
completed: 2026-04-22
---

# Phase 17 Plan 03: Test-only additions (M2 + m8a/b/c + D-13 comments) Summary

**Pins the predicate-pushdown per-key-domain cartesian contract via M2 pushdown test + EXPLAIN assertion, cross-references the semantic-change Trade-off from 4 annotated tests in SampleByTest/SampleByNanoTimestampTest, and closes three m8a/m8b/m8c test gaps (DST spring-forward, single-row keyed FROM/TO, exact-substring + position for FILL(PREV()) rejection).**

## Performance

- **Duration:** ~35 min
- **Started:** 2026-04-22T17:45Z (plan 03 execution begin after Plan 02 closure)
- **Completed:** 2026-04-22T17:09Z (full 709-test suite green)
- **Tasks:** 2
- **Files modified:** 3
- **Commits:** 2 (b2408afc9b + 5d1d12451c)

## Accomplishments

- Added `testFillNullPushdownEliminatesFilteredKeyFills` pinning the per-key-domain cartesian contract with both a data assertion (single row for s2 in its observed bucket, no NULL fills outside its range) and a multi-line `assertPlanNoLeakCheck` pinning `filter: s='s2'` inside the inner Async JIT Group By.
- Inserted four "Predicate pushdown past SAMPLE BY" cross-reference comments above `testSampleByAlignToCalendarFillNullWithKey1/2` in both `SampleByTest.java` and `SampleByNanoTimestampTest.java` (4 call sites total per RESEARCH.md D-13 correction).
- Added `testFillWithOffsetAndTimezoneAcrossDstSpringForward` companion to the existing fall-back fixture: Europe/Riga spring-forward on 2021-03-28 with `WITH OFFSET '00:30'`. Asserts 5-row bounded monotonic UTC sequence (pre-data, 3 NULL fills, post-data). Output frozen via probe-and-freeze.
- Added `testFillKeyedSingleRowFromTo` pinning the `hasKeyPrev()` false->true transition: one VARCHAR key row at 03:00 bracketed by FROM=00:00 TO=06:00, FILL(PREV). Asserts 3 leading NULL fills + real row + 2 trailing PREV fills, with the key 'A' carrying on every row. Output frozen via probe-and-freeze.
- Tightened `testFillPrevRejectNoArg` from a four-way `contains-any-of` check to `assertExceptionNoLeakCheck(sql, sql.indexOf("PREV("), "PREV argument must be a single column name")`. Deviation from RESEARCH.md D-23 predicted message documented inline and in the commit body.

## Task Commits

1. **Task 1: M2 pushdown test + 4 D-13 comments** - `b2408afc9b` (test + cross-ref)
2. **Task 2: m8a/m8b/m8c test gap closure** - `5d1d12451c` (test-only, 3 tests)

## Files Created/Modified

- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` - Added `testFillNullPushdownEliminatesFilteredKeyFills` (Task 1), `testFillKeyedSingleRowFromTo` and `testFillWithOffsetAndTimezoneAcrossDstSpringForward` (Task 2), tightened `testFillPrevRejectNoArg` (Task 2).
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java` - Added 3-line cross-reference comment above each of `testSampleByAlignToCalendarFillNullWithKey1` and `testSampleByAlignToCalendarFillNullWithKey2` (Task 1).
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java` - Same 3-line cross-reference comment above each of `testSampleByAlignToCalendarFillNullWithKey1` and `testSampleByAlignToCalendarFillNullWithKey2` (Task 1, D-13 4-call-site correction).

## Decisions Made

- **M2 EXPLAIN plan shape is Async JIT Group By, not plain Async Group By.** The inner Group By node receives an eagerly-JIT-compiled filter on the SYMBOL equality comparison, so the plan string reads `Async JIT Group By workers: 1` instead of `Async Group By workers: 1`. Pinned verbatim. If future optimizer work disables JIT for SYMBOL filters, this test will regress -- a reviewer should audit whether the filter is still pushed into the inner cartesian (regardless of JIT) before relaxing the assertion.
- **m8c actual error is "PREV argument must be a single column name" at position 43, not the parser-layer message RESEARCH.md D-23 predicted.** The grammar rule in `SqlCodeGenerator.generateFill` fires before ExpressionParser's arity-check layer because PREV is classified as a fill-spec keyword whose argument shape is validated by generateFill, not the expression parser. Test wording and comment updated to reflect actual behavior; m8c's intent (replace fuzzy contains-any-of with exact substring + exact position) is preserved.
- **Probe-and-freeze for m8a bucket alignment:** pre-transition UTC data at 00:00Z (= 02:00 EET) floors into UTC 23:30 bucket (previous day) because of the 30-minute offset; 5 rows total between pre-data UTC 23:30 and post-data UTC 03:30, with 3 NULL fills in between. No bucket at the non-existent local time 03:xx EET because the UTC grid is continuous.
- **Nano-mirror skipped for all four new tests** per the unit-sensitivity heuristic in the plan note: M2 (EXPLAIN plan shape) and m8c (grammar rejection wording) are unit-agnostic; m8a (UTC grid alignment during DST) uses timestamps that timestamp_floor_utc handles identically at micro and nano precision, mirroring the pattern of the existing fall-back fixture which also has no nano mirror; m8b (single-row keyed FROM/TO) exercises the shared FillRecord dispatch path whose behavior is not timestamp-driver specific. D-13 comments still land on nano mirrors directly because the two annotated tests (`testSampleByAlignToCalendarFillNullWithKey1/2`) already exist in `SampleByNanoTimestampTest`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Research bug] m8c exact error message differs from RESEARCH.md prediction**
- **Found during:** Task 2 first test run
- **Issue:** RESEARCH.md D-23 predicted `FILL(PREV())` would be rejected at the parser layer with `"too few arguments for 'PREV' [found=0,expected=1]"` thrown from `ExpressionParser.java:379-382`. Probe captured the actual error: `"PREV argument must be a single column name"` at position 43 (= `sql.indexOf("PREV(")`). Running the first draft of the tightened test (using the predicted message) failed with `'PREV argument must be a single column name' does not contain: too few arguments for 'PREV' [found=0,expected=1]`.
- **Fix:** Updated the exact-substring assertion to `"PREV argument must be a single column name"` (the actual grammar-rule message in `SqlCodeGenerator.generateFill`). The position (sql.indexOf("PREV(") = 43) matches both the predicted and actual throw sites, so no position change was needed. m8c's intent (replace fuzzy contains-any-of with exact substring + exact position) is fully preserved.
- **Files modified:** core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java (testFillPrevRejectNoArg body + explanatory comment)
- **Commit:** 5d1d12451c
- **Impact on success criteria:** Plan success criterion `grep "too few arguments for 'PREV' \[found=0,expected=1\]" core/.../SampleByFillTest.java returns >= 1` fails under the actual production behavior. Replaced by the equivalent invariant `grep "PREV argument must be a single column name"` returns >= 1 (the tightened test's exact-substring assertion). The spirit of the criterion (tight message + position assertion) is preserved; the letter changes to match reality.

---

**Total deviations:** 1 (Rule 1 research correction; test ships with correct message and deviation fully documented)

**Impact on plan:** None of the plan's 2-task structure or 2-commit landing sequence changed. The m8c tightening closes the test gap as originally intended; only the literal substring differs from research prediction.

## Probe-and-freeze outcomes

Per Phase 15 D-02 pattern, both m8a and m8b ran as probes first with placeholder expected strings, actual output captured from `assertQueryNoLeakCheck`'s assertion diff, then frozen:

- **m8a (DST spring-forward):** 5 rows at UTC :30 boundaries -- `2021-03-27T23:30:00.000000Z 1.0`, three NULL fills at 00:30, 01:30, 02:30 UTC, and `2021-03-28T03:30:00.000000Z 5.0`. UTC grid is continuous; no missing / duplicate bucket at the DST jump instant.
- **m8b (single-row keyed FROM/TO):** 6 rows all carrying key 'A' -- 3 leading NULL value rows at 00:00/01:00/02:00, real row 42.0 at 03:00, 2 trailing PREV rows 42.0 at 04:00/05:00. VARCHAR key column carries through every row including leading-null rows (FILL_KEY dispatch); DOUBLE null renders as literal string `null` (QuestDB convention).

## Test-count Delta

| Test file | Pre-plan | Post-plan | Delta | Net additions |
|---|---|---|---|---|
| `SampleByFillTest.java` | 124 | 127 | +3 | M2 pushdown, m8a spring-forward, m8b single-row keyed (m8c tightened in place) |
| `SampleByTest.java` | 303 | 303 | 0 | D-13 comments on existing tests only |
| `SampleByNanoTimestampTest.java` | 279 | 279 | 0 | D-13 comments on existing tests only |

Plan 04 D-06 drops explicit test-count lines from the PR body, so these counts are retro-only.

## Verification

`mvn -Dtest=SampleByFillTest,SampleByTest,SampleByNanoTimestampTest test -pl core` reports:
- SampleByNanoTimestampTest: `Tests run: 279, Failures: 0, Errors: 0, Skipped: 0`
- SampleByTest: `Tests run: 303, Failures: 0, Errors: 0, Skipped: 0`
- SampleByFillTest: `Tests run: 127, Failures: 0, Errors: 0, Skipped: 0`
- Combined: `Tests run: 709, Failures: 0, Errors: 0, Skipped: 0, BUILD SUCCESS`

## Next Phase Readiness

- Plan 17-04 (PR body + title edits) unblocked. The "Predicate pushdown past SAMPLE BY" Trade-off bullet Plan 04 adds to the PR body should use the exact substring "Predicate pushdown past SAMPLE BY" so `grep` matches both the bullet and the 4 comment blocks added here.
- Plan 17-04 may want to note the m8c exact error wording deviation in the PR body Implementation-notes section if the reviewer cares about per-test wording accuracy; otherwise the deviation is self-documenting via the test's own inline comment.

## Self-Check: PASSED

Verified on 2026-04-22T17:09Z:
- `grep -c "testFillNullPushdownEliminatesFilteredKeyFills" core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` returns 1 (>= 1, PASS).
- `grep -c "testFillWithOffsetAndTimezoneAcrossDstSpringForward" core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` returns 1 (>= 1, PASS).
- `grep -c "testFillKeyedSingleRowFromTo" core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` returns 1 (>= 1, PASS).
- `grep -A 20 "public void testFillPrevRejectNoArg" core/.../SampleByFillTest.java | grep -c assertExceptionNoLeakCheck` returns 1 (tightened; loose contains-any-of block gone).
- `grep -c "Predicate pushdown past SAMPLE BY" core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java` returns 2 (D-13 comments on 2 call sites, PASS).
- `grep -c "Predicate pushdown past SAMPLE BY" core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java` returns 2 (D-13 comments on 2 call sites, PASS).
- `grep -c "PREV argument must be a single column name" core/.../SampleByFillTest.java` returns 8 (m8c tight message + 7 sibling tests; the test's own exact-substring assertion is present).
- Commits `b2408afc9b` and `5d1d12451c` both exist on branch `sm_fill_prev_fast_path` (`git log --oneline -5` confirms).
- Full suite `mvn -Dtest=SampleByFillTest,SampleByTest,SampleByNanoTimestampTest test -pl core` reports 709 tests, 0 failures.

**Deviation from success criterion:** `grep "too few arguments for 'PREV'"` returns 0 (research predicted the wrong error message; test uses the actual production message). Documented in `## Deviations from Plan`.

---
*Phase: 17-verify-pr-6946-body-drift-against-landed-commits-decide-code*
*Completed: 2026-04-22*
