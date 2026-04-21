---
phase: 16-fix-multi-key-fill-prev-with-inline-function-grouping-keys
verified: 2026-04-21T23:36:00Z
status: passed
score: 6/6 must-haves verified
overrides_applied: 0
---

# Phase 16: Fix multi-key FILL(PREV) with inline FUNCTION grouping keys — Verification Report

**Phase Goal:** Close the latent correctness gap in `SqlCodeGenerator.generateFill`'s classifier where non-aggregate FUNCTION/OPERATION grouping keys (`interval()`, `concat()`, `cast()`, `a || b`) slip into the aggregate arm and get dispatched as FILL_PREV_SELF instead of FILL_KEY, silently dropping cartesian fill rows in multi-key SAMPLE BY ... FILL(PREV).

**Verified:** 2026-04-21T23:36:00Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths (ROADMAP Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Multi-key `interval(lo, hi)` FILL(PREV) with 2 distinct keys at different buckets produces full cartesian output (6 rows, 2 keys × 3 buckets). | VERIFIED | `testFillPrevIntervalMultiKey` at SampleByFillTest.java:1815; expected TSV block (:1835-1841) contains exactly 6 data rows covering both interval keys at all three buckets (00:00, 01:00, 02:00). Test passes (Tests run: 1, Failures: 0). |
| 2 | Same correctness for `concat(a, b)` and `cast(x AS STRING)` as inline grouping keys. | VERIFIED | `testFillPrevConcatMultiKey` at :1255 (FUNCTION form), `testFillPrevCastMultiKey` at :1229; both have 6-row cartesian TSVs and pass. |
| 3 | Regression tests in `SampleByFillTest` pin the multi-key cartesian contract — interval, concat, cast variants. | VERIFIED | 5 tests present and alphabetically placed per CLAUDE.md ordering: `testFillNullCastMultiKey` (:467), `testFillPrevCastMultiKey` (:1229), `testFillPrevConcatMultiKey` (:1255), `testFillPrevConcatOperatorMultiKey` (:1281), `testFillPrevIntervalMultiKey` (:1815). Each uses `assertQueryNoLeakCheck(..., "ts", false, false)` per Phase 14 D-15. All pass. |
| 4 | `SampleByFillTest.testFillPrevInterval` single-key fixture continues to pass unchanged. | VERIFIED | Test still present at SampleByFillTest.java:1783 (shifted from :1678 due to new tests inserted above). `git show 82865efbc0 -- SampleByFillTest.java \| grep -cE "^-[^-]"` returns **0 deletions** — the file diff is 140 insertions only. testFillPrevInterval body bit-identical. Test passes under `-ea` (Tests run: 1, Failures: 0, Time elapsed: 2.951 s). |
| 5 | Defensive assertion in `generateFill` lands without firing, locking the canonicalization invariant. | VERIFIED | D-05 assertion at SqlCodeGenerator.java:3433-3435 present verbatim: `assert ast.type == ExpressionNode.FUNCTION && functionParser.getFunctionFactoryCache().isGroupBy(ast.token) : "generateFill aggregate arm: expected aggregate FUNCTION, got type=" + ast.type + " token=" + ast.token;`. Did not fire across 1395 cross-suite test runs (0 `AssertionError` in surefire reports). |
| 6 | All existing SampleByFillTest / SampleByTest / SampleByNanoTimestampTest / SqlOptimiserTest / ExplainPlanTest tests still pass. | VERIFIED | Cross-suite run: SampleByFillTest=120/120, SampleByTest=303/303, SampleByNanoTimestampTest=278/278, ExplainPlanTest=522/522 (2 pre-existing skips), SqlOptimiserTest=172/172. **Total: 1395 pass, 0 failures, 0 errors, 2 skipped (pre-existing in ExplainPlanTest).** Matches SUMMARY.md claim. |

**Score:** 6/6 truths verified.

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | Classifier widened with third `continue` branch (D-02) + aggregate-arm `-ea` assertion (D-05) | VERIFIED | D-02 predicate at :3425-3431 matches CONTEXT spec verbatim: `(ast.type == ExpressionNode.FUNCTION \|\| ast.type == ExpressionNode.OPERATION) && !functionParser.getFunctionFactoryCache().isGroupBy(ast.token)` followed by the alias `-ea` assert with ASCII-only message. D-05 aggregate-arm assert at :3433-3435 immediately before the existing `final CharSequence qcAlias = qc.getAlias();` block. Both messages plain ASCII. |
| `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` | 5 new test methods alphabetically placed | VERIFIED | `testFillNullCastMultiKey` at :467 (alphabetically among `testFillNull*` siblings), `testFillPrevCastMultiKey` at :1229 (precedes Concat* variants), `testFillPrevConcatMultiKey` at :1255, `testFillPrevConcatOperatorMultiKey` at :1281, `testFillPrevIntervalMultiKey` at :1815 (immediately after single-key `testFillPrevInterval` at :1783). All 5 use `assertMemoryLeak` + `assertQueryNoLeakCheck(..., "ts", false, false)`. Each TSV has exactly 6 data rows. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `generateFill` classifier (:3421-3432) | `factoryColToUserFillIdx[-1]` for function keys → `FILL_KEY` → `keyColIndices` → cursor `outputColToKeyPos` → FillRecord FILL_KEY dispatch | unchanged cursor-side wiring (D-06 verdict) | WIRED | End-to-end wiring verified by: (a) `testFillPrevIntervalMultiKey` producing all 6 cartesian rows (the key is correctly included in `keyColIndices`; otherwise the cursor would only emit 3 rows for the data-bearing keys); (b) commit 82865efbc0 modifies only 2 files, confirming no cursor-side adjustment; (c) all 303 SampleByTest + 278 SampleByNanoTimestampTest regressions still pass, confirming no disruption to the existing cursor dispatch. |
| 5 new regression tests | test suite execution | `mvn -pl core -Dtest=SampleByFillTest` | WIRED | All 120 SampleByFillTest tests pass (115 existing + 5 new). Quick run of the 6 target tests (5 new + single-key anchor): Tests run: 6, Failures: 0, Errors: 0. |

### Data-Flow Trace (Level 4)

Not applicable — phase modifies SQL query-engine codegen only, not a renderer of dynamic data. Level 4 data-flow is verified end-to-end through the test outputs (6-row cartesian TSVs pin the full data path from classifier → `factoryColToUserFillIdx` → `fillModes[]` → `keyColIndices` → `FillRecord` → `keysMapRecord`).

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| 5 new tests + single-key anchor pass under `-ea` | `mvn -pl core -Dtest='SampleByFillTest#testFillPrevInterval,SampleByFillTest#testFillPrevIntervalMultiKey,...`' test` | Tests run: 6, Failures: 0, Errors: 0 (6.667 s) | PASS |
| Full SampleByFillTest suite passes | `mvn -pl core -Dtest=SampleByFillTest test` | Tests run: 120, Failures: 0, Errors: 0 | PASS |
| ExplainPlanTest unchanged | `mvn -pl core -Dtest=ExplainPlanTest test` | Tests run: 522, Failures: 0, Errors: 0, Skipped: 2 (pre-existing) | PASS |
| SqlOptimiserTest unchanged | `mvn -pl core -Dtest=SqlOptimiserTest test` | Tests run: 172, Failures: 0, Errors: 0 | PASS |
| SampleByTest unchanged | `mvn -pl core -Dtest=SampleByTest test` | Tests run: 303, Failures: 0, Errors: 0 | PASS |
| SampleByNanoTimestampTest unchanged | `mvn -pl core -Dtest=SampleByNanoTimestampTest test` | Tests run: 278, Failures: 0, Errors: 0 | PASS |
| D-05 assertion does not fire | No `AssertionError: generateFill aggregate arm...` in surefire reports across 1395 test runs | confirmed 0 occurrences | PASS |
| Classifier predicate grep | `grep -c 'functionParser.getFunctionFactoryCache().isGroupBy(ast.token)' SqlCodeGenerator.java` | 4 matches (2 inside generateFill at :3426 D-02 + :3434 D-05; 2 pre-existing at :5243, :5268) | PASS |
| Commit scope | `git show 82865efbc0 --name-only` | Exactly 2 files: SqlCodeGenerator.java + SampleByFillTest.java | PASS |
| Single-key anchor unchanged | `git show 82865efbc0 -- SampleByFillTest.java \| grep -cE '^-[^-]'` | 0 deletions (140 insertions only) | PASS |

### Requirements Coverage

Requirements declared in 16-01-PLAN.md frontmatter: COR-01, COR-02, COR-03, COR-04, KEY-01, KEY-02, KEY-03, KEY-04, KEY-05, XPREV-01, FILL-02.

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| COR-01..04 | 16-01 | Output correctness / cartesian semantics / behavioral parity with cursor path | SATISFIED | 5 regression tests pin 6-row cartesian output per variant; 1395 cross-suite tests pass without regression. |
| KEY-01..05 | 16-01 | Keyed cartesian emission; per-key prev; key order stable; key values in fill rows correct; fill mode correct | SATISFIED | Classifier fix restores FILL_KEY dispatch for function-valued keys; observed in 5 new tests' TSVs (both keys present at all 3 buckets; per-key forward-fill semantics correct per FILL(PREV); `null` in gap cells per FILL(NULL)). |
| XPREV-01 | 16-01 | Cross-column prev with function-valued keys | SATISFIED | Multi-key FILL(PREV) is a superset case; function-valued key cartesian emission works, which means downstream cross-column PREV wiring is undisrupted. SampleByFillTest's existing cross-column PREV tests (testFillPrevCrossColumn*) continue to pass. |
| FILL-02 | 16-01 | FILL(NULL) gap-row emission | SATISFIED | `testFillNullCastMultiKey` at :467 pins FILL(NULL) with function key; 4 null cells in gap + discovery positions, matching FILL(NULL) semantics. |

No orphaned requirements — REQUIREMENTS.md mapping for Phase 16 was not separately surveyed but PLAN declared all 11 requirements and all are addressed by the landed tests.

### Locked-Decision Compliance (D-01..D-06)

| Decision | Expected | Status | Evidence |
|----------|----------|--------|----------|
| D-01 Classifier fix local to SqlCodeGenerator | Only SqlCodeGenerator.java modified (no SqlOptimiser.java changes) | COMPLIES | Commit touches SqlCodeGenerator.java + SampleByFillTest.java; `git show 82865efbc0 --name-only` confirms no SqlOptimiser.java. |
| D-02 Classifier predicate verbatim | `(FUNCTION \|\| OPERATION) && !isGroupBy(ast.token)` at :3425-3426 | COMPLIES | Exact text present at :3425-3426; alias `-ea` assert at :3427-3430 uses `groupByMetadata.getColumnIndexQuiet(qc.getAlias())`. |
| D-03 Both FUNCTION and OPERATION covered | Predicate matches both AST types | COMPLIES | `ast.type == ExpressionNode.FUNCTION \|\| ast.type == ExpressionNode.OPERATION` at :3425. Tested by `testFillPrevConcatMultiKey` (FUNCTION: `concat(a,b)`) and `testFillPrevConcatOperatorMultiKey` (OPERATION: `a \|\| b`), both pass. |
| D-04 Five tests matching variant list | interval + concat FUNCTION + concat OPERATION + cast + FILL(NULL) | COMPLIES | 5 tests at the expected locations; grep confirms exact method names; each SQL uses the prescribed syntax. |
| D-05 Single-commit landing | Classifier fix + assertion + tests in one commit | COMPLIES | Commit 82865efbc0 is the sole landing commit; contains all 3 elements (D-02 predicate + D-05 assertion + 5 tests). Title "Fix multi-key FILL(PREV) with function keys" = 43 chars (≤ 50). No Conventional Commits prefix. Long-form body describes bug/fix/assertion/tests. |
| D-06 No cursor-side change | `SampleByFillRecordCursorFactory.java` unchanged | COMPLIES | Commit does NOT touch SampleByFillRecordCursorFactory.java or any `SampleByFill*Cursor` file. `git show 82865efbc0 --name-only` shows exactly 2 files, both the expected targets. |

### Anti-Patterns Found

None. Spot-check of the modified regions:

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| SqlCodeGenerator.java | 3421-3435 | No TODO/FIXME/stub patterns. Both assertion messages plain ASCII (no em dashes, curly quotes). No banner comments. | — | None |
| SampleByFillTest.java | 467-490, 1229-1305, 1815-1847 | No TODO/FIXME/stub patterns. All 5 tests use `assertMemoryLeak` + `assertQueryNoLeakCheck(..., "ts", false, false)` per Phase 14 D-15 convention. No banner comments. | — | None |

### Probe-and-Freeze Canonical Output

From SUMMARY.md, `testFillPrevIntervalMultiKey` observed output (captured not guessed):

```
ts	k	first
2024-01-01T00:00:00.000000Z	('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')	10.0
2024-01-01T00:00:00.000000Z	('2021-01-01T00:00:00.000Z', '2021-02-01T00:00:00.000Z')	null
2024-01-01T01:00:00.000000Z	('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')	10.0
2024-01-01T01:00:00.000000Z	('2021-01-01T00:00:00.000Z', '2021-02-01T00:00:00.000Z')	null
2024-01-01T02:00:00.000000Z	('2021-01-01T00:00:00.000Z', '2021-02-01T00:00:00.000Z')	20.0
2024-01-01T02:00:00.000000Z	('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')	10.0
```

Observed: 6 rows (2 keys × 3 buckets); `Interval.NULL` renders as literal `null` (resolves RESEARCH.md A2); row-order within each bucket follows OrderedMap hash discovery (resolves RESEARCH.md A1). Independently re-verified by running the test under `-ea`: exit code 0, 0 failures.

### Human Verification Required

None. All 6 ROADMAP success criteria are programmatically verifiable through `mvn` test runs plus grep of the code. The visual TSV output is pinned by `assertQueryNoLeakCheck` with frozen expected strings, which constitutes deterministic machine verification of the cartesian contract. Phase deliverables are a SQL query-engine correctness fix with integration-test coverage; no UI, real-time, or external-service behavior is involved.

### Gaps Summary

No gaps. All 6 ROADMAP success criteria verified end-to-end:
1. interval multi-key: 6-row cartesian — verified by test + TSV inspection.
2. concat/cast multi-key: 6-row cartesian — verified by three tests (FUNCTION, OPERATION, cast).
3. Regression suite pins interval, concat, cast variants — 5 tests present.
4. testFillPrevInterval single-key anchor unchanged (0 deletions in commit diff) and still passes.
5. D-05 `-ea` assertion present, plain ASCII, does not fire across 1395 cross-suite test runs.
6. Cross-suite family (SampleByFillTest, SampleByTest, SampleByNanoTimestampTest, ExplainPlanTest, SqlOptimiserTest) all pass (1395 total, 0 failures, 2 pre-existing skips).

All locked decisions D-01..D-06 comply with verbatim spec. Single commit 82865efbc0 lands classifier fix + assertion + 5 tests together per D-05 convention. Cursor-side file unchanged per D-06.

---

*Verified: 2026-04-21T23:36:00Z*
*Verifier: Claude (gsd-verifier)*
