---
phase: 12-replace-safety-net-reclassification-with-legacy-fallback-and
verified: 2026-04-17T17:05:00Z
status: gaps_found
score: 8/9 success criteria verified
overrides_applied: 0
gaps:
  - truth: "testSampleByFillNeedFix matches master's 3-row expected output; no duplicated buckets"
    status: failed
    reason: "Intentionally descoped at a checkpoint via user decision (Option A). Three pre-existing PR defects block the restoration and were surfaced during the attempt. Phase 12 documented them as follow-up work; the test remains in its current 6-row (buggy) form as a deliberate marker."
    artifacts:
      - path: "core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java"
        issue: "Line 5943 testSampleByFillNeedFix still asserts 6 rows and retains the 'two rows per bucket' comment at lines 6018-6021. Master's expectation is 3 rows (one per bucket)."
    missing:
      - "Resolve CTE-wrap + outer projection corruption (WITH sq AS (SAMPLE BY ...) SELECT fewer_cols FROM sq produces garbage)"
      - "Resolve fast-path toTop() state corruption (second iteration after toTop() gives wrong values)"
      - "Add missing 'insufficient fill values' grammar rule (FILL(PREV,PREV,PREV,PREV,0) for 7 aggregates should reject at pos 554, currently accepted with NULL padding)"
      - "Restore testSampleByFillNeedFix to master's 3-row expected output and delete the 'two rows per bucket' comment"

deferred:
  - truth: "Visible manifestation of fast-path toTop() / CTE-wrap corruption (captured in testSampleByFillNeedFix output '1970-01-01T00:00:00.000000Z' rows)"
    addressed_in: "Follow-up phase (to be created by user)"
    evidence: "12-04-SUMMARY.md 'Deferred Defects' section enumerates three defects: CTE-wrap + outer projection, fast-path toTop() corruption, missing 'insufficient fill values' grammar rule. User takes ownership; testSampleByFillNeedFix restoration rides on the defect fix."
---

# Phase 12: Replace Safety-Net Reclassification with Legacy Fallback Verification Report

**Phase Goal:** Replace the silent codegen safety-net reclassification (SqlCodeGenerator.java:3497-3512) with a retro-fallback mechanism that routes unsupported-type PREV aggregates to the legacy cursor path; close the LONG128/INTERVAL gap and add cross-col PREV(alias) resolution at the optimizer gate (Tier 1); implement six new FILL(PREV, PREV(...)) grammar rules (D-05..D-09); restore testSampleByFillNeedFix to master's 3-row expectation; add 19 regression tests; convert ~15 assertSql sites to assertQueryNoLeakCheck; sweep code-quality items.

**Verified:** 2026-04-17T17:05:00Z
**Status:** gaps_found (single deliberate descope; other criteria green)
**Re-verification:** No - initial verification

## Goal Achievement

### Observable Truths (Roadmap Success Criteria)

| # | Success Criterion | Status | Evidence |
|---|---|---|---|
| 1 | testSampleByFillNeedFix matches master's 3-row expected output; no duplicated buckets. | FAILED (intentional descope) | SampleByTest.java:5943 still contains the 6-row assertion; lines 6018-6021 retain the "two rows per bucket" comment. `mvn -Dtest='SampleByTest#testSampleByFillNeedFix' test` exits 1. User approved Option A at a checkpoint; defects documented in 12-04-SUMMARY.md. |
| 2 | Queries with unsupported-output-type aggregates route to the legacy cursor. | VERIFIED | 5 retro-fallback tests present and green (testFillPrev{CaseOverDecimal,ExpressionArgDecimal128,ExpressionArgString,Interval,Long128}Fallback) — each asserts `Sample By` plan and legacy-cursor output. SqlCodeGenerator.java:3555 throws FallbackToLegacyException.INSTANCE on unsupported type at codegen. |
| 3 | Safety-net block at SqlCodeGenerator.java:3497-3512 is deleted; retro-fallback mechanism replaces it. | VERIFIED | `grep 'Safety-net type check' SqlCodeGenerator.java` → 0 matches. `grep 'anyPrev' SqlCodeGenerator.java` → 0 matches. FallbackToLegacyException.java exists (50 lines, singleton, no-stack-trace). Three catch sites at lines 8043, 8259, 8307. Stash write in SqlOptimiser.java:8464 `nested.setStashedSampleByNode(sampleBy);`. |
| 4 | Grammar rules D-05..D-09 reject malformed PREV shapes with positioned SqlException. | VERIFIED | SqlCodeGenerator.java contains all four distinct error messages at lines 3456 ("PREV argument must be a single column name"), 3468 ("PREV cannot reference the designated timestamp column"), 3484 ("cannot fill target column of type"), 3514 ("FILL(PREV) chains are not supported"). 8 negative + 3 positive grammar tests green. |
| 5 | 22 new regression tests pass: 5 retro-fallback + 11 grammar (3+8) + 5 FILL_KEY + 1 TO-null. | VERIFIED | All 22 test methods declared in SampleByFillTest.java. `mvn -pl core -Dtest='SampleByFillTest' test` → 74/74 green. The plan shipped 11 grammar tests vs the CONTEXT's "8" — documented in 12-04-SUMMARY.md as comprehensive coverage. |
| 6 | TO null::timestamp produces bounded output via the hasExplicitTo LONG_NULL guard. | VERIFIED | SampleByFillRecordCursorFactory.java:593 `if (maxTimestamp == Numbers.LONG_NULL) hasExplicitTo = false;`. testFillToNullTimestamp exercises the guard; test passes. |
| 7 | toPlan emits fill=null \| prev \| value unconditionally. | VERIFIED | SampleByFillRecordCursorFactory.java:197-203 emits `fill=prev \| value \| null` unconditionally. Plan-text refreshed across 4 test files (SampleByFillTest, SampleByTest, SampleByNanoTimestampTest, ExplainPlanTest — 19 `fill: ...` occurrences observed in ExplainPlanTest alone). |
| 8 | Code-quality items applied (alphabetization, plain imports, isKeyColumn relocation, anyPrev removal, Dates.parseOffset asserts). | VERIFIED | isKeyColumn at SqlCodeGenerator.java:1230 (moved next to isFastPathPrevSupportedType at 1199-1210). anyPrev loop absent (0 matches). `assert parsed != Numbers.LONG_NULL` at SqlCodeGenerator.java:3398. FillRecord getters alphabetical per 12-REVIEW.md "Passing checks". Imports sorted. NullConstant + plain type imports added. |
| 9 | Full suite green: `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest' test`. | PARTIAL (intentional) | SampleByFillTest 74/74, ExplainPlanTest 522/522 (2 skipped, unrelated), SampleByTest 301/302 — the sole failure is the deferred testSampleByFillNeedFix (success criterion #1 descope). All other tests green. |

**Score:** 8/9 truths verified; 1 intentional descope.

### Deferred Items

Items not yet met but explicitly addressed by a follow-up phase to be created by the user (per Option A decision at checkpoint).

| # | Item | Addressed In | Evidence |
|---|---|---|---|
| 1 | testSampleByFillNeedFix restored to 3-row output | Follow-up phase (user-owned) | 12-04-SUMMARY.md "Deferred Defects" enumerates the three blocking bugs; user chose Option A and takes ownership. |
| 2 | CTE-wrap + outer projection corruption resolved | Follow-up phase (user-owned) | 12-04-SUMMARY.md defect #1. Test output shows `1970-01-01T00:00:00.000000Z` rows — garbage timestamps from the wrap path. |
| 3 | Fast-path toTop() state corruption resolved | Follow-up phase (user-owned) | 12-04-SUMMARY.md defect #2. `assertQuery(..., sizeExpected=true)` re-iterates after toTop(); second iteration produces wrong values. |
| 4 | "Insufficient fill values" grammar rule added | Follow-up phase (user-owned) | 12-04-SUMMARY.md defect #3. Master rejects `FILL(PREV,PREV,PREV,PREV,0)` for 7 aggregates at pos 554; current code pads with NULL. |

### Required Artifacts

| Artifact | Expected | Status | Details |
|---|---|---|---|
| `core/src/main/java/io/questdb/griffin/FallbackToLegacyException.java` | Singleton checked-exception signal | VERIFIED | 50 lines; `public final class FallbackToLegacyException extends SqlException`; `INSTANCE` static final; `fillInStackTrace()` returns `this`. |
| `core/src/main/java/io/questdb/griffin/model/QueryModel.java` | stashedSampleByNode field + getter/setter/clear | VERIFIED | Field at 185; clear() reset at 471; getter at 1066; setter at 1855-1856. |
| `core/src/main/java/io/questdb/griffin/model/IQueryModel.java` | Interface methods for stash | VERIFIED | `setStashedSampleByNode` at 639; corresponding getter present. |
| `core/src/main/java/io/questdb/griffin/model/QueryModelWrapper.java` | Delegation for stash getter/setter | VERIFIED | `setStashedSampleByNode` at 1246 (throws UnsupportedOperationException per wrapper contract). |
| `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` | Tier 1 gate + stash write | VERIFIED | `ColumnType.LONG128, ColumnType.INTERVAL -> true` at line 579. `nested.setStashedSampleByNode(sampleBy)` at 8464. Cross-col resolution replaces the skip at 520-525 (per 12-01-SUMMARY deviation note). |
| `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | Retro-fallback, grammar, code-quality | VERIFIED | Throw at 3555; three catches at 8043/8259/8307; D-05..D-09 error messages at 3456/3468/3484/3514. isKeyColumn at 1230. Dates.parseOffset assert at 3398. anyPrev loop removed. SampleByFillRecordCursorFactory import in alphabetical slot. |
| `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` | hasExplicitTo guard, unconditional fill=, alphabetization, plain imports | VERIFIED | Guard at 593; `sink.attr("fill").val("...")` three times at 198/200/202; hasAnyConstantFill at 227-237. |
| `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` | 22 new tests + 42 assertSql→assertQueryNoLeakCheck + plan-text refresh | VERIFIED | All 22 test method names found. `grep 'assertSql' = 5`, `grep 'assertQueryNoLeakCheck' = 56`. |
| `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java` | Plan-text refresh (sampleByPushdownPlan helper) | VERIFIED (partial) | Helper updated; testSampleByFillNeedFix NOT restored (intentional descope). |
| `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java` | Plan-text refresh | VERIFIED | Helper and assertions updated (per 12-04-SUMMARY). |
| `core/src/test/java/io/questdb/test/griffin/ExplainPlanTest.java` | Plan-text refresh | VERIFIED | 19 `fill: null|prev|value` occurrences in the expected plan text — matches the 7 sites refreshed by plan 12-04 (each site may contain multiple fill attributes in nested plans). |

### Key Link Verification

| From | To | Via | Status | Details |
|---|---|---|---|---|
| SqlOptimiser.rewriteSampleBy | QueryModel.setStashedSampleByNode | `nested.setStashedSampleByNode(sampleBy)` before `nested.setSampleBy(null)` | WIRED | Line 8464 immediately before the clear block. |
| SqlOptimiser.isUnsupportedPrevType | SqlCodeGenerator.isFastPathPrevSupportedType | Symmetric type-tag matrix | WIRED | Line 579 includes LONG128 and INTERVAL; matches isFastPathPrevSupportedType at 1199-1210. |
| SqlCodeGenerator.generateFill (unsupported-type detection) | FallbackToLegacyException.INSTANCE | `throw FallbackToLegacyException.INSTANCE` at codegen check site | WIRED | Line 3555. |
| SqlCodeGenerator.generateSelectGroupBy (three generateFill call sites) | SqlCodeGenerator.generateSampleBy | `catch (FallbackToLegacyException) { walk chain; setSampleBy(stashed); generateSampleBy(...); }` | WIRED | Three catches at 8043, 8259, 8307. Identical 13-line handler body each (see WR-03 in 12-REVIEW.md). |
| SampleByFillCursor.initialize | hasExplicitTo field | `if (maxTimestamp == Numbers.LONG_NULL) hasExplicitTo = false;` | WIRED | Line 593. |
| SampleByFillRecordCursorFactory.toPlan | PlanSink fill= attribute | Unconditional emission of one of null/prev/value | WIRED | Lines 197-203 (if/else-if/else chain). |

### Data-Flow Trace (Level 4)

Not applicable — phase 12 is a compile-time / code-generation change. No runtime data source populates a visible artifact.

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|---|---|---|---|
| Core module compiles | `mvn -pl core compile -DskipTests` (implicit via test runs) | Build succeeds | PASS |
| SampleByFillTest green | `mvn -pl core -Dtest='SampleByFillTest' test` | 74 run, 0 failures | PASS |
| ExplainPlanTest green | `mvn -pl core -Dtest='ExplainPlanTest' test` | 522 run, 0 failures, 2 skipped (unrelated) | PASS |
| SampleByTest (expected 1 deferred failure) | `mvn -pl core -Dtest='SampleByTest' test` | 302 run, 1 failure (testSampleByFillNeedFix — deferred) | PASS-WITH-KNOWN-FAILURE |
| FallbackToLegacyException imports & usage | `grep 'FallbackToLegacyException' SqlCodeGenerator.java` | 1 import, 1 throw, 3 catches | PASS |
| Grammar error messages present | `grep` of four error strings | All 4 found | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|---|---|---|---|---|
| PTSF-02 | 12-01 | Explicit source type support matrix — numeric types on fast path, unsupported types fall back to legacy | SATISFIED | Tier 1 gate closes LONG128/INTERVAL at SqlOptimiser.java:579; cross-col alias resolution in hasPrevWithUnsupportedType; retro-fallback catches the residue at SqlCodeGenerator.java:3555 + 8043/8259/8307. |
| PTSF-04 | 12-01, 12-03, 12-04 | `prev(alias)` referencing unsupported type triggers legacy path fallback (plan shows Sample By, not Async Group By) | SATISFIED | Five testFillPrev*Fallback tests assert `Sample By` plan (not `Sample By Fill`) for UUID/DECIMAL128/DECIMAL256/STRING/LONG128/INTERVAL aggregates. All five green. |
| COR-01 | 12-04 | All 302 existing SampleByTest tests pass | NEEDS HUMAN (de facto) | 301/302 green; the sole failure is testSampleByFillNeedFix — descoped per Option A. This requirement was previously Complete (Phase 5); phase 12 does not introduce new regressions beyond the deliberate descope. |
| COR-02 | 12-04 | No native memory leaks (assertMemoryLeak passes for all tests) | SATISFIED | All 22 new tests wrap assertMemoryLeak; 74/74 SampleByFillTest green confirms no leak. |
| COR-04 | 12-03, 12-04 | Fill cursor output matches cursor-path output exactly for all fill modes | SATISFIED (modulo descope) | Retro-fallback re-dispatches unsupported cases to the legacy cursor; 5 fallback tests prove parity. Exception: the CTE-wrap + toTop paths (three deferred defects) produce wrong values — these pre-existing defects are outside the current phase scope but are the reason testSampleByFillNeedFix can't be restored yet. |

### Anti-Patterns Found

Scanned all files modified in phase 12 (per key-files listed in each SUMMARY):

| File | Line | Pattern | Severity | Impact |
|---|---|---|---|---|
| SqlCodeGenerator.java | 8043-8056, 8259-8272, 8307-8320 | Three byte-identical 13-line catch blocks (duplication) | Warning (WR-03) | Maintenance hazard; any change must be applied three times. Extract a private helper. |
| SqlCodeGenerator.java | retro-fallback catches | Only `setSampleBy(stashed)` is restored; `sampleByFrom`, `sampleByTo`, `sampleByOffset`, `sampleByTimezoneName` stay null | Warning (WR-01) | Queries that match retro-fallback conditions AND specify FROM/TO/OFFSET would run without those bounds. No regression test covers this combination — risk is theoretical but untested. |
| SqlCodeGenerator.java | 8047-8056 | Chain-walk lands on a QueryModelWrapper whose setSampleBy/setStashedSampleByNode throws UnsupportedOperationException | Warning (WR-02) | If the walk lands on a wrapper, retro-fallback converts a recoverable condition into an unhandled exception that escapes to the user. |
| SqlCodeGenerator.java | 3513 | Chain-rejection position is generic (always points at first PREV regardless of which column's chain is malformed) | Warning (WR-04) | UX degradation relative to other grammar rules in this phase. Minor. |
| SqlCodeGenerator.java | 70 | Redundant same-package import `import io.questdb.griffin.FallbackToLegacyException` | Info (IN-01) | Harmless. Plan explicitly required the import line (acceptance criterion). |
| FallbackToLegacyException.java | 40 | INSTANCE inherits SqlException's mutable `message` and `position` fields | Info (IN-02) | If future code writes to INSTANCE.put(...), the singleton mutates and leaks across threads. Doc note or override stubs suggested. |
| SampleByFillTest.java:1684-1709 | testFillPrevRejectNoArg | Uses assertSql + try/catch instead of assertExceptionNoLeakCheck; permissive `contains(...)` disjuncts | Info (IN-03) | Tighten to `Assert.assertTrue(e.getMessage().contains("PREV"))`. Minor. |
| SampleByFillRecordCursorFactory.java:227-237 | hasAnyConstantFill walks fillModes on every toPlan call | O(n) lookup per explain call | Info (IN-04) | Invariant for factory lifetime; could be cached. Strictly optional. |
| SampleByFillRecordCursorFactory.java:87 | fillModes field is final | Noted for completeness | Info (IN-05) | No issue. |
| SqlCodeGenerator.java:3398-3399 | `Dates.parseOffset` assert message uses string concatenation | With -ea off, no-op; with -ea on, allocation only on failure | Info (IN-06) | Correct pattern. |

Summary: 0 Blockers, 4 Warnings, 6 Info. All warnings already documented in 12-REVIEW.md; none block the phase goal but WR-01 and WR-02 deserve follow-up.

### Human Verification Required

None. All phase behaviors have automated coverage via the four test classes. The three deferred defects (CTE-wrap, toTop, insufficient fill values) surface in `testSampleByFillNeedFix`'s failure output but are explicitly descoped to a follow-up phase.

### Gaps Summary

One deliberate gap: testSampleByFillNeedFix was not restored to master's 3-row expected output. This was a checkpoint decision (Option A) taken by the user after three pre-existing PR defects were uncovered during the attempt:

1. **CTE-wrap + outer projection corruption.** `WITH sq AS (SAMPLE BY ... FILL(...)) SELECT fewer_cols FROM sq` produces garbage timestamps, wrong values, and extra rows. Visible in the `testSampleByFillNeedFix` failure output where fill rows emit `1970-01-01T00:00:00.000000Z`.
2. **Fast-path toTop() state corruption.** `assertQuery(..., sizeExpected=true)` re-iterates after toTop(); the fast-path cursor does not correctly reset state between re-uses.
3. **Missing "insufficient fill values" grammar rule.** Master rejects `FILL(PREV,PREV,PREV,PREV,0)` for 7 aggregates at pos 554; current code accepts the under-specified clause and pads with NULL. Never appeared in CONTEXT.md's D-05..D-09 grammar list.

The testSampleByFillNeedFix test therefore remains in its current 6-row (buggy) form as a deliberate marker that phase 12 did not attempt the restoration. The user takes ownership of a new phase to address the three defects.

Additionally, 12-REVIEW.md raised four warnings worth tracking for follow-up (WR-01 lost FROM/TO/OFFSET/TIMEZONE metadata on retro-fallback; WR-02 QueryModelWrapper throws when walk lands on it; WR-03 three duplicated 13-line catch blocks; WR-04 generic chain-rejection position) and six info findings. None block the phase goal, but WR-01 and WR-02 are latent correctness concerns that would benefit from attention in the defect-fix follow-up phase.

Apart from the intentional descope, every other success criterion is met. Automated test runs confirm 74/74 SampleByFillTest, 522/522 ExplainPlanTest (2 unrelated skips), and 301/302 SampleByTest (sole failure = deferred test).

---

*Verified: 2026-04-17T17:05:00Z*
*Verifier: Claude (gsd-verifier)*
