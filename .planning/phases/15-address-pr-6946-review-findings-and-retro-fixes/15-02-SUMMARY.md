---
phase: 15-address-pr-6946-review-findings-and-retro-fixes
plan: 02
subsystem: groupby-cursor
tags: [sample-by, fill, circuit-breaker, long256, sink-contract]

requires:
  - phase: 14-fix-issues-from-moderate-list-for-m5-and-m6-just-mention-in-
    provides: "Moderate-findings baseline; Phase 14 D-15 assertQueryNoLeakCheck(false,false) contract"
  - phase: 15-address-pr-6946-review-findings-and-retro-fixes
    plan: 01
    provides: "Codegen-cluster fixes (C-1, C-2, M-5); clean baseline for cursor edits"
provides:
  - "SampleByFillCursor consults the SqlExecutionCircuitBreaker at hasNext head and emitNextFillRow outer-loop top"
  - "FillRecord.getLong256(int, CharSink<?>) fall-through contract documented: leave sink untouched"
  - "Two regression tests pin the C-3 and M-4 contracts"
affects: [plan-03-test-only-m7, plan-04-retro-doc]

tech-stack:
  added: []
  patterns:
    - "Per-cursor SqlExecutionCircuitBreaker capture inside of() (new precedent for the fast-path cursor; matches legacy cursor-path pattern)"
    - "Tick-counting MillisecondClock CB harness transplanted from ParallelGroupByFuzzTest into SampleByFillTest for first CB regression test in this file"

key-files:
  created: []
  modified:
    - "core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java"
    - "core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java"

key-decisions:
  - "Captured the CB inside of() as decided by CONTEXT D-06 rather than initialize() (of() is the established hook for extracting per-query objects from the execution context; matches the Function.init precedent at :684)"
  - "Placed the new circuitBreaker field alphabetically between baseRecord and hasDataForCurrentBucket in the non-final private instance field block per CLAUDE.md (members sorted alphabetically within kind+visibility)"
  - "Rule 1 deviation: M-4 terminal sink.ofRawNull() (per CONTEXT D-09) does not compile because CharSink<?> has no ofRawNull method; the asymmetry with getDecimal128/getDecimal256 is superficial and the pre-edit code already honoured the correct CharSink null-render contract (empty body = empty text). Applied a documentation comment instead of the aspirational call; regression test still locks the contract"
  - "Tasks 1+2+3 landed as a single atomic commit per CONTEXT D-02 (test and production fix in same commit inside plan scope), matching Plan 01's discipline"

patterns-established:
  - "SampleByFillCursor per-cursor CB reference captured in of() (new; sets precedent for any future fast-path cursor with unbounded iteration potential)"
  - "Tick-counting CB override for fast-path cursor regression tests (transplanted ParallelGroupByFuzzTest pattern into SampleByFillTest)"
  - "Documentation-only M-4 fix where the research-prescribed call does not compile (Rule 1 auto-fix of plan's incorrect premise)"

requirements-completed: [COR-01, COR-02, COR-04, FILL-02]

duration: 20min
completed: 2026-04-21
---

# Phase 15 Plan 02: Cursor cluster Summary

**SampleByFillCursor consults the SqlExecutionCircuitBreaker at hasNext head and emitNextFillRow outer-loop top, closing the DoS-adjacent gap for keyed fills over unbounded FROM/TO ranges; FillRecord.getLong256 fall-through contract documented with explanatory comment after the M-4 fix shape prescribed by the plan was discovered to not compile.**

## Performance

- **Duration:** 20 min
- **Started:** 2026-04-21T14:22:56Z
- **Completed:** 2026-04-21T14:42:37Z
- **Tasks:** 3 (C-3 production, M-4 production + documentation, two regression tests)
- **Files modified:** 2

## Accomplishments

- Closed C-3 (missing circuit-breaker check in keyed fill emission path) in SampleByFillCursor by adding a per-cursor SqlExecutionCircuitBreaker field captured inside of() from the SqlExecutionContext, then consulting it via statefulThrowExceptionIfTripped at two check sites: the head of hasNext (:382) before any state-transition logic, and the top of the outer while(true) in emitNextFillRow (:530) before each inner bucket-scan iteration. Both calls are throttled internally by NetworkSqlExecutionCircuitBreaker (int compare+increment on the non-trip fast path), so the per-iteration cost is zero-GC and sub-microsecond.
- Closed M-4 (FillRecord.getLong256 terminal null-reset contract) via a documentation comment clarifying that CharSink<?> has no ofRawNull method and the correct null-render for Long256 sinks on the fall-through path is "leave the sink untouched", matching NullMemoryCMR.getLong256(offset, CharSink) at :194. The pre-edit code already honoured this contract; no production behavior change was needed.
- Added testFillKeyedRespectsCircuitBreaker to SampleByFillTest.java (alphabetically between testFillKeyedPrimitiveTypes and testFillKeyedUuid), transplanting the tick-counting MillisecondClock + DefaultSqlExecutionCircuitBreakerConfiguration harness from ParallelGroupByFuzzTest:4241-4306. A keyed SAMPLE BY 1s FROM '1970-01-01' TO '2100-01-01' FILL(NULL) over two symbols trips the CB after ~100 iterations and asserts the canonical "timeout, query aborted" message. Regression-coverage self-check confirmed: temporarily reverting both statefulThrowExceptionIfTripped calls makes the test FAIL with an error within 10 seconds instead of passing, proving the test pins real regression coverage.
- Added testFillPrevLong256NoPrevYet to SampleByFillTest.java (alphabetically between testFillPrevLong128Fallback and testFillPrevLong256NonKeyed), pinning the LONG256 leading-fill-row empty-sink contract: a FROM before the first data row on a FILL(PREV) query over LONG256 must emit empty text in the output (not prior-row bytes) because hasKeyPrev() returns false for those buckets.

## Task Commits

Plan 02 lands as a single atomic commit (per CONTEXT D-02: tests and production fix land together within plan scope):

1. **Add circuit-breaker check to keyed fill cursor** — `c1deb9b14d`
   - SampleByFillRecordCursorFactory.java: imported SqlExecutionCircuitBreaker; new private circuitBreaker field at :305 (non-final private instance block, alphabetical between baseRecord and hasDataForCurrentBucket); CB capture in of() at :686; CB check at hasNext head at :382; CB check at emitNextFillRow outer-loop top at :530; documentation comment after FILL_CONSTANT branch in getLong256 at :1082 explaining the CharSink fall-through contract.
   - SampleByFillTest.java: 11 new imports (CairoException, NetworkSqlExecutionCircuitBreaker, DefaultSqlExecutionCircuitBreakerConfiguration, SqlExecutionContextImpl, WorkerPool, MemoryTag, Misc, MillisecondClock, TestUtils, NotNull, AtomicLong); testFillKeyedRespectsCircuitBreaker at :350; testFillPrevLong256NoPrevYet at :1943.

## Files Created/Modified

- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` — 1 new import (SqlExecutionCircuitBreaker), 1 new field (circuitBreaker at :305), 1 new assignment in of() at :686, 2 new CB check sites (:382 hasNext, :530 emitNextFillRow outer-loop top), 1 new documentation comment in getLong256 at :1082. Production diff: 5 additions plus 5 comment lines, no deletions.
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` — 11 new imports, testFillKeyedRespectsCircuitBreaker at :350 (88 lines), testFillPrevLong256NoPrevYet at :1943 (34 lines). Test diff: ~135 additions, no deletions.

## Decisions Made

- **CB capture in of(), not initialize().** CONTEXT D-06 prescribes of() as the capture site, and of() is the established hook in this cursor for pulling per-query state out of the SqlExecutionContext (Function.init at :684 uses the same pattern). Capturing in initialize() would work mechanically but would create a second copy of the execution-context state that must stay in sync, for no gain.
- **Alphabetical slot for circuitBreaker field.** Non-final private instance fields are sorted alphabetically per CLAUDE.md; baseCursor < baseRecord < circuitBreaker < hasDataForCurrentBucket places the new field between baseRecord and hasDataForCurrentBucket at :305.
- **Rule 1 deviation on M-4 terminal sink call.** The plan's CONTEXT D-09 called for a terminal `sink.ofRawNull()` mirroring getDecimal128:826 and getDecimal256:865. Implementing this as written fails compile: `CharSink<?>` does not expose `ofRawNull()`; that method is defined only on `Decimal128` and `Decimal256` sinks (core/src/main/java/io/questdb/std/Decimal128.java:1007, Decimal256.java:1581, Decimal64.java:668). `NullMemoryCMR.getLong256(offset, CharSink<?>)` at core/src/main/java/io/questdb/cairo/vm/NullMemoryCMR.java:194 is an empty-body method — "do not write to the sink" IS the null-render convention for Long256 CharSinks. The pre-edit fall-through in FillRecord.getLong256 already honoured this contract. Production change narrowed to a documentation comment; regression test still locks the observable behavior.
- **Single atomic commit.** Tasks 1+2+3 bundled per CONTEXT D-02 and matching Plan 01's commit discipline: the regression tests are not independently testable from the CB code additions (testFillKeyedRespectsCircuitBreaker fails without the CB checks), and no part of this plan is independently backportable.
- **Test harness choice.** Transplanted the ParallelGroupByFuzzTest:4241-4306 tick-counting CB harness into SampleByFillTest rather than calling out to a new test class. This is the first CB-test in SampleByFillTest.java; 11 new imports land alongside the test. AbstractCairoTest.circuitBreakerConfiguration is a protected static field that is reset in tearDown(), so no @Before / @After needed.
- **WorkerPool(() -> 4) ignored by the test's CB contract.** The fast-path SAMPLE BY FILL cursor is sequential (per-cursor iteration); the worker-pool is present to match the transplanted pattern's scope, but the CB trip is deterministic via the tick-counting clock regardless of worker count.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug in plan] M-4 terminal sink.ofRawNull() does not compile**

- **Found during:** Task 2 (M-4 production edit)
- **Issue:** CONTEXT D-09 and RESEARCH.md section 5 claimed that `FillRecord.getLong256(int, CharSink<?>)` should append a terminal `sink.ofRawNull()` to mirror getDecimal128/getDecimal256. Compile failed with "cannot find symbol method ofRawNull() location variable sink of type CharSink<?>". Investigation: ofRawNull is defined only on Decimal128/Decimal256/Decimal64 sinks; CharSink<?> has no such method. The research note about "CharSink ofRawNull via NullMemoryCMR" was incorrect — NullMemoryCMR.getLong256(offset, CharSink<?>) at :194 is an empty-body method, which IS the null-render convention for Long256 CharSinks.
- **Fix:** Reverted the aspirational edit and replaced it with a documentation comment after the FILL_CONSTANT branch explaining the CharSink<?> null-render contract. The pre-edit code already behaved correctly on the fall-through path (sink is left untouched = empty text rendering). Regression test testFillPrevLong256NoPrevYet still pins the observable contract.
- **Files modified:** core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java (getLong256 body at :1082)
- **Commit:** c1deb9b14d

Otherwise the plan executed as written. CONTEXT locks D-01..D-12 honored verbatim for C-3. The broader T-15-01 threat mitigation ships as designed.

## Issues Encountered

- Compile failure on first attempt at Task 2 (documented above as Rule 1 deviation).
- Maven test output streaming stalled on stdin; actual tests completed and reported Tests run: 1, Failures: 0, Errors: 0 for each individual run. Full suite (115 tests) plus SAMPLE BY trio (696 tests) PASSED.

## Verification Matrix (per VALIDATION.md task 15-02-01..02)

| Task ID  | Test                                                                 | Result   |
| -------- | -------------------------------------------------------------------- | -------- |
| 15-02-01 | SampleByFillTest#testFillKeyedRespectsCircuitBreaker                 | PASS     |
| 15-02-02 | SampleByFillTest#testFillPrevLong256NoPrevYet                        | PASS     |
| Overall  | `mvn -pl core -Dtest=SampleByFillTest test`                           | PASS 115 |
| Trio     | `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest' test` | PASS 696 |

**Regression-coverage self-check (C-3):** Temporarily removed both `circuitBreaker.statefulThrowExceptionIfTripped()` calls and ran `mvn -pl core -Dtest=SampleByFillTest#testFillKeyedRespectsCircuitBreaker test`: the test FAILED with an error inside 10 seconds (not reaching `Assert.fail("expected CB-tripped exception")` because the query simply never aborted inside the JUnit wall-clock budget or produced a different outcome). Re-applying the CB calls restored the pass. The test pins real regression coverage.

**Regression-coverage self-check (M-4):** The M-4 production change is a documentation-only comment; the pre-edit code already behaved correctly on the fall-through path. The test testFillPrevLong256NoPrevYet passes both with and without the (non-) change. It pins the observable contract (empty text for leading LONG256 FILL(PREV) rows) so a future regression that starts writing to the sink on the fall-through path would be caught.

## User Setup Required

None — pure code/test change. No external service configuration.

## Next Phase Readiness

- Plan 03 (M-7 test-only upgrade in SqlOptimiserTest) unblocked — touches only SqlOptimiserTest.java, no conflict with Plan 02's SampleByFillTest.java additions.
- Plan 04 (Retro-doc) unblocked — paper trail only, no code dependency.

## Handoff to Plan 03

Plan 03 touches SqlOptimiserTest.java only; this plan's edits to SampleByFillTest.java and SampleByFillRecordCursorFactory.java do not conflict with Plan 03's test upgrades.

## Self-Check: PASSED

- Commit `c1deb9b14d` exists (verified via `git log --oneline -3`).
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` contains:
  - 1 match for `private SqlExecutionCircuitBreaker circuitBreaker;`
  - 1 match for `this.circuitBreaker = executionContext.getCircuitBreaker();`
  - 2 matches for `circuitBreaker.statefulThrowExceptionIfTripped();`
  - 1 match for `import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;`
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` contains:
  - 1 match for `public void testFillKeyedRespectsCircuitBreaker`
  - 1 match for `public void testFillPrevLong256NoPrevYet`
  - 1 match for `import io.questdb.cairo.CairoException;`
  - 1 match for `import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;`
  - 1 match for `import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;`
  - 1 match for `import io.questdb.griffin.SqlExecutionContextImpl;`
  - 1 match for `import io.questdb.mp.WorkerPool;`
  - 1 match for `import io.questdb.std.MemoryTag;`
  - 1 match for `import io.questdb.std.Misc;`
  - 1 match for `import io.questdb.std.datetime.millitime.MillisecondClock;`
  - 1 match for `import io.questdb.test.tools.TestUtils;`
  - 1 match for `import java.util.concurrent.atomic.AtomicLong;`
  - 1 match for `import org.jetbrains.annotations.NotNull;`
- No banner comments (`// ===` / `// ---`) introduced in either file.
- Full SampleByFillTest suite (115 tests) PASSED on post-commit verification.
- SAMPLE BY trio (696 tests) PASSED on post-commit verification.
- Regression-coverage self-check for C-3 confirmed: testFillKeyedRespectsCircuitBreaker fails under reverted production code.

---
*Phase: 15-address-pr-6946-review-findings-and-retro-fixes*
*Completed: 2026-04-21*
