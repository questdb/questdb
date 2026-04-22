---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
status: executing
stopped_at: Phase 17 Plan 04 complete -- all 4 plans landed; SC#8 (/review-pr 6946 re-run) pending user action
last_updated: "2026-04-22T18:15:00.000Z"
last_activity: 2026-04-22
progress:
  total_phases: 17
  completed_phases: 16
  total_plans: 34
  completed_plans: 34
  percent: 100
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-09)

**Core value:** SAMPLE BY FILL queries execute on the GROUP BY fast path with identical output to the cursor path, enabling parallel execution.
**Current focus:** Phase 17 — verify-pr-6946-body-drift-against-landed-commits-decide-code

## Current Position

Phase: 17 (verify-pr-6946-body-drift-against-landed-commits-decide-code) -- COMPLETE (pending SC#8 /review-pr 6946 re-run)
Plan: 4 of 4 (ALL PLANS COMPLETE)
Status: Plan 17-04 shipped; todo move commit 4529631666 on sm_fill_prev_fast_path; PR #6946 body (5 edits) + title renamed via external gh edits; phase-wide ASCII normalization applied (41 non-ASCII bytes -> 0); D-26 todo retired to .planning/todos/completed/ with completed_in: 17-01-PLAN.md
Last activity: 2026-04-22

Progress: [##########] 100%

## Performance Metrics

**Velocity:**

- Total plans completed: 24
- Total execution time across tracked phases: ~10h

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 02 | 1 | 43m | 43m |
| 03 | 1 | 112m | 112m |
| 04 | 1 | 27m | 27m |
| 06 | 1 | 4m | 4m |
| 07 | 1 | 49m | 49m |
| 08 | 1 | 273m | 273m |
| 09 | 1 | 23m | 23m |
| 10 | 1 | 16m | 16m |
| 11 | 1 | ~210m (retroactive estimate) | ~210m |
| 12 | 4 | - | - |
| 13 | 6 | - | - |
| 14 | 4 | - | - |

Phase 5 absorbed into phases 7–10; no direct execution time attributed.
| Phase 12 P01 | 12 | 3 tasks | 5 files |
| Phase 12 P02 | 14min | 3 tasks | 1 files |
| Phase 12-replace-safety-net-reclassification-with-legacy-fallback-and P03 | 22min | 3 tasks | 1 files |
| Phase 12 P04 | 80min | 3 tasks | 4 files |
| Phase 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi P01 | ~100 min | 2 tasks | 1 files |
| Phase 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi P02 | ~100 min | 2 tasks | 2 files |
| Phase 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi P03 | 30m | 1 tasks | 1 files |
| Phase 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi P04 | 45m | 1 tasks | 9 files |
| Phase 13 P05 | ~65 min | 2 tasks | 2 files |
| Phase 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi P06 | ~75 min | 2 tasks | 4 files |
| Phase 14 P01 | 23min | 3 tasks | 4 files |
| Phase 14 P02 | 39min | 3 tasks | 4 files |
| Phase 14 P03 | 20min | 2 tasks | 2 files |
| Phase 14-fix-issues-from-moderate-list-for-m5-and-m6-just-mention-in- P04 | 35min | 4 tasks | 3 files |
| Phase 15 P01 | 13min | 3 tasks | 3 files |
| Phase 15 P02 | 20min | 3 tasks | 2 files |
| Phase 15 P03 | 20min | 1 tasks | 1 files |
| Phase 15 P04 | 5min | 1 tasks | 1 files |
| Phase 16 P01 | ~25min | 5 tasks | 2 files |
| Phase 17 P01 | ~25min | 2 tasks | 3 files |
| Phase 17 P02 | ~40min | 3 tasks | 5 files |
| Phase 17 P03 | ~35min | 2 tasks | 3 files |
| Phase 17 P04 | ~15min | 1 tasks | 1 files (+ 2 external artefacts) |

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table. Recent decisions affecting delivered code:

- [Phase 1]: Unified SampleByFillRecordCursorFactory handles NULL/PREV/VALUE via per-column fillModes
- [Phase 1]: Sort inside generateFill via generateOrderBy(); followedOrderByAdvice=true skips outer sort
- [Phase 2]: Two-pass streaming for keyed fill (pass 1 discovers keys, toTop(), pass 2 streams with fill)
- [Phase 2]: Per-column fillModes (FILL_CONSTANT, FILL_PREV_SELF) with constantFills ObjList
- [Phase 2]: Build SortedRecordCursorFactory explicitly in generateFill — optimizer strips ORDER BY before code generation
- [Phase 2]: No peek-ahead in hasNext dataTs==nextBucketTimestamp branch — corrupts SortedRecordCursor record position
- [Phase 2]: Early-exit guard placed after data fetch (not before) so isBaseCursorExhausted is set correctly
- [Phase 3]: FILL_KEY = -3 distinguishes key columns from aggregates in fillModes array
- [Phase 3]: OrderedMap stores key combinations with per-key prev in MapValue slots [keyIndex, hasPrev, prevCols...]
- [Phase 3]: keyPosOffset compensates for value columns preceding key columns in MapRecord index space
- [Phase 3]: symbolTableColIndices covers all map columns (value + key) for SYMBOL resolution
- [Phase 3]: Non-keyed PREV preserved via simplePrev fallback when keysMap is null
- [Phase 04]: FILL(PREV(col_name)) resolved via ExpressionNode.FUNCTION type with paramCount=1, alias resolved against output metadata
- [Phase 04]: Optimizer gate relaxed from size-1-only to hasLinearFill() predicate, allowing multi-fill specs on fast path
- [Phase 06]: Zero-key guard in initialize() returns early with maxTimestamp=Long.MIN_VALUE to produce empty result
- [Phase 07]: IntList prevSourceCols replaces boolean hasPrevFill for per-column PREV snapshot tracking
- [Phase 07]: Two-layer type defense: optimizer gate (best-effort) + generateFill safety net (SqlException)
- [Phase 08]: supportsRandomAccess=false for all fill cursor tests; ASOF JOIN also routes through fast path
- [Phase 09]: Assert guard (not null-check) at findValue() because pass 1 discovers all keys from the same cursor
- [Phase 09]: Iterative while(true) loop in emitNextFillRow replaces recursive hasNext call; hasNext call sites fall through on false
- [Phase 10]: Propagate fillOffset only for non-timezone queries to avoid double-application of offset
- [Phase 11]: FILL_KEY dispatch extended to all 128/256-bit getters (UUID, Long256, Decimal128/256)
- [Phase 11]: Null sentinels use GeoHashes.*_NULL and Decimals.*_NULL rather than 0 / Numbers.*_NULL so null is distinguishable from legitimate zero
- [Phase 11]: Function ownership transferred from fillValues to constantFillFuncs by nulling source slot to prevent double-close on error path
- [Phase 12]: FallbackToLegacyException: singleton + fillInStackTrace override for zero-allocation retro-fallback signal from codegen to caller
- [Phase 12]: stashedSampleByNode nulled in clear() (not clearSampleBy()) to protect against callers that bypass the sample-by-specific reset
- [Phase 12]: Tier 1 cross-column gate preserves PREV(key_col): LITERAL-source branch passes through regardless of key column type, since the runtime reads key values from keysMapRecord
- [Phase 12]: hasAnyConstantFill predicate uses instanceof NullConstant as the 'real user-supplied constant' distinguisher; NullConstant is the fill-slot sentinel for 'no constant here'
- [Phase 12]: fillModes added as outer-class field on SampleByFillRecordCursorFactory (wired from existing constructor parameter) so hasAnyConstantFill in toPlan can iterate it
- [Phase 12-replace-safety-net-reclassification-with-legacy-fallback-and]: Chain-walk through nested-model chain in each fallback catch is the deterministic two-step action - walks always, asserts on no-stash case
- [Phase 12-replace-safety-net-reclassification-with-legacy-fallback-and]: Grammar rule order: D-08 first (malformed), then D-05 (PREV(ts)), then D-06 (self-normalize) via continue, then D-09 (type-tag), then D-07 (chain) as post-pass
- [Phase 12-replace-safety-net-reclassification-with-legacy-fallback-and]: Nested try/catch at each of three generateFill call sites prevents outer catch (Throwable e) from swallowing FallbackToLegacyException - inner catch runs first before outer can close factory
- [Phase 12]: Plan 12-04 descoped sub-task C (testSampleByFillNeedFix restoration) per user Option A after checkpoint surfaced three pre-existing defects; user takes ownership of defects in a separate phase
- [Phase 12]: 22 new regression tests pin phase 12 production changes (retro-fallback, grammar D-05..D-09, FILL_KEY 128/256-bit dispatch, geo null sentinels, TO-null guard); 42 assertSql conversions per D-10; plan-text refresh across 4 test files
- [Phase 13]: D-02 verdict (a): chain.clear() on SortedRecordCursor.of() reuse sufficient; SortedRecordCursor chosen as vehicle for Plan 02 rowId rewrite
- [Phase 13]: chain.clear() fix shipped as Commit 1 of phase 13 (standalone, backportable, bisectable) per D-03/D-07
- [Phase 13]: Plan 01 accepted as complete with caveat: pre-existing SampleByTest#testSampleByFillNeedFix assertion #2 failure is Plan 05 scope per D-06 (user decision at checkpoint, Option 1)
- [Phase 13]: Plan 02: SampleByFillCursor stores PREV snapshots as chain rowId (one per key or one simplePrevRowId); MapValue schema fixed at 3 LONG slots; FillRecord getters uniformly delegate PREV reads to prevRecord; 10 KIND_* dispatch constants removed
- [Phase 13]: Plan 02: prevRecord aliased once in initialize() at the post-buildChain peek-success branch; single assignment covers both keyed (after pass-1 loop + toTop) and non-keyed (after first hasNext peek) paths — never in of() to avoid recordB repositioning
- [Phase 13]: Plan 02: FILL_KEY for Array/Bin/BinLen kept as pre-existing null/-1 fallthrough (scope-trimmed during verification after two regression tests pinned the old behavior); rowId rewrite preserves var-width FILL_KEY semantics unchanged
- [Phase 13]: Plan 02: Retro-fallback machinery (FallbackToLegacyException, prevSourceCols, isFastPathPrevSupportedType, three try/catch sites) retained; Plan 04 deletes it after Plan 03 validates the fast path across every currently-unsupported type
- [Phase 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi]: Plan 03: 13 per-type FILL(PREV) tests land with assertSql data-correctness only; plan-text assertion deferred to Plan 04 per Option B (preserves D-07 commit sequencing)
- [Phase 13]: Plan 04: retro-fallback machinery (FallbackToLegacyException, stashedSampleByNode, codegen detection, three try/catch sites, optimizer+codegen gates) deleted end-to-end; Chars.contains plan-text style added to 13 Plan-03 per-type tests
- [Phase 13]: Plan 04: testFillPrevSymbolLegacyFallbackNano (SampleByNanoTimestampTest) deleted as a parallel of testFillPrevSymbolLegacyFallback; testFillPrevLong128Fallback retained because it pins compile-time rejection of first(LONG128) independent of fill routing
- [Phase 13]: Plan 04: testSampleByFillNeedFix now passes as a positive side effect of the SYMBOL/STRING unlock — unexpected, Plan 05 scope may shrink; Plan 05 planner to verify absorption or close SEED-002 Defects 1 and 2 against the Plan 02+04 branch state
- [Phase 13]: Branch C fired in Plan 05: Defect 1 (CTE+outer-projection fill corruption) NOT absorbed by Plan 02 rowId rewrite; applied user-approved Option D targeted alias-mapping fix in SqlCodeGenerator.generateFill per CONTEXT.md D-06 full-solution commitment
- [Phase 13]: Assertion #3 snapshot updated in lockstep with Option D fix (Rule 2 deviation): pre-fix buggy positional assignment produced a different observed output than post-fix correct semantic output; updated assertSql snapshot to reflect the corrected output; Plan 06 will convert to master's assertException form
- [Phase 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi]: Plan 06: WR-04 precise chain-rejection position via parallel ObjList<ExpressionNode> with fallback; Defect 3 insufficient-fill grammar check (size > 1 AND size < aggNonKeyCount) placed between per-column loop and chain check; aggNonKeyCount reuses Plan 05's userFillIdx counter
- [Phase 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi]: Plan 06: existing chain-rejection tests testFillPrevRejectMutualChain (pos 55) and testFillPrevRejectThreeHopChain (pos 65) pass unchanged after WR-04 - positions coincide with fillValuesExprs[0].position because chain starts at first aggregate with first fill expr; WR-04 path is exercised via perColFillNodes, not fallback
- [Phase 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi]: Plan 06 deviation (Rule 2): testSampleFillValueNotEnough in SampleByTest and SampleByNanoTimestampTest restored from printSql(silent buggy behavior) to assertException, aligning with master's form and required by Defect 3 grammar landing
- [Phase 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi]: Phase 13 CLOSED: all 6 commits of D-07 landed. SEED-001 WR-01/02/03 obsoleted (Plan 04), WR-04 + Defect 3 shipped (Plan 06). SEED-002 Defect 1 fixed (Plan 05), Defect 2 absorbed (Plan 02). Phase 12 Success Criterion #1 closed with all 3 testSampleByFillNeedFix assertions matching master's form
- [Phase 14]: Hoist factoryColToUserFillIdx above bare/per-column if-else so both branches share alias mapping; bare FILL(PREV) branch classifies via mapping rather than isKeyColumn(factoryIdx,...)
- [Phase 14]: Tighten FILL under-spec broadcast predicate to isBareBroadcastable: PREV requires ExpressionNode.LITERAL type; NULL keyword stays broadcastable; PREV(colX) FUNCTION-typed no longer broadcasts
- [Phase 14]: Cross-column PREV type check uses needsExactTypeMatch flag: full-int equality for DECIMAL (ColumnType.isDecimal covers tag range DECIMAL8..DECIMAL), GEOHASH (ColumnType.isGeoHash), and ARRAY (tag); other types keep tag-level equality
- [Phase 14]: D-16 ghost-test renamed with aggregated-shape assertion: wrap inner FILL(42,42,42) query in SELECT ts, count(*) rows, count_distinct(x) keys FROM (...) GROUP BY ts ORDER BY ts to get a readable 9-row expectation instead of a 4311-row literal
- [Phase 14]: FillRecord.getArray/getBin/getBinLen extended to full 4-branch dispatch (FILL_KEY + cross-column-PREV-to-key + FILL_PREV_SELF/cross-col-PREV + FILL_CONSTANT + default null); closes M-2 silent key drop on ARRAY/BINARY key columns
- [Phase 14]: FillRecord.getInterval added between getInt and getLong with Interval.NULL as default sentinel; FillRecord Javadoc documents getRecord/getRowId/getUpdateRowId as intentionally-unoverridden plumbing; closes M-8 INTERVAL crash
- [Phase 14]: D-14 per-type FILL(PREV) tests land 11 @Test methods; INTERVAL test uses interval(lo, hi) inline key because INTERVAL is non-persistable and has no first(INTERVAL) aggregate; BINARY non-keyed test dropped (no first(BINARY)), coverage via keyed cursor-walk instead
- [Phase 14]: Pre-existing testSampleFillPrevAllTypes and testSampleFillValueAllKeyTypes (SampleByTest + SampleByNanoTimestampTest) expected outputs updated to reflect corrected post-M-2 BINARY rendering on fill rows (key bytes carried, not empty); inline comments updated accordingly
- [Phase 14]: rewriteSampleBy wraps setFillFrom/setFillTo with createToUtcCall when isSubDay AND sampleByTimezoneName is present; hasSubDayTimezoneWrap boolean captures the guard once and drives both wraps, keeping lockstep with the tsFloor FROM wrap at :8308
- [Phase 14]: Inline ternary wrap instead of a new helper method; mirrors the inline style of the other three createToUtcCall call sites (tsFloor at :8308 and FROM/TO fallback at :8462/:8465); no cursor-side change per D-09
- [Phase 14]: M-4 regression tests: Europe/London TIME ZONE during June 2024 (BST UTC+1); SQL clause order is SAMPLE BY <unit> FROM ... TO ... FILL(...) ALIGN TO CALENDAR TIME ZONE '...' (TIME ZONE last); assertQueryNoLeakCheck uses (..., false, false) to match SAMPLE BY FILL factory contract
- [Phase 14]: Plan 04 M-7 ownership fix: assign this.base inside try after RecordTreeChain succeeds AND null this.base in catch before cascaded close() — two-step edit; either step alone is incomplete (step 1 alone still double-frees on a late throw from SortKeyEncoder.createRankMaps; step 2 alone won't compile with final modifier)
- [Phase 14]: Plan 04 D-18 chain-walk helper: testSampleByCursorReleasesMemoryOnClose walks factory.getBaseFactory() with self-loop guard (next == cur) until expectedClass.isInstance match; one helper edit covers all three FILL CALENDAR tests; FIRST OBSERVATION tests and non-FILL CALENDAR test unaffected because their expected class is on the first step of the chain
- [Phase 14]: Plan 04 D-20: PR #6946 ## Trade-offs section gained 2 bullets (M-5 O(unique_keys × buckets) memory envelope, M-6 3-pass scan multiplier); appended after existing 4 bullets per D-19 policy; user approved verbatim at checkpoint; gh pr edit 6946 verified via grep on both bullet substrings
- [Phase 14]: Plan 04 notable flag: M-7 regression test passes pre-fix under idempotent close() — most QuestDB factory classes clear internal pointer state after first free, so latent pre-fix double-free rarely produces observable assertMemoryLeak imbalance; test still locks clean exception-propagation contract + catch-block sequence + canonical config-override-driven throw pattern
- [Phase 15]: Plan 01: C-1+C-2 unified fix mirrors SampleByFillValueRecordCursorFactory.createPlaceHolderFunction:144-173 verbatim in SqlCodeGenerator.generateFill per-column FILL_CONSTANT branch (Chars.isQuoted gate + TimestampDriver.parseQuotedLiteral re-parse + Misc.free on stale slot before setQuick replacement)
- [Phase 15]: Plan 01: M-5 broader fix covers both alias-path and fallback-path ColumnType.isTimestamp guards (broader than D-10 literal wording per RESEARCH recommendation); existing skip-fill 'if (timestampIndex < 0)' path catches non-TIMESTAMP resolution on either path
- [Phase 15]: Plan 01 commit discipline: Tasks 1+2+3 landed as single commit (Fix TIMESTAMP fill constant unit drift, 9df205bac5) per CONTEXT D-02; test restorations cannot pass without the codegen fix and the codegen fix regresses the existing test bodies without restoration, so splitting would be artificial
- [Phase 15]: Plan 02: C-3 fix captures SqlExecutionCircuitBreaker inside of() into new private field; hasNext head + emitNextFillRow outer-loop top each call statefulThrowExceptionIfTripped (throttled / zero-GC on non-trip path)
- [Phase 15]: Plan 02 Rule 1 deviation: M-4 terminal sink.ofRawNull() does not compile because CharSink<?> has no ofRawNull method (only Decimal128/256 sinks do); NullMemoryCMR.getLong256(offset, CharSink) uses empty-body convention to render null as empty text; pre-edit code already correct; documentation comment added
- [Phase 15]: Plan 02: C-3 regression test testFillKeyedRespectsCircuitBreaker transplants ParallelGroupByFuzzTest:4241-4306 tick-counting MillisecondClock CB harness into SampleByFillTest; regression-coverage self-check confirmed test fails under reverted production code within 10 seconds
- [Phase 15]: Plan 03: probe-and-freeze captured Q1/Q2 outputs via temporary probeM7Outputs test using printSql; two literal-key variants got bounded to '2018-01-31' + WHERE x <= 4 + assertQueryNoLeakCheck(false,false) per D-11 and Phase 14 D-15
- [Phase 15]: Plan 03 Rule 4 deviation: computed-key variants (concat('1', s)) stay compile-only because all four bounded shapes trip the SampleByFillCursor.hasNext() defensive guard at line 486; inline comment anchors the defect and keeps pre-Phase-15 coverage until a future phase fixes bucket-grid computation for FUNCTION-typed projections
- [Phase 15]: Plan 04 retro-doc: 15-04-SUMMARY.md consolidates three post-Phase-14 commits (6c2c44237c narrow-decimal FILL_KEY, 2696df1749 decimal128/256 sink null + -ea assert + 2 tests, a986070e43 SampleByFillRecordCursorFactory cleanup); closes ROADMAP Success Criterion #7
- [Phase 15]: Plan 04 cross-link: 2696df1749 established sink.ofRawNull() null contract for Decimal128/256; Plan 02 Task 2 M-4 closure determined CharSink<?> does NOT expose ofRawNull() so getLong256's null contract is 'leave the sink untouched' per NullMemoryCMR.getLong256(offset, CharSink) - recorded inline in SUMMARY to link the two commits
- [Phase 16]: Plan 01: D-02 classifier widening landed verbatim — third continue branch `(FUNCTION || OPERATION) && !functionParser.getFunctionFactoryCache().isGroupBy(ast.token)` slots between existing timestamp_floor continue and the aggregate fall-through; alias assert inside the branch confirms non-timestamp factory resolution under -ea
- [Phase 16]: Plan 01: D-05 aggregate-arm -ea assertion placed immediately after the D-02 branch, before any aggregate-specific logic runs; did NOT fire across 1395 cross-suite tests — residual-arm invariant holds across the entire FILL test surface
- [Phase 16]: Plan 01: D-06 cursor-side wiring verified unchanged per RESEARCH.md trace — SampleByFillRecordCursorFactory.java NOT modified; factoryColToUserFillIdx[-1] for function-key columns flows through both bare-FILL(PREV) and per-column branches to FILL_KEY, picked up by keyColIndices derivation at :3668 automatically
- [Phase 16]: Plan 01: probe-and-freeze captured Interval.NULL rendering as literal `null` string (not empty text), disambiguating RESEARCH.md A2; row order within buckets matches testFillPrevKeyedIndependent precedent — new data row first, prior-discovered key forward-filled second
- [Phase 16]: Plan 01: single commit 82865efbc0 per CONTEXT.md D-05 same-commit rule; 43-char title, no Conventional Commits prefix, long-form body per CLAUDE.md; 5 new regression tests alphabetically placed (testFillNullCastMultiKey, testFillPrevCastMultiKey, testFillPrevConcatMultiKey, testFillPrevConcatOperatorMultiKey, testFillPrevIntervalMultiKey)
- [Phase 17]: Plan 01: pass-1 CB poll at SampleByFillRecordCursorFactory.java:604 shipped standalone — no paired regression test, deviation from plan's Phase 15 D-02 spec approved by user after prior-session checkpoint surfaced that upstream SortedRecordCursor.buildChain() -> AsyncGroupByRecordCursor.buildMap():237 polls the cancellation CB before our :604 poll executes, making a differential test infeasible; commit body documents master-parity (origin/master:SampleByFillPrevRecordCursor.java:171 + SampleByFillValueRecordCursor.java:183) as the defense-in-depth justification (commit f05fa2eb25)
- [Phase 17]: Plan 01: widened SqlCodeGenerator needsExactTypeMatch to include TIMESTAMP and INTERVAL tags; testFillPrevCrossColumnTimestampUnitMismatch pins the Variant A path (TIMESTAMP_MICRO source -> TIMESTAMP_NS target rejection); Variant B (INTERVAL unit mismatch) dropped after DDL spike found no user-facing keyword maps to INTERVAL_TIMESTAMP_NANO (ColumnType.nameTypeMap only exposes 'interval' -> INTERVAL_TIMESTAMP_MICRO) — production widening still covers INTERVAL for future DDL; regression-coverage self-check confirmed reverting two new predicate lines makes test fail within ~3s (commit 889a4676b9)
- [Phase 17]: Plan 02: m1 slot-null + m2 field reorder (single alphabetical block) + m5 rationale comment + m6 Record.getLong256(CharSink) contract comment + m7 QueryModel.toSink0 fillOffset emission landed as one commit (2a4070b851); inline Misc.free(...) ownership-transfer form relies on Misc.free returning the freed object so the slot is nulled synchronously, guarding against a latent double-close if TimestampConstant.newInstance ever throws
- [Phase 17]: Plan 02: m3 PARTIAL refactor gated on new SampleByFillKeyedResetBenchmark (JMH @Param({10,100,1000,10000}) uniqueKeys, @Warmup(3)/@Measurement(5)/@Fork(1)). int[] outputColToKeyPos -> IntList LANDS at 30.5 ns/op vs 47.4 ns/op (1.55x faster, setAll reuses backing array). boolean[] keyPresent -> BitSet REVERTS at 939.6 vs 47.4 ns/op (20x slower at uniqueKeys=1000, way past the 5% gate; BitSet.set()'s per-call wordIndex+checkCapacity+OR dominates even though clear() is O(words)). Benchmark retained in-repo for future BitSet re-evaluation (commit 3dbbbde82d)
- [Phase 17]: Plan 02: m4 FillRecordDispatchTest shipped as standalone file (not inline in SampleByFillTest.java) with 30 @Test methods covering 35 typed-getter names across FILL_KEY / FILL_PREV_SELF / FILL_CONSTANT / cross-col-PREV-to-aggregate / default-null-sentinel dispatch branches. Plan's original synthetic-FillRecord-via-reflection-or-visibility-widening approach dropped per D-20 Claude's Discretion clause: FillRecord is a private class inside a private static class; SQL-level per-getter property tests are more robust to future refactors and don't leak internal dispatch surface. 4 failing scenarios on first draft (bucket keyed ordering, geohash constants, first(long256) returning null) all fixed via ORDER BY wrap / rnd_geohash + count assertion / key-column FILL_KEY path instead of first() aggregate (commit 8838de6801)
- [Phase 17]: Plan 03: testFillNullPushdownEliminatesFilteredKeyFills pins the per-key-domain cartesian contract via both data assertion (s2 emits only its observed bucket; no leading/trailing NULL fills) and multi-line assertPlanNoLeakCheck (filter: s='s2' nested inside Async JIT Group By, not at outer Sample By Fill). Four D-13 cross-reference comments land on testSampleByAlignToCalendarFillNullWithKey1/2 in BOTH SampleByTest and SampleByNanoTimestampTest per RESEARCH.md D-13 4-call-site correction. Three gap-closer tests: m8a testFillWithOffsetAndTimezoneAcrossDstSpringForward (Europe/Riga 2021-03-28 + WITH OFFSET 00:30; 5-row bounded UTC sequence, probe-and-freeze), m8b testFillKeyedSingleRowFromTo (single-row VARCHAR key + FROM/TO; 6 rows with hasKeyPrev false->true transition exactly once, probe-and-freeze), m8c testFillPrevRejectNoArg tightened from contains-any-of to exact-substring + position. Nano mirrors skipped for all four new tests per unit-sensitivity heuristic (mechanisms are unit-agnostic or timestamp-driver independent). SampleByFillTest test count 124 -> 127. Two commits: b2408afc9b (M2 + 4 D-13 comments), 5d1d12451c (m8a/b/c)
- [Phase 17]: Plan 03 deviation (Rule 1 - research bug): m8c exact error message differs from RESEARCH.md D-23 prediction. Actual FILL(PREV()) rejection fires at SqlCodeGenerator.generateFill grammar-rule layer with 'PREV argument must be a single column name' at position 43 (sql.indexOf('PREV(')), not ExpressionParser's 'too few arguments for PREV [found=0,expected=1]' at the same position. The grammar rule takes precedence because PREV is a fill-spec keyword whose argument shape is validated by generateFill before the parser's arity check runs. Test uses actual production message; m8c's intent (replace fuzzy contains-any-of with exact substring + exact position) fully preserved; deviation recorded in 17-03-SUMMARY.md and commit body
- [Phase 17]: Plan 04 (PR body + title + D-28 todo retirement) closed the drift-audit cycle. Five PR body edits landed via gh pr edit: D-11 M1 pre-1970 row in "What's covered" block 2, D-03 K x B bullet rewrite with cancellation-CB disambiguation parenthetical, D-05 two stale M3.4 sentence rewrites (Implementation-notes pitfall + "ALIGN TO CALENDAR WITH OFFSET + FILL without TO" row), D-06 drop of both test-count lines from Test plan. Title renamed per D-24 to "feat(sql): add cross-column FILL(PREV) and move SAMPLE BY FILL onto fast path". D-28 todo moved from pending to completed with completed_at + completed_in: 17-01-PLAN.md YAML fields. Single commit 4529631666 captures the todo move (PR body + title are external artefacts, no git commits). Optional "sub-day + TZ + FROM + OFFSET" row NOT added per RESEARCH.md D-05 master-support verdict (master's cursor path supported the shape via SampleByFillNullRecordCursorFactory timezone+offset constructor args; under D-01 rule (a) fast-path participation is an implementation detail, not net-new behavior)
- [Phase 17]: Plan 04 deviation (Rule 2 - phase-wide ASCII normalization): Current PR body contained 41 pre-existing non-ASCII bytes outside the five Plan 04 edit sites (34 em-dashes U+2014, 2 Unicode multiplication signs U+00D7, 4 right-arrows U+2192, 1 robot emoji U+1F916). The plan's acceptance criterion "non-ASCII byte count on updated body == 0" is only satisfiable by normalizing the whole body, not just the new/rewritten text. Applied Unicode -> ASCII mapping table across the entire body via Python script; final body is pure ASCII (verified via LC_ALL=C byte scan). Robot emoji footer dropped entirely; ASCII "Generated with Claude Code" link text preserved
- [Phase 17]: Plan 04 deviation (Rule 4 - SC#8 deferral): ROADMAP SC#8 (/review-pr 6946 re-run confirming all items closed or closed-under-D-01) deferred to post-commit manual step. Invoking the review-pr skill within Plan 04's single-task scope would require parallel-agent spawning outside the plan's atomic-commit boundary; deferred with explicit SUMMARY note pointing at the pending user action. All 11 Phase 17 findings are addressed in code/tests/body with the expected D-01 classification; SC#8 re-run should confirm zero regressions

### Roadmap Evolution

- Phase 6 added: Keyed fill with FROM/TO range (was incorrectly listed as out of scope)
- Phase 11 added retroactively via `/gsd-add-phase hardening`; code landed first (commits `2125201f30`, `28b00e8340`, `a6355c3e65`), paper trail back-filled via `/gsd-forensics` on 2026-04-14
- Phase 5 closed as "Absorbed by 7–10" on 2026-04-14 — its four `must_have.truths` were satisfied cumulatively by the finer-grained phases rather than as a single batch
- Phase 12 added: Replace safety-net reclassification with legacy fallback and tighten optimizer PREV gate. Driven by `/review-pr` finding that the codegen safety-net silently rewrites expression-argument aggregates with unsupported output types as `FILL_KEY`, producing duplicated fill rows (see `testSampleByFillNeedFix` regression vs master). Also closes LONG128/INTERVAL gap between `isUnsupportedPrevType` and `isFastPathPrevSupportedType`. Scope expanded to include missing regression tests for phase-11 production changes (UUID / Long256 / Decimal128 / Decimal256 key-column FILL_KEY dispatch, geo "no-prev-yet" null sentinels, selective `assertSql→assertQueryNoLeakCheck` conversions), removal of the redundant `anyPrev` detection loop, `hasExplicitTo` guard for runtime-null `TO`, alphabetizing FillRecord getters / SampleByFillCursor members / imports / `isKeyColumn`, replacing FQN signatures with plain imports, emitting `fill=` unconditionally in `toPlan`, and asserting on `Dates.parseOffset` failure. PR metadata (title, body tone, `.planning/` diff noise) excluded — tracked separately
- Phase 13 added: Migrate FILL(PREV) snapshots from materialized values to rowId-based replay. Borrowed verbatim from branch `sm_fill_prev_fast_all_types` (their phase 12; research complete with GO verdict, candidate a). Replaces per-type `simplePrev[]`/MapValue snapshot dispatch in `SampleByFillRecordCursorFactory` with a single chain rowId per key, read lazily via `baseCursor.recordAt(prevRecord, prevRowId)`. Prerequisite `SortedRecordCursor.chain.clear()` fix ships as its own independent commit. Seeds SEED-001 and SEED-002 auto-surface during `/gsd-plan-phase 13` with trigger `rowId migration phase is planned` — bucket-1 plumbing follow-ups (WR-01..WR-04, Defect 3) to be re-sorted as either fixed-here-alongside or obsoleted-if-retro-fallback-deleted; bucket-2 cursor defects (toTop state corruption, CTE-wrap projection corruption) to be verified as absorbed by the rewrite
- Phase 14 added: Fix Moderate-severity findings from `/review-pr 6946` (M-1..M-4, M-7, M-8, M-9; plus Mn-13 leak on success path). M-5 (SortedRecordCursor memory) and M-6 (triple-pass scan) are NOT in scope — surface as explicit Trade-offs / Future work items in PR #6946 description. Test ideas borrowed from Mn-11: per-type FILL(PREV) for BOOLEAN/BYTE/SHORT/INT/LONG/DATE/TIMESTAMP/IPv4/BINARY/INTERVAL, Decimal precision/scale mismatch rejection, ARRAY/GEOHASH dimensionality/bit-width rejection, rename or remove ghost test `testSampleByFromToIsDisallowedForKeyedQueries`, replace fuzzy 4-message assertion in `testFillPrevRejectNoArg`.
- Phase 15 added: Address second-pass `/review-pr 6946` findings that surfaced after Phase 14 closed. In scope: 3 Critical findings (C-1 TIMESTAMP fill constant 1000× unit-conversion bug hidden by an altered test expectation in `testTimestampFillNullAndValue`; C-2 silent acceptance of unquoted numeric fill values for TIMESTAMP columns hidden by `testTimestampFillValueUnquoted` losing its `assertException`; C-3 missing `SqlExecutionCircuitBreaker` check in keyed fill emission — regression vs. legacy cursor-path cursors) plus 3 Moderate findings worth folding in (M-4 `getLong256(int, CharSink)` missing terminal `sink.ofRawNull()` fallthrough matching the getDecimal128/256 pattern; M-5 `timestampIndex` fallback in `generateFill` not verifying the resolved column is actually TIMESTAMP; M-7 `testSampleByFromToParallelSampleByRewriteWithKeys` lost its output assertion when the keyed FROM-TO behavior flipped from reject to succeed). Retro-documents 3 post-Phase-14 non-phase commits (`6c2c44237c` narrow-decimal FILL_KEY coverage, `2696df1749` decimal128/256 sink null fall-through fix + `-ea` assert + 2 tests, `a986070e43` SampleByFillRecordCursorFactory clean-up). Out of scope: M-6 (`CairoException.critical` guard test + multi-worker test — larger harness work) and Minor items (dead `isKeyColumn`, em-dashes, PR title, `.planning/` diff noise — house-keeping for merge time).
- Phase 16 added (2026-04-21): Fix multi-key FILL(PREV) with inline FUNCTION grouping keys. Promoted from the 2026-04-21 TODO captured during Phase 15's defensive-assertion experiment. Scope: close the cartesian-drop bug in `SqlCodeGenerator.generateFill` classifier where non-aggregate FUNCTION grouping keys (`interval(lo, hi)`, `concat(a, b)`, `cast(x AS STRING)`) slip into the aggregate arm, dispatched as FILL_PREV_SELF instead of FILL_KEY. Single-key `testFillPrevInterval` hides the bug. Empirical probe (2 distinct interval keys, 3 buckets) produces 3 rows instead of expected 6. Two fix options (local classifier widening vs. upstream canonicalization in `rewriteSampleBy`) to be decided at `/gsd-discuss-phase 16`. Land defensive assertion as final hardening once classifier is correct.
- Phase 17 added (2026-04-22): Address `/review-pr 6946` follow-ups — whole plan (2 Moderate + 9 Minor findings), with M3 requiring mandatory discuss+plan research because each of its four landed-commit sub-items may require a code fix or new test rather than a pure PR-body edit. Canonical scope document: `~/.claude/plans/let-s-discuss-issues-one-gentle-elephant.md`. Covers M1 (pre-1970 guard doc), M2 (predicate-pushdown test), M3 (four stale commits — `9df205bac5` TIMESTAMP unit drift + alias guard, `c1deb9b14d` cancellation CB regression, `82865efbc0` phase-16 FUNCTION classifier, `289d43090a` TZ+FROM+OFFSET fill-grid; one factually-wrong Implementation-notes sentence; stale test counts 110→123 / 278→279), m1 `SqlCodeGenerator.java:3654` slot-null fix, m2 `SampleByFillCursor` field ordering, m3 `int[]`/`boolean[]`→`IntList`/`BitSet` refactor with benchmark, m4 `FillRecord` dispatch property test, m5 assert-vs-throw rationale comment, m6 `getLong256(CharSink)` contract comment, m7 `QueryModel.toSink0` fillOffset, m8 test gaps (DST spring-forward + single-row keyed+FROM/TO + `testFillPrevRejectNoArg` tightening), m9 PR title rename. Close once `/review-pr 6946` shows all items closed or explicitly deferred.

### Pending Todos

- _Completed 2026-04-21 via Phase 16 Plan 01_: Fix multi-key FILL(PREV) with inline FUNCTION grouping keys — landed in commit 82865efbc0.
- _Completed 2026-04-22 via Phase 17 Plan 01 commit 889a4676b9_: Reject cross-column FILL(PREV) across TIMESTAMP and INTERVAL unit mismatches — widened needsExactTypeMatch + Variant A regression test landed. Variant B (INTERVAL DDL) deferred: no user-facing DDL keyword maps to INTERVAL_TIMESTAMP_NANO; production widening covers INTERVAL for future DDL. D-28 retirement landed in Phase 17 Plan 04 commit 4529631666; todo moved from .planning/todos/pending/ to .planning/todos/completed/ with completed_at: 2026-04-22 + completed_in: 17-01-PLAN.md.
- Upgrade `SqlOptimiserTest#testSampleByFromToKeyedQuery` from `printSql` smoke test to plan assertion (or delete) — captured 2026-04-22 from `/review` finding 4.1; not covered by phase 17. See `.planning/todos/pending/2026-04-22-upgrade-sqloptimisertest-testsamplebyfromtokeyedquery-from-p.md`.
- Tighten SampleByFillTest CB field reset and constructor-throw exception assertion — captured 2026-04-22 from `/review` findings 4.6+4.7; not covered by phase 17. See `.planning/todos/pending/2026-04-22-tighten-samplebyfilltest-cb-field-reset-and-constructor-thro.md`.

### Blockers/Concerns

None blocking merge. Open pre-merge cleanup items:

- `/review-pr` has not been re-run since the phase 11 commits landed (success criterion #7 unverified)
- PR #6946 body references `FillPrevRangeRecordCursorFactory`, which no longer exists — update before merge
- PR #6946 missing `Performance` label

## Session Continuity

Last session: 2026-04-22T18:15:00.000Z
Stopped at: Phase 17 Plan 04 complete -- todo move commit 4529631666 on sm_fill_prev_fast_path; PR #6946 body (5 edits) + title renamed via external gh edits; all 4 phase 17 plans landed. SC#8 (/review-pr 6946 re-run confirming all items closed or closed-under-D-01) pending user action.
Resume file: N/A (phase complete); next action is user-run /review-pr 6946
