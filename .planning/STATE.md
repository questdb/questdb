---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
status: Not planned — ready for /gsd-plan-phase 15
stopped_at: Completed 14-04-PLAN.md — Phase 14 ready for verification
last_updated: "2026-04-21T13:15:23.489Z"
last_activity: 2026-04-21
progress:
  total_phases: 15
  completed_phases: 13
  total_plans: 24
  completed_plans: 24
  percent: 100
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-09)

**Core value:** SAMPLE BY FILL queries execute on the GROUP BY fast path with identical output to the cursor path, enabling parallel execution.
**Current focus:** Phase 15 — address-pr-6946-review-findings-and-retro-fixes

## Current Position

Phase: 15
Plan: Not started
Status: Not planned — ready for /gsd-plan-phase 15
Last activity: 2026-04-21

Progress: [#########-] 93%

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

### Roadmap Evolution

- Phase 6 added: Keyed fill with FROM/TO range (was incorrectly listed as out of scope)
- Phase 11 added retroactively via `/gsd-add-phase hardening`; code landed first (commits `2125201f30`, `28b00e8340`, `a6355c3e65`), paper trail back-filled via `/gsd-forensics` on 2026-04-14
- Phase 5 closed as "Absorbed by 7–10" on 2026-04-14 — its four `must_have.truths` were satisfied cumulatively by the finer-grained phases rather than as a single batch
- Phase 12 added: Replace safety-net reclassification with legacy fallback and tighten optimizer PREV gate. Driven by `/review-pr` finding that the codegen safety-net silently rewrites expression-argument aggregates with unsupported output types as `FILL_KEY`, producing duplicated fill rows (see `testSampleByFillNeedFix` regression vs master). Also closes LONG128/INTERVAL gap between `isUnsupportedPrevType` and `isFastPathPrevSupportedType`. Scope expanded to include missing regression tests for phase-11 production changes (UUID / Long256 / Decimal128 / Decimal256 key-column FILL_KEY dispatch, geo "no-prev-yet" null sentinels, selective `assertSql→assertQueryNoLeakCheck` conversions), removal of the redundant `anyPrev` detection loop, `hasExplicitTo` guard for runtime-null `TO`, alphabetizing FillRecord getters / SampleByFillCursor members / imports / `isKeyColumn`, replacing FQN signatures with plain imports, emitting `fill=` unconditionally in `toPlan`, and asserting on `Dates.parseOffset` failure. PR metadata (title, body tone, `.planning/` diff noise) excluded — tracked separately
- Phase 13 added: Migrate FILL(PREV) snapshots from materialized values to rowId-based replay. Borrowed verbatim from branch `sm_fill_prev_fast_all_types` (their phase 12; research complete with GO verdict, candidate a). Replaces per-type `simplePrev[]`/MapValue snapshot dispatch in `SampleByFillRecordCursorFactory` with a single chain rowId per key, read lazily via `baseCursor.recordAt(prevRecord, prevRowId)`. Prerequisite `SortedRecordCursor.chain.clear()` fix ships as its own independent commit. Seeds SEED-001 and SEED-002 auto-surface during `/gsd-plan-phase 13` with trigger `rowId migration phase is planned` — bucket-1 plumbing follow-ups (WR-01..WR-04, Defect 3) to be re-sorted as either fixed-here-alongside or obsoleted-if-retro-fallback-deleted; bucket-2 cursor defects (toTop state corruption, CTE-wrap projection corruption) to be verified as absorbed by the rewrite
- Phase 14 added: Fix Moderate-severity findings from `/review-pr 6946` (M-1..M-4, M-7, M-8, M-9; plus Mn-13 leak on success path). M-5 (SortedRecordCursor memory) and M-6 (triple-pass scan) are NOT in scope — surface as explicit Trade-offs / Future work items in PR #6946 description. Test ideas borrowed from Mn-11: per-type FILL(PREV) for BOOLEAN/BYTE/SHORT/INT/LONG/DATE/TIMESTAMP/IPv4/BINARY/INTERVAL, Decimal precision/scale mismatch rejection, ARRAY/GEOHASH dimensionality/bit-width rejection, rename or remove ghost test `testSampleByFromToIsDisallowedForKeyedQueries`, replace fuzzy 4-message assertion in `testFillPrevRejectNoArg`.
- Phase 15 added: Address second-pass `/review-pr 6946` findings that surfaced after Phase 14 closed. In scope: 3 Critical findings (C-1 TIMESTAMP fill constant 1000× unit-conversion bug hidden by an altered test expectation in `testTimestampFillNullAndValue`; C-2 silent acceptance of unquoted numeric fill values for TIMESTAMP columns hidden by `testTimestampFillValueUnquoted` losing its `assertException`; C-3 missing `SqlExecutionCircuitBreaker` check in keyed fill emission — regression vs. legacy cursor-path cursors) plus 3 Moderate findings worth folding in (M-4 `getLong256(int, CharSink)` missing terminal `sink.ofRawNull()` fallthrough matching the getDecimal128/256 pattern; M-5 `timestampIndex` fallback in `generateFill` not verifying the resolved column is actually TIMESTAMP; M-7 `testSampleByFromToParallelSampleByRewriteWithKeys` lost its output assertion when the keyed FROM-TO behavior flipped from reject to succeed). Retro-documents 3 post-Phase-14 non-phase commits (`6c2c44237c` narrow-decimal FILL_KEY coverage, `2696df1749` decimal128/256 sink null fall-through fix + `-ea` assert + 2 tests, `a986070e43` SampleByFillRecordCursorFactory clean-up). Out of scope: M-6 (`CairoException.critical` guard test + multi-worker test — larger harness work) and Minor items (dead `isKeyColumn`, em-dashes, PR title, `.planning/` diff noise — house-keeping for merge time).

### Pending Todos

None.

### Blockers/Concerns

None blocking merge. Open pre-merge cleanup items:

- `/review-pr` has not been re-run since the phase 11 commits landed (success criterion #7 unverified)
- PR #6946 body references `FillPrevRangeRecordCursorFactory`, which no longer exists — update before merge
- PR #6946 missing `Performance` label

## Session Continuity

Last session: 2026-04-20T15:52:07.980Z
Stopped at: Completed 14-04-PLAN.md — Phase 14 ready for verification
Resume file: None
