# SUBSAMPLE-as-window Phase 4b: window-framework upgrades to narrow the cursor gap

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Narrow the measured ~2.7–3.9× gap between the keep-flag window path and the old SUBSAMPLE cursor by two general `CachedWindowLight` upgrades — (#1) eliminate `pass2`'s redundant per-row base re-read, and (#3) fuse the `WHERE __keep` filter so the window cursor emits only the selected rows. Each upgrade is benchmarked in isolation (decompose harness) before the next. Then (Phase 4c, separate) pursue full parity (#4 row-selecting mode) if a material gap remains.

**Evidence driving this (decompose m4 @1M):** raw scan 4.0ms; cursor 7.4ms; window_compute 21.6ms (≈90% of the gap); window_full 26.3ms (filter +4.6ms). Compute breakdown: per-row `narrowChain` record + a `pass2` loop that `recordAt`-re-reads all N base rows (only to recompute `isNullRow`) + a separate `FilteredRecordCursor` pass. The cursor does one sequential compact pass + select + emit kept.

**Architecture:** These are GENERAL window-framework changes (help every window function, gated by capability so non-participating functions are unaffected). #1 adds a `WindowFunction` capability to skip base repositioning in `pass2` when the function doesn't read the base record; the keep-flag functions record pass1's null-pattern so pass2 needs no record. #3 detects the desugared `WHERE <single selecting-window-boolean>` shape and executes a fused cursor that yields only kept rows (no per-row boolean materialization, no separate filter).

**Tech Stack:** Java, QuestDB griffin window framework, JMH, JUnit4.

## Global Constraints

- JDK 25 at `/usr/lib/jvm/java-25-openjdk-amd64`; build/test from `/home/nick/claude/wt/oss/subsample-fixes`.
- **Correctness first, byte-identical:** every existing test stays green — `SubsampleTest` (162), the six `*WindowFunctionTest`s, and the broad window suite. The kill-switch A/B and the benchmark's in-run correctness guard (window row-count == cursor row-count) must still hold. A behavior change is a FAIL, not an accepted trade.
- **Capability-gated, non-invasive:** default behavior for all other window functions is unchanged. New `WindowFunction` methods are `default` and conservative (the safe/old path).
- Benchmark every task with `SubsampleWindowDecomposeBenchmark` (and the full `SubsampleWindowVsCursorBenchmark` grid at the end) using the standard `--add-exports/--add-opens` set passed to the outer `java` AND via `-jvmArgsAppend` (see `.superpowers/sdd/phase4a-benchmark-report.md`).
- Do NOT touch the old cursor, the desugaring gates, or the kill-switch. Do NOT change the algorithms' selection math.

**Commands:**
```bash
cd /home/nick/claude/wt/oss/subsample-fixes && export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64
mvn -q -pl core -am compile -DskipTests
mvn -q -pl core -Dtest='SubsampleTest,M4WindowFunctionTest,MinMaxWindowFunctionTest,LttbWindowFunctionTest,UniformWindowFunctionTest,CadenceWindowFunctionTest,SdtWindowFunctionTest' test
mvn -q -pl benchmarks -am package -DskipTests
```

---

## Task 1: Upgrade #1 — eliminate pass2's redundant base re-read

**Files:**
- Modify: `core/.../engine/window/WindowFunction.java` (new capability), `core/.../engine/window/CachedWindowLightRecordCursorFactory.java` (skip `positionRecordABaseOnly` when the function opts out), `core/.../engine/functions/window/M4FunctionFactory.java` (the shared `BucketSelectWindowFunction`: record pass1 null-pattern, use it in pass2, opt out of base re-read).
- Test: `core/.../test/.../engine/window/M4WindowFunctionTest.java` (+ existing coverage).

**Interfaces:**
- Produces: `WindowFunction.pass2NeedsBaseRecord()` default `true`.

- [ ] **Step 1: Read the mechanism.** `CachedWindowLightRecordCursorFactory.computeWindow` — the `unordered2PassFunctions` pass2 loop calls `positionRecordABaseOnly(rIdx)` (=`baseCursor.recordAt(...)`, random access) then `pass2(record,...)` for every row. `BucketSelectWindowFunction.pass2` uses `record` ONLY via `isNullRow(record)` (to keep `pass2Ordinal` aligned with pass1's non-null count). `pass1` already sees every row's null-ness. Confirm the ordered2Pass and other loops so the capability is applied consistently.
- [ ] **Step 2: Failing test.** In `M4WindowFunctionTest`, add `testPass2NoBaseReadByteIdentical`: a table with interleaved NULL/NaN values + normal values, assert `m4(ts,v,target) OVER (ORDER BY ts)` keep-set (and the `SUBSAMPLE m4` cross-check) is byte-identical before/after — this pins that removing the pass2 re-read does not change which rows are kept (esp. around nulls, the alignment-sensitive case). RED only once the impl is stubbed to a wrong path; otherwise it's a guard that must stay green.
- [ ] **Step 3: Add the capability.** `WindowFunction.pass2NeedsBaseRecord()` `default { return true; }`. Document: return false only if `pass2` never reads the base `Record` argument.
- [ ] **Step 4: Record pass1 null-pattern in `BucketSelectWindowFunction`.** In `pass1`, in addition to buffering non-null (ts,value), record per-absolute-row null-ness so pass2 needs no record. Options: a `DirectLongList` bitset sized to the row count (1 bit/row), OR store null run-structure. pass1 is called once per row in absolute order, so appending a bit per row is O(1). Reset/reopen must clear it; free native memory in `close()`/`reset()` (mirror the `selected`/gap-scratch lifecycle — see the lttb native-leak fix).
- [ ] **Step 5: Rewrite `pass2` to use the cached null-pattern**, not `isNullRow(record)`. Same keep logic; `record` unused. Override `pass2NeedsBaseRecord()` → `false`.
- [ ] **Step 6: Skip repositioning in the factory.** In `CachedWindowLightRecordCursorFactory`, in the unordered2Pass (and ordered2Pass, for consistency/safety) pass2 loops, call `positionRecordABaseOnly(rIdx)` only when at least one function in the group returns `pass2NeedsBaseRecord()==true`; otherwise skip it. Keep the record valid where any function still needs it.
- [ ] **Step 7: GREEN + measure.** Full test set green (byte-identical). Rebuild benchmarks; run `SubsampleWindowDecomposeBenchmark` — record the new `window_compute`/`window_full` vs the 21.6/26.3 baseline. Note the isolated delta in the report.
- [ ] **Step 8: Commit** `perf(window): skip pass2 base re-read for keep-flag functions (cache pass1 null-pattern)` + record the measured delta.

---

## Task 2: Upgrade #3 — fuse the keep-filter (emit only selected rows)

**Files:**
- Modify: `core/.../griffin/SqlCodeGenerator.java` (detect the desugared `WHERE <selecting-window-boolean>` shape and build a fused cursor) and/or a new `core/.../engine/window/CachedWindowSelectRecordCursorFactory.java`; `core/.../engine/window/WindowFunction.java` (a capability to expose the selected rows, e.g. `getSelectedRowIds()` / `isRowSelecting()`); the keep-flag functions to expose their `selected` set mapped to absolute rows.
- Test: `SubsampleTest.java` (plan + byte-identity), the `*WindowFunctionTest`s.

**Interfaces:**
- Consumes Task 1. Produces: a fused execution for the `SELECT cols FROM (SELECT cols, keepfn() OVER(...) __keep FROM src) WHERE __keep` shape when `keepfn` is the sole selecting keep-flag window function and `__keep` is exactly the filter.

- [ ] **Step 1: Characterize the fuse target.** Read `desugarSubsample` (`SqlOptimiser.java` ~9727): the shape is outer-project → `filterModel WHERE __keep_subsample` → inner window model with one `WindowExpression` whose function is the keep-flag fn. Confirm how codegen currently builds `Filter(__keep) → CachedWindowLight`.
- [ ] **Step 2: Add the row-selecting capability.** `WindowFunction.isRowSelecting()` `default false`; for selecting functions, a way to iterate the kept absolute row indices after `preparePass2` (the `selected` buffer holds non-null buffer ordinals; map to absolute rows using the pass1 null-pattern from Task 1 + the buffered rowIds). Keep-flag functions override.
- [ ] **Step 3: Failing plan+data test.** `testSdt/​M4FusedSelectByteIdentical`: the desugared query's rows are byte-identical to Task-0 behavior, and the plan shows the fused node (no separate `Filter`, no `__keep` boolean column). Fill actual plan after implementing.
- [ ] **Step 4: Implement the fused cursor.** When codegen sees the fuse target, build a cursor that runs the window compute (buffer+pass1+preparePass2) then emits ONLY the selected absolute rows (random-access the base for the kept rows, like the cursor does), skipping per-row boolean materialization and the separate filter. Fall back to the existing `CachedWindowLight + Filter` for any shape that doesn't match exactly (multiple window fns, extra filter terms, non-selecting fn) — conservative pattern match.
- [ ] **Step 5: GREEN + measure.** All tests byte-identical; run `SubsampleWindowDecomposeBenchmark` + the full `SubsampleWindowVsCursorBenchmark` grid. Record window-vs-cursor after #1+#3 vs the Phase-4a baseline.
- [ ] **Step 6: Commit** `perf(window): fuse keep-flag filter into a row-selecting window cursor` + record the measured gap.

---

## Task 3: Consolidated measurement + decision input

- [ ] **Step 1:** Re-run the full `SubsampleWindowVsCursorBenchmark` grid (100k/1M + 10M m4/lttb point) with #1+#3 applied.
- [ ] **Step 2:** Update `.superpowers/sdd/window-upgrades-analysis.md` with the before/after table and the remaining gap. State whether #4 (full row-selecting parity, Phase 4c) is warranted or the gap is now acceptable to flip the default.
- [ ] **Step 3: Commit** the updated analysis.

---

## Self-Review

**Spec coverage:** #1 (pass2 re-read elimination) + #3 (filter fusion) with per-task isolated measurement, byte-identical throughout, capability-gated so other window functions are unaffected. #4 deferred to Phase 4c pending #1+#3 results.

**Placeholder scan:** plan/measurement pastes are deliberate fill-from-run steps. The capability methods and the fuse pattern-match are concrete; fall-through is conservative (unmatched shapes keep the existing path).

**Type consistency:** `pass2NeedsBaseRecord()` (default true) and `isRowSelecting()` (default false) are conservative `WindowFunction` defaults; keep-flag functions override. pass1 null-pattern is native memory with the same reset/reopen/close lifecycle as `selected`. Fused cursor falls back to `CachedWindowLight + Filter` for non-matching shapes.

## Execution Handoff
(Provided after user review.)
