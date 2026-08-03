# SUBSAMPLE-as-window Phase 4c: narrowChain elimination + fuse sdt

> **Historical phase plan:** This body records the final pre-retirement optimization phase. The authoritative final state is the completed [Phase 5 plan](2026-07-22-subsample-window-phase5-delete-cursor.md): SUBSAMPLE is window-only and further pass1 optimization is deferred.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Push the fused keep-flag window path closer to cursor parity by (Task 1) skipping the per-row `narrowChain` materialization on the fused path — the last per-row overhead the value-inspecting m4/minmax/lttb still pay — and (Task 2) giving `sdt` a row-selecting exposure so `SUBSAMPLE sdt` also fuses (it is currently unfused, ~3×).

**State after Phase 4b (#1+#3):** window=fused vs cursor — uniform 1.2×, cadence 1.25×, lttb 1.84×, minmax 1.96×, m4 1.95× (was 3.2–3.9×). Fused path emits only kept rows via `CachedWindowLightRecordCursorFactory`'s row-selecting mode (gated on the desugar-only `subsampleKeepFlag` marker). sdt stays on the unfused `CachedWindowLight + Filter` path (its `SwingingDoor` writes a per-row keep-byte to a `MemoryARW`, no `selected`/`nullBits`).

**Architecture:** Task 1 — in fused row-selecting mode the window function's boolean output is dropped (only kept rows are emitted), so the per-row `narrowChain.beginRecord()` in the buffering loop materializes an output record that is never read. Skip it on the fused path (contained; the non-fused path keeps materializing as today). Task 2 — sdt already knows its full keep-set after its single forward pass (keep-bytes in the buffer, finalized by `preparePass2`); expose the kept ABSOLUTE row indices via the same `isRowSelecting()`/`getSelectedRows`-style contract the `BucketSelectWindowFunction` family uses, so codegen fuses `SUBSAMPLE sdt` identically.

**Tech Stack:** Java, QuestDB griffin window framework, JMH, JUnit4.

## Global Constraints

- JDK 25 at `/usr/lib/jvm/java-25-openjdk-amd64`; build/test from `/home/nick/claude/wt/oss/subsample-fixes`.
- **Byte-identical, correctness first:** every existing test stays green (`SubsampleTest` 165, all `*WindowFunctionTest`, `SqlOptimiserTest`, `WindowFunctionTest` 636). The fusion Critical from Phase 4b (only the desugar-marked `__keep` fuses) MUST remain closed — do not widen the fuse gate. A behavior change is a FAIL.
- **Contained + gated:** Task 1 changes ONLY the fused (rowSelecting && subsampleKeepFlag) buffering path; the ordinary window path is untouched and byte-identical. Task 2 adds sdt to the SAME fused contract; sdt's unfused behavior when the marker is absent stays exactly as today.
- Benchmark each task with `SubsampleWindowDecomposeBenchmark` and the `SubsampleWindowVsCursorBenchmark` grid, with the standard `--add-exports/--add-opens` to outer `java` AND `-jvmArgsAppend` (see `.superpowers/sdd/phase4a-benchmark-report.md`).
- Do NOT touch the old cursor, the desugaring gate, the kill-switch, or the `SwingingDoor` algorithm math.

**Commands:**
```bash
cd /home/nick/claude/wt/oss/subsample-fixes && export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64
mvn -q -pl core -am compile -DskipTests
mvn -q -pl core -Dtest='SubsampleTest,M4WindowFunctionTest,MinMaxWindowFunctionTest,LttbWindowFunctionTest,UniformWindowFunctionTest,CadenceWindowFunctionTest,SdtWindowFunctionTest,SqlOptimiserTest,WindowFunctionTest' test
mvn -q -pl benchmarks -am package -DskipTests
```

---

## Task 1: Skip the per-row narrowChain on the fused path

**Files:** Modify `core/.../engine/window/CachedWindowLightRecordCursorFactory.java`. Test `M4WindowFunctionTest`/`SubsampleTest`.

**Interfaces:** Consumes the Phase-4b fused row-selecting mode.

- [ ] **Step 1: Confirm narrowChain is unread on the fused path.** Read `computeWindow` (buffering loop `narrowChain.beginRecord()`), the fused output cursor (`hasNext`/`positionRecordA`/`getRecord`), and `WindowLightRecord` — verify that in fused (rowSelecting && subsampleKeepFlag, single function whose boolean output is dropped) mode NOTHING reads a narrowChain column (the projected columns resolve to base columns; the keep column is dropped). If ANY read exists, that read must be re-sourced or the task falls back — report it.
- [ ] **Step 2: Byte-identity guard test.** In `SubsampleTest`/`M4WindowFunctionTest`: interleaved NULL/NaN + normal rows; assert `SUBSAMPLE m4/minmax/lttb(...)` rows byte-identical before/after (this is the safety net for skipping narrowChain). Keep green throughout.
- [ ] **Step 3: Skip beginRecord() + allocation on the fused path.** In `computeWindow`'s buffering loop, when the cursor is in fused row-selecting mode, do NOT call `narrowChain.beginRecord()` (and avoid allocating/growing narrowChain). Keep `baseRowIds.add(...)` and `pass1(...)` (the function still buffers ts+value). Guard by the existing fused-mode flag; the non-fused branch is unchanged. Ensure `recordA.of(...)`/record wiring tolerates the unused narrowChain in fused mode (or is only wired in the non-fused branch).
- [ ] **Step 4: GREEN + measure.** Full test set byte-identical. Rebuild benchmarks; run `SubsampleWindowDecomposeBenchmark` + `SubsampleWindowVsCursorBenchmark -p method=m4,minmax,lttb -p rows=1000000 -f 1 -wi 3 -i 5`. Record m4/minmax/lttb window vs cursor before(≈1.9×)→after.
- [ ] **Step 5: Commit** `perf(window): skip narrowChain materialization on the fused keep-flag path` + measured delta.

---

## Task 2: Fuse `SUBSAMPLE sdt` (row-selecting exposure for the swinging-door function)

**Files:** Modify `core/.../engine/functions/window/SdtWindowFunctionFactory.java` (both non-partitioned + partitioned functions, or scope to non-partitioned + document); possibly `WindowFunction`/`BaseWindowFunction` if a shared helper fits. Test `SdtWindowFunctionTest`/`SubsampleTest`.

**Interfaces:** Produces: sdt implements `isRowSelecting()==true` + exposes kept absolute rows, so codegen fuses `SUBSAMPLE sdt` via the SAME `tryFuseKeepFlagFilter` path (no codegen change — it already keys on `isRowSelecting() && isSubsampleKeepFlag()`).

- [ ] **Step 1: Read sdt's structure.** `SdtWindowFunctionFactory` — the forward `SwingingDoor` pass writes a per-row keep-byte to a buffer (`MemoryARW`); `getPassCount()==TWO_PASS`; pass2 materializes the buffer→BOOLEAN chain. Confirm the keep-set is fully finalized after pass1/`preparePass2` (the eager-tentative-marking + back-patch converges by end of input). Identify how to read the per-row keep-byte at absolute row index.
- [ ] **Step 2: Byte-identity test.** In `SubsampleTest`: `SUBSAMPLE sdt(price, 0.5)` rows byte-identical to the current (unfused) output, over data with a null gap (RESPECT-NULLS flush) + normal rows; AND the plan now shows the fused node (no separate Filter/`__keep`). Fill actual plan after implementing.
- [ ] **Step 3: Implement the row-selecting exposure.** Override `isRowSelecting()==true`; expose kept ABSOLUTE row indices (iterate the finalized keep-byte buffer 0..N, collect positions where keep==1) via the same contract `BucketSelectWindowFunction` uses (match the method name/shape codegen + the fused cursor already call — read how M4's `getSelectedRows` is consumed). Preserve the exact keep-set the current pass2 produces (including the null-gap flush). The fused cursor then emits only those rows.
- [ ] **Step 4: Partitioned sdt.** If the partitioned sdt function can't cleanly expose a single ascending absolute-row keep-set (per-partition state), and `SUBSAMPLE sdt` never uses PARTITION BY (the desugar produces `OVER (ORDER BY ts)` only), scope `isRowSelecting()` to the NON-partitioned function and leave the partitioned one false (unfused) — the fuse gate already excludes PARTITION BY. Document this.
- [ ] **Step 5: GREEN + measure.** All tests byte-identical (esp. the null-gap sdt cross-check). Run `SubsampleWindowVsCursorBenchmark` for sdt (add sdt to the method param locally, or a focused run) window vs cursor — but note sdt has NO cursor, so compare fused-sdt vs unfused-sdt (kill-switch can't disable sdt; instead A/B by temporarily reverting, or measure fused vs the Phase-4b unfused number ~3× via the decompose approach). Record the improvement.
- [ ] **Step 6: Commit** `perf(window): fuse SUBSAMPLE sdt via row-selecting swinging-door exposure` + measured delta.

---

## Task 3: Consolidated measurement + final state

- [ ] **Step 1:** Re-run the full `SubsampleWindowVsCursorBenchmark` grid (100k/1M) + the 10M m4/lttb point, with #1+#3+#4 applied.
- [ ] **Step 2:** Update `.superpowers/sdd/window-upgrades-analysis.md` with the final before→after table (Phase 4a baseline → 4b → 4c) and the remaining gap per method. State the honest final position: which methods are at parity, which remain slower and why, and the recommendation for the landing/default decision.
- [ ] **Step 3: Commit** the updated analysis.

---

## Self-Review

**Spec coverage:** Task 1 (narrowChain elim, fused path only) + Task 2 (sdt row-selecting fusion) + Task 3 (final measurement). Byte-identical throughout; the Phase-4b fusion Critical stays closed (fuse gate unchanged, still marker-gated). sdt partitioned scoped-out if needed (PARTITION BY never fuses anyway).

**Placeholder scan:** plan/measurement pastes are deliberate fill-from-run. narrowChain-unread is verified (Step 1) before skipping. sdt keep-set exposure preserves the exact current keep-set (byte-identity test is the oracle).

**Type consistency:** Task 1 gated by the existing fused-mode flag; non-fused path unchanged. Task 2 reuses the existing `isRowSelecting()`/selected-rows contract + `tryFuseKeepFlagFilter` (no codegen change). sdt exposes absolute-row keep positions from its finalized keep-byte buffer.

## Execution Handoff
(Provided after user review.)
