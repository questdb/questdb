# SUBSAMPLE-as-window Phase 5: delete the legacy cursor (window is the only path)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make the keep-flag window path TOTAL (every valid `SUBSAMPLE` shape migrates, every invalid one errors clearly, no fall-through) and then DELETE `SubsampleRecordCursorFactory` + its dispatch + the kill-switch, so the window functions are the sole execution path. Then future optimization has one path to improve. Decided by the user (window default + remove legacy cursor; perf optimization paused at the current ~1.9× structural ceiling for value-inspecting methods).

> **Completed 2026-07-23:** Tasks 1, 2a, 2b, 2c, 3, and 4 are complete. Total routing landed in `79b66d3f56`; cursor/config/obsolete-benchmark deletion landed in `6c5482d5ae`. The required 2,139-test regression suite and benchmark packaging pass. SUBSAMPLE now has one execution implementation: the keep-flag window path.
>
> **Historical checklist notice:** The remaining body preserves the pre-implementation plan and its original present-tense assumptions. The completion banner above and final-state section below are authoritative.

**Why factory work comes first:** the OLD cursor supports **bind-variable** `target`/`stride`/`seed` (`isConstant() || isRuntimeConstant()`, coerced at runtime). The window factories currently require **compile-time constant** `target`/`stride` (only `seed` accepts runtime-constant), so bind-var shapes fall through to the cursor today. Deleting the cursor without regressing bind-var `SUBSAMPLE` requires the window factories to accept runtime-constant `target`/`stride` first.

**Architecture:** Widen the 5 count/value window factories (uniform/cadence/m4/minmax/lttb) to accept runtime-constant (bind-var) `target`/`stride`, reading the value at cursor-open (reopen/of) rather than construction — mirroring how `seed` is already handled. Then widen + totalize the `rewriteSubsample` gates: migrate every valid shape (incl. bind-var target/stride, `cadence(1)` — the window already supports stride 1, and random/NULL seed — already supported), and throw clear `SqlException`s for invalid shapes (non-numeric/expression value, wrong arity, bad interval) at rewrite time, exactly like the `sdt` total gate. Remove the `cairo.subsample.window.enabled` kill-switch (with no cursor, "off" has nowhere to route). Delete the cursor factory, the `METHOD_*` dispatch, and `generateSubsample`'s cursor construction; keep the algorithm classes (`M4Algorithm` etc.) — the window functions call their `select`.

**Tech Stack:** Java, QuestDB griffin window framework, JUnit4.

## Global Constraints

- JDK 25 at `/usr/lib/jvm/java-25-openjdk-amd64`; build/test from `/home/nick/claude/wt/oss/subsample-fixes`.
- **The oracle changes here (by design):** previously fall-through cases used the cursor; now they migrate (or error). SubsampleTest cases that asserted cursor plans/errors for now-migrated shapes must be updated to the new window behavior. This is NOT weakening — it's the intended deletion. But VALID-query result rows must stay correct (cross-check migrated bind-var/cadence(1)/random-seed queries against the pre-deletion cursor output BEFORE deleting, captured as golden values).
- **No lost valid functionality:** every query the cursor accepted must still work via the window path (bind-var target/stride/seed/compdev/gap, cadence(1), random seed, all methods). Enumerate the cursor's accepted shapes and prove each migrates.
- Keep the algorithm classes (`M4Algorithm`/`MinMaxAlgorithm`/`LttbAlgorithm`/`SubsampleAlgorithm`/`SwingingDoor`) — window functions depend on them. Delete only the cursor factory + dispatch + codegen cursor path + parser cursor-only bits + the kill-switch config.
- Do NOT change the window functions' selection math or the fusion.

**Commands:**
```bash
cd /home/nick/claude/wt/oss/subsample-fixes && export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64
mvn -q -pl core -am compile -DskipTests
mvn -q -pl core -Dtest='SubsampleTest,M4WindowFunctionTest,MinMaxWindowFunctionTest,LttbWindowFunctionTest,UniformWindowFunctionTest,CadenceWindowFunctionTest,SdtWindowFunctionTest,SqlOptimiserTest,SqlParserTest,WindowFunctionTest' test
```

---

## Task 1: Widen window factories to accept bind-variable target/stride

**Files:** Modify `UniformFunctionFactory.java`, `CadenceFunctionFactory.java`, `CadenceSeedFunctionFactory.java`, `M4FunctionFactory.java`, `LttbFunctionFactory.java`, `LttbGapFunctionFactory.java`. Tests in the respective `*WindowFunctionTest`.
**Interfaces:** Produces window functions that accept `target`/`stride` as `isConstant() || isRuntimeConstant()`.

- [ ] **Step 1: Read the seed precedent.** `CadenceSeedFunctionFactory` already accepts a runtime-constant seed (`!isConstant() && !isRuntimeConstant()` → throw; else `coerceRuntimeConstantType` + read at the right time). Mirror this for `target`/`stride`.
- [ ] **Step 2: Failing bind-var tests.** In each `*WindowFunctionTest`, add a bind-variable target test: `m4(ts, v, $1) OVER (ORDER BY ts)` with `$1` bound to e.g. 4, asserting the same keep-set as the constant `m4(ts, v, 4)`. Include a re-bind case (bind $1=4 then $1=8, same compiled factory, different result) proving the value is read per-execution, not frozen at compile.
- [ ] **Step 3: Widen each factory.** Replace `if (!targetArg.isConstant()) throw "target must be a constant"` with `if (!targetArg.isConstant() && !targetArg.isRuntimeConstant()) throw "target must be a constant or bind variable"`. Read `target`/`stride` at cursor-open (in the function's `reopen()`/`of()` or first-use), NOT at `newInstance` — a bind var isn't set at compile. Validate range (`>= 2`, integer type) at read time with the same messages/positions. Ensure the fused row-selecting path + `preparePass2` see the resolved target before pass1 needs it.
- [ ] **Step 4: GREEN.** The bind-var tests pass; all existing window tests stay green (constant target still works — `isConstant()` implies the value is readable at open too).
- [ ] **Step 5: Commit** `feat(window): accept bind-variable target/stride in keep-flag window functions`.

---

## Task 2: Make the desugar gates TOTAL (migrate all valid, error all invalid, no fall-through)

**Files:** Modify `SqlOptimiser.java` (`rewriteSubsample` + helpers); remove the kill-switch read. Tests `SubsampleTest.java`.
**Interfaces:** Consumes Task 1.

- [ ] **Step 1: Enumerate the cursor's accepted + rejected shapes** from `generateSubsample` (arg parsing + the per-method arity/type errors) — this is the totality spec. Capture golden result rows (via the still-present cursor) for the shapes that currently fall through and will now migrate: bind-var target/stride/seed/compdev/gap, `cadence(1)`, random/NULL-seed cadence, per method.
- [ ] **Step 2: Widen the gates to migrate everything valid.** Accept runtime-constant (bind-var) target/stride/compdev/gap (via new `isConstantOrRuntimeConstant*` checks). Migrate `cadence(1)` (window supports stride 1) and random/NULL-seed cadence (window supports it). The value arg must still be a bare-column LITERAL (expression value columns are invalid — see Step 3).
- [ ] **Step 3: Totalize — throw at rewrite time for invalid shapes** (no fall-through, mirroring the `sdt` gate): non-numeric/expression value column that reaches the factory rejects at runtime with its numeric message (keep that behavior); wrong arity, bad interval gap, etc. → the SAME messages/positions the cursor produced (reproduce them at the subsample position). Every `token ∈ {uniform,cadence,m4,minmax,lttb,sdt}` path ends in migrate-or-throw; nothing reaches `generateSubsample`'s cursor branch.
- [ ] **Step 4: Remove the kill-switch.** Delete the `isSubsampleWindowEnabled()` read in `rewriteSubsample` (window is unconditional now). Task 3 deletes the config itself.
- [ ] **Step 5: GREEN.** All SubsampleTest cases pass — migrated shapes byte-identical to the captured golden (Step 1); invalid shapes error with the reconciled messages. Update the tests whose expected plan/message reflected the cursor for now-migrated shapes.
- [ ] **Step 6: Commit** `feat(sql): total SUBSAMPLE window gates (migrate bind-vars/cadence(1)/random-seed; error invalid; drop fall-through)`.

---

## Task 3: Delete the cursor, dispatch, codegen cursor path, and kill-switch config

**Files:** Delete `SubsampleRecordCursorFactory.java` (+ any cursor-only helpers it owns). Modify `SqlCodeGenerator.java` (remove `generateSubsample`'s method-dispatch + cursor construction; keep the desugar-produced window path), `SqlParser.java` (remove cursor-only subsample handling if any beyond clause parsing the desugar still needs), `CairoConfiguration.java`/`DefaultCairoConfiguration.java`/`CairoConfigurationWrapper.java`/`PropServerConfiguration.java`/`PropertyKey.java` (remove `isSubsampleWindowEnabled` + `cairo.subsample.window.enabled`).
**Interfaces:** Consumes Task 2 (nothing routes to the cursor anymore).

- [ ] **Step 1: Confirm no live references.** Grep `SubsampleRecordCursorFactory`, `METHOD_LTTB`/`METHOD_M4`/etc., `isSubsampleWindowEnabled` — all references should be dead after Task 2 (only the to-delete sites + tests). If `generateSubsample` is still reachable for any shape, Task 2 wasn't total — STOP and fix Task 2.
- [ ] **Step 2: Delete** the cursor factory + the codegen dispatch/construction + the kill-switch config across all sites. Keep the algorithm classes (`M4Algorithm` etc.) — verify the window functions still reference them.
- [ ] **Step 3: Compile + full regression.** `mvn -q -pl core -am compile -DskipTests` then the full test command. Fix any now-dead test (e.g. `testSubsampleWindowKillSwitch` — the switch is gone; delete or repurpose it) and any test importing the deleted cursor.
- [ ] **Step 4: Commit** `refactor(sql): delete legacy SubsampleRecordCursorFactory + dispatch + kill-switch (window is the sole path)`.

---

## Task 4: Full reconciliation + final regression

- [ ] **Step 1:** Full suite green: `SubsampleTest`, all `*WindowFunctionTest`, `SqlOptimiserTest`, `SqlParserTest`, `WindowFunctionTest`. Every SubsampleTest case now exercises the window path.
- [ ] **Step 2:** Grep for any remaining `Subsample` cursor references, dead imports, dead helpers (e.g. `subsample.max.rows` if it only gated the cursor). Clean up.
- [ ] **Step 3:** Update the docs/analysis noting the cursor is gone; the window path is total. Note the deferred optimization (value-inspecting ~1.9× structural ceiling) for future work.
- [ ] **Step 4: Commit** `docs: SUBSAMPLE is window-only; note deferred value-inspecting optimization`.

---

## Self-Review

**Spec coverage:** factory bind-var widening (Task 1) → total gates (Task 2) → delete cursor+dispatch+kill-switch (Task 3) → reconcile+regression (Task 4). No valid query regresses (bind-vars/cadence(1)/random-seed migrate, cross-checked against the cursor golden before deletion); invalid shapes error with reconciled messages. Algorithm classes retained.

**Placeholder scan:** the totality spec is the cursor's own accepted/rejected shape set (Step 2.1 enumerates it); golden values captured from the live cursor before deletion. Error messages reconciled to the cursor's, not invented (except where the window factory's own message already applied, e.g. non-numeric value).

**Type consistency:** target/stride become `isConstant() || isRuntimeConstant()`, read at open-time; gates migrate-or-throw for all 6 tokens; kill-switch removed everywhere; cursor + METHOD_* + generateSubsample cursor branch deleted; algorithm classes kept.

## Execution Handoff
(Provided after user review.)

---

## SCOPE EXPANSION (2026-07-22, after Task 2 BLOCKED finding + user decision "extend desugar, then delete")

The cursor handles TWO contexts the window desugar cannot yet: SUBSAMPLE-after-aggregation and SUBSAMPLE-in-join. User chose to EXTEND the desugar to cover both (no feature loss), THEN delete. This inserts two tasks BEFORE the old Task 2 (which becomes 2c). New order: 2a → 2b → 2c → 3 → 4.

### Task 2a: desugar SUBSAMPLE-after-aggregation (SAMPLE BY / GROUP BY)
- **Problem:** `rewriteSampleBy` runs before `rewriteSubsample`; the model is aggregating, so `desugarSubsample` adding the keep `WindowExpression` INTO that model throws "Window function is not allowed in context of aggregation."
- **Fix:** when the subsample's model is an aggregation (SAMPLE BY/GROUP BY), don't inject the window into it — WRAP it: build a projection model OVER the aggregating model, attach the keep window + filter to the WRAPPER (the window's `OVER (ORDER BY ts)` references the aggregation's output designated timestamp, which survives). i.e. `SELECT cols FROM (<aggregating model>) <window+keep-filter>`.
- Oracle: the ~6 aggregation tests (`testLttbAfterSampleBy`, `testUniformAfterSampleBy`, `testCadenceAfterSampleBy`, `testSubsampleWithGroupBy`, `testSampleByLosesDesignationButSubsampleStillWorks`, …) must stay byte-identical, now via the window path. Cross-check vs the still-present cursor before shifting them.
- Note the early user insight: "sample by loses the designated timestamp, but it should generate an outer order by; generate the order by then subsample."

### Task 2b: desugar SUBSAMPLE-in-join (ASOF/other joins)
- **Problem:** desugar's `OVER (ORDER BY ts)` uses the bare `ts` token → ambiguous across join branches → SqlException. The current `subsampleInJoinContext` guard falls through to the cursor.
- **Fix:** QUALIFY the order-by timestamp with the designated timestamp's source (the join's master/left table alias), so `OVER (ORDER BY <alias>.ts)` is unambiguous. Remove/replace the join fall-through guard once qualification works. Verify the join's designated timestamp resolution.
- Oracle: `testSubsampleWithJoin`, `testSubsampleWithActualJoin`, `testSubsampleNotHoistedFromJoinBranch`, `testSubsampleBranchLocalInJoin`, `testM4OverJoinFallsThroughToCursor` (this one asserts the cursor plan — update to the window plan) — byte-identical rows via the window path.

### Task 2c (was Task 2): total gates + remove kill-switch
- As originally specified, but now aggregation + join contexts MIGRATE (via 2a/2b) instead of throwing. sdt still refuses agg/join (its total gate throws) — unless 2a/2b naturally cover sdt too (check: sdt could also fuse post-aggregation; decide per byte-identity).
- After 2a+2b+2c: EVERY SUBSAMPLE shape migrates-or-throws; `generateSubsample`'s cursor branch is unreachable → Task 3 deletes it.

**Final state (2026-07-23):** Tasks 1/2a/2b/2c/3/4 complete. The legacy cursor, codegen dispatch, kill-switch, cursor max-row configuration, cursor-only uniform/cadence helpers, and obsolete cursor-comparison benchmarks are deleted. Value-inspecting algorithm classes remain because the window factories call them. Incoming row order is preserved (ascending input stays ascending; descending input need not be force-sorted).
