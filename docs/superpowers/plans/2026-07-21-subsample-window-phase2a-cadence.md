# SUBSAMPLE-as-window Phase 2a: `cadence`

> **Historical phase plan:** This body records assumptions at the start of Phase 2a. The authoritative final state is the completed [Phase 5 plan](2026-07-22-subsample-window-phase5-delete-cursor.md): SUBSAMPLE is window-only and the legacy cursor/configuration are deleted.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add a user-visible `cadence(stride [, seed])` boolean keep-flag window function and migrate `SUBSAMPLE cadence(...)` to it, byte-identically to the old cursor, with javier's cadence SUBSAMPLE tests as the oracle. Reuses the `uniform` (Phase 1) TWO_PASS pattern and desugaring.

**Architecture:** `cadence` is TWO_PASS (the last-row pin needs the total row count — it cannot be ZERO_PASS/streaming). It mirrors `UniformFunctionFactory` structurally (pass1 counts rows; `preparePass2` computes the kept-ordinal set; pass2 writes the boolean), swapping the selection math for `CadenceAlgorithm`'s stride+offset+last-pin and adding the offset/seed handling. The desugaring extends `rewriteSubsample` with a `cadence` gate that builds a 1- or 2-arg window call.

**Tech Stack:** Java, QuestDB griffin window framework, JUnit4 (`AbstractCairoTest`).

## Global Constraints

- JDK 25 at `/usr/lib/jvm/java-25-openjdk-amd64`; build/test from `/home/nick/claude/wt/oss/subsample-fixes`.
- New window function under `core/src/main/java/io/questdb/griffin/engine/functions/window/`; auto-registers via classpath scan.
- Apache header (from `UniformFunctionFactory.java`) on every new `.java` file; fluent `assertQuery(...)`/`assertQuery(...).fails(...)` tests only (NO local `printSql`/`assertSql` helper).
- Result type `ColumnType.BOOLEAN`; TWO_PASS; requires ORDER BY; rejects framing and PARTITION BY.
- **Selection = `CadenceAlgorithm` exactly** (over ordinals 0..n-1 instead of buffer positions): keep ordinal 0; if `stride == 1` keep all; if `stride > n` keep only ordinal 0 (NO last-pin); else keep `stride+offset, 2*stride+offset, …` (compute `pos` as `long` to avoid the int overflow the branch already fixed) and pin the last ordinal `n-1`.
- **Offset/seed = `SubsampleRecordCursorFactory.computeCadenceOffset` exactly:** `SEED_MODE_NONE` (no seed arg) → offset 0; `SEED_MODE_DETERMINISTIC` (constant seed) → splitmix64 mix then `Math.floorMod(h, stride)`; `SEED_MODE_RANDOM` → `contextRnd.nextInt(stride)`. RANDOM offset is per-execution (compute at cursor open / `preparePass2`), not at compile time.
- **Oracle:** every existing `cadence` case in `core/src/test/java/io/questdb/test/griffin/SubsampleTest.java` stays green, byte-identical. `cadence(1)` and other non-migrated shapes fall through to the untouched cursor.
- Do NOT touch the old `SubsampleRecordCursorFactory` / other algorithms.

**Commands:**
```bash
cd /home/nick/claude/wt/oss/subsample-fixes && export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64
mvn -q -pl core -am compile -DskipTests
mvn -q -pl core -Dtest=CadenceWindowFunctionTest test
mvn -q -pl core -Dtest=SubsampleTest test
```

---

## File Structure

- `core/.../engine/functions/window/CadenceFunctionFactory.java` — the `cadence(stride[,seed])` boolean window function + its non-partitioned TWO_PASS function. Mirrors `UniformFunctionFactory`.
- `core/.../test/griffin/engine/window/CadenceWindowFunctionTest.java` — direct window-function tests.
- `core/.../griffin/SqlOptimiser.java` — extend `rewriteSubsample`'s gate + the builder to handle `cadence`.
- (regression) `SubsampleTest.java` — oracle + 1 new plan-shape test.

---

## Task 1: `cadence` keep-flag window function

**Files:**
- Create: `core/src/main/java/io/questdb/griffin/engine/functions/window/CadenceFunctionFactory.java`
- Test: `core/src/test/java/io/questdb/test/griffin/engine/window/CadenceWindowFunctionTest.java`
- Reference (read, don't modify): `UniformFunctionFactory.java` (this branch — the TWO_PASS position-only template: getPassCount/pass1/preparePass2/pass2/getType BOOLEAN/putByte slot/reopen/toTop/reset/toPlan, the `selected` DirectLongList + monotonic pass2 pointer, and the documented pass1/pass2 same-order dependency), `CadenceAlgorithm.java` and `SubsampleRecordCursorFactory.computeCadenceOffset` (the math to re-home).

**Interfaces:**
- Produces (consumed by Task 2): a window function named `cadence`, signatures `cadence(L)` (stride) and `cadence(LL)` (stride, seed), stride constant `>= 1`, result `ColumnType.BOOLEAN`, `cadence(stride[,seed]) OVER (ORDER BY ts)`.

- [ ] **Step 1: Verify `cadence` is free as a window function**

Run: `grep -rn "getSignature\|SIGNATURE" core/src/main/java/io/questdb/griffin/engine/functions/ | grep -i cadence`
Expected: nothing. If a `cadence` function exists, STOP and report.

- [ ] **Step 2: Write failing tests**

Create `CadenceWindowFunctionTest.java` (Apache header from `UniformFunctionFactory.java`). Use the fluent `assertQuery(sql).returns(expected)` / `assertQuery(sql).fails(pos, msg)` API (mirror `UniformWindowFunctionTest.java` in the same package for the exact fluent call shape). Hand-compute expected via the selection rule.

```java
package io.questdb.test.griffin.engine.window;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CadenceWindowFunctionTest extends AbstractCairoTest {

    @Test
    public void testStrideNoSeed() throws Exception {
        // n=10, stride=3, offset=0 (no seed). keep ordinals: 0, then 3,6,9, then pin last (9 already there).
        // -> ordinals 0,3,6,9 -> rows 1,4,7,10
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, x from long_sequence(10)");
            assertQuery(
                    "ts\tv\tkeep\n" +
                            "1970-01-01T00:00:00.000001Z\t1.0\ttrue\n" +
                            "1970-01-01T00:00:00.000002Z\t2.0\tfalse\n" +
                            "1970-01-01T00:00:00.000003Z\t3.0\tfalse\n" +
                            "1970-01-01T00:00:00.000004Z\t4.0\ttrue\n" +
                            "1970-01-01T00:00:00.000005Z\t5.0\tfalse\n" +
                            "1970-01-01T00:00:00.000006Z\t6.0\tfalse\n" +
                            "1970-01-01T00:00:00.000007Z\t7.0\ttrue\n" +
                            "1970-01-01T00:00:00.000008Z\t8.0\tfalse\n" +
                            "1970-01-01T00:00:00.000009Z\t9.0\tfalse\n" +
                            "1970-01-01T00:00:00.000010Z\t10.0\ttrue\n",
                    "select ts, v, cadence(3) over (order by ts) keep from t"
            );
        });
    }

    @Test
    public void testStrideOne() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, x from long_sequence(3)");
            assertQuery(
                    "ts\tv\tkeep\n" +
                            "1970-01-01T00:00:00.000001Z\t1.0\ttrue\n" +
                            "1970-01-01T00:00:00.000002Z\t2.0\ttrue\n" +
                            "1970-01-01T00:00:00.000003Z\t3.0\ttrue\n",
                    "select ts, v, cadence(1) over (order by ts) keep from t"
            );
        });
    }

    @Test
    public void testDeterministicSeedOffset() throws Exception {
        // With a constant seed the offset shifts the stride start deterministically.
        // Compute the EXPECTED offset by porting computeCadenceOffset(stride, seed) and hand-derive rows,
        // OR (simpler) assert it matches the OLD cursor via SUBSAMPLE in Task 2. For the direct test,
        // pick stride/seed and fill the expected table from the first run (this is the algorithm's own
        // deterministic output; verify it is stable and non-trivial, i.e. offset != 0).
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, x from long_sequence(10)");
            assertQuery(
                    "FILL_FROM_FIRST_RUN",
                    "select ts, v, cadence(3, 42) over (order by ts) keep from t"
            );
        });
    }

    @Test
    public void testRejectsNonConstantStride() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, cadence(v::long) over (order by ts) from t")
                    .fails(19, "stride must be a constant");
        });
    }
}
```

- [ ] **Step 3: Run to verify RED**

Run: `mvn -q -pl core -Dtest=CadenceWindowFunctionTest test`
Expected: FAIL — `cadence` unknown.

- [ ] **Step 4: Implement `CadenceFunctionFactory`**

Copy `UniformFunctionFactory` and adapt. Key differences from uniform:
1. **Two signatures:** register `cadence(L)` and `cadence(LL)`. (Two factory instances or one signature with optional arg — follow how existing window functions register optional-arg variants, e.g. how a 1-or-2-arg window function is registered; if the framework needs two SIGNATURE strings, provide both via two getSignature-returning factories or the multi-signature mechanism — verify against an existing optional-arg window/aggregate factory.)
2. **newInstance:** validate ORDER BY present, default frame, no PARTITION BY (same as uniform). Read `stride = args.get(0)` (must be constant `>= 1`, else `"stride must be a constant"`/`"stride must be positive"`). If a 2nd arg (seed) is present, capture the seed Function + set `seedMode = DETERMINISTIC` (constant seed) or `RANDOM` if the seed is a NULL literal (match the old cursor's seed-mode rule — verify what `null`/absent/constant map to in `SubsampleRecordCursorFactory`'s cadence path); no 2nd arg → `SEED_MODE_NONE`.
3. **Offset:** re-home `computeCadenceOffset` verbatim (splitmix64 + `Math.floorMod`; RANDOM via a per-execution `Rnd`). Compute the offset per-execution — in `preparePass2()` (which runs each execution after pass1) or a cursor-open hook — NOT once at newInstance, so RANDOM re-randomizes per run. For the `Rnd`, use the same per-execution random source the old cursor uses (`SubsampleRecordCursorFactory`'s `getCursor` computes it from the execution context — mirror how it obtains `contextRnd`).
4. **preparePass2:** `totalRows = count`. Compute `offset` (per seed mode). Fill `selected` (a `DirectLongList`) with the kept ordinals using the `CadenceAlgorithm` rule over `[0, totalRows)`: add 0; if stride==1 add all; if stride>totalRows stop (no pin); else `for (long pos=(long)stride+offset; pos<totalRows; pos+=stride) selected.add(pos);` then pin `totalRows-1` unless already last.
5. **pass1/pass2/getType/putByte-slot/reopen/toTop/reset/toPlan:** identical structure to `UniformFunction` (BOOLEAN, byte slot, monotonic pass2 pointer over `selected`, keep-all sentinel when stride==1). `toPlan` renders `cadence(stride[, seed]) over (order by [ts])`.

Provide the full class in this step, mirroring `UniformFunctionFactory` line-for-line except the selection/offset math above.

- [ ] **Step 5: Run to verify GREEN + fill deterministic-seed expected**

Run: `mvn -q -pl core -Dtest=CadenceWindowFunctionTest test`
For `testDeterministicSeedOffset`, paste the actual (stable, offset!=0) output into the expected table and re-run to PASS. Fix `.fails` position integers to the actual reported positions (keep messages).

- [ ] **Step 6: Lock the EXPLAIN plan** (mirror `UniformWindowFunctionTest.testExplainPlan`) — add a plan test, paste the actual `cadence(...) over (order by [ts])` plan text, PASS.

- [ ] **Step 7: Commit**
```bash
git add core/src/main/java/io/questdb/griffin/engine/functions/window/CadenceFunctionFactory.java \
        core/src/test/java/io/questdb/test/griffin/engine/window/CadenceWindowFunctionTest.java
git commit -m "feat(window): add cadence() keep-flag window function"
```

---

## Task 2: `SUBSAMPLE cadence(...)` desugaring

**Files:**
- Modify: `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` (extend `rewriteSubsample` gate + builder).
- Test: `SubsampleTest.java` (oracle + 1 plan test).

**Interfaces:**
- Consumes: the `cadence` window function (Task 1).

- [ ] **Step 1: Baseline** — `mvn -q -pl core -Dtest=SubsampleTest test` PASS (143/143). List the `SUBSAMPLE cadence` cases — the oracle.

- [ ] **Step 2: Failing plan/routing test** — add `testCadenceDesugarsToWindowFilter` (mirror `testUniformDesugarsToWindowFilter`), expecting a window+filter plan (fill actual in Step 4).

- [ ] **Step 3: Run to confirm current plan is the old cursor** for cadence (records the pre-rewrite `Subsample` plan).

- [ ] **Step 4: Extend `rewriteSubsample`**

In `rewriteSubsample`'s gate (currently keyed on `"uniform"` with `paramCount == 1`), add a `cadence` branch:
- Accept `Chars.equalsIgnoreCase(subsample.token, "cadence")` with `subsample.paramCount == 1 || subsample.paramCount == 2`.
- Gate conditions mirror uniform: designated timestamp present, non-aggregation context, and the FIRST arg (stride) is a compile-time constant `>= 1`. **Additional cadence-specific exclusions to preserve byte-identical fall-through:** if the old cursor special-cases any shape (e.g. `cadence(1)` returns the base cursor directly, or a NULL/RANDOM seed changes behavior), and the window function would diverge, leave those on the old path. Confirm `cadence(1)`, `cadence(stride, <random/null seed>)`, and bind-var stride all either produce identical results via the window OR fall through. Prefer falling through when unsure.
- Build the window call node: 1 arg → `paramCount=1; rhs=clone(stride)`; 2 args → `paramCount=2; lhs=clone(stride); rhs=clone(seed)` (per the ExpressionNode 2-arg convention). Everything else (WindowExpression `__keep_subsample` with `setIncludeIntoWildcard(false)`, `addOrderBy(ts)`, filter/outer wrapper, LIMIT re-lift, union bubble-up, `setSubsample(null,0)`) is the METHOD-AGNOSTIC path already in `desugarUniformSubsample` — refactor that method to `desugarSubsample(...)` taking the built window-call node so both uniform and cadence share it, OR add a parallel `desugarCadenceSubsample` that reuses the shared tail. Prefer the refactor (DRY).

- [ ] **Step 5: Lock the plan test** — paste the actual rewritten cadence plan, PASS.

- [ ] **Step 6: FULL SubsampleTest — oracle green**

Run: `mvn -pl core -Dtest=SubsampleTest test`
Expected: all PASS. Migrated `cadence` cases byte-identical; `uniform` (Phase 1) still green; other methods still on the old cursor.

- [ ] **Step 7: Broad sanity** — `mvn -q -pl core -Dtest='SubsampleTest,UniformWindowFunctionTest,CadenceWindowFunctionTest,SqlOptimiserTest,WindowFunctionTest' test` PASS.

- [ ] **Step 8: Commit**
```bash
git add core/src/main/java/io/questdb/griffin/SqlOptimiser.java core/src/test/java/io/questdb/test/griffin/SubsampleTest.java
git commit -m "feat(sql): desugar SUBSAMPLE cadence to the cadence keep-flag window function"
```

---

## Self-Review

**Spec coverage:** `cadence` keep-flag window function (Task 1) + desugaring migration (Task 2). **Pass-class correction:** the spec's `cadence` = ZERO_PASS is WRONG — the last-row pin needs the total row count, so `cadence` is TWO_PASS like `uniform` (documented in Architecture). No SUBSAMPLE algorithm is genuinely single-pass; noted honestly. Offset/seed re-homed exactly (Global Constraints). Byte-identical oracle enforced (Task 2 Steps 1, 6). Old cursor untouched. DRY via the shared `desugarSubsample` tail.

**Placeholder scan:** `FILL_FROM_FIRST_RUN` and the plan-text pastes are deliberate lock-the-actual-output steps (deterministic algorithm output / machine-generated plan text), with the surrounding steps specifying how to obtain them — not missing logic. Task 1 Step 4 mandates the FULL class (mirroring uniform); the optional-arg-signature and seed-mode-mapping details are flagged for verification against the old cursor, which is the authoritative source.

**Type consistency:** `cadence(L)`/`cadence(LL)` → BOOLEAN; byte slot; `__keep_subsample` alias reused; `NAME="cadence"` matches the method token and the gate; the shared `desugarSubsample` tail is reused by both uniform and cadence.

## Execution Handoff

(Provided after user review.)
