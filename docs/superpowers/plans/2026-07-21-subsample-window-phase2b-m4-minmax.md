# SUBSAMPLE-as-window Phase 2b: `m4` + `minmax`

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add user-visible `m4(ts, value, target)` and `minmax(ts, value, target)` boolean keep-flag window functions and migrate `SUBSAMPLE m4/minmax(value, target)` to them, byte-identically, with javier's m4/minmax SUBSAMPLE tests as the oracle. Establishes the **value-inspecting** window-function pattern (reads a value column) and the **value-column desugaring** (3-arg call node).

**Architecture:** These are TWO_PASS value-inspecting window functions: pass1 appends `(ordinal, ts, value)` into a native 24-byte-entry buffer (the existing `SubsampleAlgorithm` layout) and counts; `preparePass2` calls the UNCHANGED `M4Algorithm.select` / `MinMaxAlgorithm.select` over that buffer to get the selected ordinals; pass2 marks the boolean keep per row (monotonic pointer walk, like `UniformFunction`). The desugaring extends `rewriteSubsample` with an m4/minmax gate that builds a 3-arg `m4(ts, value, target)` window call (args added back-to-front). The algorithm classes' `select` (and buffer helpers) are made accessible so the math is reused, not re-copied.

**Tech Stack:** Java, QuestDB griffin window framework, native memory (`MemoryARW`/`Unsafe`), JUnit4.

## Global Constraints

- JDK 25 at `/usr/lib/jvm/java-25-openjdk-amd64`; build/test from `/home/nick/claude/wt/oss/subsample-fixes`.
- New window functions auto-register via classpath scan; Apache header on every new `.java`; fluent `assertQuery` tests only.
- Result `ColumnType.BOOLEAN`; TWO_PASS; require ORDER BY; reject framing + PARTITION BY.
- **Reuse the algorithm math — do NOT re-copy it.** Call `M4Algorithm.select` / `MinMaxAlgorithm.select` (make them + the needed `SubsampleAlgorithm` buffer constants/helpers `public`, or place the new factories in `io.questdb.griffin.engine.table` — whichever is the smaller, cleaner change). The buffer entry layout is `[rowId/ordinal:long@0][timestamp:long@8][value:double@16]` = 24 bytes.
- **Signatures:** `m4(NDl)` / `minmax(NDl)` = (timestamp, double value, const long target). Value column may be INT/LONG/SHORT/BYTE/FLOAT/DOUBLE (promote to double); target constant `>= 2`.
- **Arg order in the window factory:** `args.getQuick(0)=ts`, `getQuick(1)=value`, `getQuick(2)=target`. The desugaring builds the 3-arg node by `args.add` **back-to-front**: `add(target)`, `add(value)`, `add(ts)` (FunctionParser reverses; confirmed via `rewriteSampleBy`'s tsFloorFunc).
- **Value-column validation in the window function:** reproduce the old cursor's numeric-type error (`"numeric column expected, got: <type>"`) so error cases stay byte-identical (the optimiser-level desugaring cannot check column type; the factory can, since the parser resolves the column arg to a typed Function before newInstance).
- **Oracle:** every existing `m4`/`minmax` case in `SubsampleTest.java` stays green, byte-identical; migrated via the window, everything unsafe falls through to the untouched cursor.
- Do NOT touch the old cursor's runtime path or the other algorithms (lttb/uniform/cadence).

**Commands:**
```bash
cd /home/nick/claude/wt/oss/subsample-fixes && export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64
mvn -q -pl core -am compile -DskipTests
mvn -q -pl core -Dtest=M4WindowFunctionTest test
mvn -q -pl core -Dtest=SubsampleTest test
```

---

## File Structure

- `core/.../engine/functions/window/M4FunctionFactory.java` — `m4(ts,value,target)` value-inspecting TWO_PASS window function + a shared base for the buffered-value-inspecting pattern (native buffer append in pass1, `select` in preparePass2, keep-mark in pass2).
- `core/.../engine/functions/window/MinMaxFunctionFactory.java` — `minmax(ts,value,target)`, reusing the shared base; differs only in which `select` it calls.
- (accessibility) `M4Algorithm.java`, `MinMaxAlgorithm.java`, `SubsampleAlgorithm.java` — widen `select`/constants to `public` as needed (no logic change).
- `core/.../griffin/SqlOptimiser.java` — extend `rewriteSubsample` with the m4/minmax gate + a `desugarValueInspectingSubsample` builder feeding the shared `desugarSubsample` tail.
- Tests: `M4WindowFunctionTest.java`, `MinMaxWindowFunctionTest.java`.

---

## Task 1: value-inspecting window-function base + `m4`

**Files:**
- Create: `core/src/main/java/io/questdb/griffin/engine/functions/window/M4FunctionFactory.java`
- Test: `core/src/test/java/io/questdb/test/griffin/engine/window/M4WindowFunctionTest.java`
- Modify: `M4Algorithm.java` / `SubsampleAlgorithm.java` (widen visibility only).
- Reference: `UniformFunctionFactory.java` (TWO_PASS boolean template, `selected` + monotonic pass2 walk, BOOLEAN byte slot), `AbstractBivariateStatWindowFunctionFactory.java` (multi-arg per-row `Function` value reads: `arg.getDouble(record)`, `record.getTimestamp(idx)`), and any window fn that manages a `MemoryARW` (for the native buffer). `M4Algorithm.select` (the math to call unchanged).

**Interfaces:**
- Produces: window function `m4`, signature `m4(NDl)`, BOOLEAN, `m4(ts,value,target) OVER (ORDER BY ts)`.

- [ ] **Step 1: Verify `m4` is free as a function name.**
Run: `grep -rn "SIGNATURE\|getSignature" core/src/main/java/io/questdb/griffin/engine/functions/ | grep -iw m4` → expect nothing; else STOP.

- [ ] **Step 2: Write failing tests** (`M4WindowFunctionTest.java`, fluent style). Oracle = `M4Algorithm` by hand.
```java
package io.questdb.test.griffin.engine.window;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class M4WindowFunctionTest extends AbstractCairoTest {

    @Test
    public void testKeepsAllWhenFewRows() throws Exception {
        // n=3, target=8 -> numBuckets=2, but few rows: keeps all (cap >= n)
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t values (1::timestamp,10.0),(2::timestamp,20.0),(3::timestamp,30.0)");
            assertQuery("ts\tv\tkeep\n" +
                    "1970-01-01T00:00:00.000001Z\t10.0\ttrue\n" +
                    "1970-01-01T00:00:00.000002Z\t20.0\ttrue\n" +
                    "1970-01-01T00:00:00.000003Z\t30.0\ttrue\n",
                    "select ts, v, m4(ts, v, 8) over (order by ts) keep from t");
        });
    }

    @Test
    public void testMatchesM4AlgorithmOnSpike() throws Exception {
        // Deterministic spike; keep first/min/max/last per time bucket. Fill expected from
        // the FIRST run, then hand-verify it equals the old-cursor SUBSAMPLE m4 output
        // (Task 2 cross-checks via SUBSAMPLE). Choose data with a clear min/max per bucket.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, case when x%5=0 then 100.0 else x end from long_sequence(20)");
            assertQuery("FILL_FROM_FIRST_RUN",
                    "select ts, v from (select ts, v, m4(ts, v, 8) over (order by ts) keep from t) where keep");
        });
    }

    @Test
    public void testRejectsNonNumericValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, s symbol) timestamp(ts)");
            assertQuery("select ts, m4(ts, s, 8) over (order by ts) from t")
                    .fails(17, "numeric"); // match the old cursor's numeric-column error substring
        });
    }

    @Test
    public void testRejectsNonConstantTarget() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, m4(ts, v, v::long) over (order by ts) from t")
                    .fails(21, "target must be a constant");
        });
    }
}
```

- [ ] **Step 3: RED** — `mvn -q -pl core -Dtest=M4WindowFunctionTest test` fails (unknown function).

- [ ] **Step 4: Widen algorithm visibility.** Make `M4Algorithm.select(...)` `public` (was package-private) and any `SubsampleAlgorithm` constants/helpers the buffer append needs (`ENTRY_SIZE`, `getTimestamp`, `getValue` if used) `public`. No logic change. Compile.

- [ ] **Step 5: Implement `M4FunctionFactory`** (put the shared buffered-value-inspecting base logic here as a static inner base class or a small helper the minmax factory reuses in Task 3):
  - `getSignature()` → `"m4(NDl)"`. `newInstance`: validate ORDER BY, default frame, no PARTITION BY (like uniform). `tsArg=args.get(0)`, `valueArg=args.get(1)`, `targetArg=args.get(2)`. Validate `valueArg` type is numeric (INT/LONG/SHORT/BYTE/FLOAT/DOUBLE) else throw `"numeric column expected, got: " + ColumnType.nameOf(...)` at the value arg position (reproduce the old error). Validate `targetArg` constant `>= 2` else `"target must be a constant"`/`"target points must be at least 2"`.
  - TWO_PASS function: a `MemoryARW` native buffer (24 bytes/entry). `pass1(record,...)`: read `ts=tsArg.getTimestamp(record)` (or `getLong`), `value=valueArg.getDouble(record)`; append `[ordinal, ts, doubleToLongBits(value)]` (ordinal = running count) to the buffer; `count++`. `preparePass2()`: call `M4Algorithm.select(bufferAddr, (int)count, target, selectedList, circuitBreaker)` → `selectedList` holds ascending buffer positions (= ordinals); reset the pass2 pointer. `pass2(record,...)`: monotonic-walk `selectedList` against the running pass2 ordinal; write BOOLEAN byte (`Unsafe.putByte`) keep. `getType()` BOOLEAN. `reopen/toTop/reset`: reset count/pointer, clear the buffer (truncate) and the selected list; free `MemoryARW` in `close()`/`reset()`.
  - `toPlan`: `m4(ts, v, target) over (order by [ts])`.

- [ ] **Step 6: GREEN + fill/lock** — run; fill `FILL_FROM_FIRST_RUN` from the actual (stable) output; add + lock an EXPLAIN plan test; fix `.fails` positions to actual (keep message substrings).

- [ ] **Step 7: Commit**
```bash
git add core/src/main/java/io/questdb/griffin/engine/functions/window/M4FunctionFactory.java \
        core/src/test/java/io/questdb/test/griffin/engine/window/M4WindowFunctionTest.java \
        core/src/main/java/io/questdb/griffin/engine/table/M4Algorithm.java \
        core/src/main/java/io/questdb/griffin/engine/table/SubsampleAlgorithm.java
git commit -m "feat(window): add m4() value-inspecting keep-flag window function"
```

---

## Task 2: `SUBSAMPLE m4(value, target)` desugaring

**Files:** Modify `SqlOptimiser.java`; Test `SubsampleTest.java`.
**Interfaces:** Consumes `m4` (Task 1).

- [ ] **Step 1: Baseline** — `SubsampleTest` green; list the `m4` oracle cases; classify migrate vs fall-through.
- [ ] **Step 2: Failing plan test** `testM4DesugarsToWindowFilter` (fill actual later).
- [ ] **Step 3: Confirm current m4 plan is the old cursor.**
- [ ] **Step 4: Extend `rewriteSubsample`.** Add an `m4` branch (and structure it so `minmax` in Task 3 reuses it): gate = token in {m4,minmax}, paramCount==2, designated ts, non-agg, target (arg[1]) constant `>= 2` (reuse `isConstantUniformTarget`). Build the 3-arg window node via a new `desugarValueInspectingSubsample(model, nested, subsample, timestamp, fnName)`:
  - `ExpressionNode call = expressionNodePool.next().of(FUNCTION, fnName, 0, subsample.position); call.paramCount = 3;`
  - `call.args.add(clone(subsample.args.get(1)))` /*target*/, `call.args.add(clone(subsample.args.get(0)))` /*value column*/, `call.args.add(literal(timestamp.token))` /*ts*/  — back-to-front so the factory sees ts,value,target.
  - Then feed `call` to the shared `desugarSubsample(model, nested, timestamp, call)` tail (the same one uniform/cadence use).
  - **Value column:** pass `subsample.args.get(0)` (the column expression) as-is; do NOT resolve/type-check it at optimiser time — the window function validates the type and normal SQL resolution handles existence. IF an existing error-case oracle test (bad/non-numeric/missing column) diverges (different message/position via the window path), narrow the gate to fall through that shape, or make the window function reproduce the exact message — driven by the oracle in Step 6.
- [ ] **Step 5: Lock the plan test** (paste actual rewritten m4 plan).
- [ ] **Step 6: FULL SubsampleTest — oracle green.** All pass; migrated m4 byte-identical; uniform/cadence still green; minmax/lttb/others on the old cursor. Debug any divergence against the specific case.
- [ ] **Step 7: Broad sanity** `mvn -q -pl core -Dtest='SubsampleTest,M4WindowFunctionTest,SqlOptimiserTest,WindowFunctionTest' test`.
- [ ] **Step 8: Commit** `feat(sql): desugar SUBSAMPLE m4 to the m4 keep-flag window function`.

---

## Task 3: `minmax` (function + desugaring)

**Files:** Create `MinMaxFunctionFactory.java` + `MinMaxWindowFunctionTest.java`; widen `MinMaxAlgorithm.select` public; extend `rewriteSubsample`'s gate to include `minmax`; add oracle plan test.
**Interfaces:** Reuses Task 1's value-inspecting base + Task 2's `desugarValueInspectingSubsample`.

- [ ] **Step 1: Verify `minmax` free.**
- [ ] **Step 2: Failing tests** (`MinMaxWindowFunctionTest`, mirror `M4WindowFunctionTest`; keep min/max per bucket; `numBuckets=target/2`).
- [ ] **Step 3: RED.**
- [ ] **Step 4: Widen `MinMaxAlgorithm.select` public.**
- [ ] **Step 5: Implement `MinMaxFunctionFactory`** — identical to `M4FunctionFactory` except it calls `MinMaxAlgorithm.select`. Reuse the shared buffered-value-inspecting base from Task 1 (factor it if not already). Signature `minmax(NDl)`.
- [ ] **Step 6: GREEN + lock plan.**
- [ ] **Step 7: Extend the desugaring gate** to include `minmax` (the Task-2 gate already handles `{m4,minmax}` if you structured it so; otherwise add `minmax`). Add `testMinMaxDesugarsToWindowFilter`.
- [ ] **Step 8: FULL SubsampleTest green** (m4 + minmax migrated; rest unchanged) + broad sanity.
- [ ] **Step 9: Commit** `feat(window,sql): add minmax() window function and desugar SUBSAMPLE minmax`.

---

## Self-Review

**Spec coverage:** value-inspecting keep-flag window functions m4 (Task 1) + minmax (Task 3), reusing the algorithm math (Global Constraints); value-column desugaring via 3-arg back-to-front node (Task 2); byte-identical oracle (Tasks 2, 3). lttb deferred to Phase 2c (noted in the phase intro — materially more complex). Pass class: TWO_PASS (m4/minmax need the global time range — cannot be single-pass), consistent with the spec.

**Placeholder scan:** `FILL_FROM_FIRST_RUN` / plan-text pastes are deliberate lock-the-actual-output steps (deterministic algorithm output / machine plan text). The value-column error-parity is specified with a concrete fallback (reproduce the message in the factory, or narrow the gate), driven by the oracle — not left vague. The shared-base "factor it if not already" is a real instruction (extract the buffered-value-inspecting base in Task 1, reuse in Task 3).

**Type consistency:** `m4(NDl)`/`minmax(NDl)` → BOOLEAN; byte slot; args (ts,value,target) at getQuick(0,1,2); the desugaring adds target,value,ts back-to-front; `desugarValueInspectingSubsample` + the shared `desugarSubsample` tail reused; `select` made public on both algorithm classes.

## Execution Handoff
(Provided after user review.)
