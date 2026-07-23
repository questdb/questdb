# SUBSAMPLE-as-window Phase 1: desugaring spine + `uniform`

> **Historical phase plan:** This body records assumptions at the start of Phase 1. The authoritative final state is the completed [Phase 5 plan](2026-07-22-subsample-window-phase5-delete-cursor.md): SUBSAMPLE is window-only and the legacy cursor/configuration are deleted.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the "SUBSAMPLE → keep-flag window function" pattern end-to-end by (1) adding a user-visible `uniform(N)` boolean keep-flag window function and (2) rewriting `SUBSAMPLE uniform(N)` into a windowed subquery + filter, with javier's existing `uniform` SUBSAMPLE tests as the regression oracle. Other SUBSAMPLE methods stay on the old cursor untouched.

**Architecture:** `uniform` becomes a TWO_PASS window function (modeled on `NtileFunctionFactory`) returning `boolean`: pass1 writes each row's 0-based ordinal into its cached slot, `preparePass2` computes the selected-position set from the total count, pass2 writes `true`/`false`. A new `SqlOptimiser.rewriteSubsample()` (modeled on `rewriteSampleBy`) rewrites only the `uniform` method into `SELECT <cols> FROM (SELECT <cols>, uniform(N) OVER (ORDER BY ts) __keep FROM <src>) WHERE __keep`; all other methods fall through unchanged to the existing custom cursor.

**Tech Stack:** Java, QuestDB griffin SQL engine, JUnit4 (`AbstractCairoTest`), the window-function framework.

## Global Constraints

- JDK 25 at `/usr/lib/jvm/java-25-openjdk-amd64`; build/test from `/home/nick/claude/wt/oss/subsample-fixes`.
- New window function under `core/src/main/java/io/questdb/griffin/engine/functions/window/`; auto-registers via classpath scan (no list edit).
- Apache license header (copy verbatim from `NtileFunctionFactory.java`) on every new `.java` file — production AND test — or the CI license/format check fails.
- Tests use fluent `assertQuery(...)` / `assertException(...)` (house convention).
- **Regression oracle:** every existing `uniform`-related case in `core/src/test/java/io/questdb/test/griffin/SubsampleTest.java` MUST stay green — the SQL surface (`SUBSAMPLE uniform(N)`) is unchanged.
- `uniform` window-function result type is `ColumnType.BOOLEAN` (so `WHERE __keep` works directly).
- The `uniform` selection formula is exactly `UniformAlgorithm`'s: keep row at 0-based ordinal `p` iff `p ∈ { (i*(n-1) + (n-1)/2) / (N-1) : i∈[0,N) }` where `n` = total rows, `N` = target; if `n <= N` keep all.
- Do NOT touch the old `SubsampleRecordCursorFactory` / the other algorithms in this phase.

**Build/test commands:**
```bash
cd /home/nick/claude/wt/oss/subsample-fixes
export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64
mvn -q -pl core -am compile -DskipTests
mvn -pl core -Dtest=UniformWindowFunctionTest test
mvn -pl core -Dtest=SubsampleTest test
```

---

## File Structure

- `core/src/main/java/io/questdb/griffin/engine/functions/window/UniformFunctionFactory.java` — the `uniform(N)` boolean keep-flag window function factory + its non-partitioned TWO_PASS function class. One responsibility: decide, over an ordered row sequence, which rows a uniform downsample keeps.
- `core/src/test/java/io/questdb/test/griffin/engine/window/UniformWindowFunctionTest.java` — direct window-function tests (`uniform(N) OVER (ORDER BY ts)`).
- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` — add `rewriteSubsample(...)` + its call site in `optimise()`.
- (regression) `core/src/test/java/io/questdb/test/griffin/SubsampleTest.java` — untouched; used as oracle.

---

## Task 1: `uniform` keep-flag window function

**Files:**
- Create: `core/src/main/java/io/questdb/griffin/engine/functions/window/UniformFunctionFactory.java`
- Test: `core/src/test/java/io/questdb/test/griffin/engine/window/UniformWindowFunctionTest.java`
- Reference (read, don't modify): `NtileFunctionFactory.java` (TWO_PASS template: getPassCount/pass1/preparePass2/pass2/reset/reopen/toTop/toPlan/setColumnIndex), and `SdtWindowFunctionFactory` on branch `feat/swinging-door` if available (a boolean TWO_PASS window fn returning via the chain).

**Interfaces:**
- Produces (consumed by Task 2): a window function named `uniform`, signature `uniform(L)` (one long target, validated constant, `>= 1`), result `ColumnType.BOOLEAN`, usable as `uniform(N) OVER (ORDER BY ts)`.

- [ ] **Step 1: Verify the name `uniform` is free as a window function**

Run: `cd /home/nick/claude/wt/oss/subsample-fixes && grep -rn "\"uniform\"\|uniform(" core/src/main/java/io/questdb/griffin/engine/functions/ | grep -i "getSignature\|SIGNATURE" | head`
Expected: no existing function factory claims `uniform(...)`. If one does, STOP and report — we'd need a different public name or scalar/window disambiguation. (SUBSAMPLE already uses `uniform` as a method token, so it should be free.)

- [ ] **Step 2: Write the failing direct window-function tests**

Create `UniformWindowFunctionTest.java` (Apache header from `NtileFunctionFactory.java` first). The oracle for expected rows is the `UniformAlgorithm` formula computed by hand.

```java
package io.questdb.test.griffin.engine.window;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class UniformWindowFunctionTest extends AbstractCairoTest {

    @Test
    public void testKeepsAllWhenTargetGteRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, x from long_sequence(3)");
            // target 5 >= 3 rows -> keep all
            assertSql(
                    "ts\tv\tkeep\n" +
                            "1970-01-01T00:00:00.000001Z\t1.0\ttrue\n" +
                            "1970-01-01T00:00:00.000002Z\t2.0\ttrue\n" +
                            "1970-01-01T00:00:00.000003Z\t3.0\ttrue\n",
                    "select ts, v, uniform(5) over (order by ts) keep from t"
            );
        });
    }

    @Test
    public void testEvenlySpacedSelection() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, x from long_sequence(5)");
            // n=5, N=3: divisor=2, range=4, half=1. pos(i)=(i*4+1)/2 -> 0, 2, 4 (0-based) => rows 1,3,5
            assertSql(
                    "ts\tv\tkeep\n" +
                            "1970-01-01T00:00:00.000001Z\t1.0\ttrue\n" +
                            "1970-01-01T00:00:00.000002Z\t2.0\tfalse\n" +
                            "1970-01-01T00:00:00.000003Z\t3.0\ttrue\n" +
                            "1970-01-01T00:00:00.000004Z\t4.0\tfalse\n" +
                            "1970-01-01T00:00:00.000005Z\t5.0\ttrue\n",
                    "select ts, v, uniform(3) over (order by ts) keep from t"
            );
        });
    }

    @Test
    public void testFilterYieldsReducedSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, x from long_sequence(5)");
            assertSql(
                    "ts\tv\n" +
                            "1970-01-01T00:00:00.000001Z\t1.0\n" +
                            "1970-01-01T00:00:00.000003Z\t3.0\n" +
                            "1970-01-01T00:00:00.000005Z\t5.0\n",
                    "select ts, v from (select ts, v, uniform(3) over (order by ts) keep from t) where keep"
            );
        });
    }

    @Test
    public void testRejectsNonConstantTarget() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertExceptionNoLeakCheck(
                    "select ts, uniform(v::long) over (order by ts) from t",
                    19,
                    "target must be a constant"
            );
        });
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `mvn -q -pl core -Dtest=UniformWindowFunctionTest test`
Expected: FAIL — `uniform` is not a known window function.

- [ ] **Step 4: Implement `UniformFunctionFactory`**

Create `UniformFunctionFactory.java`. Model precisely on `NtileFunctionFactory` (non-partitioned `NtileFunction`), but return BOOLEAN keep and compute the uniform selection set in `preparePass2`. Complete implementation:

```java
package io.questdb.griffin.engine.functions.window;

// imports: ObjList, IntList, CairoConfiguration, SqlExecutionContext, SqlException, Function,
// Record, WindowContext, ColumnType, Numbers, Unsafe, WindowSPI, PlanSink, DirectLongList,
// MemoryTag. Confirm exact packages against NtileFunctionFactory.

public class UniformFunctionFactory extends AbstractWindowFunctionFactory {
    public static final String NAME = "uniform";
    private static final String SIGNATURE = NAME + "(L)";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions,
                                CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        windowContext.validate(position, supportNullsDesc());
        if (!windowContext.isOrdered()) {
            throw SqlException.$(position, "uniform() requires ORDER BY");
        }
        if (!windowContext.isDefaultFrame()) {
            throw SqlException.$(position, "uniform() does not support framing; remove ROWS/RANGE clause");
        }
        if (windowContext.getPartitionByRecord() != null) {
            throw SqlException.$(position, "uniform() does not support PARTITION BY");
        }
        Function targetArg = args.getQuick(0);
        if (!targetArg.isConstant()) {
            throw SqlException.$(argPositions.getQuick(0), "target must be a constant");
        }
        long target = targetArg.getLong(null);
        if (target < 1 || target == Numbers.LONG_NULL) {
            throw SqlException.$(argPositions.getQuick(0), "target must be a positive constant");
        }
        return new UniformFunction(target);
    }

    // Non-partitioned, TWO_PASS. Cached-record-chain slot holds a long: pass1 writes the row ordinal,
    // pass2 overwrites it with 0/1 (read as BOOLEAN by the outer cursor).
    static class UniformFunction extends BaseWindowFunction {
        private final long target;
        private long count;                 // running ordinal in pass1
        private long totalRows;             // set in preparePass2
        private final DirectLongList selected = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
        private long selCursor;             // pass2 monotonic pointer into `selected`

        UniformFunction(long target) {
            super(null); // no value arg
            this.target = target;
        }

        @Override
        public void close() { super.close(); selected.close(); }

        @Override
        public int getPassCount() { return WindowFunction.TWO_PASS; }

        @Override
        public int getType() { return ColumnType.BOOLEAN; }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), count);
            count++;
        }

        @Override
        public void preparePass2() {
            totalRows = count;
            selected.clear();
            if (totalRows <= target) {
                // keep all: leave `selected` empty and mark everything true in pass2 via a flag
                selCursor = -1; // sentinel: keep-all
                return;
            }
            long divisor = target - 1;
            long range = totalRows - 1;
            long half = divisor / 2;
            long prev = -1;
            for (long i = 0; i < target; i++) {
                long pos = (i * range + half) / divisor;
                if (pos != prev) { // dedup (positions are non-decreasing)
                    selected.add(pos);
                    prev = pos;
                }
            }
            selCursor = 0;
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            long ordinal = Unsafe.getUnsafe().getLong(spi.getAddress(recordOffset, columnIndex));
            boolean keep;
            if (selCursor == -1) {
                keep = true; // keep-all
            } else {
                // pass2 visits rows in the same ascending ordinal order; monotonic pointer walk.
                keep = selCursor < selected.size() && selected.get(selCursor) == ordinal;
                if (keep) selCursor++;
            }
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), keep ? 1L : 0L);
        }

        @Override
        public void reopen() { count = 0; totalRows = 0; selCursor = 0; selected.clear(); }

        @Override
        public void reset() { super.reset(); count = 0; totalRows = 0; selCursor = 0; selected.clear(); }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME).val('(').val(target).val(')');
            sink.val(" over (order by [ts])"); // adjust to actual toPlan idiom in step 6
        }

        @Override
        public void toTop() { super.toTop(); count = 0; totalRows = 0; selCursor = 0; selected.clear(); }
    }
}
```

Notes for the executor:
- Confirm the exact base class + method set against `NtileFunctionFactory`'s non-partitioned function (it may extend a different base than `BaseWindowFunction`; use whatever Ntile's non-partitioned variant extends). `columnIndex` comes from `setColumnIndex` (inherited).
- `getType()` BOOLEAN but the slot is written as a `long` (0/1). Confirm the cached cursor reads a BOOLEAN column from a long slot correctly — mirror exactly how a boolean-returning window function (or Ntile's long) stores/reads. If the chain needs a 1-byte boolean slot, write with `Unsafe.putByte` and adjust; match the framework's boolean column storage.
- The monotonic-pointer read in pass2 relies on pass2 visiting rows in the same ascending order pass1 assigned ordinals — verify this holds for the non-partitioned ordered cached cursor (it does for Ntile). If pass2 order isn't guaranteed ascending-ordinal, fall back to a membership check (e.g. binary search in `selected`).
- `reopen`/`toTop`/`reset` may be on `Reopenable` rather than the base — mirror how `SdtWindowFunctionFactory` (which needed `implements Reopenable`) or Ntile handle it.

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn -q -pl core -Dtest=UniformWindowFunctionTest test`
Expected: PASS. If only the `.fails` position integer is off, read the actual position and update the literal (keep the message text).

- [ ] **Step 6: Lock the EXPLAIN plan text**

Add to `UniformWindowFunctionTest`:
```java
    @Test
    public void testExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertPlanNoLeakCheck(
                    "select ts, uniform(3) over (order by ts) from t",
                    "uniform(3) over (order by [ts])"  // paste the ACTUAL plan text from the first failure
            );
        });
    }
```
Run it, copy the actual plan text into the expected literal, re-run to PASS, and fix `toPlan` in the factory if the rendered text is malformed.

- [ ] **Step 7: Commit**

```bash
git add core/src/main/java/io/questdb/griffin/engine/functions/window/UniformFunctionFactory.java \
        core/src/test/java/io/questdb/test/griffin/engine/window/UniformWindowFunctionTest.java
git commit -m "feat(window): add uniform() keep-flag window function"
```

---

## Task 2: `SUBSAMPLE uniform(N)` desugaring

**Files:**
- Modify: `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` (add `rewriteSubsample` + call site).
- Test: `core/src/test/java/io/questdb/test/griffin/SubsampleTest.java` (existing `uniform` cases are the oracle; add a plan-shape assertion).

**Interfaces:**
- Consumes (from Task 1): the `uniform(N)` boolean window function.
- Produces: `SUBSAMPLE uniform(N)` now executes via the window rewrite; all other methods unchanged.

- [ ] **Step 1: Baseline — capture current `uniform` SUBSAMPLE behavior**

Run: `mvn -q -pl core -Dtest=SubsampleTest test`
Expected: PASS (140/140 currently). List the tests whose SQL contains `SUBSAMPLE uniform` — these are the oracle Task 2 must keep green through the new path.

- [ ] **Step 2: Write a failing routing test**

Add to `SubsampleTest.java` (after an existing uniform test): assert the plan now goes through the window rewrite (a `Window`/`Filter` chain), not `Subsample`:
```java
    @Test
    public void testUniformDesugarsToWindowFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts)");
            // After the rewrite, EXPLAIN must show a windowed subquery + filter, not a Subsample node.
            assertPlanNoLeakCheck(
                    "SELECT ts, v FROM t SUBSAMPLE uniform(3)",
                    "PLAN_TEXT_TO_PASTE"  // fill from the first run once Step 4 is in place
            );
        });
    }
```
(This test will be finalized in Step 4/5 by pasting the actual rewritten plan; for now it documents intent.)

- [ ] **Step 3: Run to confirm current plan is the old cursor**

Run: `mvn -q -pl core -Dtest=SubsampleTest#testUniformDesugarsToWindowFilter test`
Expected: FAIL showing the current plan contains a `Subsample`/`SubsampleRecordCursor…` node (the pre-rewrite state). Record that actual text.

- [ ] **Step 4: Implement `rewriteSubsample` in `SqlOptimiser`**

Add a method `rewriteSubsample(IQueryModel model, SqlExecutionContext ctx)` modeled on `rewriteSampleBy` (`SqlOptimiser.java:8861`) and using the node/model-construction recipe below. Then call it in `optimise()` immediately AFTER the `rewriteSampleBy` call (around `SqlOptimiser.java:12414`) and BEFORE `resolveNamedWindows`/`rewriteSelectClause`, so the injected window column is picked up by the normal window machinery.

Behavior:
1. Recurse into nested/join/union models (mirror `rewriteSampleBy`'s recursion + `replaceAndTransferDependents`).
2. If `model.getSubsample() == null`, return unchanged.
3. Read the method call node: `ExpressionNode call = model.getSubsample()` (a FUNCTION node; `call.token` = method name, `call.args` = args, arg order is reversed per QuestDB convention — verify against how SqlParser built it).
4. **If `call.token` is NOT `uniform`, return the model unchanged** (leave the subsample node in place → the existing custom-cursor path handles it). This is the partial-migration gate.
5. For `uniform`: build the rewrite using the pooled constructors (exact recipe from research):
   - Determine the designated timestamp column name for `OVER (ORDER BY ts)`. If the model/base has a designated timestamp, use its name; if none, use `OVER ()` (input order) per the "skip sort if absent" decision.
   - Build the window call: `ExpressionNode uni = expressionNodePool.next(); uni.token = "uniform"; uni.type = FUNCTION; uni.paramCount = 1; uni.args.add(<target node from call.args>);`
   - `WindowExpression keepCol = windowExpressionPool.next().of("__keep_subsample", uni); uni.windowExpression = keepCol;` then, if a timestamp exists, `ExpressionNode ob = expressionNodePool.next(); ob.token = <tsColumn>; ob.type = LITERAL; keepCol.addOrderBy(ob, 0);`
   - Add `keepCol` to the model that projects from the source (`model.addBottomUpColumn(keepCol)` — confirm this is the right column list for the level SUBSAMPLE sits on).
   - Wrap in an outer select via `wrapWithSelectModel(model, ...)` / `createWrapperModel(model)` (`SqlOptimiser.java:3100`), and set `outer.setWhereClause(keepLiteral)` where `keepLiteral` is a `LITERAL` ExpressionNode with token `"__keep_subsample"`. Ensure the outer projection is the ORIGINAL columns only (not `__keep_subsample`).
   - Clear the clause: `model.setSubsample(null, 0)`.
6. Return the rewritten (possibly wrapper) model.

Concrete node/model construction reference (from `rewriteSampleBy`): allocate every node via `expressionNodePool.next()` / `queryModelPool.next()` / `windowExpressionPool.next()` / `queryColumnPool.next()` — never `new`. `windowExpressionPool` is already a field on `SqlOptimiser` (declared ~line 271). `createWrapperModel` (line 3100) builds a two-level `_model → _nested → model` wrapper; use it and put the WHERE + original-column projection on the appropriate level.

> This is model surgery; the exact column-list level and wrapper depth must be validated against the compiler and the oracle tests. Iterate: get it compiling, then make `testFilterYieldsReducedSet`-equivalent SUBSAMPLE cases pass, matching the old cursor's output row-for-row.

- [ ] **Step 5: Finalize the routing/plan test**

Run: `mvn -q -pl core -Dtest=SubsampleTest#testUniformDesugarsToWindowFilter test`
Read the ACTUAL rewritten plan (should now show a window function + a filter over the base, no `Subsample` node). Paste it into the expected literal. Re-run to PASS.

- [ ] **Step 6: Run the FULL SubsampleTest — oracle must stay green**

Run: `mvn -pl core -Dtest=SubsampleTest test`
Expected: all cases PASS. Specifically the `SUBSAMPLE uniform(N)` cases now execute via the window rewrite and produce byte-identical results to before; all NON-uniform method cases (`lttb`/`m4`/`minmax`/`cadence`) still pass via the untouched custom cursor.
If a `uniform` case regresses, the rewrite's output/order differs from the old cursor — debug against that specific case (it is the spec's correctness oracle).

- [ ] **Step 7: Run a broad optimiser sanity check**

Run: `mvn -q -pl core -Dtest='SubsampleTest,WindowFunctionTest,UniformWindowFunctionTest' test`
Expected: PASS — the new `optimise()` step doesn't disturb ordinary window queries.

- [ ] **Step 8: Commit**

```bash
git add core/src/main/java/io/questdb/griffin/SqlOptimiser.java \
        core/src/test/java/io/questdb/test/griffin/SubsampleTest.java
git commit -m "feat(sql): desugar SUBSAMPLE uniform(N) to a windowed keep-flag subquery"
```

---

## Self-Review

**Spec coverage (Phase 1 scope):**
- Keep-flag window function (`uniform`, user-visible, BOOLEAN) → Task 1.
- Minimum passes: `uniform` is TWO_PASS in the framework (needs the total count; the window engine counts via pass1 rather than exposing `base.size()`). This CORRECTS the spec's aspirational "ONE_PASS via size()" — noted here; a `size()`-based ONE_PASS optimization is deferred to a follow-up (it requires plumbing base size into the function, which the framework doesn't cleanly expose).
- SUBSAMPLE desugaring, `uniform` only, others fall through → Task 2 (partial-migration gate on `call.token == "uniform"`).
- "Skip sort if absent" ordering → Task 2 Step 4 (`OVER (ORDER BY ts)` when a timestamp exists, else `OVER ()`).
- Regression oracle (existing tests green) → Task 2 Steps 1, 6.
- Old cursor untouched this phase → enforced (gate + Global Constraints).

**Placeholder scan:** The two `PLAN_TEXT_TO_PASTE` markers are deliberate lock-the-actual-output steps (the plan text is machine-generated; the engineer pastes the real value and re-runs), not missing logic — the surrounding steps specify exactly how to obtain and lock them. The Task 2 `rewriteSubsample` body is described as a precise recipe rather than final Java because it is model surgery that must be compiled and validated against the oracle; the node-construction calls, the intercept line, and the correctness oracle are all concrete.

**Type consistency:** `uniform(L)` → BOOLEAN throughout; the cached slot stores a `long` 0/1 written/read consistently in pass1/pass2; `__keep_subsample` is the alias used in both the injected column and the WHERE literal; `NAME = "uniform"` matches the SUBSAMPLE method token and the desugaring gate.

## Execution Handoff

(Provided after user review.)
