# DELETE windowed survivor-replace Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound the memory (and transient Parquet disk) of an arbitrary-predicate `DELETE` so it cannot OOM the database on a large table, by rewriting the survivor set in adaptive time-windows onto the already-proven `beginReplaceRange`/`applyReplaceRangeWindow`/`finishReplaceRange` primitive.

**Architecture:** `OperationExecutor.executeDelete`'s arbitrary path (`replaceWithSurvivors`) changes from one whole-range `replaceRange` (stages every survivor into O3 at once → OOM) to a loop that tiles the populated range `[minTs, maxTs]` into ~`rows-per-step` windows, applying one `applyReplaceRangeWindow` per window (bounded O3 staging + bounded Parquet convert) under a single terminal `finishReplaceRange` (one seqTxn advance, crash-safe). Window sizing reuses `MatViewRefreshJob.estimateBucketsForRows`; each window's survivor cursor is the survivor factory bound to `[wLo, wHiExcl)` via bind variables (an interval scan, so total read stays one pass over the table).

**Tech Stack:** QuestDB core (Java 25 / GraalVM), Maven. `TableWriter` O3 replace machinery, `MatViewRefreshJob.estimateBucketsForRows`, `SqlCompilerImpl.generateDelete`, bind-variable interval scans. Enterprise: `com.questdb.security` + PGWire ACL tests.

## Global Constraints

- **Single seqTxn advance per WAL txn** (default / atomic path). The DELETE persists the sequencer txn exactly once — in `finishReplaceRange`'s `commit00()`. Never add an intermediate commit inside the atomic window loop.
- **Atomicity is opt-out, not gone.** The default path (`cairo.wal.delete.disk.bounded=false`) is atomic: readers see the DELETE fully applied or not at all. The opt-in disk-bounded path (Task 6, `=true`) commits per window at seqTxn S-1 and is therefore **NON-atomic** — concurrent readers may observe a large DELETE partially applied during WAL apply — but remains crash-safe (a crash leaves durable seqTxn S-1; re-apply redoes the whole DELETE, finished windows idempotent as survivors-of-survivors) and reaches the same final state. Only the disk-bounded path relaxes atomicity; C1 (memory) is fixed on both paths.
- **Correctness identical to whole-range delete.** The surviving row set is exactly `NOT(pred)`, byte-identical to the current whole-range implementation, for any window count. Windows must *tile* `[minTs, maxTs+1)` contiguously (window K's `hiExcl` == window K+1's `lo`) so deleted rows in inter-survivor gaps are covered by exactly one window.
- **Per-window cursor MUST be an interval scan**, not a filtered full scan. N windows must sum to ONE pass over the table (O(tableRows) read), not O(N·tableRows). This is a hard performance gate (Task 4 spike).
- **Time-range (empty-replace) path unchanged.** `deleteOp.isPureTimeRange()` → `deleteTimeRange` is already O(deleted); windowing applies ONLY to the arbitrary survivor path.
- **Crash → idempotent re-apply.** A crash before `finishReplaceRange` leaves durable seqTxn at S-1; ApplyWal2TableJob re-runs the whole DELETE; finished windows re-apply as no-ops (survivors-of-survivors).
- Java tests use fluent `assertSql`/`assertQuery`/`assertSqlCursors` with exact oracles — never `printSql` + `TestUtils.assertEquals`.
- New Java files start with the Apache header block used across core (copy from an existing `core/src/...` file; it opens with `/*+`).
- Config default: `cairo.wal.delete.rows.per.step` = `1_000_000L` (mirrors `cairo.mat.view.rows.per.query.estimate`).
- Build/test: `mvn -q -pl core test -Dtest=<Class> -DfailIfNoTests=false`, then read `core/target/surefire-reports/<fqcn>.txt` for the authoritative `Tests run: N, Failures: 0, Errors: 0` line. Do NOT change `JAVA_HOME`.
- Never `git add -A`/`git add .` (the `.superpowers/` ledger is gitignored scratch); add named files only.

## File Structure

- `core/src/main/java/io/questdb/cairo/TableWriter.java` — **Task 1 DONE** (committed `90c3b03c49`): `beginReplaceRange` / `applyReplaceRangeWindow` / `finishReplaceRange` / `abortReplaceRange` + `replaceRangeRowsBefore`. No further change unless a later task needs a new accessor.
- `core/src/main/java/io/questdb/cairo/CairoConfiguration.java` + `DefaultCairoConfiguration.java` + `io/questdb/PropServerConfiguration.java` + `io/questdb/PropertyKey.java` — Task 2: the `getWalDeleteRowsPerStep()` knob.
- `core/src/main/java/io/questdb/cairo/wal/OperationExecutor.java` — Tasks 3, 5, 6, 8: window-step helper, the windowed `replaceWithSurvivors` loop, per-window Parquet convert, and DELETE-apply logging.
- `core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java` + `griffin/engine/ops/DeleteOperation.java` — Task 4: per-window ts-bound bind variables on the survivor factory.
- `core/src/test/java/io/questdb/test/cairo/wal/DeleteWindowedApplyTest.java` (new) — Task 4 spike + Task 7 integration (multi-window correctness, crash-safety, memory-shape, Parquet, edges).
- `core/src/test/java/io/questdb/test/griffin/DeleteTest.java` — Task 5/6 behavioral additions.
- `core/src/test/java/io/questdb/test/griffin/WriteFenceEntryPointMatrixTest.java` — Task 10: behavioral WriteFence DELETE test.
- `docs/reference/sql/delete.md` (or the DELETE docs page) — Task 9: RESUME-WAL recovery note.
- Enterprise (`~/claude/wt/ent/delete-statement`): `questdb-ent/src/test/java/.../PGWireAclTest.java` (or sibling) + `PermissionTest.java` — Task 11.

---

## Task 1: replaceRange windowed primitive — DONE

**Status:** Complete, committed `90c3b03c49` on branch `delete-statement`. Proven by `TableWriterReplaceRangeDirectTest` (11 tests) + regression `DeleteTest` (39) + `WalWriterReplaceRangeTest` (59), all green. No action — listed so the ledger records it and later tasks know the interface below exists.

**Interfaces produced (already in `TableWriter`):**
- `void beginReplaceRange()` — flush + open REPLACE_RANGE txn (captures `replaceRangeRowsBefore`).
- `void applyReplaceRangeWindow(long loTs, long hiExclTs, @Nullable RecordCursor survivorCursor, @Nullable RecordToRowCopier copier, int timestampCursorIndex, @Nullable SqlExecutionContext ctx)` — stage + apply ONE window; no commit. Empty/inverted window is a no-op. Survivor ts must be in `[loTs, hiExclTs)`.
- `long finishReplaceRange()` — single `commit00()` + housekeep + shrink; returns rows removed since begin.
- `void abortReplaceRange()` — reset dedup/mem state without committing (caller does the txn rollback).
- Existing `long replaceRange(...)` retained as a behaviour-preserving wrapper (begin + one applyWindow + finish).

---

## Task 2: `cairo.wal.delete.rows.per.step` configuration knob

**Files:**
- Modify: `core/src/main/java/io/questdb/PropertyKey.java` (add the enum constant)
- Modify: `core/src/main/java/io/questdb/cairo/CairoConfiguration.java` (interface method)
- Modify: `core/src/main/java/io/questdb/cairo/DefaultCairoConfiguration.java` (default impl)
- Modify: `core/src/main/java/io/questdb/PropServerConfiguration.java` (field + read + accessor)
- Test: `core/src/test/java/io/questdb/PropServerConfigurationTest.java` (or the existing config test that asserts a mat-view knob default — grep `getMatViewRowsPerQueryEstimate` in test sources and mirror it)

**Interfaces:**
- Consumes: nothing.
- Produces: `long CairoConfiguration.getWalDeleteRowsPerStep()` (default `1_000_000L`), used by Task 3.

- [ ] **Step 1: Add the property key.** In `PropertyKey.java`, find `CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE` and add next to it, following the exact enum style (constant name + string key):

```java
CAIRO_WAL_DELETE_ROWS_PER_STEP("cairo.wal.delete.rows.per.step"),
```

- [ ] **Step 2: Add the interface method.** In `CairoConfiguration.java`, near `getMatViewRowsPerQueryEstimate()`:

```java
/**
 * Target number of rows staged per window of an arbitrary-predicate DELETE's survivor-replace. The
 * apply path walks the deleted range in windows of roughly this many rows, bounding peak O3 memory to
 * one window regardless of table size. Default 1,000,000.
 */
long getWalDeleteRowsPerStep();
```

- [ ] **Step 3: Add the default.** In `DefaultCairoConfiguration.java`, near `getMatViewRowsPerQueryEstimate()`:

```java
@Override
public long getWalDeleteRowsPerStep() {
    return 1_000_000L;
}
```

- [ ] **Step 4: Wire PropServerConfiguration.** In `PropServerConfiguration.java`: add the field beside `matViewRowsPerQueryEstimate` (`private final long walDeleteRowsPerStep;`), read it in the constructor beside line ~1658:

```java
this.walDeleteRowsPerStep = getLong(properties, env, PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, 1_000_000L);
```

and add the accessor beside the inner-class `getMatViewRowsPerQueryEstimate()` (~4208):

```java
@Override
public long getWalDeleteRowsPerStep() {
    return walDeleteRowsPerStep;
}
```

- [ ] **Step 5: Write the config test.** In the config test class that already asserts `getMatViewRowsPerQueryEstimate()` defaults/overrides, add:

```java
@Test
public void testWalDeleteRowsPerStepDefault() throws Exception {
    PropServerConfiguration configuration = newPropServerConfiguration(); // use the class's existing factory/helper
    Assert.assertEquals(1_000_000L, configuration.getCairoConfiguration().getWalDeleteRowsPerStep());
}

@Test
public void testWalDeleteRowsPerStepOverride() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("cairo.wal.delete.rows.per.step", "250000");
    PropServerConfiguration configuration = newPropServerConfiguration(properties); // mirror the mat-view knob test's override form
    Assert.assertEquals(250_000L, configuration.getCairoConfiguration().getWalDeleteRowsPerStep());
}
```

(Match the surrounding test's exact construction helpers — copy the mat-view knob test method bodies and swap key/getter/values.)

- [ ] **Step 6: Run tests.** `mvn -q -pl core test -Dtest=<ConfigTestClass> -DfailIfNoTests=false`; expect `Tests run: N, Failures: 0, Errors: 0` including the two new methods.

- [ ] **Step 7: Commit.**

```bash
git add core/src/main/java/io/questdb/PropertyKey.java core/src/main/java/io/questdb/cairo/CairoConfiguration.java core/src/main/java/io/questdb/cairo/DefaultCairoConfiguration.java core/src/main/java/io/questdb/PropServerConfiguration.java core/src/test/java/io/questdb/<ConfigTestPath>.java
git commit -m "feat(delete): add cairo.wal.delete.rows.per.step config knob (default 1M)"
```

---

## Task 3: window-step sizing helper

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/wal/OperationExecutor.java` (add a package-private static helper)
- Test: `core/src/test/java/io/questdb/test/cairo/wal/OperationExecutorWindowStepTest.java` (new)

**Interfaces:**
- Consumes: `CairoConfiguration.getWalDeleteRowsPerStep()` (Task 2) — passed in by the caller as `rowsPerStep`.
- Produces: `static long OperationExecutor.deleteWindowStep(long minTs, long maxTs, long tableRows, long rowsPerStep)` → a ts-width (in the table's ts unit) spanning ~`rowsPerStep` rows; `Long.MAX_VALUE` when the whole range should be one window. Used by Task 5.

- [ ] **Step 1: Write the failing test.** Create `OperationExecutorWindowStepTest.java` (Apache header). This is a pure-function test — no engine needed.

```java
package io.questdb.test.cairo.wal;

import io.questdb.cairo.wal.OperationExecutor;
import org.junit.Assert;
import org.junit.Test;

public class OperationExecutorWindowStepTest {

    @Test
    public void testUniformDensityGivesRowsPerStepWidth() {
        // 1000 rows uniformly over ts [0, 999] (span 1000). rowsPerStep=100 -> ~100 ts units per window.
        Assert.assertEquals(100, OperationExecutor.deleteWindowStep(0, 999, 1000, 100));
    }

    @Test
    public void testStepAtLeastOne() {
        // Denser than one row per ts unit: step floors at 1 (never 0, which would not advance the loop).
        Assert.assertEquals(1, OperationExecutor.deleteWindowStep(0, 9, 1_000_000, 100));
    }

    @Test
    public void testRowsPerStepExceedsTableGivesSingleWindow() {
        // rowsPerStep >= tableRows -> step spans the whole populated range (one window).
        long step = OperationExecutor.deleteWindowStep(0, 999, 1000, 10_000);
        Assert.assertTrue("step must cover the whole span", step >= 1000);
    }

    @Test
    public void testEmptyTableSingleWindow() {
        Assert.assertEquals(Long.MAX_VALUE, OperationExecutor.deleteWindowStep(0, 0, 0, 100));
    }

    @Test
    public void testHugeSpanNoOverflow() {
        // Near-max span must not overflow to a negative/zero step (double math in estimateBucketsForRows).
        long step = OperationExecutor.deleteWindowStep(0, (Long.MAX_VALUE >> 1), 1_000_000_000L, 1_000_000L);
        Assert.assertTrue("step positive", step > 0);
    }
}
```

- [ ] **Step 2: Run to verify failure.** `mvn -q -pl core test -Dtest=OperationExecutorWindowStepTest -DfailIfNoTests=false` → FAIL (`deleteWindowStep` not defined / compile error).

- [ ] **Step 3: Implement the helper.** In `OperationExecutor.java`, add the import `import io.questdb.cairo.mv.MatViewRefreshJob;` and the method (place it near `replaceWithSurvivors`):

```java
/**
 * Ts-width (in the table's designated-timestamp unit) that spans roughly {@code rowsPerStep} rows over the
 * populated range {@code [minTs, maxTs]}, used to tile an arbitrary DELETE's survivor-replace into
 * memory-bounded windows. Reuses {@link MatViewRefreshJob#estimateBucketsForRows} with {@code bucket=1},
 * {@code partitionDuration=span}, {@code partitionCount=1}, which reduces to
 * {@code max(1, span * rowsPerStep / tableRows)} computed in double (overflow-safe for large spans). Returns
 * {@code Long.MAX_VALUE} (one window) for an empty table.
 */
static long deleteWindowStep(long minTs, long maxTs, long tableRows, long rowsPerStep) {
    if (tableRows <= 0) {
        return Long.MAX_VALUE;
    }
    final long span = maxTs - minTs + 1; // caller guarantees maxTs >= minTs (non-empty populated range)
    return MatViewRefreshJob.estimateBucketsForRows(rowsPerStep, tableRows, 1, span, 1);
}
```

- [ ] **Step 4: Run to verify pass.** Same command → `Tests run: 5, Failures: 0, Errors: 0`. If `testRowsPerStepExceedsTableGivesSingleWindow` fails because the estimate caps below `span`, confirm the estimate: with rowsPerStep 10000 > tableRows 1000, `span*10000/1000 = 10*span >= span` — passes. If `testHugeSpanNoOverflow` fails, the double math in `estimateBucketsForRows` is the guard; do not reimplement in long.

- [ ] **Step 5: Commit.**

```bash
git add core/src/main/java/io/questdb/cairo/wal/OperationExecutor.java core/src/test/java/io/questdb/test/cairo/wal/OperationExecutorWindowStepTest.java
git commit -m "feat(delete): window-step sizing helper (reuse estimateBucketsForRows)"
```

---

## Task 4: per-window ts-bounded survivor cursor (bind-var interval) — SPIKE-GATED

**Why this is the risk task:** the survivor factory is `SELECT * FROM t WHERE NOT(pred)`. To feed one window at a time we bound it to `[wLo, wHiExcl)`. This MUST become an *interval scan* (reads only the window's partitions), or N windows re-scan the whole table N times (quadratic). The spike proves the interval-scan property on the real generated factory BEFORE the executor is rewired. If it cannot be achieved, STOP and escalate (fallback options are noted at the end of this task).

**Files:**
- Modify: `core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java` (`generateDelete`: at apply time, AND two designated-ts bind-var bounds into the survivor nested model before `generateSelectOneShot`)
- Modify: `core/src/main/java/io/questdb/griffin/engine/ops/DeleteOperation.java` (expose the two bind-variable names so the executor can rebind per window)
- Test: `core/src/test/java/io/questdb/test/cairo/wal/DeleteWindowedApplyTest.java` (new — the spike)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - The survivor factory built at apply time carries two named timestamp bind variables, `:__del_win_lo` and `:__del_win_hi`, ANDed onto `NOT(pred)` as `designatedTs >= :__del_win_lo AND designatedTs < :__del_win_hi`.
  - `String DeleteOperation.WINDOW_LO_BIND = "__del_win_lo";` and `String DeleteOperation.WINDOW_HI_BIND = "__del_win_hi";` (public constants) — used by Task 5 to `bindVariableService.setTimestamp(name, value)` per window.
  - Semantics: with `:__del_win_lo = Long.MIN_VALUE` and `:__del_win_hi = Long.MAX_VALUE` the survivor factory returns the SAME rows as today's whole-range factory (backward compatible for any non-windowed caller/test).

- [ ] **Step 1: Write the failing spike test.** Create `DeleteWindowedApplyTest.java` (Apache header, extends `AbstractCairoTest`). The spike asserts (a) a window-bounded survivor recompile returns only in-window survivors, and (b) it executes as an interval scan (via `explain`), so windowing is not quadratic. It compiles the survivor SELECT directly (the shape `generateDelete` produces) rather than driving a full apply, to isolate the cursor mechanism.

```java
package io.questdb.test.cairo.wal;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class DeleteWindowedApplyTest extends AbstractCairoTest {

    // (a) A designated-ts window bound on the survivor SELECT returns only in-window survivors.
    @Test
    public void testWindowBoundSurvivorSelectReturnsOnlyInWindow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x long) timestamp(ts) partition by DAY WAL");
            execute("insert into t select timestamp_sequence('1970-01-01T00:00:00.000000Z', 60*60*1000000L), x from long_sequence(96)");
            drainWalQueue();
            // Survivors of "delete where x % 2 = 0" = odd x; window = [day2, day3) i.e. rows 24..47.
            assertSql(
                    "ts\tx\n" +
                            "1970-01-02T01:00:00.000000Z\t26\n", // first odd-x survivor in the window (spot-checked head)
                    "select ts, x from t where not (x % 2 = 0) " +
                            "and ts >= '1970-01-02T00:00:00.000000Z' and ts < '1970-01-02T02:00:00.000000Z'"
            );
        });
    }

    // (b) The window bound compiles to an INTERVAL SCAN over the designated timestamp (not a full scan +
    // filter). This is the non-negotiable performance property: N windows must sum to one table pass.
    @Test
    public void testWindowBoundSurvivorSelectUsesIntervalScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x long) timestamp(ts) partition by DAY WAL");
            execute("insert into t select timestamp_sequence('1970-01-01T00:00:00.000000Z', 60*60*1000000L), x from long_sequence(96)");
            drainWalQueue();
            // The plan for a designated-ts range predicate must contain "Interval forward scan" (QuestDB's
            // interval-scan operator), proving only the window's partitions are read.
            assertSqlContains(
                    "select * from t where not (x % 2 = 0) " +
                            "and ts >= '1970-01-02T00:00:00.000000Z' and ts < '1970-01-03T00:00:00.000000Z'",
                    "Interval forward scan"
            );
        });
    }

    // Helper: assert an EXPLAIN of `sql` contains `needle`. (If AbstractCairoTest already exposes an
    // explain-contains helper, use it instead and delete this.)
    private void assertSqlContains(String sql, String needle) throws Exception {
        final StringSink sink = new StringSink();
        try (io.questdb.griffin.SqlCompiler compiler = engine.getSqlCompiler();
             io.questdb.cairo.sql.RecordCursorFactory f = compiler.compile("explain " + sql, sqlExecutionContext).getRecordCursorFactory();
             io.questdb.cairo.sql.RecordCursor c = f.getCursor(sqlExecutionContext)) {
            io.questdb.test.tools.TestUtils.printCursor(c, f.getMetadata(), true, sink, io.questdb.std.str.NoopPrinter.INSTANCE == null ? null : null);
        }
        // Simpler: use the codebase's existing assertPlanNoLeakCheck / assertPlan helper if present.
        org.junit.Assert.assertTrue("expected interval scan in plan:\n" + sink, sink.toString().contains(needle));
    }
}
```

> Implementer note: QuestDB has an established EXPLAIN-plan assertion helper (`assertPlanNoLeakCheck`, or `assertPlan` in `AbstractCairoTest`/`AbstractSqlParserTest` — grep `Interval forward scan` and `assertPlan` in `core/src/test`). Use it directly for `testWindowBoundSurvivorSelectUsesIntervalScan` and delete the hand-rolled helper above; the literal expected-plan string comes from running the query once and copying the printed plan. The literal survivor row in (a) must be copied from an actual run (the exact head row), not guessed.

- [ ] **Step 2: Run the spike test to confirm the interval-scan property on a *plain* SELECT.** `mvn -q -pl core test -Dtest=DeleteWindowedApplyTest -DfailIfNoTests=false`. Expected: the interval-scan test PASSES for a literal-bound SELECT (this proves the general mechanism — a designated-ts range yields an interval scan). If it does NOT, the interval-scan fast path does not cover this predicate shape at all → **STOP, escalate to the human**: the design is not viable as drawn.

- [ ] **Step 3: Implement bind-var injection in `generateDelete`.** In `SqlCompilerImpl.generateDelete`, only when `executionContext.isWalApplication()`, AND a designated-ts range bounded by two named bind variables onto the nested survivor model's WHERE, before `generateSelectOneShot`. Add the two constants to `DeleteOperation` first:

```java
// DeleteOperation.java
public static final String WINDOW_LO_BIND = "__del_win_lo";
public static final String WINDOW_HI_BIND = "__del_win_hi";
```

In `generateDelete` (replace the `generateSelectOneShot` block, apply-time branch):

```java
RecordCursorFactory survivorFactory;
if (executionContext.isWalApplication()) {
    // Bound the survivor scan to a per-window designated-ts interval via two named bind variables, so the
    // executor can re-drive this ONE factory window-by-window (each getCursor is an interval scan over the
    // window). With the bounds set to (MIN, MAX) the factory is identical to the un-windowed survivor scan.
    final int tsIndex = metadata.getTimestampIndex();
    final CharSequence tsColumn = metadata.getColumnName(tsIndex);
    andWindowBounds(model.getNestedModel(), tsColumn); // AND: <ts> >= :__del_win_lo AND <ts> < :__del_win_hi
    executionContext.getBindVariableService().setTimestamp(DeleteOperation.WINDOW_LO_BIND, Long.MIN_VALUE);
    executionContext.getBindVariableService().setTimestamp(DeleteOperation.WINDOW_HI_BIND, Long.MAX_VALUE);
    survivorFactory = generateSelectOneShot(model.getNestedModel(), executionContext, false);
} else {
    // Query-thread compile: validate predicate columns then discard (DELETE travels as SQL text).
    survivorFactory = generateSelectOneShot(model.getNestedModel(), executionContext, false);
    survivorFactory.close();
    survivorFactory = null;
}
```

Implement `andWindowBounds` by constructing the two comparison `ExpressionNode`s (`>=` and `<`) with a designated-ts column literal on the left and a bind-variable node (`:__del_win_lo` / `:__del_win_hi`) on the right, ANDed onto the model's existing `whereClause` (create the `and` nodes via the same `ExpressionNode.FACTORY`/`expressionNodePool` the compiler already uses; grep `ExpressionNode.OPERATION` + `and`/`>=`/`<` construction in `SqlCompilerImpl`/`SqlOptimiser` for the exact node-pool idiom). The bind variables are named (`:name`), which `WhereClauseParser` extracts into a `RuntimeIntervalModel` designated-ts intrinsic — the interval-scan property Step 4 verifies.

- [ ] **Step 4: Extend the spike to the ACTUAL generated factory + rebind.** Add a test that drives `generateDelete` at apply context: compile the DELETE with `isWalApplication` set, get the survivor factory, set the two bind variables to a window, `getCursor`, and assert (a) only in-window survivors come back and (b) re-`getCursor` with a different window returns that window's survivors (proving one factory serves all windows). Also assert the factory's plan is an interval scan.

```java
@Test
public void testGeneratedSurvivorFactoryRebindsPerWindow() throws Exception {
    assertMemoryLeak(() -> {
        execute("create table t (ts timestamp, x long) timestamp(ts) partition by DAY WAL");
        execute("insert into t select timestamp_sequence('1970-01-01T00:00:00.000000Z', 60*60*1000000L), x from long_sequence(96)");
        drainWalQueue();
        // Drive generateDelete at apply context (isWalApplication) and rebind two windows on one factory.
        // Use the same harness OperationExecutor uses: compile "delete from t where x % 2 = 0" with an
        // apply-context SqlExecutionContext, obtain the DeleteOperation's survivor factory, and for each of
        // window [day1,day2) and [day3,day4) set :__del_win_lo/:__del_win_hi then assert the cursor yields
        // exactly that window's odd-x rows. (See OperationExecutor.executeDelete for the apply-context setup;
        // mirror it. Exact expected rows copied from a real run.)
        // ... concrete harness per the executor's apply-context pattern ...
    });
}
```

> This step's concrete harness mirrors `OperationExecutor.executeDelete`'s apply-context compile (an `SqlExecutionContext` with `isWalApplication()==true`). If wiring an apply-context compile in a unit test proves heavy, fold this assertion into the Task 7 end-to-end multi-window test instead and keep Steps 1–3's plain-SELECT interval-scan proof as the Task 4 gate — but do not skip the interval-scan assertion.

- [ ] **Step 5: Run the full spike.** `mvn -q -pl core test -Dtest=DeleteWindowedApplyTest -DfailIfNoTests=false`. All green ⇒ gate cleared: one survivor factory, rebindable per window, interval-scanned. Regression-check the un-windowed default still matches the old survivor scan by running `DeleteTest`: `mvn -q -pl core test -Dtest=DeleteTest -DfailIfNoTests=false` → 39/39 (the MIN/MAX default bounds must leave existing whole-range behaviour unchanged, since Task 5 is not wired yet).

- [ ] **Step 6: Commit.**

```bash
git add core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java core/src/main/java/io/questdb/griffin/engine/ops/DeleteOperation.java core/src/test/java/io/questdb/test/cairo/wal/DeleteWindowedApplyTest.java
git commit -m "feat(delete): per-window bind-var interval bounds on survivor factory (spike-proven interval scan)"
```

**Fallbacks if Step 2/4 shows no interval scan (escalate first, then choose):** (1) build the survivor factory per window by cloning the nested model and adding a *literal* ts intrinsic (`generateSelectOneShot` per window — one compile per window, correct and interval-scanned, extra compile cost); (2) fall back to **per-partition** windows (bound = one partition) if sub-partition bind-var bounds are the only failing part — coarser but still bounds memory to one partition and needs only literal partition-floor bounds. Do not silently downgrade; record the decision in the ledger.

---

## Task 5: windowed `replaceWithSurvivors` loop

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/wal/OperationExecutor.java` (`replaceWithSurvivors`)
- Test: `core/src/test/java/io/questdb/test/griffin/DeleteTest.java` (multi-window correctness under a tiny `rows.per.step`)

**Interfaces:**
- Consumes: `deleteWindowStep` (Task 3); `DeleteOperation.WINDOW_LO_BIND`/`WINDOW_HI_BIND` (Task 4); `TableWriter.beginReplaceRange`/`applyReplaceRangeWindow`/`finishReplaceRange`/`abortReplaceRange` (Task 1).
- Produces: `replaceWithSurvivors` now bounds peak O3 memory to ~one window. Same `long` return (rows removed). Same single-commit contract as before (verified by the existing seqTxn assertions in `DeleteTest`/`QwpEgressDdlExecTest`).

- [ ] **Step 1: Write the failing test.** In `DeleteTest.java`, add a multi-window correctness test that forces many windows via a tiny `rows.per.step` and checks the survivor set exactly. Use the class's existing property-override mechanism (grep `setProperty`/`node1.setProperty`/`overrideProperty` in `DeleteTest`/`AbstractCairoTest` and mirror it):

```java
@Test
public void testArbitraryDeleteWindowedManyWindowsMatchesOracle() throws Exception {
    // Force ~1 row per window so a 240-row table splits into many windows: exercises cross-window
    // accumulation + tiling (deleted rows in inter-survivor gaps must be covered).
    setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1");
    assertMemoryLeak(() -> {
        execute("create table t (ts timestamp, x long, s symbol) timestamp(ts) partition by DAY WAL");
        execute("insert into t select timestamp_sequence('1970-01-01T00:00:00.000000Z', 60*60*1000000L), x, rnd_symbol('a','b','c') from long_sequence(240)");
        drainWalQueue();
        execute("delete from t where x % 7 = 0"); // arbitrary predicate, non-time-range
        drainWalQueue();
        // Oracle: exactly the NOT-predicate survivors, in ts order, unchanged content.
        assertSql(
                "count\n" + (240 - 240 / 7) + "\n",
                "select count() from t"
        );
        assertSqlCursors(
                "select ts, x, s from t order by ts",
                "select ts, x, s from (select timestamp_sequence('1970-01-01T00:00:00.000000Z', 60*60*1000000L) ts, x, rnd_symbol('a','b','c') s from long_sequence(240)) where not (x % 7 = 0) order by ts"
        );
        // Table must not be suspended.
        assertSql("suspended\nfalse\n", "select suspended from wal_tables() where name = 't'");
    });
}
```

> If `assertSqlCursors(String,String)` is not the exact signature in this test base, use the form the other `DeleteTest` cases use to compare a live table against a NOT-predicate reference. The `rnd_symbol` oracle reproduces identical values only if the seed is identical — if the harness reseeds per statement, instead build a `ref` table via `create table ref as (select * from t where not (x % 7 = 0))` BEFORE the delete and compare `t` to `ref` after (the pattern used in `TableWriterReplaceRangeDirectTest`).

- [ ] **Step 2: Run to verify failure.** `mvn -q -pl core test -Dtest=DeleteTest#testArbitraryDeleteWindowedManyWindowsMatchesOracle -DfailIfNoTests=false` → FAIL (still whole-range; property not yet read, or — if you assert window count via a hook — no windowing). At minimum it compiles against the not-yet-added behaviour; confirm it runs and (pre-implementation) the memory-shape is unbounded (behaviourally it may still pass on a tiny table — that's why Task 7 adds the large-table memory-shape check; here the value is the tiling/gap correctness across many windows).

- [ ] **Step 3: Implement the windowed loop.** Replace `replaceWithSurvivors` body. Keep the copier construction; swap the single `replaceRange` for the begin/loop/finish sequence. Read `rowsPerStep` from config; compute `step`; tile `[minTs, maxTs]`; per window rebind the survivor bounds and `applyReplaceRangeWindow`.

```java
private long replaceWithSurvivors(SqlCompiler compiler, TableWriter tableWriter, DeleteOperation deleteOp) throws SqlException {
    final RecordCursorFactory survivorFactory = deleteOp.getSurvivorFactory();
    assert survivorFactory != null : "survivor factory must be built at WAL apply time (isWalApplication)";

    if (tableWriter.getPartitionCount() == 0) {
        return 0; // empty table
    }

    final int timestampCursorIndex = tableWriter.getMetadata().getTimestampIndex();
    entityColumnFilter.of(survivorFactory.getMetadata().getColumnCount());
    final RecordToRowCopier copier = RecordToRowCopierUtils.generateCopier(
            compiler.getAsm(),
            survivorFactory.getMetadata(),
            tableWriter.getMetadata(),
            entityColumnFilter,
            engine.getConfiguration()
    );

    final long minTs = tableWriter.getMinTimestamp();
    final long maxTs = tableWriter.getMaxTimestamp();
    final long rowsPerStep = engine.getConfiguration().getWalDeleteRowsPerStep();
    final long step = deleteWindowStep(minTs, maxTs, tableWriter.size(), rowsPerStep);
    final BindVariableService bind = executionContext.getBindVariableService();

    tableWriter.beginReplaceRange();
    boolean finished = false;
    try {
        long wLo = minTs;
        while (wLo <= maxTs) {
            // hiExcl = min(wLo + step, maxTs + 1), overflow-safe: if step covers the rest, this is the last window.
            final long remaining = maxTs - wLo + 1; // >= 1
            final long wHiExcl = (step >= remaining) ? (maxTs + 1) : (wLo + step);

            bind.setTimestamp(DeleteOperation.WINDOW_LO_BIND, wLo);
            bind.setTimestamp(DeleteOperation.WINDOW_HI_BIND, wHiExcl);
            try (RecordCursor survivorCursor = survivorFactory.getCursor(executionContext)) {
                tableWriter.applyReplaceRangeWindow(wLo, wHiExcl, survivorCursor, copier, timestampCursorIndex, executionContext);
            }
            wLo = wHiExcl;
        }
        final long removed = tableWriter.finishReplaceRange();
        finished = true;
        return removed;
    } finally {
        if (!finished) {
            tableWriter.abortReplaceRange(); // executeDelete's catch performs the txn rollback + setSeqTxn(S-1)
        }
    }
}
```

Add imports: `io.questdb.cairo.sql.BindVariableService` (or the correct package — grep `getBindVariableService()` return type). Keep `import io.questdb.cairo.mv.MatViewRefreshJob;` from Task 3.

- [ ] **Step 4: Run to verify pass.** `mvn -q -pl core test -Dtest=DeleteTest -DfailIfNoTests=false` → all `DeleteTest` green including the new method (39 + 1). If the new test's survivor content mismatches, the tiling boundary is wrong — confirm `wHiExcl` of window K equals `wLo` of window K+1 (no gap, no overlap) and that `applyReplaceRangeWindow`'s in-range assertion (`ts ∈ [wLo,wHiExcl)`) never trips (it would mean the survivor cursor returned an out-of-window row → the bind bounds are not applied).

- [ ] **Step 5: Commit.**

```bash
git add core/src/main/java/io/questdb/cairo/wal/OperationExecutor.java core/src/test/java/io/questdb/test/griffin/DeleteTest.java
git commit -m "feat(delete): windowed survivor-replace bounds arbitrary-DELETE memory to ~rows-per-step (C1)"
```

---

## Task 6: opt-in non-atomic disk-bounded delete (H1) — SPIKE-GATED

**Decision (user, 2026-07-11):** bound transient Parquet disk even in the worst case, accepting non-atomicity. Made **opt-in** via `cairo.wal.delete.disk.bounded` (default `false`): default stays the atomic Task-5 path (C1 fixed, disk unchanged); `=true` switches the arbitrary route to per-window convert+replace+commit — memory AND transient-disk bounded, but **non-atomic** (readers may observe a partial delete during apply) and partitions the delete rewrites still end native (inherent). Crash-safe on both paths.

**Design (disk-bounded path):** do NOT `setSeqTxn(seqTxn)` up front. Loop windows; per window: convert that window's Parquet partitions (up to native, its own commit) then `replaceRange(window)` (its own commit) — every window commits at the still-current durable seqTxn `S-1`, progressively deleting. After the last window, one final `commitSeqTxn(seqTxn)` advances the durable seqTxn to `S`. A crash mid-loop leaves durable seqTxn `S-1`; ApplyWal2TableJob re-runs the whole DELETE; already-deleted windows re-apply as no-ops (their survivor cursor now returns only survivors = survivors-of-survivors) and already-native partitions re-convert as no-ops. Final state identical.

**Files:**
- Modify: `core/src/main/java/io/questdb/PropertyKey.java`, `CairoConfiguration.java`, `DefaultCairoConfiguration.java`, `PropServerConfiguration.java` (the `getWalDeleteDiskBounded()` boolean knob — mirror Task 2's plumbing exactly, default `false`)
- Modify: `core/src/main/java/io/questdb/cairo/wal/OperationExecutor.java` (`executeDelete` branch on the knob; new `replaceWithSurvivorsDiskBounded`; a window-scoped Parquet convert)
- Test: `core/src/test/java/io/questdb/test/cairo/wal/DeleteDiskBoundedApplyTest.java` (new — spike + integration)

**Interfaces:**
- Consumes: `deleteWindowStep` (Task 3), the per-window survivor bind vars (Task 4).
- Produces: `boolean CairoConfiguration.getWalDeleteDiskBounded()`; when true, the arbitrary DELETE apply bounds transient Parquet-convert disk to one window.

- [ ] **Step 1: SPIKE — prove per-window-commit crash-safety FIRST.** Before wiring, prove the S-1-loop-then-commitSeqTxn(S) scheme is crash-safe and idempotent, on a real WAL table, using a direct `OperationExecutor`-style driver or a fault point. Write `DeleteDiskBoundedApplyTest.testPerWindowCommitReappliesIdempotentlyAfterMidLoopCrash`:

```java
// Multi-window arbitrary delete over a Parquet table with disk.bounded=true. Force a crash after an
// intermediate window commits (release the writer / stop draining mid-apply), then re-drain, and assert:
//   - final table == NOT-predicate oracle (idempotent),
//   - table not suspended,
//   - durable seqTxn advanced by exactly 1 for the DELETE (read from wal_tables()/sequencer).
// The crash injection uses the harness's existing fault hooks (grep test hooks for a way to interrupt
// ApplyWal2TableJob mid-txn; if none, model the crash by applying window-by-window via a test-visible
// entry point and releasing+reopening the writer between windows, as TableWriterReplaceRangeDirectTest's
// rollback test models a crash with rollback()).
```

Run: `mvn -q -pl core test -Dtest=DeleteDiskBoundedApplyTest#testPerWindowCommitReappliesIdempotentlyAfterMidLoopCrash -DfailIfNoTests=false`. **If the re-apply is not idempotent (wrong rows, suspended, or seqTxn wrong), STOP and escalate** — the disk-bounded scheme is not viable and the default atomic path is the only shippable one. This is the gate; do not wire Step 3 until it is green.

- [ ] **Step 2: Add the `disk.bounded` config knob.** Mirror Task 2 exactly for a boolean: `CAIRO_WAL_DELETE_DISK_BOUNDED("cairo.wal.delete.disk.bounded")`, `boolean getWalDeleteDiskBounded()` (default `false` in `DefaultCairoConfiguration`), `getBoolean(properties, env, PropertyKey.CAIRO_WAL_DELETE_DISK_BOUNDED, false)` in `PropServerConfiguration`, + the accessor. Add default + override tests beside Task 2's.

- [ ] **Step 3: Implement the disk-bounded apply path.** In `executeDelete`, branch: when `!deleteOp.isPureTimeRange() && engine.getConfiguration().getWalDeleteDiskBounded() && tableWriterHasParquet(tableWriter)`, call `replaceWithSurvivorsDiskBounded(...)` and DO NOT `setSeqTxn(seqTxn)` up front (that path manages seqTxn itself); otherwise keep the current flow (up-front `convertParquetPartitionsForDelete` + `setSeqTxn(seqTxn)` + `replaceWithSurvivors`/`deleteTimeRange`). Implement:

```java
// Non-atomic, disk-bounded arbitrary delete. Each window is its own commit at seqTxn S-1; a final
// commitSeqTxn(seqTxn) advances to S. Bounds BOTH staged O3 memory and transient Parquet-convert disk to
// one window. NON-ATOMIC: a concurrent reader may observe a partially-applied delete during apply. Crash
// mid-loop -> durable S-1 -> whole delete re-applied, finished windows idempotent (survivors-of-survivors),
// already-native partitions re-convert as no-ops.
private long replaceWithSurvivorsDiskBounded(SqlCompiler compiler, TableWriter tableWriter, DeleteOperation deleteOp, long seqTxn) throws SqlException {
    final RecordCursorFactory survivorFactory = deleteOp.getSurvivorFactory();
    assert survivorFactory != null;
    if (tableWriter.getPartitionCount() == 0) {
        tableWriter.commitSeqTxn(seqTxn); // empty table: still advance seqTxn
        return 0;
    }
    final int timestampCursorIndex = tableWriter.getMetadata().getTimestampIndex();
    entityColumnFilter.of(survivorFactory.getMetadata().getColumnCount());
    final RecordToRowCopier copier = RecordToRowCopierUtils.generateCopier(
            compiler.getAsm(), survivorFactory.getMetadata(), tableWriter.getMetadata(), entityColumnFilter, engine.getConfiguration());

    final long minTs = tableWriter.getMinTimestamp();
    final long maxTs = tableWriter.getMaxTimestamp();
    final long step = deleteWindowStep(minTs, maxTs, tableWriter.size(), engine.getConfiguration().getWalDeleteRowsPerStep());
    final BindVariableService bind = executionContext.getBindVariableService();

    long removed = 0;
    long wLo = minTs;
    while (wLo <= maxTs) {
        final long remaining = maxTs - wLo + 1;
        final long wHiExcl = (step >= remaining) ? (maxTs + 1) : (wLo + step);
        // Convert only THIS window's Parquet partitions to native (its own commit at S-1), so at most one
        // window's partitions are transiently native.
        convertParquetPartitionsForDeleteWindow(tableWriter, wLo, wHiExcl);
        bind.setTimestamp(DeleteOperation.WINDOW_LO_BIND, wLo);
        bind.setTimestamp(DeleteOperation.WINDOW_HI_BIND, wHiExcl);
        try (RecordCursor c = survivorFactory.getCursor(executionContext)) {
            // Single-call replaceRange => this window is its own commit (still at durable seqTxn S-1).
            removed += tableWriter.replaceRange(wLo, wHiExcl, c, copier, timestampCursorIndex, executionContext);
        }
        wLo = wHiExcl;
    }
    tableWriter.commitSeqTxn(seqTxn); // FINAL: advance durable seqTxn S-1 -> S (one small commit)
    return removed;
}
```

with the window-scoped convert (its own commit — correct here because this path is intentionally multi-commit):

```java
private void convertParquetPartitionsForDeleteWindow(TableWriter tableWriter, long wLo, long wHiExcl) {
    final int partitionCount = tableWriter.getPartitionCount();
    int converted = 0;
    for (int i = 0; i < partitionCount; i++) {
        if (tableWriter.getPartitionFormat(i) != PartitionFormat.PARQUET) continue;
        final long floor = tableWriter.getPartitionTimestamp(i);
        final long nextFloor = (i == partitionCount - 1) ? (tableWriter.getMaxTimestamp() + 1) : tableWriter.getPartitionTimestamp(i + 1);
        if (floor < wHiExcl && nextFloor > wLo) { // partition [floor,nextFloor) overlaps window
            tableWriter.convertPartitionParquetToNative(floor, false);
            converted++;
        }
    }
    if (converted > 0) {
        tableWriter.commitPendingParquetToNativeConversions();
    }
}
```

Add a small `tableWriterHasParquet(TableWriter)` helper (loop `getPartitionFormat(i) == PARQUET`). Note `commitSeqTxn(long)` already exists (`TableWriter.java:1548`: `setSeqTxn` + `commitTxWriter`). The `executeDelete` catch path is unchanged: a throw mid-loop rolls back the current window and `setSeqTxn(seqTxn - 1)` so the apply job retries the whole delete.

- [ ] **Step 4: Add correctness + non-atomicity integration tests.** In `DeleteDiskBoundedApplyTest` (all with `setProperty(PropertyKey.CAIRO_WAL_DELETE_DISK_BOUNDED, "true")`):
  - arbitrary delete over an all-Parquet multi-partition table, small rows-per-step → many windows → result == NOT-predicate `ref`, not suspended;
  - no-match delete → nothing removed, not suspended;
  - the same delete with `disk.bounded=false` → identical final result (proves the two paths agree on the end state);
  - single-window (huge rows-per-step) → identical to `ref`.
  Use `ref`-table oracles as in Task 5.

- [ ] **Step 5: Run.** `mvn -q -pl core test -Dtest='DeleteDiskBoundedApplyTest,DeleteTest' -DfailIfNoTests=false` → all green.

- [ ] **Step 6: Commit.**

```bash
git add core/src/main/java/io/questdb/PropertyKey.java core/src/main/java/io/questdb/cairo/CairoConfiguration.java core/src/main/java/io/questdb/cairo/DefaultCairoConfiguration.java core/src/main/java/io/questdb/PropServerConfiguration.java core/src/main/java/io/questdb/cairo/wal/OperationExecutor.java core/src/test/java/io/questdb/test/cairo/wal/DeleteDiskBoundedApplyTest.java
git commit -m "feat(delete): opt-in non-atomic disk-bounded arbitrary delete (H1, cairo.wal.delete.disk.bounded)"
```

> **Task 9 doc dependency:** the RESUME-WAL note MUST also document that with `cairo.wal.delete.disk.bounded=true` a DELETE is non-atomic — a concurrent reader may observe it partially applied while WAL apply is in progress — and that it is still crash-safe (a crash re-applies the whole delete).

---

## Task 7: end-to-end windowed DELETE integration tests

**Files:**
- Test: `core/src/test/java/io/questdb/test/cairo/wal/DeleteWindowedApplyTest.java` (extend from Task 4)

**Interfaces:**
- Consumes: the full windowed path (Tasks 3–6).
- Produces: nothing (tests only).

- [ ] **Step 1: Add the integration tests.** All under `setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "<small>")` to force multiple windows. Each asserts against an exact oracle and asserts not-suspended. Cover:

```java
// (1) Single-window equivalence: high rows-per-step -> one window -> identical to whole-range.
@Test public void testSingleWindowEqualsWholeRange() throws Exception { /* rows.per.step huge; delete x%2=0; compare to ref */ }

// (2) Crash-safety / idempotent re-apply: interleave a reader-release + re-drain so the delete re-applies.
@Test public void testWindowedDeleteReappliesIdempotently() throws Exception {
    // rows.per.step small; delete; engine.releaseInactive(); drainWalQueue() again -> unchanged result, one seqTxn.
}

// (3) Zero-match delete over many windows: no rows removed, table not suspended, unchanged content.
@Test public void testWindowedZeroMatchDeleteIsNoOp() throws Exception { /* delete where x < 0 (matches none) */ }

// (4) All-match window: a predicate that empties an interior window entirely (window's survivor cursor empty).
@Test public void testWindowedDeleteEmptiesInteriorWindow() throws Exception { /* delete a whole day's worth via non-time predicate that covers it */ }

// (5) PARTITION BY NONE single-partition table windowed by row density.
@Test public void testWindowedDeletePartitionByNone() throws Exception { /* create ... partition by NONE WAL; delete x%3=0 */ }
```

Write each with a concrete `ref` table built before the delete and `assertSqlCursors`/`assertSql` exact comparisons after (mirror Task 5 Step 1). For (2), assert one seqTxn advance by reading `sequencerTxn`/`writerTxn` from `wal_tables()` before/after re-drain (grep the columns in existing WAL tests) OR assert the row count + content are unchanged after the second drain (idempotence is the observable property).

- [ ] **Step 2: (Memory-shape) Add a large-table window-count check.** Prove windowing actually splits work (not one giant window) at scale, without asserting RSS directly:

```java
@Test
public void testLargeTableProducesManyWindows() throws Exception {
    setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1000");
    assertMemoryLeak(() -> {
        execute("create table t (ts timestamp, x long) timestamp(ts) partition by DAY WAL");
        execute("insert into t select (x*1000000L)::timestamp, x from long_sequence(50000)"); // 50k rows
        drainWalQueue();
        execute("create table ref as (select * from t where not (x % 2 = 0))");
        execute("delete from t where x % 2 = 0");
        drainWalQueue();
        assertSql("suspended\nfalse\n", "select suspended from wal_tables() where name = 't'");
        assertSqlCursors("select ts, x from t order by ts", "select ts, x from ref order by ts");
        // ~50 windows expected (50000 rows / 1000 per step). If a window-count log/metric is added in Task 8,
        // assert it here; otherwise correctness at 50k with a 1k step is the observable memory-bound proxy.
    });
}
```

- [ ] **Step 3: Run.** `mvn -q -pl core test -Dtest=DeleteWindowedApplyTest -DfailIfNoTests=false` → all green.

- [ ] **Step 4: Commit.**

```bash
git add core/src/test/java/io/questdb/test/cairo/wal/DeleteWindowedApplyTest.java
git commit -m "test(delete): end-to-end windowed DELETE (crash-safety, edges, memory-shape)"
```

---

## Task 8: DELETE-apply observability (M1)

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/wal/OperationExecutor.java` (`executeDelete` / `replaceWithSurvivors`)
- Test: none new (log-only); a manual assertion is unnecessary — logging is validated by not breaking existing tests.

**Interfaces:**
- Consumes: Tasks 3–6.
- Produces: one INFO line per DELETE apply.

- [ ] **Step 1: Add the INFO log.** At the end of a successful `replaceWithSurvivors` (and once in `deleteTimeRange`), log strategy + window count + rows. Track the window count in the loop:

```java
// in replaceWithSurvivors, count windows and log after finishReplaceRange:
LOG.info().$("DELETE windowed survivor-replace [table=").$(tableWriter.getTableToken())
        .$(", windows=").$(windowCount)
        .$(", rowsPerStep=").$(rowsPerStep)
        .$(", removed=").$(removed).I$();
```

and in `executeDelete`, near the branch, one line distinguishing the route:

```java
LOG.info().$("DELETE apply [table=").$(tableToken).$(", strategy=")
        .$(deleteOp.isPureTimeRange() ? "time-range" : "survivor-window")
        .$(", seqTxn=").$(seqTxn).I$();
```

(Use the existing `LOG` field in `OperationExecutor`. Match the `$`-builder idiom already used in this file.)

- [ ] **Step 2: Run a smoke test.** `mvn -q -pl core test -Dtest=DeleteTest -DfailIfNoTests=false` → still 41 green (no behavioural change). Optionally eyeball the log line in the test output.

- [ ] **Step 3: Commit.**

```bash
git add core/src/main/java/io/questdb/cairo/wal/OperationExecutor.java
git commit -m "feat(delete): log apply strategy, window count, and rows removed (M1)"
```

---

## Task 9: RESUME-WAL recovery note (M2)

**Files:**
- Modify: the DELETE documentation page (find it: `grep -rl "DELETE FROM" docs/` or the reference SQL docs added with the feature — likely `docs/reference/sql/delete.md`)
- Test: none (docs).

**Interfaces:** none.

- [ ] **Step 1: Add the operator note.** Under a "Recovery / failure behaviour" subsection of the DELETE docs:

```markdown
### Failure and recovery

A `DELETE` is applied atomically at WAL apply time. If the apply fails (for example, the table is
suspended by an unrelated error mid-apply), the transaction is not committed and is retried by the apply
job. If an operator force-resumes the table past the DELETE transaction with
`ALTER TABLE <t> RESUME WAL FROM TRANSACTION <n+1>`, the DELETE is **skipped** — its rows are NOT deleted.
Re-issue the `DELETE` after resuming if the rows must still be removed.
```

- [ ] **Step 2: Commit.**

```bash
git add docs/reference/sql/delete.md
git commit -m "docs(delete): document RESUME WAL skips an un-applied DELETE (M2)"
```

---

## Task 10: behavioral WriteFence DELETE test (OSS-MEDIUM)

**Files:**
- Test: `core/src/test/java/io/questdb/test/griffin/WriteFenceEntryPointMatrixTest.java`

**Interfaces:** none (test only). The existing matrix already classifies DELETE as FENCED at the entry points (http/pg); this adds a BEHAVIOURAL assertion that a DELETE is actually refused when the write fence is engaged, not merely listed in the matrix.

- [ ] **Step 1: Add the behavioral test.** Mirror the existing behavioral fence tests in the file for UPDATE/INSERT (grep the file for how a fenced statement is asserted to be rejected — e.g., a `WRITE FENCE`/read-only assertion), and add the DELETE analogue: engage the fence, attempt `DELETE FROM t WHERE ...`, assert it is rejected with the same error/behaviour as the other fenced writes.

```java
@Test
public void testDeleteIsRefusedUnderWriteFence() throws Exception {
    // ... set up a table, engage the write fence exactly as the sibling UPDATE fence test does ...
    // ... assert "delete from t where x > 0" is rejected with the fenced-write error ...
}
```

(Concrete body copied from the file's nearest sibling fenced-write behavioural test, swapping the statement for `DELETE`.)

- [ ] **Step 2: Run.** `mvn -q -pl core test -Dtest=WriteFenceEntryPointMatrixTest -DfailIfNoTests=false` → green.

- [ ] **Step 3: Commit.**

```bash
git add core/src/test/java/io/questdb/test/griffin/WriteFenceEntryPointMatrixTest.java
git commit -m "test(delete): behavioral write-fence refusal for DELETE (OSS-MEDIUM)"
```

---

## Task 11: Enterprise ACL round-trip + level tests (ENT-3)

**Repo:** `~/claude/wt/ent/delete-statement` (Enterprise). This task is independent of the OSS windowing (authorization only) and can be done any time after the OSS branch compiles; it closes the lvl3-Enterprise test gaps. Do NOT bump the OSS submodule here.

**Files:**
- Test: `questdb-ent/src/test/java/com/questdb/.../PGWireAclTest.java` (the PGWire ACL round-trip test; find the sibling GRANT/REVOKE UPDATE or TRUNCATE case: `grep -rl "GRANT UPDATE\|GRANT TRUNCATE" questdb-ent/src/test`)
- Test: `questdb-ent/src/test/java/com/questdb/.../PermissionTest.java` (or wherever `testIsTableLevel`/`testIsColumnLevel` live: `grep -rln "isTableLevel\|isColumnLevel" questdb-ent/src/test`)

**Interfaces:** none (tests only). Exercises `Permission.DELETE` (=73) + `authorizeTableDelete` already built on this branch.

- [ ] **Step 1: PGWire GRANT/REVOKE DELETE round-trip.** Copy the nearest sibling (UPDATE or TRUNCATE) round-trip test and swap to DELETE: as an admin, `GRANT DELETE ON t TO user`; connect as `user`; `DELETE FROM t WHERE ...` succeeds; `REVOKE DELETE ON t FROM user`; the same `DELETE` is then rejected with the permission error; and with no grant it is rejected. Assert exact outcomes as the sibling does.

- [ ] **Step 2: Level lists.** In the `testIsTableLevel`/`testIsColumnLevel` tests, add `DELETE` to the table-level expected set (and confirm it is absent from column-level), mirroring how `TRUNCATE`/`DROP` appear. Two-line additions.

- [ ] **Step 3: Run.** From the ent repo: `mvn -q -pl questdb-ent test -Dtest='PGWireAclTest,PermissionTest' -DfailIfNoTests=false` → green. (If the test-jar `FileSystemNotFoundException` gotcha blocks local run, note it and rely on CI, as recorded for the sibling ACL tests.)

- [ ] **Step 4: Commit (ent repo).**

```bash
git -C ~/claude/wt/ent/delete-statement add questdb-ent/src/test/java/com/questdb/...
git -C ~/claude/wt/ent/delete-statement commit -m "test(delete): PGWire GRANT/REVOKE DELETE round-trip + table-level permission (ENT-3)"
```

---

## Self-Review

**Spec coverage** (spec `2026-07-11-delete-windowed-survivor-replace-design.md`):
- §2 approach / windowed loop → Task 5. ✅
- §3 window sizing (estimateBucketsForRows) → Task 3. ✅
- §4 per-window bind-var survivor cursor → Task 4. ✅
- §5 replaceRange begin/apply/finish → Task 1 (done). ✅
- §6 crash-safety → preserved by Global Constraints + Task 5's single begin/finish + Task 7(2) idempotence test. ✅
- §6a gate → Task 1 (proven). ✅
- §7 Parquet / H1 → Task 6, redesigned per the user's 2026-07-11 decision as an **opt-in non-atomic disk-bounded path** (`cairo.wal.delete.disk.bounded`, default false): per-window convert+replace+commit at S-1 then final `commitSeqTxn(S)`. Bounds transient disk in all cases at the cost of atomicity; default path stays atomic. Spike-gated on per-window-commit crash-safety. ✅ (non-atomicity documented in Task 9)
- §8 config → Task 2. ✅
- §9 M1/M2 → Tasks 8, 9. ✅
- §10 Enterprise ENT-3 → Task 11. ✅
- §11 edges → Task 7. ✅
- §12 testing → Tasks 5–7. ✅
- OSS-MEDIUM WriteFence → Task 10. ✅

**Placeholder scan:** Tasks 4, 6, 10, 11 intentionally defer some *literal* strings (exact plan text, exact sibling-test bodies, exact ent test paths) to a `grep`-and-copy instruction because the precise form lives in unchanged code the implementer must read; each names the exact anchor to copy from and the exact assertion to make. No logic is left as "TODO". The one genuine open decision — Task 6's per-window-convert seqTxn safety — is called out with a default (A) and an escalation, not hidden.

**Type consistency:** `deleteWindowStep(minTs, maxTs, tableRows, rowsPerStep)` (Task 3) is consumed with those exact args in Task 5. `DeleteOperation.WINDOW_LO_BIND`/`WINDOW_HI_BIND` (Task 4) are the exact names bound in Task 5. `getWalDeleteRowsPerStep()` (Task 2) is read in Tasks 5/8. `beginReplaceRange`/`applyReplaceRangeWindow`/`finishReplaceRange`/`abortReplaceRange` (Task 1) are called with the signatures listed. Consistent.

**Known risks carried into execution:** (1) Task 4 interval-scan property — gated by an embedded spike, escalate on failure; (2) Task 6 per-window-commit crash-safety for the opt-in disk-bounded path — gated by the Step 1 spike, escalate if re-apply is not idempotent (then only the default atomic path ships). Both are proven before wiring, matching Task 1's methodology.

**Task ordering note:** Task 6 depends on Tasks 2–5 (config plumbing, step helper, bind-var cursor, the window loop) and reuses Task 4's per-window survivor bind vars, so it runs after Task 5. Tasks 8–11 are independent of Task 6.
