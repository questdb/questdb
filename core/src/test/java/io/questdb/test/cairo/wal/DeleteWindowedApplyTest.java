/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.ops.DeleteOperation;
import io.questdb.std.Chars;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Task 4 spike: proves the apply-time survivor factory (built by SqlCompilerImpl.generateDelete when
 * isWalApplication) can be bounded to a per-window designated-timestamp interval via two operation-owned
 * indexed bind variables, that this bound
 * executes as an INTERVAL SCAN (so N windows sum to one table pass rather than N full scans), and that the
 * SAME factory is rebindable window by window. This is the linchpin: OperationExecutor (Task 5) drives this
 * one factory per window, re-running getCursor with new bounds.
 * <p>
 * Task 7 adds end-to-end integration coverage on top of the spike: each test below drives a REAL
 * {@code execute("delete ...")} through {@code drainWalQueue()} under a tiny
 * {@code cairo.wal.delete.rows.per.step} (Task 5's windowed {@code OperationExecutor.replaceWithSurvivors}
 * loop) to force multiple partition-aligned windows where the fixture spans partitions, and checks the result
 * against an exact pre-delete {@code ref} snapshot table
 * (never a second, independently-reseeded {@code rnd_*} statement) plus a direct
 * {@code TableSequencerAPI.isSuspended} not-suspended check.
 */
public class DeleteWindowedApplyTest extends AbstractDeleteApplyTest {

    // Designated-timestamp day boundaries (micros) for the fixture below: hourly rows x=1..96 over 4 days.
    private static final long DAY1_LO = 0L;                 // 1970-01-01T00:00:00Z, rows x=1..24
    private static final long DAY2_LO = 86_400_000_000L;    // 1970-01-02T00:00:00Z
    private static final long DAY3_LO = 172_800_000_000L;   // 1970-01-03T00:00:00Z, rows x=49..72
    private static final long DAY4_LO = 259_200_000_000L;   // 1970-01-04T00:00:00Z

    // (b) The ACTUAL generated survivor factory (bind-variable bounds, not literals) executes as an interval
    // scan. This is the non-negotiable performance gate: the window bound must engage QuestDB's interval-scan
    // operator so only the window's partitions are read.
    @Test
    public void testGeneratedSurvivorFactoryUsesIntervalScan() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulate();
            try (SqlExecutionContext applyContext = newApplyContext();
                 SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cc = compiler.compile("delete from t where x % 2 = 0", applyContext);
                try (DeleteOperation op = cc.getDeleteOperation()) {
                    final RecordCursorFactory factory = op.getSurvivorFactory();
                    Assert.assertNotNull("survivor factory must be built at WAL apply time", factory);
                    planSink.of(factory, applyContext);
                    final CharSequence plan = planSink.getSink();
                    Assert.assertTrue(
                            "expected an interval scan on the generated survivor factory, plan was:\n" + plan,
                            Chars.contains(plan, "Interval forward scan")
                    );
                }
            }
        });
    }

    // (a) + (c) The generated survivor factory returns exactly the in-window survivors, and re-binding a
    // DIFFERENT window on the SAME factory returns that window's survivors (proving one factory serves all
    // windows). Also checks the compiled-in defaults (MIN+1, MAX) return the whole-range survivor set.
    @Test
    public void testGeneratedSurvivorFactoryRebindsPerWindow() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulate();
            try (SqlExecutionContext applyContext = newApplyContext();
                 SqlCompiler compiler = engine.getSqlCompiler()) {
                applyContext.getBindVariableService().setInt(0, 42);
                applyContext.getBindVariableService().setInt("__del_win_lo", 11);
                applyContext.getBindVariableService().setStr("__del_win_hi", "user-value");
                // Mirrors OperationExecutor.executeDelete's apply-context compile (isWalApplication()==true).
                final CompiledQuery cc = compiler.compile("delete from t where x % 2 = 0", applyContext);
                try (DeleteOperation op = cc.getDeleteOperation()) {
                    final RecordCursorFactory factory = op.getSurvivorFactory();
                    Assert.assertNotNull("survivor factory must be built at WAL apply time", factory);
                    Assert.assertEquals(1, op.getWindowLoBindVariableIndex());
                    Assert.assertEquals(2, op.getWindowHiBindVariableIndex());
                    Assert.assertEquals(ColumnType.INT, applyContext.getBindVariableService().getFunction(0).getType());
                    Assert.assertEquals(42, applyContext.getBindVariableService().getFunction(0).getInt(null));
                    Assert.assertEquals(ColumnType.INT, applyContext.getBindVariableService().getFunction(":__del_win_lo").getType());
                    Assert.assertEquals(11, applyContext.getBindVariableService().getFunction(":__del_win_lo").getInt(null));
                    Assert.assertEquals(ColumnType.STRING, applyContext.getBindVariableService().getFunction(":__del_win_hi").getType());
                    Assert.assertEquals("user-value", applyContext.getBindVariableService().getFunction(":__del_win_hi").getStrA(null).toString());

                    // Default bounds compiled in by generateDelete: the whole survivor set (odd x, 1..95).
                    Assert.assertEquals(oddsInclusive(1, 96), collectX(factory, applyContext));

                    // Window [day1, day2) -> rows x=1..24, survivors (odd x) 1..23.
                    setWindow(op, applyContext, DAY1_LO, DAY2_LO);
                    Assert.assertEquals(oddsInclusive(1, 24), collectX(factory, applyContext));

                    // Re-bind a different window [day3, day4) on the SAME factory -> rows x=49..72, odds 49..71.
                    setWindow(op, applyContext, DAY3_LO, DAY4_LO);
                    Assert.assertEquals(oddsInclusive(49, 72), collectX(factory, applyContext));

                    // Re-bind back to the first window to confirm the rebind is stateless/repeatable.
                    setWindow(op, applyContext, DAY1_LO, DAY2_LO);
                    Assert.assertEquals(oddsInclusive(1, 24), collectX(factory, applyContext));
                }
            }
        });
    }

    @Test
    public void testPredicateReplayStabilityClassification() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulate();
            try (SqlExecutionContext applyContext = newApplyContext();
                 SqlCompiler compiler = engine.getSqlCompiler()) {
                applyContext.getBindVariableService().setLong(0, 7);
                applyContext.getBindVariableService().setLong("divisor", 7);
                try (DeleteOperation operation = compiler.compile("DELETE FROM t WHERE x % 7 = 0", applyContext).getDeleteOperation()) {
                    Assert.assertTrue(operation.isPredicateReplayStable());
                }
                try (DeleteOperation operation = compiler.compile("DELETE FROM t WHERE x % $1 = 0", applyContext).getDeleteOperation()) {
                    Assert.assertTrue("WAL-captured indexed binds must be replay-stable", operation.isPredicateReplayStable());
                }
                try (DeleteOperation operation = compiler.compile("DELETE FROM t WHERE x % :divisor = 0", applyContext).getDeleteOperation()) {
                    Assert.assertTrue("WAL-captured named binds must be replay-stable", operation.isPredicateReplayStable());
                }
                try (DeleteOperation operation = compiler.compile("DELETE FROM t WHERE rnd_boolean()", applyContext).getDeleteOperation()) {
                    Assert.assertFalse(operation.isPredicateReplayStable());
                }
                try (DeleteOperation operation = compiler.compile(
                        "DELETE FROM t WHERE geo_distance_meters(0, 0, rnd_double(), 0) > 50000",
                        applyContext
                ).getDeleteOperation()) {
                    Assert.assertFalse("wrapped random functions must not be replay-stable", operation.isPredicateReplayStable());
                }
                try (DeleteOperation operation = compiler.compile("DELETE FROM t WHERE ts < now()", applyContext).getDeleteOperation()) {
                    Assert.assertFalse(operation.isPredicateReplayStable());
                }
                try (DeleteOperation operation = compiler.compile(
                        "DELETE FROM t WHERE ts < timestamp_shuffle(0, 600000000000)",
                        applyContext
                ).getDeleteOperation()) {
                    Assert.assertFalse("timestamp_shuffle must not be replay-stable", operation.isPredicateReplayStable());
                }
                try (DeleteOperation operation = compiler.compile(
                        "DELETE FROM t WHERE rnd_interval()::STRING = ''",
                        applyContext
                ).getDeleteOperation()) {
                    Assert.assertFalse("isRandom functions must not be replay-stable", operation.isPredicateReplayStable());
                }
            }
        });
    }

    // Baseline (Step 2 gate): a designated-ts range on a plain SELECT of the survivor shape compiles to an
    // interval scan. Proves the general mechanism a literal-bound range engages the interval-scan operator;
    // the bind-variable bounds above are the runtime equivalent.
    @Test
    public void testWindowBoundLiteralSelectUsesIntervalScan() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulate();
            final CharSequence plan = getPlanSink(
                    "select * from t where not (x % 2 = 0) " +
                            "and ts >= '1970-01-02T00:00:00.000000Z' and ts < '1970-01-03T00:00:00.000000Z'"
            ).getSink();
            Assert.assertTrue(
                    "expected an interval scan for a designated-ts range predicate, plan was:\n" + plan,
                    Chars.contains(plan, "Interval forward scan")
            );
        });
    }

    // Regression test (CRITICAL fix): on a TIMESTAMP_NS designated-timestamp WAL table, generateDelete used to
    // set the __del_win_lo/__del_win_hi defaults via BindVariableService.setTimestamp, which is unconditionally
    // MICROS-typed. At apply time the survivor factory's runtime interval evaluated that bind variable through
    // NanosTimestampDriver.from(value, TIMESTAMP_MICRO) -> microsToNanos -> Math.multiplyExact, and the
    // Long.MAX_VALUE default overflowed: ImplicitCastException, WAL apply fails, and the table is SUSPENDED
    // without deleting anything. DeleteOperation.setWindowBound (used by generateDelete) fixes this by setting
    // the bind variable in the designated-timestamp column's OWN unit (setTimestampNano for TIMESTAMP_NANO).
    // Before the fix this test fails with the table suspended; after the fix it deletes exactly the matched
    // rows and leaves the table healthy.
    @Test
    public void testDeleteArbitraryPredicateOnNanosTimestampDoesNotSuspendTable() throws Exception {
        assertMemoryLeak(() -> {
            // Hourly rows over 4 daily partitions (x=1..96), mirroring createAndPopulate() but in nanos.
            execute("create table t (ts timestamp_ns, x long) timestamp(ts) partition by DAY WAL");
            execute("insert into t select timestamp_sequence_ns(0, 60*60*1000000000L), x from long_sequence(96)");
            drainWalQueue();
            // Independent oracle snapshot, never touched by the DELETE.
            execute("create table t_ref as (select * from t)");

            final TableToken tableToken = engine.verifyTableName("t");

            // An arbitrary (non-time-range) predicate forces the whole-range survivor-replace path
            // (OperationExecutor.replaceWithSurvivors), which is exactly where the bind-variable unit bug bites.
            execute("delete from t where x % 2 = 0");
            drainWalQueue();

            Assert.assertFalse(
                    "table must not be suspended by a DELETE on a TIMESTAMP_NS designated-timestamp column",
                    engine.getTableSequencerAPI().isSuspended(tableToken)
            );
            // Surviving rows must be exactly the NOT(x % 2 = 0) set (odd x, 1..95), in table order.
            assertSqlCursors(
                    "select * from t_ref where not (x % 2 = 0)",
                    "select * from t"
            );
        });
    }

    // ------------------------------------------------------------------------------------------------------
    // Task 7: end-to-end windowed DELETE integration tests. These minimize the target row count per window;
    // the atomic route rounds each high to a partition boundary. They drive a real execute()+drainWalQueue() and compare
    // against an EXACT oracle: a `ref` table snapshotted from `t` BEFORE the delete runs (never a re-evaluated
    // rnd_* expression), plus a direct TableSequencerAPI.isSuspended not-suspended check.
    // ------------------------------------------------------------------------------------------------------

    /**
     * (1) Single-window equivalence: a rows-per-step far larger than the table collapses the windowed loop to
     * exactly ONE window (the whole populated range in a single {@code applyReplaceRangeWindow} call), so the
     * windowed path must produce the identical result a plain whole-range delete would.
     */
    @Test
    public void testSingleWindowEqualsWholeRange() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "100000000"); // >> table size -> one window
        assertMemoryLeak(() -> {
            createAndPopulate();
            execute("create table ref as (select * from t where not (x % 2 = 0))");

            execute("delete from t where x % 2 = 0");
            drainWalQueue();

            Assert.assertFalse("table must not be suspended after the DELETE",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            assertSqlCursors("select * from ref", "select * from t");
        });
    }

    /**
     * (2) Idempotent re-apply / crash-safety proxy: a tiny rows-per-step forces one window per populated day.
     * Once the delete
     * is fully applied, {@code engine.releaseInactive()} drops every cached reader/writer (simulating a clean
     * restart) and {@code drainWalQueue()} runs again with nothing new enqueued (the delete's seqTxn is
     * already durably applied). Re-applying must be a genuine no-op: identical content, table still healthy.
     */
    @Test
    public void testWindowedDeleteReappliesIdempotently() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1"); // partition alignment -> one window per day
        assertMemoryLeak(() -> {
            createAndPopulate();
            execute("create table ref as (select * from t where not (x % 2 = 0))");

            execute("delete from t where x % 2 = 0");
            drainWalQueue();

            Assert.assertFalse("table must not be suspended after the DELETE",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            assertSqlCursors("select * from ref", "select * from t");

            // Simulate a restart (drop every cached reader/writer) and re-drain with nothing new queued.
            engine.releaseInactive();
            drainWalQueue();

            Assert.assertFalse("table must not be suspended after the DELETE",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            assertSqlCursors("select * from ref", "select * from t");
        });
    }

    /**
     * (3) Zero-match delete over partition-aligned windows: {@code x < 0} matches nothing, so every window's survivor
     * cursor returns its whole slice unchanged. The windowed loop must be a full no-op: identical row count
     * and content, table still healthy.
     */
    @Test
    public void testWindowedZeroMatchDeleteIsNoOp() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1");
        assertMemoryLeak(() -> {
            createAndPopulate();
            execute("create table ref as (select * from t)"); // nothing will match; ref == whole table

            execute("delete from t where x < 0");
            drainWalQueue();

            Assert.assertFalse("table must not be suspended after the DELETE",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            assertQuery("select count(*) from t").noRandomAccess().expectSize().returns("count\n96\n");
            assertSqlCursors("select * from ref", "select * from t");
        });
    }

    /**
     * (4) All-match interior window: a residual (non-time-range) predicate matches every row of day 2
     * (x=25..48), an INTERIOR day - neither the table's first nor its last partition. Partition alignment
     * makes that day one entirely-empty-survivor window. Days 1, 3 and 4's windows are untouched (their
     * survivor cursors return every row), so this exercises an interior empty window sandwiched between normal
     * ones. The predicate is
     * deliberately expressed on {@code x}, not {@code ts}: a time-range predicate would be classified
     * {@code isPureTimeRange()} and take the single-shot fast path instead of this windowed loop.
     */
    @Test
    public void testWindowedDeleteEmptiesInteriorWindow() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1");
        assertMemoryLeak(() -> {
            createAndPopulate();
            // Day 2 is rows x=25..48 (see DAY2_LO/DAY3_LO above): the whole day, nothing else.
            execute("create table ref as (select * from t where not (x >= 25 and x <= 48))");

            execute("delete from t where x >= 25 and x <= 48");
            drainWalQueue();

            Assert.assertFalse("table must not be suspended after the DELETE",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            assertSqlCursors("select * from ref", "select * from t");
        });
    }

    /**
     * (5) Single-partition safety: even a tiny row target must collapse to one partition-aligned window. This
     * prevents the atomic replace transaction from rewriting one physical partition under the same name twice.
     * <p>
     * NOTE ON DEVIATION FROM THE BRIEF: a literal {@code PARTITION BY NONE ... WAL} table cannot be created -
     * WAL write mode is rejected on a non-partitioned table ("WAL Write Mode can only be used on partitioned
     * tables", see {@code AlterTableWalEnabledTest#testWalEnabledNonPartitionedTable}; also asserted in
     * {@code CairoEngine.createTable}: {@code !isWalEnabled() || PartitionBy.isPartitioned(...)}), and the
     * windowed DELETE path only runs at WAL-apply time. Using {@code PARTITION BY DAY WAL} with every row
     * confined to a single calendar day gives the same property a NONE table would have (exactly one
     * physical partition, confirmed below), which is what this test actually needs to exercise.
     */
    @Test
    public void testWindowedDeletePartitionByNone() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1");
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x long) timestamp(ts) partition by DAY WAL");
            // 300 one-minute rows = 5 hours, all inside 1970-01-01: exactly one physical partition.
            execute("insert into t select timestamp_sequence('1970-01-01T00:00:00.000000Z', 60*1000000L), x from long_sequence(300)");
            drainWalQueue();
            assertQuery("select count(*) from table_partitions('t')")
                    .noRandomAccess().expectSize().returns("count\n1\n");
            execute("create table ref as (select * from t where not (x % 3 = 0))");

            execute("delete from t where x % 3 = 0");
            drainWalQueue();

            Assert.assertFalse("table must not be suspended after the DELETE",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            assertSqlCursors("select * from ref", "select * from t");
        });
    }

    /**
     * Task 7 Step 2 (memory-shape proxy): a 50k-row table with one row per minute and rows-per-step=1000 tiles
     * into roughly 35 day-aligned windows. This doesn't assert RSS directly (out of scope for a JUnit test),
     * but proves correctness at a scale where the windowed loop is genuinely exercised many times over - not
     * collapsed to one window as
     * in {@link #testSingleWindowEqualsWholeRange} - the observable proxy for "peak memory bounded to ~one
     * window" that Task 5's windowed rewrite targets.
     */
    @Test
    public void testDeleteCommitInvalidatesMatViewWhenHousekeepingFails() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x long) timestamp(ts) partition by DAY WAL");
            execute("insert into t select timestamp_sequence('1970-01-01T00:00:00.000000Z', 60*60*1000000L), x from long_sequence(96)");
            drainWalQueue();
            execute("create materialized view t_1h as (select ts, sum(x) x from t sample by 1h) partition by DAY");
            drainWalAndMatViewQueues();

            final boolean[] faulted = {false};
            TableWriter.setReplaceRangePostCommitObserver(() -> {
                if (!faulted[0]) {
                    faulted[0] = true;
                    throw io.questdb.cairo.CairoException.nonCritical().put("post-commit housekeeping fault");
                }
            });
            try {
                execute("delete from t where x % 2 = 0");
                drainWalQueue();
            } finally {
                TableWriter.setReplaceRangePostCommitObserver(null);
            }

            Assert.assertTrue(faulted[0]);
            Assert.assertTrue("post-commit apply failure must suspend the table",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            engine.releaseInactive();
            Assert.assertEquals(48, count("select count(*) from t"));
            drainWalAndMatViewQueues();
            assertQuery("select view_status, invalidation_reason from materialized_views where view_name = 't_1h'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("view_status\tinvalidation_reason\ninvalid\tdelete operation\n");
        });
    }

    @Test
    public void testLargeTableProducesManyWindows() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1000");
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x long) timestamp(ts) partition by DAY WAL");
            execute("insert into t select (x*60*1000000L)::timestamp, x from long_sequence(50000)"); // 50k minute rows
            drainWalQueue();
            execute("create table ref as (select * from t where not (x % 2 = 0))");

            execute("delete from t where x % 2 = 0");
            drainWalQueue();

            Assert.assertFalse("table must not be suspended after the DELETE",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            assertSqlCursors("select ts, x from ref order by ts", "select ts, x from t order by ts");
        });
    }

    /**
     * Verifies the density/partition-aligned tiling arithmetic and observes the real WAL apply loop using the
     * resulting many-window route. This is a row-count/window-routing oracle, not a process-RSS measurement.
     */
    @Test
    public void testWindowArithmeticAndActualApplyUseManyWindows() throws Exception {
        final long rowsPerStep = 1000;
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, String.valueOf(rowsPerStep));
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x long) timestamp(ts) partition by DAY WAL");
            // 50k rows, one per minute: enough day partitions for many safe atomic windows. A day contains
            // 1440 rows / 720 survivors, so rounding a 1000-row density target to a day boundary stays bounded.
            execute("insert into t select (x*60*1000000L)::timestamp, x from long_sequence(50000)");
            drainWalQueue();
            execute("create table ref as (select * from t where not (x % 2 = 0))");

            // Read the table's populated range + row count exactly as OperationExecutor.replaceWithSurvivors does
            // (tableWriter.getMinTimestamp()/getMaxTimestamp()/size()), from the pre-delete committed state the
            // window loop reads throughout.
            final long minTs = count("select min(ts) from t");
            final long maxTs = count("select max(ts) from t");
            final long tableRows = count("select count(*) from t");
            final long totalSurvivors = count("select count(*) from t where not (x % 2 = 0)");
            Assert.assertEquals(50000, tableRows);
            Assert.assertEquals(25000, totalSurvivors);

            // The exact step the apply loop uses for this rows.per.step.
            final long step = WalUtils.deleteWindowStep(minTs, maxTs, tableRows, rowsPerStep);
            Assert.assertTrue("windowed step must be a strict sub-range of the populated span", step < (maxTs - minTs + 1));

            // Tile [minTs, maxTs+1) with the loop's EXACT formula and measure each window's staged survivor slice
            // (== the in-window NOT-predicate count the survivor cursor returns and copies into O3).
            long windowCount = 0;
            long maxWindowStaged = 0;
            long summedStaged = 0;
            long wLo = minTs;
            while (wLo <= maxTs) {
                final long wHiExcl = WalUtils.deleteAtomicWindowHiExcl(
                        wLo,
                        step,
                        maxTs,
                        ColumnType.TIMESTAMP,
                        PartitionBy.DAY
                );
                final long staged = count("select count(*) from t where not (x % 2 = 0) and ts >= " + wLo + " and ts < " + wHiExcl);
                maxWindowStaged = Math.max(maxWindowStaged, staged);
                summedStaged += staged;
                windowCount++;
                wLo = wHiExcl;
            }

            // Sanity: the windows partition the survivor set exactly (no double count / gap).
            Assert.assertEquals("windows must exactly partition the survivor set", totalSurvivors, summedStaged);
            // The loop genuinely tiled into many windows (not collapsed to one).
            Assert.assertTrue("expected many windows, got " + windowCount, windowCount >= 20);
            // C1 bound: NO single window stages more than rows.per.step rows...
            Assert.assertTrue(
                    "peak per-window staged survivors (" + maxWindowStaged + ") must be <= rows.per.step (" + rowsPerStep + ")",
                    maxWindowStaged <= rowsPerStep
            );
            // ...and the peak is a small fraction of the whole survivor set (the memory windowing actually saves).
            Assert.assertTrue(
                    "peak per-window staged survivors (" + maxWindowStaged + ") must be << total survivors (" + totalSurvivors + ")",
                    maxWindowStaged * 10 <= totalSurvivors
            );

            // NEGATIVE CONTROL: rows.per.step >> table size collapses to ONE window whose staged slice IS the
            // ENTIRE survivor set - the whole-table peak windowing exists to avoid. This is what maxWindowStaged
            // would equal if windowing were disabled.
            final long hugeStep = WalUtils.deleteWindowStep(minTs, maxTs, tableRows, 100_000_000L);
            Assert.assertTrue("a rows.per.step >> table size must collapse to a single window",
                    hugeStep >= (maxTs - minTs + 1));
            final long singleWindowStaged = count("select count(*) from t where not (x % 2 = 0) and ts >= " + minTs + " and ts < " + (maxTs + 1));
            Assert.assertEquals("the single (unwindowed) window stages the ENTIRE survivor set", totalSurvivors, singleWindowStaged);
            Assert.assertTrue(
                    "windowing must bound the peak far below the unwindowed whole-survivor-set staging",
                    maxWindowStaged * 10 <= singleWindowStaged
            );

            // Finally, observe the real executor rather than relying only on the independently tiled arithmetic.
            final AtomicLong actualWindowCount = new AtomicLong();
            WalUtils.setDeleteWindowObserver(actualWindowCount::set);
            try {
                execute("delete from t where x % 2 = 0");
                drainWalQueue();
            } finally {
                WalUtils.setDeleteWindowObserver(null);
            }
            Assert.assertTrue("real WAL apply must use many windows, was " + actualWindowCount, actualWindowCount.get() >= 20);
            Assert.assertFalse("table must not be suspended after the DELETE",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            assertSqlCursors("select ts, x from ref order by ts", "select ts, x from t order by ts");
        });
    }

    /**
     * Task 7 (extra - folds in a Task 5 review coverage gap): deletes EVERY row of the table's LAST partition
     * (day 4, x=73..96) with a tiny rows-per-step. Atomic windows align to day boundaries, so the last window of the
     * whole loop is always the one that reaches the table's chronologically-last partition, this exercises
     * {@code TableWriter}'s fully-removed-last-partition path and the trailing housekeeping performed by
     * {@code finishReplaceRange}. The existing {@code DeleteTest} windowed cases only mutate partitions in
     * place ({@code x % 7 = 0} never empties a whole partition) and never reach this branch at all.
     * <p>
     * Asserts day 4's rows are gone AND its partition directory itself is physically removed (not just zero
     * matching rows), days 1-3 are exactly intact against {@code ref}, and the table is not suspended.
     */
    @Test
    public void testWindowedDeleteEmptiesWholeMultiWindowPartition() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1"); // partition alignment -> one window per day
        assertMemoryLeak(() -> {
            createAndPopulate();
            // Day 4 is rows x=73..96 (see DAY4_LO above): the WHOLE last day, nothing else.
            execute("create table ref as (select * from t where not (x >= 73))");

            execute("delete from t where x >= 73");
            drainWalQueue();

            Assert.assertFalse("table must not be suspended after the DELETE",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            // The partition itself must be physically removed, not just empty of matching rows.
            assertQuery("select count(*) from table_partitions('t') where name = '1970-01-04'")
                    .noRandomAccess().expectSize().returns("count\n0\n");
            assertSqlCursors("select * from ref", "select * from t");
        });
    }

    /**
     * D3 (whole-PR level-3, on-disk orphan-directory oracle): the ATOMIC multi-window replace must reclaim
     * EVERY window's superseded partition directory, not just the final window's.
     * {@code TableWriter.processO3Block} clears {@code partitionRemoveCandidates} at the start of each window's
     * apply, and the single drain ({@code finishReplaceRange} -> {@code housekeep} ->
     * {@code processPartitionRemoveCandidates}) runs once after the whole bracket. So before the fix, each new
     * window wiped the previous window's candidates and only the FINAL window's superseded dirs reached the
     * drain - every earlier window's fully-dropped partition directory leaked on disk (detached from the table,
     * invisible to {@code table_partitions()}), reclaimed only by the next writer open/rollback. The fix
     * accumulates each window's candidates into {@code replaceRangeRemoveCandidates} and drains them once after
     * {@code commit00}.
     * <p>
     * Oracle: a tiny {@code rows.per.step} tiles a {@code DELETE FROM t WHERE x <= 120} (arbitrary residual
     * predicate -> windowed survivor-replace) into six day-aligned windows, fully dropping
     * days 1..5 (each in an early window) and keeping day 6. Read the on-disk state RIGHT AFTER apply, WITHOUT
     * reopening the writer or running the async purge (either would independently re-scan and reclaim the leak,
     * masking it). The fix reclaims each window's transient superseded partition-version dir synchronously in
     * {@code finishReplaceRange}'s {@code housekeep}, so on disk NO calendar-day partition has a duplicate
     * physical version dir. The bug leaves each partition's superseded version behind - a SECOND physical dir for
     * the same calendar day (12 dirs vs the fix's 6, one per day).
     */
    @Test
    public void testMultiWindowReplaceReclaimsEveryWindowsSupersededPartitionDirs() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1"); // one window per day
        assertMemoryLeak(() -> {
            // 144 hourly rows over 6 daily partitions: day1 x=1..24, day2 x=25..48, ..., day6 x=121..144.
            execute("create table t (ts timestamp, x long) timestamp(ts) partition by DAY WAL");
            execute("insert into t select timestamp_sequence('1970-01-01T00:00:00.000000Z', 60*60*1000000L), x from long_sequence(144)");
            drainWalQueue();
            // Independent oracle snapshot of the survivors (day 6 only), never touched by the DELETE.
            execute("create table ref as (select * from t where not (x <= 120))");

            final TableToken tableToken = engine.verifyTableName("t");
            // Arbitrary predicate (residual on x, NOT a pure time range) forces the windowed survivor-replace:
            // empties days 1..5 (x=1..120), keeps day 6 (x=121..144).
            execute("delete from t where x <= 120");
            drainWalQueue();

            Assert.assertFalse("table must not be suspended after the DELETE",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            assertSqlCursors("select * from ref", "select * from t");
            // Exactly one LIVE partition (day 6).
            assertQuery("select count(*) from table_partitions('t')")
                    .noRandomAccess().expectSize().returns("count\n1\n");

            // ON-DISK oracle, read RIGHT AFTER apply with no writer reopen / async purge (either would
            // independently reclaim the leak and mask it): the fix reclaims every window's transient superseded
            // partition-version dir synchronously in finishReplaceRange's housekeep, so NO calendar-day partition
            // has a duplicate physical version dir on disk. The bug leaves each dropped/rewritten partition's
            // superseded version behind, i.e. a SECOND physical dir for the same calendar day.
            final int duplicatePartitionDirs = countDuplicatePartitionVersionDirs(tableToken);
            Assert.assertEquals(
                    "no calendar-day partition may have a duplicate (superseded) physical version dir after the " +
                            "multi-window replace; leftover duplicates are the earlier-window candidate leak",
                    0,
                    duplicatePartitionDirs
            );
        });
    }

    /**
     * F-D coverage: drive VAR-SIZE columns (varchar + string, including NULLs) through the survivor-replace
     * forced-O3 staging path. Every other DELETE test uses only fixed-width columns
     * (long/int/timestamp/symbol/decimal), so {@code TableWriter.applyReplaceRangeWindow}'s var-size copier was
     * never exercised by the windowed DELETE apply. The pass proved this path correct for var-size empirically;
     * this locks it in permanently. This is the varchar/string analogue of
     * {@code TableWriterReplaceRangeDirectTest#testReplaceRangeSurvivorsWithDecimalColumn}, but through the full
     * windowed WAL-apply path.
     * <p>
     * Fixture: a single BY DAY partition, rows inserted out of designated-timestamp order (ts permuted by a
     * coprime multiply) so the INSERT's WAL apply O3-sorts them into the partition; the survivor-replace copier
     * under test then stages the surviving var-size values, which arrive ts-ordered from the survivor cursor
     * (the O3 survivor sort itself is exercised elsewhere). A varchar and a string column each with a surviving NULL
     * (x=7 -> null varchar, x=13 -> null string; both odd, so both survive the delete and flow through the
     * copier). {@code rows.per.step=1} still yields one safe window for the single physical partition;
     * {@code delete where x % 2 = 0} partially survives it. Assert content AND order against the NOT-predicate
     * oracle + exact survivor count.
     */
    @Test
    public void testVarSizeColumnsThroughSurvivorReplace() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1"); // one safe partition-aligned window
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x long, v varchar, s string) timestamp(ts) partition by DAY WAL");
            // 48 rows, ALL on 1970-01-01, minute-of-day permuted by ((x-1)*37 % 48) (37 coprime to 48) so the
            // rows are inserted out of designated-timestamp order and the WAL apply O3-sorts them. NULL var-size
            // at x=7 (varchar) and x=13 (string) - both odd -> survive the delete -> exercise the NULL path.
            execute(
                    "insert into t select " +
                            "((((x - 1) * 37) % 48) * 60L * 1000000L)::timestamp ts, " +
                            "x, " +
                            "case when x = 7 then cast(null as varchar) else cast('v' || x as varchar) end v, " +
                            "case when x = 13 then cast(null as string) else cast('s' || x as string) end s " +
                            "from long_sequence(48)"
            );
            drainWalQueue();
            // Independent oracle snapshot of the survivors (odd x), never touched by the DELETE.
            execute("create table t_ref as (select * from t)");

            execute("delete from t where x % 2 = 0");
            drainWalQueue();

            Assert.assertFalse("table must not be suspended after the DELETE",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            // Content AND order (both tables are designated-timestamp ordered): every surviving var-size value,
            // incl. the NULLs, must round-trip through the forced-O3 survivor copier byte-for-byte.
            assertSqlCursors("select * from t_ref where not (x % 2 = 0)", "select * from t");
            // Exact survivor count: odd x in 1..48 == 24 rows.
            Assert.assertEquals(24, count("select count(*) from t"));
        });
    }

    /**
     * Empties one sibling of a real split partition, releases cached readers/writers, and verifies the surviving
     * sibling remains durable and readable after the replace-range removal candidates are drained.
     */
    @Test
    public void testSplitPartitionSiblingEmptiedDrainKeepsLivePartitionsIntact() throws Exception {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1"); // aggressive: prefix split threshold ~0 rows
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1");     // keep split siblings in one logical-day window
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x long) timestamp(ts) partition by DAY WAL");
            // Three days of minute data (x=1..4319), the last day (1970-01-03) full to 23:58 - mirrors the
            // WAL split recipe in WalTableFailureTest#testForceDropPartitionRangeNotOnDiskWithSplits.
            execute("insert into t select timestamp_sequence('1970-01-01T00:00:00.000000Z', 60*1000000L), x from long_sequence(60*24*3 - 1)");
            drainWalQueue();
            // O3-insert into the last day LATE at 23:00 (1970-01-03T23:00 == 255600000000us), x=7001..7060. A late
            // O3 insert makes the prefix (00:00..22:59) dominate the suffix (23:00..23:58)+o3, satisfying the split
            // heuristic (prefix > 2*(suffix+merge+o3)), so 1970-01-03 splits into two same-day-floor siblings.
            execute("insert into t select (255600000000L + (x - 1) * 1000000L)::timestamp, 7000 + x from long_sequence(60)");
            drainWalQueue();

            // Self-verifying fixture: 1970-01-03 must now be a REAL split -> two same-day-floor partition versions.
            Assert.assertEquals("fixture must produce a real split partition (two same-day-floor 1970-01-03 versions)",
                    2, count("select count(*) from table_partitions('t') where minTimestamp >= '1970-01-03T00:00:00.000000Z' and minTimestamp < '1970-01-04T00:00:00.000000Z'"));

            // Split boundary: rows with ts < 23:00 are the head sibling (x=1..headMaxX), the rest is the tail
            // sibling. Read it rather than hardcode so the fixture stays valid if the split point ever shifts.
            final long headMaxX = count("select max(x) from t where ts < '1970-01-03T23:00:00.000000Z'");

            // Independent oracle snapshot of the survivors (x <= headMaxX), never touched by the DELETE.
            execute("create table t_ref as (select * from t)");

            // "keep only x <= headMaxX" (the verifier's fixture): FULLY empties the tail split sibling (all its
            // rows have x > headMaxX) while the head sibling and the earlier days survive UNTOUCHED - driving the
            // parent line-split removal branch that queues a frozen-txn (nameTxn == txWriter.txn) removal candidate.
            execute("delete from t where x > " + headMaxX);
            drainWalQueue();

            engine.releaseInactive();
            Assert.assertFalse("a drain collision must surface as a suspended table",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            assertSqlCursors("select * from t_ref where not (x > " + headMaxX + ")", "select * from t");
        });
    }

    // Collects the survivor cursor's x column (index 1 of SELECT *) in cursor order as a comma-separated string.
    private static String collectX(RecordCursorFactory factory, SqlExecutionContext executionContext) throws SqlException {
        final StringBuilder sink = new StringBuilder();
        try (RecordCursor cursor = factory.getCursor(executionContext)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                if (sink.length() > 0) {
                    sink.append(',');
                }
                sink.append(record.getLong(1));
            }
        }
        return sink.toString();
    }

    // Expected in-order odd x values in [loX, hiX] (the survivors of "delete where x % 2 = 0").
    private static String oddsInclusive(int loX, int hiX) {
        final StringBuilder sink = new StringBuilder();
        for (int x = loX; x <= hiX; x++) {
            if ((x & 1) == 1) {
                if (sink.length() > 0) {
                    sink.append(',');
                }
                sink.append(x);
            }
        }
        return sink.toString();
    }

    private static void setWindow(DeleteOperation operation, SqlExecutionContext executionContext, long loInclusive, long hiExclusive) throws SqlException {
        executionContext.getBindVariableService().setTimestamp(operation.getWindowLoBindVariableIndex(), loInclusive);
        executionContext.getBindVariableService().setTimestamp(operation.getWindowHiBindVariableIndex(), hiExclusive);
    }

    private void createAndPopulate() throws Exception {
        execute("create table t (ts timestamp, x long) timestamp(ts) partition by DAY WAL");
        execute("insert into t select timestamp_sequence('1970-01-01T00:00:00.000000Z', 60*60*1000000L), x from long_sequence(96)");
        drainWalQueue();
    }

    // A WAL-apply execution context: isWalApplication()==true, so generateDelete negates the predicate and
    // keeps the survivor factory (with the window bind-var bounds), exactly as OperationExecutor does at apply.
    private SqlExecutionContext newApplyContext() {
        final SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1) {
            @Override
            public boolean isWalApplication() {
                return true;
            }
        };
        context.with(AllowAllSecurityContext.INSTANCE, new BindVariableServiceImpl(engine.getConfiguration()), new Rnd());
        return context;
    }
}
