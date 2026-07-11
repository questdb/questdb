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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.ops.DeleteOperation;
import io.questdb.std.Chars;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Task 4 spike: proves the apply-time survivor factory (built by SqlCompilerImpl.generateDelete when
 * isWalApplication) can be bounded to a per-window designated-timestamp interval via the two named bind
 * variables {@link DeleteOperation#WINDOW_LO_BIND}/{@link DeleteOperation#WINDOW_HI_BIND}, that this bound
 * executes as an INTERVAL SCAN (so N windows sum to one table pass rather than N full scans), and that the
 * SAME factory is rebindable window by window. This is the linchpin: OperationExecutor (Task 5) drives this
 * one factory per window, re-running getCursor with new bounds.
 * <p>
 * Task 7 adds end-to-end integration coverage on top of the spike: each test below drives a REAL
 * {@code execute("delete ...")} through {@code drainWalQueue()} under a tiny
 * {@code cairo.wal.delete.rows.per.step} (Task 5's windowed {@code OperationExecutor.replaceWithSurvivors}
 * loop) to force many windows, and checks the result against an exact pre-delete {@code ref} snapshot table
 * (never a second, independently-reseeded {@code rnd_*} statement) plus a {@code wal_tables()} not-suspended
 * check.
 */
public class DeleteWindowedApplyTest extends AbstractCairoTest {

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
                // Mirrors OperationExecutor.executeDelete's apply-context compile (isWalApplication()==true).
                final CompiledQuery cc = compiler.compile("delete from t where x % 2 = 0", applyContext);
                try (DeleteOperation op = cc.getDeleteOperation()) {
                    final RecordCursorFactory factory = op.getSurvivorFactory();
                    Assert.assertNotNull("survivor factory must be built at WAL apply time", factory);

                    // Default bounds compiled in by generateDelete: the whole survivor set (odd x, 1..95).
                    Assert.assertEquals(oddsInclusive(1, 96), collectX(factory, applyContext));

                    // Window [day1, day2) -> rows x=1..24, survivors (odd x) 1..23.
                    setWindow(applyContext, DAY1_LO, DAY2_LO);
                    Assert.assertEquals(oddsInclusive(1, 24), collectX(factory, applyContext));

                    // Re-bind a different window [day3, day4) on the SAME factory -> rows x=49..72, odds 49..71.
                    setWindow(applyContext, DAY3_LO, DAY4_LO);
                    Assert.assertEquals(oddsInclusive(49, 72), collectX(factory, applyContext));

                    // Re-bind back to the first window to confirm the rebind is stateless/repeatable.
                    setWindow(applyContext, DAY1_LO, DAY2_LO);
                    Assert.assertEquals(oddsInclusive(1, 24), collectX(factory, applyContext));
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
    // Task 7: end-to-end windowed DELETE integration tests. All force MANY windows via
    // cairo.wal.delete.rows.per.step, drive the delete through a real execute()+drainWalQueue(), and compare
    // against an EXACT oracle: a `ref` table snapshotted from `t` BEFORE the delete runs (never a re-evaluated
    // rnd_* expression), plus a wal_tables() not-suspended check.
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

            assertQuery("select suspended from wal_tables() where name = 't'")
                    .noRandomAccess().returns("suspended\nfalse\n");
            assertSqlCursors("select * from ref", "select * from t");
        });
    }

    /**
     * (2) Idempotent re-apply / crash-safety proxy: a tiny rows-per-step forces many windows. Once the delete
     * is fully applied, {@code engine.releaseInactive()} drops every cached reader/writer (simulating a clean
     * restart) and {@code drainWalQueue()} runs again with nothing new enqueued (the delete's seqTxn is
     * already durably applied). Re-applying must be a genuine no-op: identical content, table still healthy.
     */
    @Test
    public void testWindowedDeleteReappliesIdempotently() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1"); // ~1 row/window -> many windows
        assertMemoryLeak(() -> {
            createAndPopulate();
            execute("create table ref as (select * from t where not (x % 2 = 0))");

            execute("delete from t where x % 2 = 0");
            drainWalQueue();

            assertQuery("select suspended from wal_tables() where name = 't'")
                    .noRandomAccess().returns("suspended\nfalse\n");
            assertSqlCursors("select * from ref", "select * from t");

            // Simulate a restart (drop every cached reader/writer) and re-drain with nothing new queued.
            engine.releaseInactive();
            drainWalQueue();

            assertQuery("select suspended from wal_tables() where name = 't'")
                    .noRandomAccess().returns("suspended\nfalse\n");
            assertSqlCursors("select * from ref", "select * from t");
        });
    }

    /**
     * (3) Zero-match delete over many windows: {@code x < 0} matches nothing, so every window's survivor
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

            assertQuery("select suspended from wal_tables() where name = 't'")
                    .noRandomAccess().returns("suspended\nfalse\n");
            assertQuery("select count(*) from t").noRandomAccess().expectSize().returns("count\n96\n");
            assertSqlCursors("select * from ref", "select * from t");
        });
    }

    /**
     * (4) All-match interior window(s): a residual (non-time-range) predicate matches every row of day 2
     * (x=25..48), an INTERIOR day - neither the table's first nor its last partition - with a tiny
     * rows-per-step so that day is covered by several small windows, all with an entirely empty survivor
     * cursor. Days 1, 3 and 4's windows are untouched (their survivor cursors return every row), so this
     * exercises a run of interior all-empty windows sandwiched between normal ones. The predicate is
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

            assertQuery("select suspended from wal_tables() where name = 't'")
                    .noRandomAccess().returns("suspended\nfalse\n");
            assertSqlCursors("select * from ref", "select * from t");
        });
    }

    /**
     * (5) Single-partition table windowed purely by row density: no partition boundary is ever in play, so
     * every window boundary is exactly where {@code deleteWindowStep}'s row-density estimate puts it.
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

            assertQuery("select suspended from wal_tables() where name = 't'")
                    .noRandomAccess().returns("suspended\nfalse\n");
            assertSqlCursors("select * from ref", "select * from t");
        });
    }

    /**
     * Task 7 Step 2 (memory-shape proxy): a 50k-row table with rows-per-step=1000 tiles into roughly 50
     * windows. This doesn't assert RSS directly (out of scope for a JUnit test), but proves correctness at a
     * scale where the windowed loop is genuinely exercised many times over - not collapsed to one window as
     * in {@link #testSingleWindowEqualsWholeRange} - the observable proxy for "peak memory bounded to ~one
     * window" that Task 5's windowed rewrite targets.
     */
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

            assertQuery("select suspended from wal_tables() where name = 't'")
                    .noRandomAccess().returns("suspended\nfalse\n");
            assertSqlCursors("select ts, x from ref order by ts", "select ts, x from t order by ts");
        });
    }

    /**
     * Task 7 (extra - folds in a Task 5 review coverage gap): deletes EVERY row of the table's LAST partition
     * (day 4, x=73..96) with a tiny rows-per-step, so that single partition spans MANY windows (rows.per.step
     * of 1 over a 4-day/96-row table puts roughly 20+ windows inside one day). Because the last window of the
     * whole loop is always the one that reaches the table's chronologically-last partition, this is the shape
     * that exercises {@code TableWriter}'s "partition fully removed by a windowed replace" guard
     * ({@code o3ConsumePartitionUpdateSink}, ~{@code TableWriter.java:8710}: skip queuing a same-bracket
     * {@code srcNameTxn} as a removal candidate) in a way that would actually surface a regression - per the
     * Task 5 report, a fully-emptied INTERIOR partition's spurious candidate is silently cleared by the next
     * window before it is ever drained, while the last partition's candidate list is the one that survives
     * uncleared to {@code finishReplaceRange}'s trailing housekeep. The existing {@code DeleteTest} windowed
     * cases only mutate partitions in place ({@code x % 7 = 0} never empties a whole partition) and never
     * reach this branch at all.
     * <p>
     * Asserts day 4's rows are gone AND its partition directory itself is physically removed (not just zero
     * matching rows), days 1-3 are exactly intact against {@code ref}, and the table is not suspended.
     */
    @Test
    public void testWindowedDeleteEmptiesWholeMultiWindowPartition() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_DELETE_ROWS_PER_STEP, "1"); // ~1 row/window -> day 4 spans many windows
        assertMemoryLeak(() -> {
            createAndPopulate();
            // Day 4 is rows x=73..96 (see DAY4_LO above): the WHOLE last day, nothing else.
            execute("create table ref as (select * from t where not (x >= 73))");

            execute("delete from t where x >= 73");
            drainWalQueue();

            assertQuery("select suspended from wal_tables() where name = 't'")
                    .noRandomAccess().returns("suspended\nfalse\n");
            // The partition itself must be physically removed, not just empty of matching rows.
            assertQuery("select count(*) from table_partitions('t') where name = '1970-01-04'")
                    .noRandomAccess().expectSize().returns("count\n0\n");
            assertSqlCursors("select * from ref", "select * from t");
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

    private static void setWindow(SqlExecutionContext executionContext, long loInclusive, long hiExclusive) throws SqlException {
        executionContext.getBindVariableService().setTimestamp(DeleteOperation.WINDOW_LO_BIND, loInclusive);
        executionContext.getBindVariableService().setTimestamp(DeleteOperation.WINDOW_HI_BIND, hiExclusive);
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
