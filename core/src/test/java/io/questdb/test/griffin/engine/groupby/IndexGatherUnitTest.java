/*+*****************************************************************************
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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.AsyncGroupByNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.IndexGatherPageFrameCursor;
import io.questdb.griffin.engine.table.IndexGatherRecordCursorFactory;
import io.questdb.griffin.engine.table.IndexScanChoiceRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Worker-claimed units of the index-driven parallel GROUP BY ({@code TASK_UNIT}): tasks that are
 * a partition run and a slice of the list's keys, which the slot that takes one counts and reads,
 * against the tasks the query's thread builds from the index's counts, against the frame dispatch
 * switch off, and against the serial GROUP BY over the index scan. Exact aggregates only, so every
 * arm must match the serial one character for character, also on a worker pool.
 */
public class IndexGatherUnitTest extends AbstractCairoTest {
    private static final String EXACT = "count() n, sum(z) sz, min(z) mnz, max(z) mxz, min(y) mny, max(y) mxy, " +
            "min(ts) mnt, max(ts) mxt, count(x) cx";
    // order-sensitive, exact: only where every group draws from one key
    private static final String FIRST_LAST = ", first(z) fz, last(z) lz";
    private static final String[] QUERIES = {
            "select sym, " + EXACT + FIRST_LAST + " from t where sym in ('S0', 'S1', 'S2', 'R0', 'R1', 'R7', 'P1', 'P3', null) order by sym",
            "select sym, " + EXACT + " from t where sym in ('R0', 'R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7', 'R8', 'R9', 'R10', 'R11') order by sym",
            "select " + EXACT + " from t where sym in ('S0', 'S5', 'R3', 'P2')",
            "select " + EXACT + FIRST_LAST + " from t where sym = 'S5'",
            "select " + EXACT + " from t where sym = 'R9'",
            "select sym, timestamp_floor('1h', ts) h, " + EXACT + " from t where sym in ('S0', 'S5', 'R8') order by sym, h",
            "select s2, " + EXACT + " from t where sym in ('S0', 'S5', 'R8', 'R9') and y > 100 order by s2",
            "select sym, " + EXACT + " from t where sym in ('S0', 'S2', 'R3', 'NOPE') and ts in '1970-01-02' order by sym",
            "select sym, " + EXACT + " from t where sym in ('S0', 'S8', 'R3') and ts between '1970-01-01T05' and '1970-01-03T17' order by sym",
            // order-sensitive: a key's rows of a run on one slot, in order
            "select sym, array_agg(z) a from t where sym in ('S1', 'R2', 'P0') order by sym",
            "select sym, first(z) f, last(z) l, first_not_null(x) fn, last_not_null(x) ln from t where sym in ('S0', 'S8', 'R4') order by sym",
            "select array_agg(z) a from t where sym = 'R5'",
    };

    @Override
    @After
    public void tearDown() throws Exception {
        IndexGatherPageFrameCursor.setUnitModeMinProbes(4096);
        super.tearDown();
    }

    @Test
    public void testReopenAfterAppendsInPlace() throws Exception {
        // A cached factory reopened after rows were appended in place to the last partitions, whose
        // readers it takes without a reload (their names do not change): counted tasks and units both
        // read the new rows, as the serial GROUP BY does.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                createTable(engine, ctx, 60_000);
                final String sql = "select sym, count() n, sum(z) s, max(z) m from t where sym in ('S0', 'S2', 'R3', 'R7') order by sym";
                for (long minProbes : new long[]{Long.MAX_VALUE, 1}) {
                    IndexGatherPageFrameCursor.setUnitModeMinProbes(minProbes);
                    setFeatures(true, false);
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
                    try (RecordCursorFactory factory = engine.select(sql, ctx)) {
                        for (int round = 0; round < 3; round++) {
                            final String actual = print(factory, ctx);
                            Assert.assertEquals(minProbes == 1, findGather(factory).getPageFrameCursorForTesting().isUnitMode());
                            setFeatures(false, false);
                            TestUtils.assertEquals(sql + " [round " + round + ", minProbes " + minProbes + ']', runToString(engine, ctx, sql), actual);
                            setFeatures(true, false);
                            // in order, after the last row: into the last partition, in place
                            engine.execute("insert into t select case when x % 2 = 0 then 'S0' else 'R7' end, x, 7, 1_000_000 * (" + round + " + 1) + x, 's', "
                                    + "(max_ts + x)::timestamp from long_sequence(500) cross join (select max(ts) max_ts from t)", ctx);
                        }
                    }
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testUnitsMatchSerialOnTestContext() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, 60_000);
            assertArms(engine, sqlExecutionContext, false);
        });
    }

    @Test
    public void testUnitsMatchSerialOnWorkerPool() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                createTable(engine, ctx, 200_000);
                assertArms(engine, ctx, true);
            }, configuration, LOG);
        });
    }

    @Test
    public void testEstimateProbesStayWithinBudget() throws Exception {
        // 96 hourly partitions and a list of 50 keys: 4800 (partition, key) counts, which the
        // estimate samples down to its budget with the switch on, and reads in full with it off;
        // the plan choice and the result are the same
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, 60_000);
            final StringBuilder in = new StringBuilder();
            for (int i = 0; i < 50; i++) {
                in.append(i == 0 ? "" : ", ").append("'R").append(i).append('\'');
            }
            final String sql = "select sym, count() n, sum(z) s from t where sym in (" + in + ") order by sym";
            setFeatures(false, false);
            final String expected = runToString(engine, sqlExecutionContext, sql);
            setFeatures(true, true);
            for (boolean dispatch : new boolean[]{false, true}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, String.valueOf(dispatch));
                try (RecordCursorFactory factory = select(sql)) {
                    TestUtils.assertEquals(sql, expected, print(factory, sqlExecutionContext));
                    final long probes = findGather(factory).getLastEstimateProbes();
                    if (dispatch) {
                        Assert.assertTrue("probes " + probes, probes > 0 && probes <= 4096);
                    } else {
                        Assert.assertTrue("probes " + probes, probes >= 4800 && probes % 50 == 0);
                    }
                }
            }
        });
    }

    @Test
    public void testUnitModeIsChosenPerOpen() throws Exception {
        // one cached factory: a long list over the partitions builds units, a short one counted
        // tasks, as the bind variables change
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, 60_000);
            setFeatures(true, false);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
            // 4 days in 96 hourly partitions; 1 key = 96 probes, 12 keys = 1152
            IndexGatherPageFrameCursor.setUnitModeMinProbes(1000);
            final String sql = "select sym, count() n, sum(z) s from t where sym in ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) order by sym";
            final String[] many = {"R0", "R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8", "R9", "R10", "R11"};
            final String[] one = {"R0", "R0", "R0", "R0", "R0", "R0", "R0", "R0", "R0", "R0", "R0", "R0"};
            try (RecordCursorFactory factory = select(sql)) {
                final IndexGatherRecordCursorFactory gather = findGather(factory);
                Assert.assertNotNull(gather);
                for (int round = 0; round < 2; round++) {
                    for (String[] list : new String[][]{many, one}) {
                        bindVariableService.clear();
                        for (int i = 0; i < list.length; i++) {
                            bindVariableService.setStr(i, list[i]);
                        }
                        final String actual = print(factory, sqlExecutionContext);
                        final IndexGatherPageFrameCursor cursor = gather.getPageFrameCursorForTesting();
                        Assert.assertEquals(list == many, cursor.isUnitMode());
                        Assert.assertEquals(list == many, cursor.getTaskCounts()[3] > 0);
                        setFeatures(false, false);
                        TestUtils.assertEquals(sql, runToString(engine, sqlExecutionContext, sql), actual);
                        setFeatures(true, false);
                    }
                }
            }
        });
    }

    private static void assertArms(CairoEngine engine, SqlExecutionContext ctx, boolean expectWorkers) throws SqlException {
        long unitTasks = 0;
        long workerTasks = 0;
        for (String sql : QUERIES) {
            // serial GROUP BY over the index scan
            setFeatures(false, false);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
            final String expected = runToString(engine, ctx, sql);
            setFeatures(true, false);
            // the switch off: tasks from counts, on the query's thread
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "false");
            IndexGatherPageFrameCursor.setUnitModeMinProbes(1);
            assertGather(engine, ctx, sql, expected, false);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
            // the switch on, below the threshold: counted tasks
            IndexGatherPageFrameCursor.setUnitModeMinProbes(Long.MAX_VALUE);
            assertGather(engine, ctx, sql, expected, false);
            // the switch on, units
            IndexGatherPageFrameCursor.setUnitModeMinProbes(1);
            for (int run = 0; run < 3; run++) {
                final long[] counts = assertGather(engine, ctx, sql, expected, true);
                unitTasks += counts[3];
                workerTasks += counts[2];
            }
        }
        Assert.assertTrue("no unit task", unitTasks > 0);
        if (expectWorkers) {
            Assert.assertTrue("no worker took a unit", workerTasks > 0);
        }
    }

    private static long[] assertGather(CairoEngine engine, SqlExecutionContext ctx, String sql, String expected, boolean units) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
            final IndexGatherRecordCursorFactory gather = findGather(factory);
            Assert.assertNotNull(sql, gather);
            if (units) {
                // the readers the units read are frozen before any worker reads them, also from a
                // fresh table reader whose index memory no earlier query mapped
                engine.releaseAllReaders();
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    cursor.hasNext();
                    final IndexGatherPageFrameCursor frames = gather.getPageFrameCursorForTesting();
                    Assert.assertTrue(sql, !frames.isUnitMode() || frames.getFrozenReaderCount() > 0);
                }
            }
            TestUtils.assertEquals(sql + (units ? " [units]" : " [counted]"), expected, print(factory, ctx));
            final IndexGatherPageFrameCursor cursor = gather.getPageFrameCursorForTesting();
            Assert.assertNotNull(sql + "\n" + explain(compiler, ctx, sql), cursor);
            Assert.assertEquals(sql + " keys=" + cursor.getKeys() + " tasks=" + cursor.getTaskCount() + " counts=" + java.util.Arrays.toString(cursor.getTaskCounts()), units && cursor.getKeys().size() > 0, cursor.isUnitMode());
            final long[] counts = cursor.getTaskCounts();
            if (!units) {
                Assert.assertEquals(sql, 0, counts[3]);
            } else {
                Assert.assertEquals(sql, 0, counts[0]);
            }
            return counts;
        }
    }

    // 4 days by the hour; S0 a third of the rows, S2, S5, S8 mid, R0..R49 rare, P<n> only on day n % 4,
    // every 23rd row NULL; z and y exact
    private static void createTable(CairoEngine engine, SqlExecutionContext ctx, int rows) throws SqlException {
        engine.execute(
                "create table t (sym symbol index type posting, x double, y int, z long, s2 symbol, ts timestamp)" +
                        " timestamp(ts) partition by hour bypass wal",
                ctx
        );
        engine.execute(
                "insert into t select " +
                        "case when x % 23 = 0 then null " +
                        "when x % 3 = 0 then 'S0' " +
                        "when x % 3 = 1 and x % 5 != 0 then 'S' || (1 + (x * 7) % 9) " +
                        "when x % 7 = 0 then 'P' || (((x * 4) / " + rows + ") * 3 + x % 3) " +
                        "else 'R' || ((x * 9) % 50) end, " +
                        "case when x % 11 = 0 then null else rnd_double() end, " +
                        "rnd_int(0, 1000, 7), rnd_long(-1000, 1000, 9), rnd_symbol('a', 'b', '', null), " +
                        "(x * (4 * 86_400_000_000L / " + rows + "))::timestamp " +
                        "from long_sequence(" + rows + ")",
                ctx
        );
    }

    private static String explain(SqlCompiler compiler, SqlExecutionContext ctx, String sql) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile("explain " + sql, ctx).getRecordCursorFactory()) {
            return print(factory, ctx);
        }
    }

    private static IndexGatherRecordCursorFactory findGather(RecordCursorFactory factory) {
        RecordCursorFactory f = factory;
        for (int i = 0; f != null && i < 6; i++) {
            if (f instanceof IndexGatherRecordCursorFactory gather) {
                return gather;
            }
            if (f instanceof IndexScanChoiceRecordCursorFactory choice) {
                f = choice.getIndexPlan();
                continue;
            }
            if ((f instanceof AsyncGroupByRecordCursorFactory || f instanceof AsyncGroupByNotKeyedRecordCursorFactory)
                    && f.getBaseFactory() instanceof IndexGatherRecordCursorFactory gather) {
                return gather;
            }
            f = f.getBaseFactory();
        }
        return null;
    }

    private static String print(RecordCursorFactory factory, SqlExecutionContext ctx) throws SqlException {
        final StringSink sink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            io.questdb.cairo.CursorPrinter.println(cursor, factory.getMetadata(), sink);
        }
        return sink.toString();
    }

    private static String runToString(CairoEngine engine, SqlExecutionContext ctx, String sql) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
            return print(factory, ctx);
        }
    }

    private static void setFeatures(boolean gather, boolean choice) {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_INDEX_GROUPBY_ENABLED, String.valueOf(gather));
        setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_CHOICE_ENABLED, String.valueOf(choice));
    }
}
