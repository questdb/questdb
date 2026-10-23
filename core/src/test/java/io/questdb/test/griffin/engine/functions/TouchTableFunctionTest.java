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

package io.questdb.test.griffin.engine.functions;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.test.TestFaultFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestWorkerCloneFunctionFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TouchTableFunctionTest extends AbstractCairoTest {

    private static final String DDL = "create table x as " +
            "(" +
            "  select" +
            "    rnd_geohash(40) g," +
            "    rnd_double(0)*100 a," +
            "    rnd_symbol(5,4,4,1) b," +
            "    timestamp_sequence(0, 100000000000) k" +
            " from long_sequence(20)" +
            "), index(b) timestamp(k) partition by DAY";

    @Test
    public void testNoSegfaultWhenAddingNewColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t1 AS (
                      SELECT generate_series as t,\s
                             (generate_series % 123456)::int as i\s
                      FROM generate_series('2025-12-25', '2025-12-26', '1s')
                    ) PARTITION BY DAY WAL;
                    """);

            drainWalQueue();

            String query = "select touch(select * from t1);";

            sink.clear();
            TestUtils.printSql(engine, sqlExecutionContext, query, sink);
            TestUtils.assertContains(sink, "data_pages");

            execute("alter table t1 add column f float;");
            drainWalQueue();

            sink.clear();
            TestUtils.printSql(engine, sqlExecutionContext, query, sink);
            TestUtils.assertContains(sink, "data_pages");

            execute("update t1 set f = 5.2f");
            drainWalQueue();

            sink.clear();
            TestUtils.printSql(engine, sqlExecutionContext, query, sink);
            TestUtils.assertContains(sink, "data_pages");
        });
    }

    @Test
    public void testTouchInParallelFilter() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        runWithPool((engine, ctx) -> {
            final int threshold = createTouchTables(engine, ctx);
            final String[] filters = {
                    "x > length(touch(SELECT * FROM touch_src))",
                    "x > length(touch(SELECT * FROM touch_src WHERE ts >= (SELECT min(ts) FROM touch_src)))"
            };
            for (String filter : filters) {
                TestWorkerCloneFunctionFactory.arm(threshold);
                try {
                    assertQuery("SELECT count(*) FROM touch_rows WHERE test_worker_clone(" + filter + ", x::DOUBLE)")
                            .withEngine(engine)
                            .withContext(ctx)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .withPlanContaining("Async Filter workers: 4")
                            .returns("count\n" + (10_000 - threshold) + "\n");
                    Assert.assertTrue(TestWorkerCloneFunctionFactory.workerEvaluations() > 0);
                    Assert.assertEquals(0, TestWorkerCloneFunctionFactory.mismatches());
                } finally {
                    TestWorkerCloneFunctionFactory.disarm();
                }
            }
        });
    }

    @Test
    public void testTouchInParallelFilterFailures() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        runWithPool((engine, ctx) -> {
            final int threshold = createTouchTables(engine, ctx);
            final String query = "SELECT count(*) FROM touch_rows WHERE x > length(touch(SELECT * FROM touch_src)) AND test_fault()";
            final String expected = "count\n" + (10_000 - threshold) + "\n";
            try {
                int failures = 0;
                RecordCursorFactory compiled;
                while (true) {
                    TestFaultFunctionFactory.armToFailAfterCompiles(failures);
                    try {
                        compiled = engine.select(query, ctx);
                        break;
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "test_fault: injected compile failure");
                        failures++;
                    }
                }
                TestFaultFunctionFactory.disarm();
                try (RecordCursorFactory factory = compiled) {
                    Assert.assertEquals("owner plus four worker clones", 5, failures);
                    TestFaultFunctionFactory.armToFailAfter(5_000);
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        cursor.hasNext();
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "test_fault: injected failure");
                    }
                    TestFaultFunctionFactory.disarm();
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, sink);
                    }
                }
            } finally {
                TestFaultFunctionFactory.disarm();
            }
        });
    }

    @Test
    public void testTouchTableNoTimestampColumnSelected() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select touch(select g,a,b from x where k in '1970-01-22')";
            try {
                execute(DDL, sqlExecutionContext);
                TestUtils.printSql(engine, sqlExecutionContext, query, sink);
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "query does not support framing execution and cannot be pre-touched");
            }
        });
    }

    @Test
    public void testTouchTableThrowOnComplexFilter() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select touch(select * from x where k in '1970-01-22' and a > 100.0)";
            try {
                execute(DDL, sqlExecutionContext);
                TestUtils.printSql(engine, sqlExecutionContext, query, sink);
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "query does not support framing execution and cannot be pre-touched");
            }
        });
    }

    @Test
    public void testTouchTableTimeInterval() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select touch(select * from x where k in '1970-01-22')";
            try {
                execute(DDL, sqlExecutionContext);
                TestUtils.printSql(engine, sqlExecutionContext, query, sink);
            } catch (SqlException ex) {
                Assert.fail(ex.getMessage());
            }
        });
    }

    @Test
    public void testTouchTableTimeRange() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select touch(select * from x where k > '1970-01-18T00:00:00.000000Z')";
            execute(DDL, sqlExecutionContext);
            try {
                TestUtils.printSql(engine, sqlExecutionContext, query, sink);
            } catch (SqlException ex) {
                Assert.fail(ex.getMessage());
            }
        });
    }

    @Test
    public void testTouchUpdateTouchAgain() throws Exception {
        assertMemoryLeak(() -> {

            final String query = "select touch(select * from x)";

            final String ddl2 = "insert into x select * from (" +
                    " select" +
                    " rnd_geohash(40)," +
                    " rnd_double(0)*100," +
                    " 'VTJW'," +
                    " to_timestamp('2019', 'yyyy') t" +
                    " from long_sequence(100)" +
                    ") timestamp (t)";

            try {
                execute(DDL);
                printSql(query);
                execute(ddl2);
                printSql(query);
            } catch (SqlException ex) {
                Assert.fail(ex.getMessage());
            }
        });
    }

    private static int createTouchTables(CairoEngine engine, SqlExecutionContext ctx) throws SqlException {
        engine.execute("CREATE TABLE touch_src (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", ctx);
        engine.execute("INSERT INTO touch_src VALUES (1, '2024-01-01')", ctx);
        engine.execute("CREATE TABLE touch_rows AS (SELECT x FROM long_sequence(10_000))", ctx);
        try (
                RecordCursorFactory factory = engine.select("SELECT length(touch(SELECT * FROM touch_src))", ctx);
                RecordCursor cursor = factory.getCursor(ctx)
        ) {
            Assert.assertTrue(cursor.hasNext());
            return cursor.getRecord().getInt(0);
        }
    }

    private void runWithPool(PoolRunnable body) throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)))) {
                TestUtils.execute(pool, (engine, _, ctx) -> body.run(engine, ctx), configuration, LOG);
            }
        });
    }

    @FunctionalInterface
    private interface PoolRunnable {
        void run(CairoEngine engine, SqlExecutionContext ctx) throws Exception;
    }
}
