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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.BatchCallback;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.LegacyPlanner;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerFactory;
import io.questdb.griffin.SqlCompilerFactoryImpl;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Os;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class LegacyPlannerTest extends AbstractCairoTest {
    private static final String LEGACY_PLAN = """
            QUERY PLAN
            VirtualRecord
              functions: [x+1]
                SelectedRecord
                    Hash Join Light
                      condition: u.v=t.x
                        PageFrame
                            Row forward scan
                            Frame forward scan on: t
                        Hash
                            PageFrame
                                Row forward scan
                                Frame forward scan on: u
            """;
    private static final CountingSqlCompilerFactory COMPILER_FACTORY = new CountingSqlCompilerFactory();
    private static final String PLAN_SQL = "EXPLAIN SELECT t.x + 1 FROM t JOIN u ON t.x = u.v";

    @BeforeClass
    public static void setUpStatic() throws Exception {
        engineFactory = COMPILER_FACTORY.engineFactory();
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testCompileBatch() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_LEGACY_PLANNER_ENABLED, "true");
            final StringSink log = new StringSink();
            final int compilesBefore = COMPILER_FACTORY.getLegacyCompiles();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.compileBatch("""
                        CREATE TABLE b (x INT);
                        INSERT INTO b VALUES (1), (2), (3);
                        SELECT x FROM b WHERE x > 1;
                        """, sqlExecutionContext, new BatchCallback() {
                    @Override
                    public void postCompile(SqlCompiler callbackCompiler, CompiledQuery cq, CharSequence queryText) throws Exception {
                        // the callback gets the compiler it called, never the legacy compiler that compiled for it
                        Assert.assertEquals(SqlCompilerImpl.class, callbackCompiler.getClass());
                        log.put('[').put(queryText).put("]\n");
                        switch (cq.getType()) {
                            case CompiledQuery.SELECT -> {
                                try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                                    printCursor(factory, log);
                                }
                            }
                            case CompiledQuery.CREATE_TABLE -> {
                                try (Operation op = cq.getOperation(); OperationFuture future = op.execute(sqlExecutionContext, null)) {
                                    future.await();
                                }
                            }
                            default -> {
                                try (OperationFuture future = cq.execute(null)) {
                                    future.await();
                                }
                            }
                        }
                    }

                    @Override
                    public boolean preCompile(SqlCompiler callbackCompiler, CharSequence sqlText) {
                        Assert.assertEquals(SqlCompilerImpl.class, callbackCompiler.getClass());
                        return true;
                    }
                });
            }
            TestUtils.assertEquals("""
                    [CREATE TABLE b (x INT);]
                    [INSERT INTO b VALUES (1), (2), (3);]
                    [SELECT x FROM b WHERE x > 1;]
                    x
                    2
                    3
                    """, log);
            Assert.assertEquals(compilesBefore + 1, COMPILER_FACTORY.getLegacyCompiles());
        });
    }

    @Test
    public void testCompilerSwitchesPlannerBetweenStatements() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (int i = 0; i < 4; i++) {
                    final boolean isLegacy = i % 2 == 0;
                    setProperty(PropertyKey.CAIRO_SQL_LEGACY_PLANNER_ENABLED, Boolean.toString(isLegacy));
                    final int compilesBefore = COMPILER_FACTORY.getLegacyCompiles();
                    try (RecordCursorFactory ignored = compiler.compile("SELECT * FROM t WHERE x > 5", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertEquals(isLegacy ? compilesBefore + 1 : compilesBefore, COMPILER_FACTORY.getLegacyCompiles());
                    }
                }
            }
        });
    }

    @Test
    public void testConcurrentFirstUseCreatesOnePlanner() throws Exception {
        assertMemoryLeak(() -> {
            final SOCountDownLatch creating = new SOCountDownLatch(1);
            final SOCountDownLatch release = new SOCountDownLatch(1);
            final AtomicInteger created = new AtomicInteger();
            final SqlCompilerFactory factory = new SqlCompilerFactory() {
                @Override
                public SqlCompilerImpl getInstance(CairoEngine engine) {
                    return SqlCompilerFactoryImpl.INSTANCE.getInstance(engine);
                }

                @Override
                public LegacyPlanner newLegacyPlanner(CairoEngine engine) {
                    created.incrementAndGet();
                    creating.countDown();
                    release.await();
                    return SqlCompilerFactory.super.newLegacyPlanner(engine);
                }
            };
            try (CairoEngine planningEngine = new CairoEngine(configuration) {
                @Override
                public SqlCompilerFactory getSqlCompilerFactory() {
                    return factory;
                }
            }) {
                final LegacyPlanner[] planners = new LegacyPlanner[2];
                final Thread first = new Thread(() -> planners[0] = planningEngine.getLegacyPlanner());
                first.start();
                creating.await();
                final Thread second = new Thread(() -> planners[1] = planningEngine.getLegacyPlanner());
                second.start();
                // the second caller waits on the engine while the first creates the planner, then finds it
                while (second.getState() != Thread.State.BLOCKED) {
                    Os.pause();
                }
                release.countDown();
                first.join();
                second.join();
                Assert.assertEquals(1, created.get());
                Assert.assertNotNull(planners[0]);
                Assert.assertSame(planners[0], planners[1]);
                Assert.assertSame(planners[0], planningEngine.getLegacyPlanner());
            }
        });
    }

    @Test
    public void testCoveringIndexNegativeLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO x VALUES
                        ('a', 1, '2024-01-01'), ('b', 2, '2024-01-01T06:00'), ('a', 3, '2024-01-01T12:00'),
                        ('c', 4, '2024-01-02'), ('a', 5, '2024-01-02T06:00'), ('b', 6, '2024-01-02T12:00')
                    """);
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)");
            setProperty(PropertyKey.CAIRO_SQL_LEGACY_PLANNER_ENABLED, "true");
            // a single key scans the covering index backward for the last rows
            assertQuery("SELECT v FROM x WHERE s = 'a' AND v > 1 LIMIT -1")
                    .withPlanContaining("CoveringIndex on: s with: v")
                    .expectSize()
                    .returns("""
                            v
                            5
                            """);
            // several keys cannot, so the last rows come from a serial scan
            assertQuery("SELECT v FROM x WHERE s IN ('a', 'b') AND v > 1 LIMIT -2")
                    .withPlanContaining("CoveringIndex on: s with: v")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            v
                            5
                            6
                            """);
        });
    }

    @Test
    public void testDdlAndDml() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            setProperty(PropertyKey.CAIRO_SQL_LEGACY_PLANNER_ENABLED, "true");
            execute("CREATE TABLE c AS (SELECT x, ts, s FROM t WHERE x <= 3) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO c SELECT x, ts, s FROM t WHERE x = 10");
            execute("UPDATE c SET s = 'k9' WHERE x = 2");
            execute("ALTER TABLE c ADD COLUMN y INT");
            execute("UPDATE c SET y = x::INT * 10 WHERE x > 1");
            execute("CREATE VIEW cv AS (SELECT x, s, y FROM c WHERE x > 1)");
            assertQuery("SELECT * FROM cv")
                    .noLeakCheck()
                    .returns("""
                            x\ts\ty
                            2\tk9\t20
                            3\tk0\t30
                            10\tk1\t100
                            """);
            assertQuery("SELECT x, ts, s, y FROM c")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            x\tts\ts\ty
                            1\t2024-01-01T00:00:00.000000Z\tk1\tnull
                            2\t2024-01-01T00:15:00.000000Z\tk9\t20
                            3\t2024-01-01T00:30:00.000000Z\tk0\t30
                            10\t2024-01-01T02:15:00.000000Z\tk1\t100
                            """);
        });
    }

    @Test
    public void testExplain() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            setProperty(PropertyKey.CAIRO_SQL_LEGACY_PLANNER_ENABLED, "true");
            printSql(PLAN_SQL);
            TestUtils.assertEquals(LEGACY_PLAN, sink);
        });
    }

    @Test
    public void testMatViewRefresh() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_LEGACY_PLANNER_ENABLED, "true");
            execute("CREATE TABLE base (ts TIMESTAMP, s SYMBOL, d DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                        ('2024-01-01T00:10', 'a', 1.0),
                        ('2024-01-01T00:20', 'b', 2.0),
                        ('2024-01-01T01:30', 'a', 4.0)
                    """);
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT ts, s, sum(d) sd FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final String reference = "SELECT timestamp_floor('h', ts) ts, s, sum(d) sd FROM base GROUP BY 1, 2 ORDER BY 1, 2";
            assertSqlCursors(reference, "SELECT * FROM mv ORDER BY ts, s");
            execute("""
                    INSERT INTO base VALUES
                        ('2024-01-01T00:50', 'a', 8.0),
                        ('2024-01-01T03:00', 'c', 16.0)
                    """);
            drainWalAndMatViewQueues();
            assertSqlCursors(reference, "SELECT * FROM mv ORDER BY ts, s");
            assertQuery("SELECT * FROM mv ORDER BY ts, s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts\tsd
                            2024-01-01T00:00:00.000000Z\ta\t9.0
                            2024-01-01T00:00:00.000000Z\tb\t2.0
                            2024-01-01T01:00:00.000000Z\ta\t4.0
                            2024-01-01T03:00:00.000000Z\tc\t16.0
                            """);
        });
    }

    @Test
    public void testParseErrorKeepsCompilerUsable() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            setProperty(PropertyKey.CAIRO_SQL_LEGACY_PLANNER_ENABLED, "true");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try {
                    compiler.compile("SELECT nope FROM t", sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals(7, e.getPosition());
                    TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column: nope");
                }
                final int compilesBefore = COMPILER_FACTORY.getLegacyCompiles();
                try (RecordCursorFactory ignored = compiler.compile("SELECT x FROM t WHERE x = 1", sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.assertEquals(compilesBefore + 1, COMPILER_FACTORY.getLegacyCompiles());
                }
            }
        });
    }

    @Test
    public void testQueriesMatchTheNewPlanner() throws Exception {
        // The legacy planner is master's front end and the new planner an independent rewrite of it, so each is the
        // other's oracle: the rows must agree for every query shape whose order both planners define.
        assertMemoryLeak(() -> {
            createTables();
            for (String sql : new String[]{
                    "SELECT s, sum(d) sd, count() c FROM t GROUP BY s ORDER BY s",
                    "SELECT count_distinct(s), first(x), last(x), min(d), max(d) FROM t",
                    "SELECT DISTINCT s FROM t ORDER BY s",
                    "SELECT ts, avg(d) FROM t SAMPLE BY 2h",
                    "SELECT ts, avg(d) FROM t WHERE s = 'k1' SAMPLE BY 30m FILL(PREV)",
                    "SELECT ts, s, sum(d) FROM t SAMPLE BY 1h FILL(NULL) ORDER BY ts, s",
                    "SELECT x, s, row_number() OVER (PARTITION BY s ORDER BY ts) rn, sum(d) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) w FROM t ORDER BY x",
                    "SELECT * FROM (t LATEST ON ts PARTITION BY s) ORDER BY s",
                    "SELECT t.x, u.v FROM t JOIN u ON (s) ORDER BY t.x, u.v",
                    "SELECT t.x, t.ts, u.v FROM t ASOF JOIN u ON (s)",
                    "SELECT t.x, t.ts, u.v FROM t LT JOIN u ON (s)",
                    "SELECT t.x, u.v FROM t LEFT JOIN u ON t.x = u.v ORDER BY t.x",
                    "SELECT t.x, u.v FROM t CROSS JOIN u WHERE t.x < 3 ORDER BY t.x, u.v",
                    "SELECT s FROM t UNION SELECT s FROM u ORDER BY s",
                    "SELECT x FROM t WHERE x < 3 UNION ALL SELECT v FROM u WHERE v > 4 ORDER BY x",
                    "SELECT s FROM u EXCEPT SELECT s FROM t WHERE x > 39 ORDER BY s",
                    "SELECT s FROM t INTERSECT SELECT s FROM u WHERE v > 3 ORDER BY s",
                    "WITH w AS (SELECT s, max(d) m FROM t GROUP BY s) SELECT t.x FROM t JOIN w ON t.s = w.s AND t.d = w.m ORDER BY t.x",
                    "SELECT * FROM t WHERE ts IN '2024-01-01T02' AND s = 'k1'",
                    "SELECT x, d FROM t ORDER BY d DESC, x LIMIT 5",
                    "SELECT x FROM t ORDER BY x LIMIT -3",
                    "SELECT x FROM t LIMIT 3, 6",
                    "SELECT x FROM t WHERE s IN (SELECT s FROM u WHERE v > 4) ORDER BY x",
                    "SELECT ts, s, d FROM t WHERE d > 5 AND s != 'k0' ORDER BY ts DESC LIMIT 4",
                    "SELECT s, x % 2 p, count() FROM t GROUP BY s, x % 2 ORDER BY s, p",
            }) {
                assertPlannersAgree(sql);
            }
        });
    }

    @Test
    public void testServerPropertySelectsPlanner() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertFalse(new DefaultCairoConfiguration(root).isSqlLegacyPlannerEnabled());
            createTables();
            assertLegacy(false);
            setProperty(PropertyKey.CAIRO_SQL_LEGACY_PLANNER_ENABLED, "true");
            assertLegacy(true);
            setProperty(PropertyKey.CAIRO_SQL_LEGACY_PLANNER_ENABLED, "false");
            assertLegacy(false);
        });
    }

    @Test
    public void testSubsampleKeepFlagFusion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_keep (id INT, flag BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO lp_keep VALUES
                        (1, true, '2024-01-01T00:00:01'), (2, false, '2024-01-01T00:00:02'), (3, true, '2024-01-01T00:00:03'),
                        (4, false, '2024-01-01T00:00:04'), (5, true, '2024-01-01T00:00:05')
                    """);
            setProperty(PropertyKey.CAIRO_SQL_LEGACY_PLANNER_ENABLED, "true");
            // the window that computes the keep flag also drops the rows the flag rejects
            assertQuery("SELECT id, flag FROM (SELECT id, flag, ts FROM lp_keep SUBSAMPLE uniform(3))")
                    .withPlanContaining("CachedWindowLightSelect")
                    .returns("""
                            id\tflag
                            1\ttrue
                            3\ttrue
                            5\ttrue
                            """);
        });
    }

    @Test
    public void testViewRevalidation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lb (x INT, y INT)");
            execute("CREATE TABLE nb (x INT, y INT)");
            execute("INSERT INTO lb VALUES (1, 2)");
            // a new view queues its check, which the view job runs with the planner the server selects
            setProperty(PropertyKey.CAIRO_SQL_LEGACY_PLANNER_ENABLED, "false");
            execute("CREATE VIEW nv AS (SELECT x, y FROM nb)");
            execute("CREATE VIEW nx AS (SELECT x FROM nb)");
            execute("ALTER TABLE nb DROP COLUMN y");
            final int compilesBeforeNewCheck = COMPILER_FACTORY.getLegacyCompiles();
            drainViewQueue();
            Assert.assertEquals(compilesBeforeNewCheck, COMPILER_FACTORY.getLegacyCompiles());
            setProperty(PropertyKey.CAIRO_SQL_LEGACY_PLANNER_ENABLED, "true");
            execute("CREATE VIEW lv AS (SELECT x, y FROM lb)");
            execute("ALTER TABLE lb DROP COLUMN y");
            final int compilesBeforeLegacyCheck = COMPILER_FACTORY.getLegacyCompiles();
            drainViewQueue();
            Assert.assertTrue(COMPILER_FACTORY.getLegacyCompiles() > compilesBeforeLegacyCheck);
            assertViewState("""
                    lv\tInvalid column: y\tinvalid
                    nv\tInvalid column: y\tinvalid
                    nx\t\tvalid
                    """);
            execute("ALTER TABLE lb ADD COLUMN y LONG");
            execute("COMPILE VIEW lv");
            drainViewQueue();
            assertViewState("""
                    lv\t\tvalid
                    nv\tInvalid column: y\tinvalid
                    nx\t\tvalid
                    """);
            assertQuery("SELECT * FROM lv")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\ty
                            1\tnull
                            """);
        });
    }

    private static void assertLegacy(boolean expected) throws SqlException {
        Assert.assertEquals(expected, sqlExecutionContext.isLegacyPlannerEnabled());
        final int compilesBefore = COMPILER_FACTORY.getLegacyCompiles();
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory ignored = compiler.compile("SELECT * FROM t WHERE x > 5", sqlExecutionContext).getRecordCursorFactory()) {
            Assert.assertEquals(expected ? compilesBefore + 1 : compilesBefore, COMPILER_FACTORY.getLegacyCompiles());
        }
    }

    private static void assertPlannersAgree(String sql) throws SqlException {
        setProperty(PropertyKey.CAIRO_SQL_LEGACY_PLANNER_ENABLED, "false");
        final int compilesBefore = COMPILER_FACTORY.getLegacyCompiles();
        final StringSink expected = new StringSink();
        printSql(sql, expected);
        Assert.assertEquals(sql, compilesBefore, COMPILER_FACTORY.getLegacyCompiles());
        Assert.assertTrue(sql + '\n' + expected, expected.toString().indexOf('\n') < expected.length() - 1);
        setProperty(PropertyKey.CAIRO_SQL_LEGACY_PLANNER_ENABLED, "true");
        final StringSink actual = new StringSink();
        printSql(sql, actual);
        Assert.assertEquals(sql, compilesBefore + 1, COMPILER_FACTORY.getLegacyCompiles());
        TestUtils.assertEquals(sql, expected, actual);
    }

    private static void createTables() throws SqlException {
        execute("""
                CREATE TABLE t AS (
                    SELECT x, timestamp_sequence('2024-01-01', 15 * 60 * 1_000_000L) ts, ('k' || (x % 3))::SYMBOL s, (x * 7 % 11)::DOUBLE d
                    FROM long_sequence(40)
                ) TIMESTAMP(ts) PARTITION BY HOUR
                """);
        execute("""
                CREATE TABLE u AS (
                    SELECT ('k' || (x % 4))::SYMBOL s, x v, timestamp_sequence('2024-01-01T00:07', 60 * 60 * 1_000_000L) ts
                    FROM long_sequence(6)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
    }

    private static void printCursor(RecordCursorFactory factory, StringSink sink) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final RecordMetadata metadata = factory.getMetadata();
            CursorPrinter.println(metadata, sink);
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                CursorPrinter.println(record, metadata, sink);
            }
        }
    }

    private void assertViewState(String expected) throws Exception {
        // the view graph lives in memory, which assertQuery()'s leak check clears
        assertQuery("SELECT view_name, invalidation_reason, view_status FROM views() ORDER BY view_name")
                .noLeakCheck()
                .returns("view_name\tinvalidation_reason\tview_status\n" + expected);
    }
}
