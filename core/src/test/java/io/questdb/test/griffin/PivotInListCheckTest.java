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
 *  distributed under the License is distributed active an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.PivotInListCheckFactory;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.metrics.QueryTracingJob;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A PIVOT whose FOR values come from a subquery compiles to a plan that a query cache may keep:
 * every later execution of the plan runs the subquery again and refuses with
 * {@link TableReferenceOutOfDateException}, the cache's signal to recompile, when the values are
 * not the ones the plan was compiled with. New data under the same values keeps the plan.
 */
public class PivotInListCheckTest extends AbstractCairoTest {
    private static final String PIVOT2 = "SELECT * FROM data2 PIVOT (sum(val) FOR a IN (SELECT c FROM ca ORDER BY c) b IN (SELECT c FROM cb ORDER BY c) GROUP BY grp) ORDER BY grp";
    private static final String PIVOT = "SELECT * FROM data PIVOT (sum(val) FOR cat IN (SELECT c FROM cats WHERE active ORDER BY c) GROUP BY grp) ORDER BY grp";

    @Test
    public void testMatViewRefreshNeverRunsPivotPlan() throws Exception {
        // A materialized view over a PIVOT is accepted, but a PIVOT result has no designated
        // timestamp, so the first refresh invalidates the view before it opens the plan: the cached
        // refresh factory never reaches the IN-list check, and the view serves no PIVOT rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE syms (sym SYMBOL)");
            execute("INSERT INTO syms VALUES ('A'), ('B')");
            execute("INSERT INTO trades VALUES ('A', 1, '2024-01-01T00:00'), ('B', 2, '2024-01-01T01:00')");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv AS (" +
                    "SELECT * FROM (SELECT ts, sym, sum(price) price FROM trades SAMPLE BY 1h) " +
                    "PIVOT (sum(price) FOR sym IN (SELECT sym FROM syms ORDER BY sym) GROUP BY ts)" +
                    ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("INSERT INTO syms VALUES ('C')");
            execute("INSERT INTO trades VALUES ('C', 3, '2024-01-01T02:00')");
            drainWalAndMatViewQueues();
            final StringSink sink = new StringSink();
            printSql("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'mv'", sink);
            TestUtils.assertContains(sink, "invalid");
            TestUtils.assertContains(sink, "timestamp type mismatch");
            printSql("mv", sink);
            TestUtils.assertEquals("ts\tA\tB\n", sink);
        });
    }

    @Test
    public void testTwoSubqueriesEachChecked() throws Exception {
        assertMemoryLeak(() -> {
            createTwoSubqueryTables();
            try (RecordCursorFactory factory = select(PIVOT2)) {
                // one check per FOR column, both held by the plan's own wrapper
                final RecordCursorFactory plan = unwrap(factory);
                Assert.assertTrue(plan.getClass().getName(), plan instanceof PivotInListCheckFactory);
                Assert.assertEquals(2, ((PivotInListCheckFactory) plan).getCheckCount());
                drain(factory);
                // unchanged values keep the plan
                drain(factory);
                drain(factory);
                execute("INSERT INTO ca VALUES ('A3')");
                assertStale(factory, "[value=A3]");
            }
            try (RecordCursorFactory factory = select(PIVOT2)) {
                drain(factory);
                execute("INSERT INTO cb VALUES ('B2')");
                assertStale(factory, "[value=B2]");
            }
            try (RecordCursorFactory factory = select(PIVOT2)) {
                drain(factory);
                execute("UPDATE ca SET c = 'A0' WHERE c = 'A1'");
                assertStale(factory, "[value=A0]");
            }
        });
    }

    @Test
    public void testVerifyRunIsNotTracedAsAQuery() throws Exception {
        // the check runs inside the PIVOT's execution: the statement's text has one trace row per
        // execution, as the TAQ harness matches traces by text, and the subquery has none of its own
        setProperty(PropertyKey.QUERY_TRACING_ENABLED, "true");
        assertMemoryLeak(() -> {
            createTables();
            final ObjList<String> registered = new ObjList<>();
            try (QueryTracingJob job = new QueryTracingJob(engine)) {
                job.run();
                // from here on: the job's own table DDL is done
                engine.getQueryRegistry().setListener((query, queryId, executionContext) -> registered.add(Chars.toString(query)));
                try (RecordCursorFactory factory = select(PIVOT)) {
                    for (int i = 0; i < 3; i++) {
                        drain(factory);
                    }
                }
                job.run();
                // the compile's subquery run and the two re-checks register no query of their own
                Assert.assertEquals(registered.toString(), 3, registered.size());
                for (int i = 0; i < 3; i++) {
                    Assert.assertEquals(PIVOT, registered.getQuick(i));
                }
                // the statements before the job started (DDL, inserts) are not traced
                final StringSink sink = new StringSink();
                printSql("SELECT query_text, count() FROM " + QueryTracingJob.TABLE_NAME + " ORDER BY 1", sink);
                TestUtils.assertEquals("query_text\tcount\n" + PIVOT + "\t3\n", sink);
            } finally {
                engine.getQueryRegistry().setListener(null);
            }
        });
    }

    @Test
    public void testCompileFailureFreesTheSubquery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // the subquery has run when the unknown column fails the compile: its plan must not leak
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.compile("SELECT * FROM data PIVOT (sum(nope) FOR cat IN (SELECT c FROM cats WHERE active) GROUP BY grp)", sqlExecutionContext);
                Assert.fail("the compile must fail");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "nope");
            }
        });
    }

    @Test
    public void testFixedValueListPlanUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile("SELECT * FROM data PIVOT (sum(val) FOR cat IN ('A', 'B') GROUP BY grp)", sqlExecutionContext);
                try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                    Assert.assertTrue(cq.isCacheable());
                    Assert.assertFalse(unwrap(factory) instanceof PivotInListCheckFactory);
                }
            }
        });
    }

    @Test
    public void testPlanIsCacheable() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(PIVOT, sqlExecutionContext);
                try (RecordCursorFactory ignore = cq.getRecordCursorFactory()) {
                    Assert.assertTrue(cq.isCacheable());
                }
            }
            // the check is not in the plan: it reads as the same PIVOT over the values as a list
            final StringSink expected = new StringSink();
            final StringSink actual = new StringSink();
            printSql("EXPLAIN " + PIVOT.replace("(SELECT c FROM cats WHERE active ORDER BY c)", "('A', 'B')"), expected);
            printSql("EXPLAIN " + PIVOT, actual);
            TestUtils.assertEquals(expected, actual);
        });
    }

    @Test
    public void testReusedPlanReadsNewDataUnderSameValues() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (RecordCursorFactory factory = select(PIVOT)) {
                assertCursor(factory, """
                        grp\tA\tB
                        g1\t10\t20
                        g2\t30\tnull
                        """);
                execute("INSERT INTO data VALUES ('g2', 'B', 5), ('g3', 'A', 7), ('g3', 'C', 1)");
                // C is in the data but not among the values: the plan stays
                for (int i = 0; i < 3; i++) {
                    assertCursor(factory, """
                            grp\tA\tB
                            g1\t10\t20
                            g2\t30\t5
                            g3\t7\tnull
                            """);
                }
            }
        });
    }

    @Test
    public void testReusedPlanRefusesChangedValue() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (RecordCursorFactory factory = select(PIVOT)) {
                assertCursor(factory, """
                        grp\tA\tB
                        g1\t10\t20
                        g2\t30\tnull
                        """);
                execute("UPDATE cats SET c = 'C' WHERE c = 'B'");
                assertStale(factory, "[value=C]");
            }
            assertFresh("""
                    grp\tA\tC
                    g1\t10\tnull
                    g2\t30\tnull
                    """, PIVOT);
        });
    }

    @Test
    public void testReusedPlanRefusesExtraValue() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (RecordCursorFactory factory = select(PIVOT)) {
                assertCursor(factory, """
                        grp\tA\tB
                        g1\t10\t20
                        g2\t30\tnull
                        """);
                execute("INSERT INTO cats VALUES ('C', true, 3)");
                execute("INSERT INTO data VALUES ('g1', 'C', 3)");
                assertStale(factory, "[value=C]");
                // and keeps refusing: it never adopts the new values
                assertStale(factory, "[value=C]");
            }
            assertFresh("""
                    grp\tA\tB\tC
                    g1\t10\t20\t3
                    g2\t30\tnull\tnull
                    """, PIVOT);
        });
    }

    @Test
    public void testReusedPlanRefusesMissingValue() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (RecordCursorFactory factory = select(PIVOT)) {
                assertCursor(factory, """
                        grp\tA\tB
                        g1\t10\t20
                        g2\t30\tnull
                        """);
                execute("UPDATE cats SET active = false WHERE c = 'B'");
                assertStale(factory, "[value=B]");
            }
            assertFresh("""
                    grp\tA
                    g1\t10
                    g2\t30
                    """, PIVOT);
        });
    }

    @Test
    public void testReusedPlanRefusesReorderedValues() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String pivot = "SELECT * FROM data PIVOT (sum(val) FOR cat IN (SELECT c FROM cats WHERE active ORDER BY rank) GROUP BY grp) ORDER BY grp";
            try (RecordCursorFactory factory = select(pivot)) {
                assertCursor(factory, """
                        grp\tA\tB
                        g1\t10\t20
                        g2\t30\tnull
                        """);
                // the same values in another order name the columns in another order
                execute("UPDATE cats SET rank = 3 WHERE c = 'A'");
                assertStale(factory, "[value=B]");
            }
            assertFresh("""
                    grp\tB\tA
                    g1\t20\t10
                    g2\tnull\t30
                    """, pivot);
        });
    }

    @Test
    public void testReusedPlanAcceptsUnorderedValuesInAnotherOrder() throws Exception {
        // without ORDER BY the subquery's row order is unspecified: a parallel GROUP BY returns
        // its groups in whatever order its workers' maps merge. Only the set of values names the
        // plan's columns, so the cached plan stays usable.
        assertMemoryLeak(() -> {
            createTables();
            final String pivot = "SELECT * FROM data PIVOT (sum(val) FOR cat IN (SELECT c FROM cats WHERE active) GROUP BY grp) ORDER BY grp";
            try (RecordCursorFactory factory = select(pivot)) {
                assertCursor(factory, """
                        grp\tA\tB
                        g1\t10\t20
                        g2\t30\tnull
                        """);
                // the same values, the subquery's rows in another order: B, then A
                execute("UPDATE cats SET active = false WHERE c = 'A'");
                execute("INSERT INTO cats VALUES ('A', true, 3)");
                assertCursor(factory, """
                        grp\tA\tB
                        g1\t10\t20
                        g2\t30\tnull
                        """);
                // a value missing is still refused
                execute("UPDATE cats SET active = false WHERE c = 'B'");
                assertStale(factory, "[value=B]");
            }
        });
    }

    private static void assertCursor(RecordCursorFactory factory, String expected) throws SqlException {
        final StringSink sink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            println(factory.getMetadata(), cursor, sink);
        }
        TestUtils.assertEquals(expected, sink);
    }

    // a fresh compile, as a cache's recompile does
    private static void assertFresh(String expected, String sql) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            assertCursor(factory, expected);
        }
    }

    private static void assertStale(RecordCursorFactory factory, String fragment) throws SqlException {
        try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
            Assert.fail("the reused plan must refuse");
        } catch (TableReferenceOutOfDateException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "PIVOT IN subquery returns other values");
            TestUtils.assertContains(e.getFlyweightMessage(), fragment);
        }
    }

    // the plan under the statement's QueryProgress
    private static RecordCursorFactory unwrap(RecordCursorFactory factory) {
        return factory instanceof QueryProgress ? factory.getBaseFactory() : factory;
    }

    private static void createTwoSubqueryTables() throws SqlException {
        execute("CREATE TABLE data2 (grp SYMBOL, a SYMBOL, b SYMBOL, val LONG)");
        execute("CREATE TABLE ca (c SYMBOL)");
        execute("CREATE TABLE cb (c SYMBOL)");
        execute("INSERT INTO ca VALUES ('A1'), ('A2')");
        execute("INSERT INTO cb VALUES ('B1')");
        execute("INSERT INTO data2 VALUES ('g1', 'A1', 'B1', 1), ('g1', 'A2', 'B1', 2), ('g2', 'A3', 'B2', 4)");
    }

    private static void drain(RecordCursorFactory factory) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            while (cursor.hasNext()) {
                cursor.getRecord().getSymA(0);
            }
        }
    }

    private static void createTables() throws SqlException {
        execute("CREATE TABLE data (grp SYMBOL, cat SYMBOL, val LONG)");
        execute("CREATE TABLE cats (c SYMBOL, active BOOLEAN, rank INT)");
        execute("INSERT INTO cats VALUES ('A', true, 1), ('B', true, 2), ('X', false, 0)");
        execute("INSERT INTO data VALUES ('g1', 'A', 10), ('g1', 'B', 20), ('g2', 'A', 30)");
    }
}
