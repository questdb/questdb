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

package io.questdb.test.griffin.engine.join;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.test.TestMatchFunctionFactory;
import io.questdb.griffin.engine.join.MaterializedRecordCursorFactory;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.CustomisableRunnable;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Parents that rewind their input, nested loop joins and negative LIMIT, read a stream GROUP BY through a copy,
 * so the aggregation runs once per cursor. Splice and serial horizon joins read it through a strict copy, which
 * gives them random access. The expected rows come from the same aggregation over a table without a designated
 * timestamp, which runs a hash GROUP BY. The test_match() filter under each stream GROUP BY counts the rewinds that
 * reach the aggregation.
 */
@RunWith(Parameterized.class)
public class MaterializedRecordCursorFactoryTest extends AbstractCairoTest {
    private static final String CROSS_JOIN = "SELECT m.x, s.* FROM m CROSS JOIN ($sub) s ORDER BY x, $keys";
    private static final String FULL_JOIN = "SELECT m.x, s.* FROM m FULL JOIN ($sub) s ON s.s < m.x * 30 ORDER BY x, $keys";
    private static final String HORIZON_JOIN = "SELECT h.offset, count() n, sum(s.s) total, sum(q.x) qx "
            + "FROM (($sub) TIMESTAMP(ts)) s HORIZON JOIN q RANGE FROM 0s TO 1h STEP 30m AS h ORDER BY 1";
    private static final String LEFT_JOIN = "SELECT m.x, s.* FROM m LEFT JOIN ($sub) s ON s.s < m.x * 30 ORDER BY x, $keys";
    private static final String MULTI_HORIZON_JOIN = "SELECT h.offset, count() n, sum(s.s) total, sum(q.x) qx, sum(r.x) rx "
            + "FROM (($sub) TIMESTAMP(ts)) s HORIZON JOIN q HORIZON JOIN q r RANGE FROM 0s TO 1h STEP 30m AS h ORDER BY 1";
    private static final String NEGATIVE_LIMIT = "SELECT * FROM (SELECT * FROM ($sub) LIMIT -10) ORDER BY $keys, c";
    private static final String RIGHT_JOIN = "SELECT m.x, s.* FROM ($sub) s RIGHT JOIN m ON s.s < m.x * 30 ORDER BY x, $keys";
    // The stream subquery, the same aggregation over the table without a designated timestamp, and the keys.
    private static final String[][] SUBQUERIES = {
            {
                    "SELECT ts, count() c, sum(v) s FROM t WHERE test_match() SAMPLE BY 1h",
                    "SELECT timestamp_floor('1h', ts) ts, count() c, sum(v) s FROM t_hash ORDER BY ts",
                    "ts"
            },
            {
                    "SELECT ts, sym, count() c, sum(v) s FROM t WHERE test_match() SAMPLE BY 1h",
                    "SELECT timestamp_floor('1h', ts) ts, sym, count() c, sum(v) s FROM t_hash ORDER BY ts, sym",
                    "ts, sym"
            },
            {
                    "SELECT ts, count() c, sum(v) s FROM t WHERE test_match() GROUP BY ts",
                    "SELECT ts, count() c, sum(v) s FROM t_hash GROUP BY ts ORDER BY ts",
                    "ts"
            },
            {
                    "SELECT ts, sym, count() c, sum(v) s FROM t WHERE test_match() GROUP BY ts, sym",
                    "SELECT ts, sym, count() c, sum(v) s FROM t_hash GROUP BY ts, sym ORDER BY ts, sym",
                    "ts, sym"
            }
    };
    private final int frameRows;
    private final boolean isParallel;
    private final int workerCount;

    public MaterializedRecordCursorFactoryTest(boolean isParallel, int workerCount, int frameRows) {
        this.isParallel = isParallel;
        this.workerCount = workerCount;
        this.frameRows = frameRows;
    }

    @Parameterized.Parameters(name = "parallel={0}, workers={1}, frameRows={2}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {false, 1, 17},
                {true, 0, 17},
                {true, 4, 17}
        });
    }

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, Boolean.toString(isParallel));
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, frameRows);
        super.setUp();
    }

    @Test
    public void testConstructorFailureClosesBase() throws Exception {
        run((engine, compiler, context) -> {
            final RuntimeException failure = new RuntimeException("injected metadata failure");
            final int[] closeCount = {0};
            try (RecordCursorFactory delegate = compiler.compile("SELECT x FROM long_sequence(1)", context).getRecordCursorFactory()) {
                final GenericRecordMetadata metadata = new GenericRecordMetadata() {
                    @Override
                    public int getColumnType(int columnIndex) {
                        throw failure;
                    }
                };
                GenericRecordMetadata.copyColumns(delegate.getMetadata(), metadata);
                final RecordCursorFactory base = new RecordCursorFactory() {
                    @Override
                    public void close() {
                        closeCount[0]++;
                        delegate.close();
                    }

                    @Override
                    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
                        return delegate.getCursor(executionContext);
                    }

                    @Override
                    public RecordMetadata getMetadata() {
                        return metadata;
                    }

                    @Override
                    public boolean recordCursorSupportsRandomAccess() {
                        return false;
                    }
                };
                // RecordArray reads column types during construction, after the wrapper adopts base.
                // No rows reach the sink: this tests consume-on-entry, including constructor failure.
                try (RecordCursorFactory ignored = new MaterializedRecordCursorFactory(configuration, base, null, true)) {
                    Assert.fail("expected metadata failure");
                } catch (RuntimeException e) {
                    Assert.assertSame(failure, e);
                }
                Assert.assertEquals(1, closeCount[0]);
            }
        });
    }

    @Test
    public void testCrossJoin() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            assertMaterialized(engine, context, CROSS_JOIN, null, true, false);
            // count() sizes the join, which sizes the copy, rewinds it and sizes it again.
            assertMaterialized(engine, context, "SELECT count() FROM m CROSS JOIN ($sub)", null, false, true);
        });
    }

    @Test
    public void testCrossJoinOverNegativeLimit() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            final String[] subquery = {
                    "SELECT * FROM (" + SUBQUERIES[1][0] + ") LIMIT -6",
                    "SELECT * FROM (" + SUBQUERIES[1][1] + ") LIMIT -6",
                    SUBQUERIES[1][2]
            };
            Assert.assertEquals(0, assertMaterialized(engine, context, CROSS_JOIN, subquery, null, true, true));
            // The join reuses the copy under the limit rather than copying the rows of the limit again.
            final String plan = TestUtils.printSqlToString(
                    engine,
                    context,
                    "EXPLAIN " + CROSS_JOIN.replace("$sub", subquery[0]).replace("$keys", subquery[2]),
                    new StringSink()
            );
            Assert.assertEquals(plan, plan.indexOf("Materialize"), plan.lastIndexOf("Materialize"));
        });
    }

    @Test
    public void testFullJoin() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            assertMaterialized(engine, context, FULL_JOIN, null, true, false);
        });
    }

    @Test
    public void testHashJoinSizeDoesNotRewindStreamMaster() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            final String sql = "SELECT a.ts, a.sym, a.c, b.s FROM (" + SUBQUERIES[1][0] + ") a JOIN (" + SUBQUERIES[0][0] + ") b ON (ts)";
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck().assertsPlanContaining("Hash Join\n");
            try (
                    RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory();
                    RecordCursor cursor = factory.getCursor(context)
            ) {
                while (cursor.hasNext()) {
                    // drain the cursor
                }
                final int topCount = TestMatchFunctionFactory.getTopCounter();
                Assert.assertEquals(-1, cursor.size());
                Assert.assertEquals(topCount, TestMatchFunctionFactory.getTopCounter());
            }
        });
    }

    @Test
    public void testHorizonJoin() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            assertMaterialized(engine, context, HORIZON_JOIN, null, true, true);
            assertMaterialized(engine, context, MULTI_HORIZON_JOIN, null, true, true);
        });
    }

    @Test
    public void testInputsHiddenFromBaseFactory() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            final String stream = SUBQUERIES[1][0];
            final String half = "SELECT * FROM (SELECT ts, sym, count() c, sum(v) s FROM t WHERE test_match() AND ts %s '2024-01-01T12' SAMPLE BY 1h)";
            final String halves = half.formatted("<") + " UNION ALL " + half.formatted(">=");
            final String window = "SELECT a.*, count() n FROM (" + stream + ") a WINDOW JOIN t b %s RANGE BETWEEN 1 hour PRECEDING AND CURRENT ROW";
            final String[][] inputs = {
                    {"Union All", halves},
                    {"Except", "SELECT * FROM (" + stream + ") EXCEPT SELECT * FROM (" + stream + ") WHERE c < 0"},
                    {"Hash Join", "SELECT a.* FROM (" + stream + ") a JOIN (SELECT DISTINCT sym FROM t) b ON (sym)"},
                    {"Window Fast Join", window.formatted("ON (sym)")},
                    {"Window Join", window.formatted("")},
                    {"Unnest", "SELECT a.ts, a.sym, a.c, a.s, u.e FROM (SELECT *, ARRAY[1.0] arr FROM (" + stream + ")) a, UNNEST(a.arr) u(e)"}
            };
            for (String[] input : inputs) {
                for (String query : new String[]{CROSS_JOIN, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN, NEGATIVE_LIMIT}) {
                    assertStreamReadOnce(engine, compiler, context, query, input[1], input[0]);
                }
            }
            assertStreamReadOnce(engine, compiler, context, NEGATIVE_LIMIT, "SELECT * FROM (" + halves + ") ORDER BY ts", "Union All Merge");

            final String hashed = "SELECT * FROM (" + SUBQUERIES[1][1] + ") EXCEPT SELECT * FROM (" + stream + ")";
            final String sql = CROSS_JOIN.replace("$sub", hashed).replace("$keys", SUBQUERIES[1][2]);
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck().assertsPlanNotContaining("Materialize");
            Assert.assertEquals(countRewinds(compiler, context, hashed), countRewinds(compiler, context, sql));
        });
    }

    @Test
    public void testLeftJoin() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            assertMaterialized(engine, context, LEFT_JOIN, null, true, false);
        });
    }

    @Test
    public void testMemoryLimit() throws Exception {
        // The aggregation fits in the limit and a copy of its 10_000 rows does not.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 65_536);
        run((engine, compiler, context) -> {
            createTables(engine, context);
            engine.execute(
                    """
                            CREATE TABLE big AS (
                                SELECT timestamp_sequence('2024-01-01', 1_000_000L) ts, x v FROM long_sequence(10_000)
                            ) TIMESTAMP(ts) PARTITION BY DAY""",
                    context
            );
            final String sub = "SELECT ts, count() c, sum(v) s FROM big WHERE test_match() GROUP BY ts";
            try (MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH)) {
                context.setMemoryTracker(tracker);
                try {
                    final int topCount = TestMatchFunctionFactory.getTopCounter();
                    assertQuery("SELECT count() FROM m CROSS JOIN (" + sub + ")")
                            .withEngine(engine)
                            .withContext(context)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .withPlanContaining("Materialize")
                            .returns("count\n50000\n");
                    // The copy released its rows and streamed the aggregation, which the join rewinds.
                    Assert.assertTrue(TestMatchFunctionFactory.getTopCounter() > topCount);
                    assertQuery("SELECT m.x, s.* FROM m FULL JOIN (" + sub + ") s ON s.s < m.x")
                            .withEngine(engine)
                            .withContext(context)
                            .noLeakCheck()
                            .failsWith("query memory limit exceeded [workload=MAT_VIEW_REFRESH");
                    Assert.assertEquals(0, tracker.getUsed());
                } finally {
                    context.setMemoryTracker(null);
                }
            }
        });
    }

    @Test
    public void testMemoryTrackerBalanced() throws Exception {
        // The single-key SAMPLE BY copy fits in 1024 bytes, the others stream or, under FULL JOIN, fail.
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 256);
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_MAX_PAGES, 4);
        run((engine, compiler, context) -> {
            createTables(engine, context);
            for (String query : new String[]{CROSS_JOIN, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN}) {
                for (String[] subquery : SUBQUERIES) {
                    final String sql = query.replace("$sub", subquery[0]).replace("$keys", subquery[2]);
                    try (
                            RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory();
                            // QueryRegistry replaces a QUERY tracker with its own, but keeps a refresh tracker.
                            MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH)
                    ) {
                        context.setMemoryTracker(tracker);
                        try {
                            for (int i = 0; i < 2; i++) {
                                try (RecordCursor cursor = factory.getCursor(context)) {
                                    long peak = 0;
                                    while (cursor.hasNext()) {
                                        peak = Math.max(peak, tracker.getUsed());
                                    }
                                    Assert.assertTrue(sql, peak > 0);
                                } catch (CairoException e) {
                                    Assert.assertSame(sql, FULL_JOIN, query);
                                    TestUtils.assertContains(e.getFlyweightMessage(), "materialized rows exceed the memory limit");
                                }
                                Assert.assertEquals(sql, 0, tracker.getUsed());
                            }
                        } finally {
                            context.setMemoryTracker(null);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testNegativeLimit() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            context.getBindVariableService().setLong(0, -6);
            for (String limit : new String[]{"-10", "-10, -4", "4, -6", "$1"}) {
                assertMaterialized(
                        engine,
                        context,
                        "SELECT * FROM (SELECT * FROM ($sub) LIMIT " + limit + ") ORDER BY $keys, c",
                        "ts",
                        true,
                        true
                );
            }
            // A constant non-negative limit never rewinds the stream after reading it.
            assertQuery("SELECT * FROM (SELECT ts, count() c FROM t SAMPLE BY 1h) LIMIT 4, 6")
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .assertsPlanNotContaining("Materialize");
        });
    }

    @Test
    public void testOverBudget() throws Exception {
        // The copy holds at most 10 rows, fewer than every subquery returns.
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 256);
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_MAX_PAGES, 1);
        run((engine, compiler, context) -> {
            createTables(engine, context);
            for (String query : new String[]{CROSS_JOIN, LEFT_JOIN, RIGHT_JOIN}) {
                for (String[] subquery : SUBQUERIES) {
                    // The copy fell back to streaming, so the join rewinds the stream GROUP BY.
                    Assert.assertTrue(query, assertMaterialized(engine, context, query, subquery, null, true, false) > 0);
                }
            }
            for (String[] subquery : SUBQUERIES) {
                // The full join tracks matched rows by the row ids of the copy, so it cannot stream.
                assertQuery(FULL_JOIN.replace("$sub", subquery[0]).replace("$keys", subquery[2]))
                        .withEngine(engine)
                        .withContext(context)
                        .noLeakCheck()
                        .failsWith("materialized rows exceed the memory limit [maxBytes=256] (raise cairo.sql.hash.join.value.max.pages)");
            }
        });
    }

    @Test
    public void testRightJoin() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            assertMaterialized(engine, context, RIGHT_JOIN, null, true, false);
        });
    }

    @Test
    public void testSpliceJoin() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            for (String query : new String[]{
                    "SELECT * FROM (($sub) TIMESTAMP(ts)) s SPLICE JOIN q",
                    "SELECT * FROM q SPLICE JOIN (($sub) TIMESTAMP(ts)) s"
            }) {
                // The multi-key rows of an hour share a timestamp, and their order, which differs between the
                // plans, decides which of them the splice pairs.
                for (int i = 0; i < SUBQUERIES.length; i += 2) {
                    Assert.assertEquals(query, 0, assertMaterialized(engine, context, query, SUBQUERIES[i], null, false, false));
                }
            }
        });
    }

    /**
     * Returns the number of rewinds that reached the stream GROUP BY.
     */
    private int assertMaterialized(
            CairoEngine engine,
            SqlExecutionContext context,
            String query,
            String[] subquery,
            CharSequence timestamp,
            boolean supportsRandomAccess,
            boolean isSizeKnown
    ) throws Exception {
        final String keys = subquery[2];
        final String expected = TestUtils.printSqlToString(
                engine,
                context,
                query.replace("$sub", subquery[1]).replace("$keys", keys),
                new StringSink()
        );
        final int topCount = TestMatchFunctionFactory.getTopCounter();
        assertQuery(query.replace("$sub", subquery[0]).replace("$keys", keys))
                .withEngine(engine)
                .withContext(context)
                .noLeakCheck()
                .timestamp(timestamp)
                .supportsRandomAccess(supportsRandomAccess)
                .expectSize(isSizeKnown)
                .withPlanContaining("Materialize", "Stream Group By")
                .returns(expected);
        return TestMatchFunctionFactory.getTopCounter() - topCount;
    }

    private void assertMaterialized(
            CairoEngine engine,
            SqlExecutionContext context,
            String query,
            CharSequence timestamp,
            boolean supportsRandomAccess,
            boolean isSizeKnown
    ) throws Exception {
        for (String[] subquery : SUBQUERIES) {
            // The parent rewinds the copy, so the stream GROUP BY under it runs once per cursor.
            Assert.assertEquals(query, 0, assertMaterialized(engine, context, query, subquery, timestamp, supportsRandomAccess, isSizeKnown));
        }
    }

    private void assertStreamReadOnce(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext context,
            String query,
            String input,
            String operator
    ) throws Exception {
        final String keys = SUBQUERIES[1][2];
        final boolean isLimit = NEGATIVE_LIMIT.equals(query);
        final String[] subquery = {input, "SELECT * FROM (" + input + ") ORDER BY " + keys, keys};
        assertMaterialized(engine, context, query, subquery, isLimit ? "ts" : null, true, isLimit);
        final String sql = query.replace("$sub", input).replace("$keys", keys);
        assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck().assertsPlanContaining(operator);
        Assert.assertEquals(sql, countRewinds(compiler, context, input), countRewinds(compiler, context, sql));
    }

    private static int countRewinds(SqlCompiler compiler, SqlExecutionContext context, String sql) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
            final int topCount = TestMatchFunctionFactory.getTopCounter();
            try (RecordCursor cursor = factory.getCursor(context)) {
                while (cursor.hasNext()) {
                    // drain the cursor
                }
            }
            return TestMatchFunctionFactory.getTopCounter() - topCount;
        }
    }

    private static void createTables(CairoEngine engine, SqlExecutionContext context) throws SqlException {
        // Every hour holds both symbols, so a limit with even bounds cuts the multi-key rows between hours.
        engine.execute(
                """
                        CREATE TABLE t AS (
                            SELECT timestamp_sequence('2024-01-01', 420_000_000L) ts,
                                   CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END::SYMBOL sym,
                                   x v
                            FROM long_sequence(200)
                        ) TIMESTAMP(ts) PARTITION BY DAY""",
                context
        );
        engine.execute("CREATE TABLE t_hash (ts TIMESTAMP, sym SYMBOL, v LONG)", context);
        engine.execute("INSERT INTO t_hash SELECT * FROM t", context);
        engine.execute("CREATE TABLE m AS (SELECT x FROM long_sequence(5))", context);
        engine.execute(
                "CREATE TABLE q AS (SELECT timestamp_sequence('2024-01-01T00:30', 3_600_000_000L) ts, x FROM long_sequence(10)) TIMESTAMP(ts) PARTITION BY DAY",
                context
        );
    }

    private void run(CustomisableRunnable runnable) throws Exception {
        assertMemoryLeak(() -> TestUtils.execute(workerCount == 0 ? null : new TestWorkerPool(workerCount), runnable, configuration, LOG));
    }
}
