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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class StreamSampleByFillTest extends AbstractCairoTest {
    private final boolean parallel;
    private final String timestampType;
    private final int workers;

    public StreamSampleByFillTest(boolean parallel, int workers, String timestampType) {
        this.parallel = parallel;
        this.workers = workers;
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "parallel={0}, workers={1}, type={2}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {false, 0, "timestamp"},
                {false, 0, "timestamp_ns"},
                {true, 0, "timestamp"},
                {true, 4, "timestamp"},
                {true, 4, "timestamp_ns"}
        });
    }

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, Boolean.toString(parallel));
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 2);
        super.setUp();
    }

    @Test
    public void testDstPrev() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            engine.execute("INSERT INTO prices VALUES ('2024-10-26T23:30', 'A', 1), "
                    + "('2024-10-27T02:30', 'B', 3)", context);
            final String sql = "SELECT ts, max(price) max FROM prices SAMPLE BY 1h FILL(PREV) "
                    + "ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'";
            final QueryAssertion assertion = timestampType.equals("timestamp_ns")
                    ? assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck().timestamp("ts").noRandomAccess()
                    .withPlanNotContaining("Stream Group By")
                    : query(context, sql).withPlanNotContaining("sort", "Sort");
            assertion.returns(precision("""
                    ts\tmax
                    2024-10-26T23:00:00.000000Z\t1.0
                    2024-10-27T00:00:00.000000Z\t1.0
                    2024-10-27T01:00:00.000000Z\t1.0
                    2024-10-27T02:00:00.000000Z\t3.0
                    """));
        });
    }

    @Test
    public void testEmptyRange() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            for (String fill : new String[]{"PREV", "NULL", "42"}) {
                String value = fill.equals("42") ? "42.0" : "null";
                query(context, "SELECT ts, max(price) max FROM prices SAMPLE BY 1h "
                        + "FROM '2024-01-01' TO '2024-01-01T02:00' FILL(" + fill + ")")
                        .withPlanNotContaining("sort", "Sort")
                        .returns(precision("ts\tmax\n"
                                + "2024-01-01T00:00:00.000000Z\t" + value + "\n"
                                + "2024-01-01T01:00:00.000000Z\t" + value + "\n"));
            }
        });
    }

    @Test
    public void testFilteredPrevWithRange() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            insertPrices(context);
            query(context, "SELECT ts, max(price) max FROM prices WHERE price > 1 SAMPLE BY 1h "
                    + "FROM '2023-12-31T23:00' TO '2024-01-01T04:00' FILL(PREV)")
                    .withPlanNotContaining("sort", "Sort")
                    .returns(precision("""
                            ts\tmax
                            2023-12-31T23:00:00.000000Z\tnull
                            2024-01-01T00:00:00.000000Z\t3.0
                            2024-01-01T01:00:00.000000Z\t3.0
                            2024-01-01T02:00:00.000000Z\t5.0
                            2024-01-01T03:00:00.000000Z\t5.0
                            """));
        });
    }

    @Test
    public void testKeyedCacheLimitAndReuse() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_BYTES, 8 * 1024);
        run((engine, compiler, context) -> {
            createPrices(context);
            engine.execute("INSERT INTO prices SELECT "
                    + "timestamp_sequence('2024-01-01'::timestamp, 120_000_000L)::" + timestampType
                    + ", 'A', x::double FROM long_sequence(2049)", context);
            context.getBindVariableService().setDouble("max_price", 2049);
            final String sql = "SELECT ts, sym, max(price) max FROM prices WHERE price <= :max_price "
                    + "SAMPLE BY 1m FILL(PREV)";
            assertCachedPlan(context, sql);
            try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory();
                 MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH)) {
                context.setMemoryTracker(tracker);
                try {
                    for (int iteration = 0; iteration < 3; iteration++) {
                        context.getBindVariableService().setDouble("max_price", 2049);
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            cursor.hasNext();
                            Assert.fail("expected replay cache limit breach");
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "breached in VirtualMemory");
                            TestUtils.assertContains(e.getFlyweightMessage(), "(raise cairo.sql.sort.value.max.bytes)");
                        }
                        Assert.assertEquals(0, tracker.getUsed());
                        context.getBindVariableService().setDouble("max_price", 3);
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            for (int pass = 0; pass < 2; pass++) {
                                int count = 0;
                                while (cursor.hasNext()) {
                                    count++;
                                }
                                Assert.assertEquals(5, count);
                                cursor.toTop();
                            }
                        }
                        Assert.assertEquals(0, tracker.getUsed());
                    }
                } finally {
                    context.setMemoryTracker(null);
                }
            }
        });
    }

    @Test
    public void testKeyedCancellationAndReuse() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            insertPrices(context);
            try (NetworkSqlExecutionCircuitBreaker breaker = new NetworkSqlExecutionCircuitBreaker(engine,
                    new DefaultSqlExecutionCircuitBreakerConfiguration() {
                        @Override
                        public int getCircuitBreakerThrottle() {
                            return 0;
                        }
                    });
                 SqlExecutionContextImpl cancellable = new SqlExecutionContextImpl(engine, Math.max(1, workers))
                         .with(context.getSecurityContext(), null, null, -1, breaker)) {
                final String sql = "SELECT ts, sym, max(price) max FROM prices SAMPLE BY 1h FILL(PREV)";
                assertCachedPlan(cancellable, sql);
                try (RecordCursorFactory factory = compiler.compile(sql, cancellable).getRecordCursorFactory()) {
                    for (boolean initialized : new boolean[]{false, true}) {
                        try (RecordCursor cursor = factory.getCursor(cancellable)) {
                            if (initialized) {
                                Assert.assertTrue(cursor.hasNext());
                            }
                            breaker.cancel();
                            try {
                                while (cursor.hasNext()) {
                                }
                                Assert.fail("expected cancellation");
                            } catch (CairoException e) {
                                Assert.assertTrue(e.getFlyweightMessage().toString(), e.isInterruption());
                            }
                        } finally {
                            breaker.clear();
                        }
                        try (RecordCursor cursor = factory.getCursor(cancellable)) {
                            int count = 0;
                            while (cursor.hasNext()) {
                                count++;
                            }
                            Assert.assertEquals(10, count);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testKeyedEmptyRange() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            for (String fill : new String[]{"PREV", "NULL", "42"}) {
                query(context, "SELECT ts, sym, max(price) max FROM prices SAMPLE BY 1h "
                        + "FROM '2024-01-01' TO '2024-01-01T02:00' FILL(" + fill + ")")
                        .withPlanContaining("cached: true").withPlanNotContaining("sort", "Sort")
                        .returns("ts\tsym\tmax\n");
            }
        });
    }

    @Test
    public void testKeyedFilteredParquet() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            insertPrices(context);
            for (boolean parquet : new boolean[]{false, true}) {
                if (parquet) {
                    engine.execute("ALTER TABLE prices CONVERT PARTITION TO PARQUET WHERE ts >= 0", context);
                }
                keyedQuery(context, "SELECT ts, sym, max(price) max FROM prices WHERE price >= 1 "
                        + "SAMPLE BY 1h FILL(PREV) ORDER BY ts, sym")
                        .returns(precision("""
                                ts\tsym\tmax
                                2024-01-01T00:00:00.000000Z\tA\t1.0
                                2024-01-01T00:00:00.000000Z\tB\t3.0
                                2024-01-01T01:00:00.000000Z\tA\t1.0
                                2024-01-01T01:00:00.000000Z\tB\t3.0
                                2024-01-01T02:00:00.000000Z\tA\t1.0
                                2024-01-01T02:00:00.000000Z\tB\t5.0
                                """));
            }
        });
    }

    @Test
    public void testKeyedMixedFillWithNullAndLateKeys() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            engine.execute("INSERT INTO prices VALUES ('2024-01-01', 'A', 1), ('2024-01-01', null, 10), "
                    + "('2024-01-01T02:00', 'B', 3), ('2024-01-01T02:00', 'A', null)", context);
            keyedQuery(context, "SELECT ts, sym, sum(price) total, max(price) hi, count() n FROM prices "
                    + "SAMPLE BY 1h FILL(PREV, NULL, 0) ORDER BY ts, sym")
                    .returns(precision("""
                            ts\tsym\ttotal\thi\tn
                            2024-01-01T00:00:00.000000Z\t\t10.0\t10.0\t1
                            2024-01-01T00:00:00.000000Z\tA\t1.0\t1.0\t1
                            2024-01-01T00:00:00.000000Z\tB\tnull\tnull\t0
                            2024-01-01T01:00:00.000000Z\t\t10.0\tnull\t0
                            2024-01-01T01:00:00.000000Z\tA\t1.0\tnull\t0
                            2024-01-01T01:00:00.000000Z\tB\tnull\tnull\t0
                            2024-01-01T02:00:00.000000Z\t\t10.0\tnull\t0
                            2024-01-01T02:00:00.000000Z\tA\tnull\tnull\t1
                            2024-01-01T02:00:00.000000Z\tB\t3.0\t3.0\t1
                            """));
        });
    }

    @Test
    public void testKeyedPrevNullAndValueSkipSort() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            engine.execute("INSERT INTO prices VALUES ('2024-01-01', 'A', 1), "
                    + "('2024-01-01T02:00', 'B', 3)", context);
            for (String strategy : new String[]{"light_encoded", "full_encoded", "light_recordchain", "full_recordchain"}) {
                setProperty(PropertyKey.CAIRO_SQL_SAMPLEBY_FILL_SORT_STRATEGY, strategy);
                for (String fill : new String[]{"PREV", "NULL", "42"}) {
                    String missingA = fill.equals("PREV") ? "1.0" : fill.equals("NULL") ? "null" : "42.0";
                    String missingB = fill.equals("42") ? "42.0" : "null";
                    keyedQuery(context, "SELECT ts, sym, max(price) max FROM prices SAMPLE BY 1h FILL(" + fill + ") ORDER BY ts, sym")
                            .returns(precision("ts\tsym\tmax\n"
                                    + "2024-01-01T00:00:00.000000Z\tA\t1.0\n"
                                    + "2024-01-01T00:00:00.000000Z\tB\t" + missingB + "\n"
                                    + "2024-01-01T01:00:00.000000Z\tA\t" + missingA + "\n"
                                    + "2024-01-01T01:00:00.000000Z\tB\t" + missingB + "\n"
                                    + "2024-01-01T02:00:00.000000Z\tA\t" + missingA + "\n"
                                    + "2024-01-01T02:00:00.000000Z\tB\t3.0\n"));
                }
            }
        });
    }

    @Test
    public void testKeyedSmallMemoryLimit() throws Exception {
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 512 * 1024);
        setProperty(PropertyKey.CAIRO_SQL_GROUPBY_ALLOCATOR_DEFAULT_CHUNK_SIZE, 4 * 1024);
        run((engine, compiler, context) -> {
            createPrices(context);
            engine.execute("INSERT INTO prices VALUES ('2024-01-01', 'A', 1), "
                    + "('2024-01-01T02:00', 'A', 3)", context);
            query(context, "SELECT ts, sym, max(price) max FROM prices SAMPLE BY 1h FILL(PREV)")
                    .withPlanContaining("cached: true").withPlanNotContaining("sort", "Sort")
                    .returns(precision("""
                            ts\tsym\tmax
                            2024-01-01T00:00:00.000000Z\tA\t1.0
                            2024-01-01T01:00:00.000000Z\tA\t1.0
                            2024-01-01T02:00:00.000000Z\tA\t3.0
                            """));
        });
    }

    @Test
    public void testKeyedUnstableAggregateKeepsHash() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            insertPrices(context);
            final String sql = "SELECT ts, sym, sum(rnd_double()) total FROM prices SAMPLE BY 1h FILL(PREV)";
            try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                String plan = TestUtils.printSqlToString(engine, context, "EXPLAIN " + sql, new StringSink()).toString();
                TestUtils.assertContains(plan, "Sample By Fill");
                TestUtils.assertContains(plan, "sort light");
                Assert.assertFalse(plan, plan.contains("Stream Group By"));
                Assert.assertFalse(plan, plan.contains("cached: true"));
                for (int iteration = 0; iteration < 3; iteration++) {
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        StringSink expected = new StringSink();
                        CursorPrinter.println(cursor, factory.getMetadata(), expected, true, false);
                        for (int pass = 0; pass < 2; pass++) {
                            cursor.toTop();
                            Assert.assertTrue(cursor.hasNext());
                            cursor.toTop();
                            StringSink actual = new StringSink();
                            CursorPrinter.println(cursor, factory.getMetadata(), actual, true, false);
                            TestUtils.assertEquals(expected, actual);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testKeyedUuidPrevWithVarcharKey() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE ids (ts " + timestampType + ", sym SYMBOL, id UUID) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO ids VALUES ('2024-01-01', 'A', '00000000-0000-0000-0000-000000000001'), "
                    + "('2024-01-01', 'B', '00000000-0000-0000-0000-000000000002'), "
                    + "('2024-01-01T02:00', 'B', '00000000-0000-0000-0000-000000000003')", context);
            keyedQuery(context, "SELECT ts, sym::varchar name, first(id) id, first(sym) original FROM ids SAMPLE BY 1h FILL(PREV) ORDER BY ts, name")
                    .returns(precision("""
                            ts\tname\tid\toriginal
                            2024-01-01T00:00:00.000000Z\tA\t00000000-0000-0000-0000-000000000001\tA
                            2024-01-01T00:00:00.000000Z\tB\t00000000-0000-0000-0000-000000000002\tB
                            2024-01-01T01:00:00.000000Z\tA\t00000000-0000-0000-0000-000000000001\tA
                            2024-01-01T01:00:00.000000Z\tB\t00000000-0000-0000-0000-000000000002\tB
                            2024-01-01T02:00:00.000000Z\tA\t00000000-0000-0000-0000-000000000001\tA
                            2024-01-01T02:00:00.000000Z\tB\t00000000-0000-0000-0000-000000000003\tB
                            """));
        });
    }

    @Test
    public void testMixedCrossColumnPrev() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            insertPrices(context);
            query(context, "SELECT ts, max(price) hi, min(price) lo, count() n FROM prices "
                    + "SAMPLE BY 1h FILL(PREV(lo), PREV, 0)")
                    .withPlanNotContaining("sort", "Sort")
                    .returns(precision("""
                            ts\thi\tlo\tn
                            2024-01-01T00:00:00.000000Z\t3.0\t1.0\t2
                            2024-01-01T01:00:00.000000Z\t1.0\t1.0\t0
                            2024-01-01T02:00:00.000000Z\t5.0\t5.0\t2
                            2024-01-01T03:00:00.000000Z\t5.0\t5.0\t0
                            2024-01-01T04:00:00.000000Z\tnull\tnull\t1
                            """));
        });
    }

    @Test
    public void testPrevNullAndValueSkipSort() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            insertPrices(context);
            for (String fill : new String[]{"PREV", "NULL", "42"}) {
                String firstFill = fill.equals("PREV") ? "3.0" : fill.equals("NULL") ? "null" : "42.0";
                String secondFill = fill.equals("PREV") ? "5.0" : firstFill;
                query(context, "SELECT ts, max(price) max FROM prices SAMPLE BY 1h FILL(" + fill + ")")
                        .withPlanNotContaining("sort", "Sort")
                        .returns(precision("ts\tmax\n"
                                + "2024-01-01T00:00:00.000000Z\t3.0\n"
                                + "2024-01-01T01:00:00.000000Z\t" + firstFill + "\n"
                                + "2024-01-01T02:00:00.000000Z\t5.0\n"
                                + "2024-01-01T03:00:00.000000Z\t" + secondFill + "\n"
                                + "2024-01-01T04:00:00.000000Z\tnull\n"));
            }
        });
    }

    @Test
    public void testPrevParquetAndLimit() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            insertPrices(context);
            engine.execute("ALTER TABLE prices CONVERT PARTITION TO PARQUET WHERE ts >= 0", context);
            query(context, "SELECT ts, max(price) max FROM prices SAMPLE BY 1h FILL(PREV) LIMIT 2")
                    .withPlanNotContaining("sort", "Sort")
                    .returns(precision("""
                            ts\tmax
                            2024-01-01T00:00:00.000000Z\t3.0
                            2024-01-01T01:00:00.000000Z\t3.0
                            """));
        });
    }

    @Test
    public void testUuidPrevKeepsRandomAccessInput() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE ids (ts " + timestampType + ", id UUID) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO ids VALUES ('2024-01-01', '00000000-0000-0000-0000-000000000001'), "
                    + "('2024-01-01T02:00', '00000000-0000-0000-0000-000000000002')", context);
            assertQuery("SELECT ts, first(id) id FROM ids SAMPLE BY 1h FILL(PREV)")
                    .withEngine(engine).withContext(context).noLeakCheck().timestamp("ts").noRandomAccess()
                    .withPlanContaining("Sample By Fill", "sort light").withPlanNotContaining("Stream Group By")
                    .returns(precision("""
                            ts\tid
                            2024-01-01T00:00:00.000000Z\t00000000-0000-0000-0000-000000000001
                            2024-01-01T01:00:00.000000Z\t00000000-0000-0000-0000-000000000001
                            2024-01-01T02:00:00.000000Z\t00000000-0000-0000-0000-000000000002
                            """));
        });
    }

    private void createPrices(SqlExecutionContext context) throws Exception {
        context.getCairoEngine().execute("CREATE TABLE prices (ts " + timestampType
                + ", sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY", context);
    }

    private static void insertPrices(SqlExecutionContext context) throws Exception {
        context.getCairoEngine().execute("INSERT INTO prices VALUES ('2024-01-01', 'A', 1), "
                + "('2024-01-01T00:10', 'B', 3), ('2024-01-01T02:00', 'A', null), "
                + "('2024-01-01T02:10', 'B', 5), ('2024-01-01T04:00', 'A', null)", context);
    }

    private String precision(String expected) {
        return timestampType.equals("timestamp_ns") ? expected.replace(".000000Z", ".000000000Z") : expected;
    }

    private void assertCachedPlan(SqlExecutionContext context, String sql) throws Exception {
        String plan = assertStreamPlan(context, sql);
        String fillPlan = plan.substring(plan.indexOf("Sample By Fill"));
        TestUtils.assertContains(fillPlan, "cached: true");
        Assert.assertFalse(plan, fillPlan.contains("sort") || fillPlan.contains("Sort"));
    }

    private String assertStreamPlan(SqlExecutionContext context, String sql) throws Exception {
        String plan = TestUtils.printSqlToString(context.getCairoEngine(), context, "EXPLAIN " + sql, new StringSink()).toString();
        TestUtils.assertContains(plan, "Sample By Fill");
        TestUtils.assertContains(plan, "Stream Group By");
        Assert.assertEquals(plan, parallel, plan.contains("Async Stream Group By") || plan.contains("Async JIT Stream Group By"));
        return plan;
    }

    private QueryAssertion keyedQuery(SqlExecutionContext context, String sql) throws Exception {
        assertCachedPlan(context, sql);
        return assertQuery(sql).withEngine(context.getCairoEngine()).withContext(context).noLeakCheck().timestamp("ts");
    }

    private QueryAssertion query(SqlExecutionContext context, String sql) throws Exception {
        assertStreamPlan(context, sql);
        return assertQuery(sql).withEngine(context.getCairoEngine()).withContext(context).noLeakCheck()
                .timestamp("ts").noRandomAccess().withPlanContaining("Sample By Fill", "Stream Group By");
    }

    private void run(CustomisableRunnable runnable) throws Exception {
        assertMemoryLeak(() -> TestUtils.execute(workers == 0 ? null : new TestWorkerPool(workers), runnable, configuration, LOG));
    }
}
