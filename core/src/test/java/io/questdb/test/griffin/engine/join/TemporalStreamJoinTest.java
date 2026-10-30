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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestMatchFunctionFactory;
import io.questdb.griffin.engine.join.MaterializedRecordCursorFactory;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
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

@RunWith(Parameterized.class)
public class TemporalStreamJoinTest extends AbstractCairoTest {
    private static final String RHS = "SELECT ts, k, v, count() AS n FROM r GROUP BY ts, k, v ORDER BY ts";
    private final boolean isParallel;
    private final String join;
    private final int workerCount;

    public TemporalStreamJoinTest(String join, boolean isParallel, int workerCount) {
        this.join = join;
        this.isParallel = isParallel;
        this.workerCount = workerCount;
    }

    @Parameterized.Parameters(name = "join={0}, parallel={1}, workers={2}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {"ASOF", false, 1},
                {"LT", false, 1},
                {"ASOF", true, 0},
                {"LT", true, 0},
                {"ASOF", true, 4},
                {"LT", true, 4}
        });
    }

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, Boolean.toString(isParallel));
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 2);
        super.setUp();
    }

    @Test
    public void testArrayValue() throws Exception {
        assertValue("DOUBLE[]", "ARRAY[1.0, 2.0]", "ARRAY[3.0]", ColumnType.encodeArrayType(ColumnType.DOUBLE, 1), false);
    }

    @Test
    public void testBinaryValue() throws Exception {
        assertValue("BINARY", "from_base64('YWxwaGE=')", "from_base64('YmV0YQ==')", ColumnType.BINARY, false);
    }

    @Test
    public void testCopiedRhsCapabilitiesAndGrowth() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 256);
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_MAX_PAGES, 16_384);
        run((engine, compiler, context) -> {
            createLargeTables(engine, context);
            final RecordCursorFactory base = compiler.compile(RHS, context).getRecordCursorFactory();
            final EntityColumnFilter columns = new EntityColumnFilter();
            columns.of(base.getMetadata().getColumnCount());
            try (
                    RecordCursorFactory copy = new MaterializedRecordCursorFactory(configuration, base,
                            RecordSinkFactory.getInstance(configuration, new BytecodeAssembler(), base.getMetadata(), columns), true);
                    MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH)
            ) {
                Assert.assertTrue(copy.recordCursorSupportsRandomAccess());
                Assert.assertFalse(copy.isRewindRecomputing());
                Assert.assertEquals(0, copy.getMetadata().getTimestampIndex());
                Assert.assertEquals(RecordCursorFactory.SCAN_DIRECTION_FORWARD, copy.getScanDirection());
                context.setMemoryTracker(tracker);
                try {
                    try (RecordCursor cursor = copy.getCursor(context)) {
                        Assert.assertTrue(cursor.hasNext());
                        final long firstId = cursor.getRecord().getRowId();
                        final Record recordB = cursor.getRecordB();
                        cursor.recordAt(recordB, firstId);
                        TestUtils.assertEquals("alpha", recordB.getVarcharA(2));
                        final long initialMemory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
                        int count = 1;
                        while (cursor.hasNext()) {
                            count++;
                        }
                        Assert.assertEquals(10_000, count);
                        final long copiedMemory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
                        Assert.assertTrue(copiedMemory > initialMemory + 256);
                        Assert.assertTrue(tracker.getUsed() >= copiedMemory);
                        cursor.recordAt(recordB, firstId);
                        Assert.assertEquals(0, recordB.getTimestamp(0));
                        TestUtils.assertEquals("alpha", recordB.getVarcharA(2));
                        Assert.assertEquals(1, recordB.getLong(3));
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                } finally {
                    context.setMemoryTracker(null);
                }
            }
            // Read an early key's retained row after thousands of later groups have grown the copy.
            engine.execute("CREATE TABLE history AS (SELECT ts, CASE WHEN ts = 0 THEN 0 ELSE 1 END AS k, v FROM r) TIMESTAMP(ts)", context);
            engine.execute("CREATE TABLE probes (ts TIMESTAMP, k INT) TIMESTAMP(ts)", context);
            engine.execute("""
                    INSERT INTO probes VALUES
                        (9_999_500_000L, 1), (10_000_500_000L, 0)
                    """, context);
            final String sub = "SELECT ts, k, v, count() AS n FROM history GROUP BY ts, k, v ORDER BY ts";
            final String sql = "SELECT l.ts, b.ts AS rts, b.v, b.n FROM probes l " + join + " JOIN (" + sub + ") b ON (k)";
            final String expected = TestUtils.printSqlToString(engine, context, sql.replace(sub, "SELECT *, 1L AS n FROM history"), new StringSink());
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck()
                    .timestamp("ts").expectSize().noRandomAccess().withPlanContaining("Materialize", lightJoinPlan(true)).returns(expected);
        });
    }

    @Test
    public void testEmptyAndSingletonRhs() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context, "VARCHAR", "'alpha'", "'beta'");
            for (int mode = 0; mode < 3; mode++) {
                // No rows, one non-null group, and one NULL-valued group.
                final String sub = RHS.replace("FROM r ", "FROM r WHERE ts = " + (mode * 2_000_000L + 1_000_000L)
                        + "::TIMESTAMP " + (mode == 0 ? "AND k = 9 " : ""));
                assertAgainstHash(engine, context, query(sub, false));
            }
            engine.execute("CREATE TABLE empty_l (ts TIMESTAMP, k INT) TIMESTAMP(ts)", context);
            assertAgainstHash(engine, context, query(RHS, true).replace("FROM l ", "FROM empty_l l "));
        });
    }

    @Test
    public void testFilteredProjectedAndSampleByRhs() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context, "VARCHAR", "'alpha'", "'beta'");
            final ObjList<String> inputs = new ObjList<>();
            inputs.add("SELECT ts, k, v, count() AS n FROM r WHERE k > 0 GROUP BY ts, k, v ORDER BY ts");
            inputs.add("SELECT ts, k, v || '!' AS v, n FROM (" + RHS + ")");
            inputs.add("SELECT ts, k, v, count() AS n FROM r SAMPLE BY 1s");
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_DISABLED, SqlJitMode.JIT_MODE_ENABLED}) {
                context.setJitMode(jitMode);
                for (int i = 0; i < inputs.size(); i++) {
                    final String sub = inputs.getQuick(i);
                    // SAMPLE BY's oracle explicitly floors the timestamp on the non-designated table.
                    final String oracle = i == 2
                            ? "SELECT timestamp_floor('1s', ts) AS ts, k, v, count() AS n FROM r_hash GROUP BY ts, k, v ORDER BY ts"
                            : sub.replace("FROM r ", "FROM r_hash ");
                    final String expected = TestUtils.printSqlToString(engine, context, query(oracle, true), new StringSink());
                    assertQuery(query(sub, true)).withEngine(engine).withContext(context).noLeakCheck()
                            .timestamp("ts").expectSize().noRandomAccess()
                            .withPlanContaining("Materialize", "Stream Group By", lightJoinPlan(true)).returns(expected);
                }
            }
        });
    }

    @Test
    public void testFixedWidthUnderSmallCopyBudget() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 256);
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_MAX_PAGES, 1);
        run((engine, compiler, context) -> {
            createLargeTables(engine, context);
            final String sub = "SELECT ts, k, count() AS n FROM r GROUP BY ts, k ORDER BY ts";
            for (int mode = 0; mode < 2; mode++) {
                final String sql = "SELECT sum(b.n) AS total FROM l " + join + " JOIN (" + sub + ") b"
                        + (mode == 0 ? "" : " ON (k)");
                assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck()
                        .assertsPlanNotContaining("Materialize");
                assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck()
                        .expectSize().noRandomAccess().withPlanContaining("Stream Group By")
                        .returns("total\n10000\n");
                assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck().fullFatJoins()
                        .expectSize().noRandomAccess()
                        .returns("total\n10000\n");
            }
        });
    }

    @Test
    public void testForcedFullFatRejectsUnsupportedValue() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context, "VARCHAR", "'alpha'", "'beta'");
            assertQuery(query(RHS, false)).withEngine(engine).withContext(context).noLeakCheck()
                    .fullFatJoins().failsWith("right side column 'v' is of unsupported type");
        });
    }

    @Test
    public void testGroupedValueProjectedAway() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context, "VARCHAR", "'alpha'", "'beta'");
            final String sql = "SELECT l.ts, b.n FROM l " + join + " JOIN (" + RHS + ") b";
            final String expected = TestUtils.printSqlToString(engine, context, sql.replace("FROM r ", "FROM r_hash "), new StringSink());
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck()
                    .timestamp("ts").expectSize().noRandomAccess().returns(expected);
        });
    }

    @Test
    public void testInvalidToleranceAfterRhsAdaptation() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context, "VARCHAR", "'alpha'", "'beta'");
            final String sql = query(RHS, true);
            // EXPLAIN constructs and closes the adapted factory without opening a cursor.
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck()
                    .assertsPlanContaining("Materialize", lightJoinPlan(true));
            // The parser rejects negative tolerance before code generation. An unsupported unit instead
            // reaches tolerance() inside generateJoinAsof/Lt, after generateJoins has adopted the copy.
            final String invalid = sql + " TOLERANCE 1Q";
            assertQuery(invalid).withEngine(engine).withContext(context).noLeakCheck()
                    .fails(invalid.length() - 2, "unsupported TOLERANCE unit [unit=Q]");
        });
    }

    @Test
    public void testIntervalProjectionOverStream() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context, "VARCHAR", "'alpha'", "'beta'");
            final String sub = "SELECT ts, interval(ts, ts) AS v, n FROM "
                    + "(SELECT ts, count() AS n FROM r GROUP BY ts ORDER BY ts)";
            assertQuery(sub).withEngine(engine).withContext(context).noLeakCheck()
                    .assertsPlanContaining("Stream Group By");
            try (RecordCursorFactory factory = compiler.compile(sub, context).getRecordCursorFactory()) {
                Assert.assertFalse(factory.recordCursorSupportsRandomAccess());
                Assert.assertTrue(factory.isRewindRecomputing());
                Assert.assertEquals(ColumnType.INTERVAL_TIMESTAMP_MICRO, factory.getMetadata().getColumnType(1));
            }
            final String sql = query(sub, false);
            final String expected = TestUtils.printSqlToString(engine, context, sql.replace("FROM r ", "FROM r_hash "), new StringSink());
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck()
                    .timestamp("ts").expectSize().noRandomAccess().columnType(1, ColumnType.INTERVAL_TIMESTAMP_MICRO).returns(expected);
        });
    }

    @Test
    public void testMultiDimensionalArrayValue() throws Exception {
        assertValue("DOUBLE[][]", "ARRAY[[1.0, NULL], [2.0, 3.0]]", "'[]'::DOUBLE[][]",
                ColumnType.encodeArrayType(ColumnType.DOUBLE, 2), false);
    }

    @Test
    public void testNativeParquetAndO3Rhs() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context, "VARCHAR", "'alpha'", "'beta'");
            engine.execute("INSERT INTO r VALUES (3_000_000::TIMESTAMP, 1, 'beta')", context);
            engine.execute("INSERT INTO r_hash VALUES (3_000_000::TIMESTAMP, 1, 'beta')", context);
            assertAgainstHash(engine, context, query(RHS, true));
            engine.execute("ALTER TABLE r CONVERT PARTITION TO PARQUET WHERE ts >= 0", context);
            assertAgainstHash(engine, context, query(RHS, true));
        });
    }

    @Test
    public void testNegativeLimitRhs() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context, "VARCHAR", "'alpha'", "'beta'");
            final String sub = "SELECT * FROM (" + RHS + ") LIMIT -3";
            assertQuery(sub).withEngine(engine).withContext(context).noLeakCheck()
                    .assertsPlanContaining("Materialize", "Stream Group By");
            try (RecordCursorFactory factory = compiler.compile(sub, context).getRecordCursorFactory()) {
                Assert.assertFalse(factory.recordCursorSupportsRandomAccess());
                // The non-strict copy makes rewinds cheap but cannot promise random access.
                Assert.assertFalse(factory.isRewindRecomputing());
            }
            assertQuery(query(sub, false)).withEngine(engine).withContext(context).noLeakCheck()
                    .timestamp("ts").expectSize().noRandomAccess().returns(expectedTextRows());
        });
    }

    @Test
    public void testPartialRewindAndFactoryReuse() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context, "VARCHAR", "'alpha'", "'beta'");
            final String sql = query(RHS.replace("FROM r ", "FROM r WHERE test_match() "), true);
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck().assertsPlanContaining("Materialize", lightJoinPlan(true));
            try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                for (int iteration = 0; iteration < 3; iteration++) {
                    final String expected = TestUtils.printSqlToString(engine, context, query(RHS, true).replace("FROM r ", "FROM r_hash "), new StringSink());
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        for (int i = 0; i < 3; i++) {
                            Assert.assertTrue(cursor.hasNext());
                        }
                        final int topCount = TestMatchFunctionFactory.getTopCounter();
                        cursor.toTop();
                        final StringSink actual = new StringSink();
                        CursorPrinter.println(cursor, factory.getMetadata(), actual);
                        TestUtils.assertEquals(expected, actual);
                        Assert.assertEquals(topCount, TestMatchFunctionFactory.getTopCounter());
                    }
                    if (iteration == 1) {
                        engine.execute("INSERT INTO r VALUES (5_500_000::TIMESTAMP, 1, 'gamma')", context);
                        engine.execute("INSERT INTO r_hash VALUES (5_500_000::TIMESTAMP, 1, 'gamma')", context);
                    }
                }
            }
            final String streamMaster = "SELECT ts, k, count() AS n FROM l GROUP BY ts, k ORDER BY ts";
            final String streamSql = query(RHS, true).replace("FROM l ", "FROM (" + streamMaster + ") l ");
            final String expected = TestUtils.printSqlToString(engine, context, query(RHS, true).replace("FROM r ", "FROM r_hash "), new StringSink());
            assertQuery(streamSql).withEngine(engine).withContext(context).noLeakCheck()
                    .timestamp("ts").expectSize(false).noRandomAccess()
                    .withPlanContaining("Materialize", "Stream Group By", lightJoinPlan(true)).returns(expected);
        });
    }

    @Test
    public void testRandomAccessRhs() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context, "VARCHAR", "'alpha'", "'beta'");
            final String sql = query(RHS + ", v", false);
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck()
                    .assertsPlanNotContaining("Materialize");
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck()
                    .timestamp("ts").expectSize().noRandomAccess().withPlanContaining("Stream Group By", "sort")
                    .returns(expectedTextRows());
        });
    }

    @Test
    public void testStrictTemporalCopyPageBudgetAndReuse() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 256);
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_MAX_PAGES, 1);
        assertCopyLimitAndReuse("materialized rows exceed the memory limit [maxBytes=256]", false);
    }

    @Test
    public void testStrictTemporalCopyQueryMemoryLimitAndReuse() throws Exception {
        // Leave room for the worker maps; the 256-byte payloads make only the retained copy exceed this limit.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 2_097_152);
        assertCopyLimitAndReuse("query memory limit exceeded [workload=MAT_VIEW_REFRESH", true);
    }

    @Test
    public void testStringValue() throws Exception {
        assertValue("STRING", "'alpha'", "'beta'", ColumnType.STRING, false);
    }

    @Test
    public void testSymbolKeysAndValuesWithVarcharPayload() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE l (ts TIMESTAMP, s SYMBOL, v VARCHAR) TIMESTAMP(ts)", context);
            engine.execute("CREATE TABLE r (ts TIMESTAMP, s SYMBOL, v VARCHAR) TIMESTAMP(ts)", context);
            // Different insertion orders give the same symbol different IDs in the two tables.
            engine.execute("""
                    INSERT INTO l VALUES
                        (0, 'B', 'B'), (2, 'A', 'A'), (4, NULL, NULL), (6, 'C', 'C'), (8, 'A', 'A')
                    """, context);
            engine.execute("""
                    INSERT INTO r VALUES
                        (1, 'A', 'old'), (3, NULL, NULL), (5, 'B', 'other'), (7, 'A', 'new')
                    """, context);
            engine.execute("CREATE TABLE r_hash (ts TIMESTAMP, s SYMBOL, v VARCHAR)", context);
            engine.execute("INSERT INTO r_hash SELECT * FROM r", context);
            final String sub = "SELECT ts, s, v, v::STRING::SYMBOL AS dynamic_symbol, n FROM "
                    + "(SELECT ts, s, v, count() AS n FROM r GROUP BY ts, s, v ORDER BY ts)";
            final String sql = "SELECT l.ts, b.s, b.dynamic_symbol, b.v, b.n FROM l " + join
                    + " JOIN (" + sub + ") b ON b.s = l.s AND b.s = l.v";
            final String expected = TestUtils.printSqlToString(engine, context, sql.replace("FROM r ", "FROM r_hash "), new StringSink());
            // The repeated key implies a master-side s = v filter, so even the RA oracle has unknown size.
            final String oracle = sql.replace("FROM r ", "FROM r_hash ");
            assertQuery(oracle).withEngine(engine).withContext(context).noLeakCheck()
                    .timestamp("ts").expectSize(false).noRandomAccess().returns(expected);
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck()
                    .timestamp("ts").expectSize(false).noRandomAccess().columnType(1, ColumnType.SYMBOL).columnType(2, ColumnType.SYMBOL)
                    .withPlanContaining("Materialize", "Stream Group By", lightJoinPlan(true), "Filter").returns(expected);
        });
    }

    @Test
    public void testTemporalCopyCancellationEarlyCloseAndReuse() throws Exception {
        run((engine, compiler, context) -> {
            createLargeTables(engine, context);
            for (int mode = 0; mode < 2; mode++) {
                final String sql = query(RHS, mode == 1);
                final String expected = TestUtils.printSqlToString(engine, context, query("SELECT ts, k, v, 1L AS n FROM r", mode == 1), new StringSink());
                assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck()
                        .timestamp("ts").expectSize().noRandomAccess().withPlanContaining("Materialize", lightJoinPlan(mode == 1)).returns(expected);
                try (
                        NetworkSqlExecutionCircuitBreaker breaker = new NetworkSqlExecutionCircuitBreaker(engine,
                                new DefaultSqlExecutionCircuitBreakerConfiguration() {
                                    @Override
                                    public int getCircuitBreakerThrottle() {
                                        return 0;
                                    }
                                });
                        SqlExecutionContextImpl cancellable = new SqlExecutionContextImpl(engine, Math.max(1, workerCount))
                                .with(context.getSecurityContext(), null, null, -1, breaker);
                        RecordCursorFactory factory = compiler.compile(sql, cancellable).getRecordCursorFactory();
                        MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH)
                ) {
                    cancellable.setMemoryTracker(tracker);
                    try {
                        for (int iteration = 0; iteration < 2; iteration++) {
                            try (RecordCursor cursor = factory.getCursor(cancellable)) {
                                Assert.assertTrue(cursor.hasNext());
                                Assert.assertTrue(tracker.getUsed() > 0);
                                if (iteration == 1) {
                                    breaker.cancel();
                                    try {
                                        while (cursor.hasNext()) {
                                            // The real breaker must interrupt the copy/join, not just compilation.
                                        }
                                        Assert.fail("expected cancellation");
                                    } catch (CairoException e) {
                                        Assert.assertTrue(e.getFlyweightMessage().toString(), e.isInterruption());
                                    }
                                }
                            } finally {
                                breaker.clear();
                            }
                            Assert.assertEquals(0, tracker.getUsed());
                            TestUtils.assertEquals(expected, readRows(factory, cancellable));
                            Assert.assertEquals(0, tracker.getUsed());
                        }
                    } finally {
                        cancellable.setMemoryTracker(null);
                    }
                }
            }
        });
    }

    @Test
    public void testTimestampTiesAndTolerance() throws Exception {
        run((engine, compiler, context) -> {
            // The grouped input has one value per key at each timestamp: no unspecified group tie order.
            for (int types = 0; types < 4; types++) {
                final String leftType = (types & 1) == 0 ? "TIMESTAMP" : "TIMESTAMP_NS";
                final String rightType = (types & 2) == 0 ? "TIMESTAMP" : "TIMESTAMP_NS";
                final long leftSecond = (types & 1) == 0 ? 1_000_000L : 1_000_000_000L;
                final long rightSecond = (types & 2) == 0 ? 1_000_000L : 1_000_000_000L;
                final String left = "l" + types;
                final String right = "r" + types;
                engine.execute("CREATE TABLE " + left + " (ts " + leftType + ", k INT) TIMESTAMP(ts)", context);
                engine.execute("CREATE TABLE " + right + " (ts " + rightType + ", k INT, v VARCHAR) TIMESTAMP(ts)", context);
                engine.execute("""
                        INSERT INTO %s VALUES
                            (0, 1), (%d, 1), (%d, 2), (%d, 1), (%d, 1), (%d, 1), (%d, 9), (%d, NULL)
                        """.formatted(left, leftSecond, leftSecond, 2 * leftSecond, 2 * leftSecond + 1,
                        3 * leftSecond, 4 * leftSecond, 4 * leftSecond), context);
                engine.execute("""
                        INSERT INTO %s VALUES
                            (%d, 1, 'a'), (%d, 2, 'b'), (%d, 1, 'c'), (%d, NULL, NULL)
                        """.formatted(right, rightSecond, rightSecond, 3 * rightSecond, 3 * rightSecond), context);
                final String sub = "SELECT ts, k, v, count() AS n FROM " + right + " GROUP BY ts, k, v ORDER BY ts";
                final String sql = "SELECT l.ts, l.k, b.ts AS rts, b.v, b.n FROM " + left + " l " + join
                        + " JOIN (" + sub + ") b ON (k) TOLERANCE 1s";
                final String expected = TestUtils.printSqlToString(engine, context,
                        sql.replace(sub, "SELECT *, 1L AS n FROM " + right), new StringSink());
                assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck()
                        .timestamp("ts").expectSize().noRandomAccess()
                        .columnType(2, (types & 2) == 0 ? ColumnType.TIMESTAMP : ColumnType.TIMESTAMP_NANO)
                        .withPlanContaining("Materialize", "Stream Group By", lightJoinPlan(true)).returns(expected);
                assertQuery(sql.replace("ORDER BY ts", "ORDER BY ts DESC")).withEngine(engine).withContext(context).noLeakCheck()
                        .failsWith("right side of time series join doesn't have ASC timestamp order");
            }
        });
    }

    @Test
    public void testUnionAllTimestampTies() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context, "VARCHAR", "'alpha'", "'beta'");
            // UNION ALL retains table scan order, including the different payloads at the same timestamp.
            engine.execute("INSERT INTO r VALUES (5_000_000::TIMESTAMP, 1, 'last')", context);
            final String sub = "SELECT * FROM (SELECT ts, v, 1L AS n FROM r UNION ALL SELECT ts, v, 1L AS n FROM r WHERE k < 0) TIMESTAMP(ts)";
            final String sql = query(sub, false);
            final String expected = TestUtils.printSqlToString(engine, context, query("SELECT ts, v, 1L AS n FROM r", false), new StringSink());
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck()
                    .timestamp("ts").expectSize().noRandomAccess()
                    .withPlanContaining("Materialize", "Union All", lightJoinPlan(false)).returns(expected);
        });
    }

    @Test
    public void testVarcharJoinKeyUnderSmallCopyBudget() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 256);
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_MAX_PAGES, 1);
        run((engine, compiler, context) -> {
            createLargeTables(engine, context);
            final String sql = "SELECT sum(b.n) AS total FROM l " + join
                    + " JOIN (SELECT ts, v, count() AS n FROM r GROUP BY ts, v ORDER BY ts) b ON (v)";
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck()
                    .assertsPlanNotContaining("Materialize");
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck()
                    .expectSize().noRandomAccess().withPlanContaining("Stream Group By")
                    .returns("total\n10000\n");
        });
    }

    @Test
    public void testVarcharKeyedValue() throws Exception {
        assertValue("VARCHAR", "'alpha'", "'beta'", ColumnType.VARCHAR, true);
    }

    @Test
    public void testVarcharValue() throws Exception {
        assertValue("VARCHAR", "'alpha'", "'beta'", ColumnType.VARCHAR, false);
    }

    @Test
    public void testVarcharNullJoinKeyStaysFullFat() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context, "VARCHAR", "'alpha'", "'beta'");
            engine.execute("CREATE TABLE keys (ts TIMESTAMP, v VARCHAR) TIMESTAMP(ts)", context);
            engine.execute("""
                    INSERT INTO keys VALUES
                        (2_000_000, 'alpha'), (4_000_000, 'missing'), (6_000_000, NULL)
                    """, context);
            final String sub = "SELECT ts, v, count() AS n FROM r GROUP BY ts, v ORDER BY ts";
            final String sql = "SELECT l.ts, b.v, b.n FROM keys l " + join + " JOIN (" + sub + ") b ON (v)";
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck().assertsPlanNotContaining("Materialize");
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck()
                    .timestamp("ts").expectSize().noRandomAccess().withPlanContaining("Stream Group By")
                    .returns("""
                            ts\tv\tn
                            1970-01-01T00:00:02.000000Z\talpha\t2
                            1970-01-01T00:00:04.000000Z\t\tnull
                            1970-01-01T00:00:06.000000Z\t\t1
                            """);
        });
    }

    @Test
    public void testWideFixedWidthValuesStayFullFat() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 256);
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_MAX_PAGES, 1);
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE l (ts TIMESTAMP, k INT) TIMESTAMP(ts)", context);
            engine.execute("CREATE TABLE r (ts TIMESTAMP, k INT, u UUID, h LONG256, d DECIMAL(25,2)) TIMESTAMP(ts)", context);
            engine.execute("""
                    INSERT INTO l VALUES
                        (2, 1), (4, 1), (6, 1)
                    """, context);
            engine.execute("""
                    INSERT INTO r VALUES
                        (1, 1, '11111111-1111-1111-1111-111111111111', '0x1234', '123.45'::DECIMAL(25,2)),
                        (3, 1, NULL, NULL, NULL),
                        (5, 1, '22222222-2222-2222-2222-222222222222', '0x5678', '-678.90'::DECIMAL(25,2))
                    """, context);
            final String sub = "SELECT ts, k, u, h, d, count() AS n FROM r GROUP BY ts, k, u, h, d ORDER BY ts";
            final String sql = "SELECT l.ts, b.u, b.h, b.d, b.n FROM l " + join + " JOIN (" + sub + ") b ON (k)";
            final String expected = TestUtils.printSqlToString(engine, context, sql.replace(sub, "SELECT *, 1L AS n FROM r"), new StringSink());
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck().assertsPlanNotContaining("Materialize");
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck()
                    .timestamp("ts").expectSize().noRandomAccess().withPlanContaining("Stream Group By").returns(expected);
        });
    }

    private void assertAgainstHash(CairoEngine engine, SqlExecutionContext context, String sql) throws Exception {
        final String expected = TestUtils.printSqlToString(engine, context, sql.replace("FROM r ", "FROM r_hash "), new StringSink());
        assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck()
                .timestamp("ts").expectSize().noRandomAccess().withPlanContaining("Materialize", "Stream Group By").returns(expected);
    }

    private void assertCopyLimitAndReuse(String error, boolean isQueryLimit) throws Exception {
        run((engine, compiler, context) -> {
            createLargeTables(engine, context, isQueryLimit);
            final String sub = RHS.replace("FROM r ", "FROM r WHERE ts <= $1 ");
            final String sql = query(sub, false);
            context.getBindVariableService().setTimestamp(0, 10_000_000_000L);
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck().assertsPlanContaining("Materialize", lightJoinPlan(false));
            try (
                    RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory();
                    MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH)
            ) {
                context.setMemoryTracker(tracker);
                try {
                    for (int iteration = 0; iteration < 2; iteration++) {
                        boolean hasCopiedRows = false;
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertTrue(tracker.getUsed() > 0);
                            hasCopiedRows = true;
                            while (cursor.hasNext()) {
                                // Strict copying must fail, never silently fall back to streaming.
                            }
                            Assert.fail("expected strict copy limit");
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), error);
                            if (isQueryLimit) {
                                TestUtils.assertContains(e.getFlyweightMessage(), "memoryTag=" + MemoryTag.NATIVE_RECORD_CHAIN);
                            }
                        }
                        Assert.assertTrue("the failure must occur after the join starts copying", hasCopiedRows);
                        Assert.assertEquals(0, tracker.getUsed());
                    }
                    context.getBindVariableService().setTimestamp(0, 0);
                    final String expected = TestUtils.printSqlToString(engine, context,
                            query("SELECT ts, k, v, 1L AS n FROM r WHERE ts <= $1", false), new StringSink());
                    TestUtils.assertEquals(expected, readRows(factory, context));
                    Assert.assertEquals(0, tracker.getUsed());
                } finally {
                    context.setMemoryTracker(null);
                }
            }
        });
    }

    private void assertValue(String type, String first, String second, int columnType, boolean isKeyed) throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context, type, first, second);
            // Prove the supported producer independently of the failing temporal join.
            assertQuery(RHS).withEngine(engine).withContext(context).noLeakCheck()
                    .assertsPlanContaining(isParallel && workerCount > 0 ? "Async Stream Group By" : "Stream Group By");
            try (RecordCursorFactory factory = compiler.compile(RHS, context).getRecordCursorFactory()) {
                Assert.assertFalse(factory.recordCursorSupportsRandomAccess());
                Assert.assertEquals(0, factory.getMetadata().getTimestampIndex());
                Assert.assertEquals(columnType, factory.getMetadata().getColumnType(2));
            }
            final String sql = query(RHS, isKeyed);
            // A non-designated table forces hash grouping plus sorting, independent of the stream implementation.
            final String expected = TestUtils.printSqlToString(engine, context, sql.replace("FROM r ", "FROM r_hash "), new StringSink());
            if (columnType == ColumnType.VARCHAR || columnType == ColumnType.STRING) {
                TestUtils.assertEquals(expectedTextRows(), expected);
            }
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck()
                    .timestamp("ts").expectSize().noRandomAccess().columnType(1, columnType)
                    .withPlanContaining("Materialize", lightJoinPlan(isKeyed)).returns(expected);
        });
    }

    private static void createLargeTables(CairoEngine engine, SqlExecutionContext context) throws SqlException {
        createLargeTables(engine, context, false);
    }

    private static void createLargeTables(CairoEngine engine, SqlExecutionContext context, boolean hasLargePayload) throws SqlException {
        engine.execute("""
                CREATE TABLE r AS (
                    SELECT timestamp_sequence(0, 1_000_000L) AS ts, 1 AS k, %s::VARCHAR AS v
                    FROM long_sequence(10_000)
                ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                """.formatted(hasLargePayload ? "'" + "x".repeat(256) + "'" : "'alpha'"), context);
        engine.execute("""
                CREATE TABLE l AS (
                    SELECT timestamp_sequence(500_000, 1_000_000L) AS ts, 1 AS k, 'alpha'::VARCHAR AS v
                    FROM long_sequence(10_000)
                ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                """, context);
    }

    private static void createTables(CairoEngine engine, SqlExecutionContext context, String type, String first, String second) throws SqlException {
        engine.execute("CREATE TABLE l (ts TIMESTAMP, k INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", context);
        engine.execute("CREATE TABLE r (ts TIMESTAMP, k INT, v " + type + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", context);
        engine.execute("""
                INSERT INTO l VALUES
                    (0::TIMESTAMP, 1), (1_000_000::TIMESTAMP, 1), (2_000_000::TIMESTAMP, 1),
                    (3_000_000::TIMESTAMP, 1), (4_000_000::TIMESTAMP, 1), (5_000_000::TIMESTAMP, 1),
                    (6_000_000::TIMESTAMP, 1)
                """, context);
        engine.execute("""
                INSERT INTO r VALUES
                    (1_000_000::TIMESTAMP, 1, %s), (1_000_000::TIMESTAMP, 1, %s),
                    (3_000_000::TIMESTAMP, 1, %s), (5_000_000::TIMESTAMP, 1, NULL)
                """.formatted(first, first, second), context);
        engine.execute("CREATE TABLE r_hash (ts TIMESTAMP, k INT, v " + type + ")", context);
        engine.execute("INSERT INTO r_hash SELECT * FROM r", context);
    }

    private String expectedTextRows() {
        return "ASOF".equals(join) ? """
                ts\tv\tn
                1970-01-01T00:00:00.000000Z\t\tnull
                1970-01-01T00:00:01.000000Z\talpha\t2
                1970-01-01T00:00:02.000000Z\talpha\t2
                1970-01-01T00:00:03.000000Z\tbeta\t1
                1970-01-01T00:00:04.000000Z\tbeta\t1
                1970-01-01T00:00:05.000000Z\t\t1
                1970-01-01T00:00:06.000000Z\t\t1
                """ : """
                ts\tv\tn
                1970-01-01T00:00:00.000000Z\t\tnull
                1970-01-01T00:00:01.000000Z\t\tnull
                1970-01-01T00:00:02.000000Z\talpha\t2
                1970-01-01T00:00:03.000000Z\talpha\t2
                1970-01-01T00:00:04.000000Z\tbeta\t1
                1970-01-01T00:00:05.000000Z\tbeta\t1
                1970-01-01T00:00:06.000000Z\t\t1
                """;
    }

    private String lightJoinPlan(boolean isKeyed) {
        return ("ASOF".equals(join) ? "AsOf Join" : "Lt Join") + (isKeyed ? " Light" : "\n");
    }

    private String query(String rhs, boolean isKeyed) {
        return "SELECT l.ts, b.v, b.n FROM l " + join + " JOIN (" + rhs + ") b" + (isKeyed ? " ON (k)" : "");
    }

    private static String readRows(RecordCursorFactory factory, SqlExecutionContext context) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(context)) {
            final StringSink result = new StringSink();
            CursorPrinter.println(cursor, factory.getMetadata(), result);
            return result.toString();
        }
    }

    private void run(CustomisableRunnable runnable) throws Exception {
        assertMemoryLeak(() -> TestUtils.execute(workerCount == 0 ? null : new TestWorkerPool(workerCount), runnable, configuration, LOG));
    }
}
