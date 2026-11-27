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
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.DelegatingRecord;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.test.TestMatchFunctionFactory;
import io.questdb.griffin.engine.join.CrossJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.MaterializedRecordCursorFactory;
import io.questdb.griffin.engine.join.NestedLoopFullJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.NestedLoopLeftJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.NullRecordFactory;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.CustomisableRunnable;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.LimitedMemoryTracker;
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
 * so the aggregation runs once per cursor. Nested loop, splice and serial horizon joins read it through a strict
 * copy, which fails at the memory limit rather than rerun the aggregation, and gives splice and horizon joins random
 * access. The expected rows come from the same aggregation over a table without a designated
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
                try (RecordCursorFactory ignored = new MaterializedRecordCursorFactory(configuration, base, null, true, true, false)) {
                    Assert.fail("expected metadata failure");
                } catch (RuntimeException e) {
                    Assert.assertSame(failure, e);
                }
                Assert.assertEquals(1, closeCount[0]);
            }
        });
    }

    @Test
    public void testCopiesInBatches() throws Exception {
        run((engine, compiler, context) -> {
            final LongList rowIds = new LongList();
            for (int rowCount : new int[]{0, 1, 255, 256, 257, 600}) {
                for (boolean isCopiedInBatches : new boolean[]{false, true}) {
                    final String label = rowCount + " rows, batches: " + isCopiedInBatches;
                    try (
                            RecordCursorFactory copy = strictCopy(compiler, context, "SELECT x FROM long_sequence(" + rowCount + ")", isCopiedInBatches);
                            RecordCursor cursor = copy.getCursor(context)
                    ) {
                        final Record record = cursor.getRecord();
                        rowIds.clear();
                        for (int i = 1; i <= rowCount; i++) {
                            Assert.assertTrue(label, cursor.hasNext());
                            Assert.assertEquals(label, i, record.getLong(0));
                            Assert.assertEquals(label, isCopiedInBatches ? Math.min(rowCount, (i + 255) / 256 * 256) : i, cursor.preComputedStateSize());
                            rowIds.add(record.getRowId());
                        }
                        Assert.assertFalse(label, cursor.hasNext());
                        Assert.assertEquals(label, rowCount, cursor.preComputedStateSize());

                        cursor.toTop();
                        final Record recordB = cursor.getRecordB();
                        for (int i = 1; i <= rowCount; i++) {
                            Assert.assertTrue(label, cursor.hasNext());
                            Assert.assertEquals(label, i, record.getLong(0));
                            cursor.recordAt(recordB, rowIds.getQuick(rowCount - i));
                            Assert.assertEquals(label, rowCount - i + 1, recordB.getLong(0));
                        }
                        Assert.assertFalse(label, cursor.hasNext());
                        Assert.assertEquals(label, rowCount, cursor.preComputedStateSize());
                    }
                }
            }
        });
    }

    @Test
    public void testCopiesInBatchesUntilMemoryLimit() throws Exception {
        // The 24-byte rows reach the limit inside a batch rather than on a batch boundary.
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_LIGHT_VALUE_PAGE_SIZE, 256);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 10_000);
        run((engine, compiler, context) -> {
            try (RecordCursorFactory copy = strictCopy(compiler, context, "SELECT x, x * 2 y, x * 3 z FROM long_sequence(1_000)", true)) {
                long rowCount = 0;
                try (MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH)) {
                    context.setMemoryTracker(tracker);
                    try (RecordCursor cursor = copy.getCursor(context)) {
                        final Record record = cursor.getRecord();
                        while (cursor.hasNext()) {
                            Assert.assertEquals(++rowCount, record.getLong(0));
                        }
                        Assert.fail("expected memory limit breach");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded [workload=MAT_VIEW_REFRESH");
                    } finally {
                        context.setMemoryTracker(null);
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                }
                // The breach fails the batch that holds it, after the parent read every earlier batch.
                Assert.assertTrue(rowCount > 0);
                Assert.assertEquals(0, rowCount % 256);
                try (RecordCursor cursor = copy.getCursor(context)) {
                    final Record record = cursor.getRecord();
                    for (int i = 1; i <= 1_000; i++) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(i, record.getLong(0));
                    }
                    Assert.assertFalse(cursor.hasNext());
                }
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
            final String[] subquery = withNegativeLimit(SUBQUERIES[1]);
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
    public void testFullFatHashJoinUnderRightNestedLoopHashesStreamOnce() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            engine.execute("CREATE TABLE k AS (SELECT CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END::SYMBOL sym, x y FROM long_sequence(2))", context);
            final String query = "SELECT k.y, s.c, m.x FROM k JOIN ($sub) s ON k.sym = s.sym RIGHT JOIN m ON s.c < m.x ORDER BY m.x, k.y, s.c";
            final String expected = TestUtils.printSqlToString(engine, context, query.replace("$sub", SUBQUERIES[1][1]), new StringSink());
            final String sql = query.replace("$sub", SUBQUERIES[1][0]);
            compiler.setFullFatJoins(true);
            try (RecordCursorFactory factory = compiler.compile("EXPLAIN " + sql, context).getRecordCursorFactory()) {
                final String plan = readRows(factory, context);
                Assert.assertTrue(plan, plan.contains("Nested Loop Right Join") && plan.contains("Hash Join\n") && !plan.contains("Materialize"));
            }
            try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                for (int pass = 0; pass < 2; pass++) {
                    Assert.assertEquals(expected, readRows(factory, context));
                }
            }
            Assert.assertEquals(0, countRewinds(compiler, context, sql));
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
    public void testHashJoinCopiesStreamInBatches() throws Exception {
        run((engine, compiler, context) -> {
            // The 1_000 rows of the aggregation fill three batches and part of a fourth.
            engine.execute(
                    """
                            CREATE TABLE b AS (
                                SELECT timestamp_sequence('2024-01-01', 1_000_000L) ts, x v FROM long_sequence(1_000)
                            ) TIMESTAMP(ts) PARTITION BY DAY""",
                    context
            );
            engine.execute("CREATE TABLE b_hash (ts TIMESTAMP, v LONG)", context);
            engine.execute("INSERT INTO b_hash SELECT * FROM b", context);
            engine.execute("CREATE TABLE h AS (SELECT timestamp_sequence('2024-01-01', 3_000_000L) ts, x FROM long_sequence(400))", context);
            final String[] subquery = {
                    "SELECT ts, count() c, sum(v) s FROM b WHERE test_match() GROUP BY ts",
                    "SELECT ts, count() c, sum(v) s FROM b_hash GROUP BY ts ORDER BY ts",
                    "ts"
            };
            for (String join : new String[]{"JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"}) {
                final String query = "SELECT h.x, s.* FROM h " + join + " ($sub) s ON h.ts = s.ts ORDER BY x, $keys";
                Assert.assertEquals(join, 0, assertMaterialized(engine, context, query, subquery, null, true, false));
            }
        });
    }

    @Test
    public void testHashJoinSizeDoesNotRewindStreamMaster() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            final String sql = "SELECT a.ts, a.sym, a.c, b.s FROM (" + SUBQUERIES[1][0] + ") a JOIN "
                    + "(SELECT ts, v s FROM t UNION ALL SELECT ts, v s FROM t) b ON (ts)";
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
    public void testHashJoinValuePageOfOneByte() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT timestamp_sequence('2024-01-01', 60_000_000L) ts, (x % 2)::VARCHAR s, x v FROM long_sequence(200)) TIMESTAMP(ts) PARTITION BY DAY");
            assertQuery("SELECT ts, s, sum(v) sv FROM t SAMPLE BY 1h ORDER BY ts, s LIMIT -2")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts\tsv
                            2024-01-01T03:00:00.000000Z\t0\t1910
                            2024-01-01T03:00:00.000000Z\t1\t1900
                            """);
            assertQuery("SELECT count(), sum(sv) FROM long_sequence(3) a CROSS JOIN (SELECT ts, s, sum(v) sv FROM t SAMPLE BY 1h) b")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            24\t60300
                            """);
        });
    }

    @Test
    public void testHashJoinReadsStreamThroughCopy() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            engine.execute("CREATE TABLE h AS (SELECT timestamp_sequence('2024-01-01', 3_600_000_000L) ts, x FROM long_sequence(5))", context);
            for (String[] join : new String[][]{
                    {"JOIN", "Hash Join Light"},
                    {"LEFT JOIN", "Hash Left Outer Join Light"},
                    {"RIGHT JOIN", "Hash Right Outer Join Light"},
                    {"FULL JOIN", "Hash Full Outer Join Light"}
            }) {
                final String query = "SELECT h.x, s.* FROM h " + join[0] + " ($sub) s ON h.ts = s.ts ORDER BY x, $keys";
                assertMaterialized(engine, context, query, null, true, false);
                assertQuery(query.replace("$sub", SUBQUERIES[0][0]).replace("$keys", SUBQUERIES[0][2]))
                        .withEngine(engine)
                        .withContext(context)
                        .noLeakCheck()
                        .assertsPlanContaining(join[1]);
            }
            for (String right : new String[]{
                    "SELECT ts, c + 1 c FROM (SELECT ts, count() c FROM t SAMPLE BY 1h)",
                    "SELECT * FROM (SELECT ts, count() c FROM t SAMPLE BY 1h) WHERE c > 1",
                    "SELECT ts, count() c FROM t SAMPLE BY 1h LIMIT 3"
            }) {
                assertQuery("SELECT h.x, s.* FROM h JOIN (" + right + ") s ON h.ts = s.ts")
                        .withEngine(engine)
                        .withContext(context)
                        .noLeakCheck()
                        .assertsPlanContaining("Hash Join Light", "Materialize", "Stream Group By");
            }
            for (String right : new String[]{
                    "SELECT ts, count() c FROM t SAMPLE BY 1h FILL(PREV)",
                    "SELECT ts, count() c FROM t SAMPLE BY 1h UNION ALL SELECT ts, count() c FROM t SAMPLE BY 1d",
                    "SELECT ts, c, row_number() OVER () n FROM (SELECT ts, count() c FROM t SAMPLE BY 1h)"
            }) {
                assertQuery("SELECT h.x, s.* FROM h JOIN (" + right + ") s ON h.ts = s.ts")
                        .withEngine(engine)
                        .withContext(context)
                        .noLeakCheck()
                        .assertsPlanContaining("Hash Join\n");
            }
        });
    }

    @Test
    public void testHorizonJoin() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            assertMaterialized(engine, context, HORIZON_JOIN, null, true, true);
            assertMaterialized(engine, context, MULTI_HORIZON_JOIN, null, true, true);
            for (String[] subquery : SUBQUERIES) {
                Assert.assertEquals(0, assertMaterialized(engine, context, HORIZON_JOIN, withNegativeLimit(subquery), null, true, true));
            }
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
    public void testJoinBudgetBoundsOnlyJoinCopies() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 256);
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_MAX_PAGES, 1);
        run((engine, compiler, context) -> {
            createTables(engine, context);
            for (String query : new String[]{CROSS_JOIN, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN}) {
                for (String[] subquery : SUBQUERIES) {
                    Assert.assertEquals(query, 0, assertMaterialized(engine, context, query, subquery, null, true, false));
                }
            }
            // The full nested loop reads its slave in place, so only the legacy planner copies it.
            final String fullJoin = "SELECT m.x, s.* FROM m FULL JOIN (SELECT * FROM t_hash UNION ALL SELECT * FROM t_hash) s ON s.v < m.x";
            assertQuery("SELECT count() FROM (" + fullJoin + ")")
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n413\n");
            setProperty(PropertyKey.CAIRO_SQL_LEGACY_PLANNER_ENABLED, "true");
            assertQuery(fullJoin)
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .failsWith("materialized rows exceed the memory limit [maxBytes=256] (raise cairo.sql.hash.join.value.max.pages)");
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
            final String crossJoin = "SELECT count() FROM m CROSS JOIN (" + sub + ")";
            final int topCount = TestMatchFunctionFactory.getTopCounter();
            try (MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH)) {
                context.setMemoryTracker(tracker);
                try {
                    for (String join : new String[]{
                            crossJoin,
                            "SELECT m.x, s.* FROM m JOIN (" + sub + ") s ON s.c = m.x",
                            "SELECT m.x, s.* FROM m LEFT JOIN (" + sub + ") s ON s.s < m.x",
                            "SELECT m.x, s.* FROM (" + sub + ") s RIGHT JOIN m ON s.s < m.x",
                            "SELECT m.x, s.* FROM m FULL JOIN (" + sub + ") s ON s.s < m.x"
                    }) {
                        assertQuery(join)
                                .withEngine(engine)
                                .withContext(context)
                                .noLeakCheck()
                                .failsWith("query memory limit exceeded [workload=MAT_VIEW_REFRESH");
                        Assert.assertEquals(join, 0, tracker.getUsed());
                    }
                } finally {
                    context.setMemoryTracker(null);
                }
            }
            Assert.assertEquals(topCount, TestMatchFunctionFactory.getTopCounter());
            assertQuery(crossJoin)
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Materialize")
                    .returns("count\n50000\n");
            Assert.assertEquals(topCount, TestMatchFunctionFactory.getTopCounter());
        });
    }

    @Test
    public void testMemoryLimitStreamsExactRows() throws Exception {
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 65_536);
        run((engine, compiler, context) -> {
            createTables(engine, context);
            engine.execute(
                    """
                            CREATE TABLE big AS (
                                SELECT timestamp_sequence('2024-01-01', 1_000_000L) ts, x v FROM long_sequence(10_000)
                            ) TIMESTAMP(ts) PARTITION BY DAY""",
                    context
            );
            final String sub = "SELECT ts, count() c, sum(v) s FROM big GROUP BY ts";
            try (MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.QUERY)) {
                context.setMemoryTracker(tracker);
                try {
                    assertQuery("SELECT count() n, sum(s.c) c, sum(s.s) s, min(s.ts) lo, max(s.ts) hi, "
                            + "sum(s.ts::LONG - 1_704_067_200_000_000) offsets FROM m CROSS JOIN (" + sub + ") s")
                            .withEngine(engine)
                            .withContext(context)
                            .noLeakCheck()
                            .failsWith("query memory limit exceeded [workload=QUERY");
                    Assert.assertEquals(0, tracker.getUsed());
                    assertQuery("SELECT count() n, sum(c) c, sum(s) s, min(ts) lo, max(ts) hi FROM (" + sub + " ORDER BY ts LIMIT -9000)")
                            .withEngine(engine)
                            .withContext(context)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .withPlanContaining("Materialize")
                            .returns("""
                                    n\tc\ts\tlo\thi
                                    9000\t9000\t49504500\t2024-01-01T00:16:40.000000Z\t2024-01-01T02:46:39.000000Z
                                    """);
                    Assert.assertEquals(0, tracker.getUsed());
                } finally {
                    context.setMemoryTracker(null);
                }
            }
        });
    }

    @Test
    public void testMemoryTrackerBalanced() throws Exception {
        // Copies of stream GROUP BY rows ignore the 1024-byte join budget, and every join balances the tracker.
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
    public void testSmallestLightValuePage() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_LIGHT_VALUE_PAGE_SIZE, 12);
        assertCopiesVarSizeRows();
    }

    @Test
    public void testSmallestValuePage() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 8);
        assertCopiesVarSizeRows();
    }

    @Test
    public void testRightJoin() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            assertMaterialized(engine, context, RIGHT_JOIN, null, true, false);
        });
    }

    @Test
    public void testRowsAndOffsetsShareTheBudget() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 4096);
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_MAX_PAGES, 16);
        run((engine, compiler, context) -> {
            createTables(engine, context);
            engine.execute(
                    "CREATE TABLE v AS (SELECT timestamp_sequence('2024-01-01', 1_000_000L) ts, ('k' || x)::VARCHAR k FROM long_sequence(3_000)) TIMESTAMP(ts) PARTITION BY DAY",
                    context
            );
            final String join = "SELECT count() FROM m FULL JOIN (SELECT * FROM v WHERE ts < '%s' UNION ALL SELECT * FROM v WHERE ts < '2024') s ON s.k < 'a'";
            assertQuery(join.formatted("2024-01-01T00:16:40"))
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n1005\n");
            // The full nested loop reads its slave in place, so only the legacy planner copies it.
            assertQuery(join.formatted("2024-01-01T01:00"))
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n3005\n");
            setProperty(PropertyKey.CAIRO_SQL_LEGACY_PLANNER_ENABLED, "true");
            assertQuery(join.formatted("2024-01-01T01:00"))
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .failsWith("materialized rows exceed the memory limit [maxBytes=65536] (raise cairo.sql.hash.join.value.max.pages)");
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
                    Assert.assertEquals(query, 0, assertMaterialized(engine, context, query, withNegativeLimit(SUBQUERIES[i]), null, false, false));
                }
            }
        });
    }

    private void assertCopiesVarSizeRows() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            final String[] subquery = {
                    "SELECT ts, sym::VARCHAR vc, sym::STRING str, count() c FROM t WHERE test_match() SAMPLE BY 1h",
                    "SELECT timestamp_floor('1h', ts) ts, sym::VARCHAR vc, sym::STRING str, count() c FROM t_hash ORDER BY ts, vc",
                    "ts, vc"
            };
            assertMaterialized(engine, context, CROSS_JOIN, subquery, null, true, false);
            assertMaterialized(engine, context, NEGATIVE_LIMIT, subquery, "ts", true, true);
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
        // A copy under the operator runs the stream once and rewinds it less than the input alone does.
        Assert.assertEquals(sql, countRuns(compiler, context, input), countRuns(compiler, context, sql));
        Assert.assertTrue(sql, countRewinds(compiler, context, sql) <= countRewinds(compiler, context, input));
    }

    @Test
    public void testBaseErrorsPropagateAndFactoryReopens() throws Exception {
        assertMemoryLeak(() -> {
            final CountingFactory base = new CountingFactory(select("SELECT x FROM long_sequence(5)"));
            try (RecordCursorFactory factory = materialize(base, 16, 1)) {
                for (int i = 0; i < 2; i++) {
                    final CairoException failure = i == 0
                            ? CairoException.nonCritical().setOutOfMemory(true).put("base getter OOM")
                            : LimitOverflowException.instance().put("base getter page limit");
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        base.getterFailure = failure;
                        Assert.assertSame(failure, Assert.assertThrows(CairoException.class, cursor::hasNext));
                        base.getterFailure = null;
                    }
                }
                for (int i = 0; i < 2; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        if (i == 1) {
                            assertRows(cursor, 1, 3);
                        }
                        base.nextFailure = CairoException.nonCritical().setOutOfMemory(true).put("base next OOM");
                        Assert.assertSame(base.nextFailure, Assert.assertThrows(CairoException.class, cursor::hasNext));
                        base.nextFailure = null;
                    }
                }
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertRows(cursor, 1, 5);
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testCalculateSizeAfterPartialPrefixRewind() throws Exception {
        assertMemoryLeak(() -> {
            final CountingFactory base = new CountingFactory(select("SELECT x FROM long_sequence(5)"));
            try (RecordCursorFactory factory = materialize(base, 16, 1);
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                assertRows(cursor, 1, 2);
                cursor.toTop();
                assertRows(cursor, 1, 1);
                final RecordCursor.Counter count = new RecordCursor.Counter();
                count.set(10);
                cursor.calculateSize(null, count);
                Assert.assertEquals(14, count.get());
                Assert.assertEquals(1, base.calculateSizeCalls);
                Assert.assertFalse(cursor.hasNext());
                cursor.toTop();
                count.clear();
                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), count);
                Assert.assertEquals(5, count.get());
            }
        });
    }

    @Test
    public void testCloseFailureReleasesCache() throws Exception {
        assertMemoryLeak(() -> {
            final CountingFactory base = new CountingFactory(select("SELECT x FROM long_sequence(5)"));
            try (RecordCursorFactory factory = materialize(base, 16, 8)) {
                final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
                final RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                assertRows(cursor, 1, 1);
                Assert.assertTrue(Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN) > before);
                base.closeFailure = new IllegalStateException("base close");
                Assert.assertSame(base.closeFailure, Assert.assertThrows(IllegalStateException.class, cursor::close));
                base.closeFailure = null;
                Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
                try (RecordCursor reopened = factory.getCursor(sqlExecutionContext)) {
                    assertRows(reopened, 1, 5);
                }
            }
        });
    }

    @Test
    public void testExactPageRetryRetainsCache() throws Exception {
        assertMemoryLeak(() -> {
            for (int maxPages : new int[]{3, 8}) {
                try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(maxPages == 3 ? 128 : 48)) {
                    sqlExecutionContext.setMemoryTracker(tracker);
                    final CountingFactory base = new CountingFactory(select("SELECT x FROM long_sequence(5)"));
                    try (RecordCursorFactory factory = materialize(base, 16, maxPages);
                         RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        assertRows(cursor, 1, 5);
                        Assert.assertFalse(cursor.hasNext());
                        Assert.assertEquals(48, tracker.getUsed());
                        final int reads = base.hasNextCalls;
                        cursor.toTop();
                        assertRows(cursor, 1, 5);
                        Assert.assertFalse(cursor.hasNext());
                        Assert.assertEquals(reads, base.hasNextCalls);
                    } finally {
                        sqlExecutionContext.setMemoryTracker(null);
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                }
            }
        });
    }

    @Test
    public void testNoInitialHeadroomAndChangingLimit() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(15)) {
                sqlExecutionContext.setMemoryTracker(tracker);
                final CountingFactory base = new CountingFactory(select("SELECT x FROM long_sequence(5)"));
                try (RecordCursorFactory factory = materialize(base, 16, 8)) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        assertRows(cursor, 1, 1);
                        Assert.assertEquals(0, tracker.getUsed());
                        tracker.setLimit(128);
                        assertRows(cursor, 2, 5);
                        Assert.assertEquals(0, tracker.getUsed());
                        cursor.toTop();
                        Assert.assertEquals(1, base.toTopCalls);
                    }
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        assertRows(cursor, 1, 2);
                        Assert.assertEquals(16, tracker.getUsed());
                        tracker.setLimit(16);
                        assertRows(cursor, 3, 5);
                        Assert.assertEquals(0, tracker.getUsed());
                    }
                } finally {
                    sqlExecutionContext.setMemoryTracker(null);
                }
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testOuterJoinConsumersKeepRecordAcrossFallback() throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 0; i < 2; i++) {
                final GenericRecordMetadata metadata = new GenericRecordMetadata();
                metadata.add(new TableColumnMetadata("m", ColumnType.LONG));
                metadata.add(new TableColumnMetadata("s", ColumnType.LONG));
                final CountingFactory master = new CountingFactory(select("SELECT x FROM long_sequence(3)"));
                final RecordCursorFactory slave = materialize(new CountingFactory(select("SELECT x FROM long_sequence(5)")), 16, 1);
                final BooleanFunction filter = new BooleanFunction() {
                    @Override
                    public boolean getBool(Record record) {
                        return record.getLong(0) > record.getLong(1);
                    }
                };
                try (RecordCursorFactory factory = i == 0
                        ? new NestedLoopLeftJoinRecordCursorFactory(metadata, master, slave, 1, filter,
                        NullRecordFactory.getInstance(slave.getMetadata()))
                        : new NestedLoopFullJoinRecordCursorFactory(configuration, metadata, master, slave, 1, filter,
                        NullRecordFactory.getInstance(master.getMetadata()), NullRecordFactory.getInstance(slave.getMetadata()))) {
                    assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().returns("""
                            m\ts
                            1\tnull
                            2\t1
                            3\t1
                            3\t2
                            """ + (i == 0 ? "" : "null\t3\nnull\t4\nnull\t5\n"));
                }
            }
        });
    }

    @Test
    public void testReplaySkipsAndCountsWithoutBaseWork() throws Exception {
        assertMemoryLeak(() -> {
            final CountingFactory base = new CountingFactory(select("SELECT x FROM long_sequence(5)"));
            try (RecordCursorFactory factory = materialize(base, 16, 8);
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertFalse(factory.recordCursorSupportsRandomAccess());
                Assert.assertThrows(UnsupportedOperationException.class, cursor::getRecordB);
                Assert.assertThrows(UnsupportedOperationException.class, () -> cursor.recordAt(cursor.getRecord(), 0));
                final Record record = cursor.getRecord();
                assertRows(cursor, 1, 5);
                Assert.assertFalse(cursor.hasNext());
                final int reads = base.hasNextCalls;
                cursor.toTop();
                final RecordCursor.Counter count = new RecordCursor.Counter();
                count.set(2);
                cursor.skipRows(count, RecordCursor.UNBOUNDED_ROW_COUNT);
                Assert.assertEquals(0, count.get());
                assertRows(cursor, 3, 3);
                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), count);
                Assert.assertEquals(2, count.get());
                Assert.assertFalse(cursor.hasNext());
                Assert.assertEquals(reads, base.hasNextCalls);
                Assert.assertEquals(0, base.toTopCalls);
                Assert.assertEquals(0, base.calculateSizeCalls);
                Assert.assertSame(record, cursor.getRecord());
                Assert.assertEquals(5, cursor.size());
            }
        });
    }

    @Test
    public void testSkipAcrossFallbackAndEmptyInput() throws Exception {
        assertMemoryLeak(() -> {
            for (int rows : new int[]{0, 5}) {
                final CountingFactory base = new CountingFactory(select("SELECT x FROM long_sequence(" + rows + ")"));
                try (RecordCursorFactory factory = materialize(base, 16, 1);
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final RecordCursor.Counter count = new RecordCursor.Counter();
                    count.set(4);
                    cursor.skipRows(count, RecordCursor.UNBOUNDED_ROW_COUNT);
                    Assert.assertEquals(rows == 0 ? 4 : 0, count.get());
                    if (rows > 0) {
                        Assert.assertEquals(1, base.skipCalls);
                        assertRows(cursor, 5, 5);
                    }
                    Assert.assertFalse(cursor.hasNext());
                    count.set(10);
                    cursor.skipRows(count, RecordCursor.UNBOUNDED_ROW_COUNT);
                    Assert.assertEquals(10, count.get());
                    cursor.toTop();
                    count.clear();
                    cursor.calculateSize(null, count);
                    Assert.assertEquals(rows, count.get());
                }
            }
        });
    }

    @Test
    public void testStreamingAfterPartialPrefixRewind() throws Exception {
        assertMemoryLeak(() -> {
            final CountingFactory base = new CountingFactory(select("SELECT x FROM long_sequence(5)"));
            try (RecordCursorFactory factory = materialize(base, 16, 1);
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
                final Record record = cursor.getRecord();
                assertRows(cursor, 1, 2);
                Assert.assertEquals(before + 16, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
                cursor.toTop();
                assertRows(cursor, 1, 3);
                Assert.assertSame(record, cursor.getRecord());
                Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
                assertRows(cursor, 4, 5);
                Assert.assertFalse(cursor.hasNext());
                cursor.toTop();
                Assert.assertEquals(1, base.toTopCalls);
                final RecordCursor.Counter count = new RecordCursor.Counter();
                count.set(3);
                cursor.skipRows(count, RecordCursor.UNBOUNDED_ROW_COUNT);
                Assert.assertEquals(1, base.skipCalls);
                Assert.assertEquals(0, count.get());
                assertRows(cursor, 4, 5);
                Assert.assertFalse(cursor.hasNext());
                Assert.assertSame(record, cursor.getRecord());
            }
        });
    }

    @Test
    public void testVariableWidthAuxiliaryHeadroom() throws Exception {
        assertMemoryLeak(() -> {
            for (long limit : new long[]{131_072, 139_264, 270_336}) {
                try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(limit)) {
                    sqlExecutionContext.setMemoryTracker(tracker);
                    final CountingFactory base = new CountingFactory(select("SELECT x, 'a'::STRING s FROM long_sequence(1)"));
                    try (RecordCursorFactory factory = materialize(base, 131_072, 2);
                         RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        assertRows(cursor, 1, 1);
                        Assert.assertEquals("a", cursor.getRecord().getStrA(1).toString());
                        Assert.assertFalse(cursor.hasNext());
                        Assert.assertEquals(limit < 139_264 ? 0 : 139_264, tracker.getUsed());
                        cursor.toTop();
                        assertRows(cursor, 1, 1);
                        Assert.assertEquals(limit < 139_264 ? 1 : 0, base.toTopCalls);
                    } finally {
                        sqlExecutionContext.setMemoryTracker(null);
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                }
            }
        });
    }

    @Test
    public void testSymbolsAndNullsAcrossFallback() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE symbols (x LONG, s SYMBOL)");
            execute("INSERT INTO symbols VALUES (1, 'a'), (2, null), (3, 'b')");
            final CountingFactory base = new CountingFactory(select("SELECT x, s FROM symbols"));
            try (RecordCursorFactory factory = materialize(base, 16, 1);
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                for (int pass = 0; pass < 2; pass++) {
                    for (int row = 1; row <= 3; row++) {
                        assertRows(cursor, row, row);
                        final String expected = row == 1 ? "a" : row == 2 ? null : "b";
                        final CharSequence symbol = record.getSymA(1);
                        Assert.assertEquals(expected, symbol == null ? null : symbol.toString());
                        final CharSequence symbolB = record.getSymB(1);
                        Assert.assertEquals(expected, symbolB == null ? null : symbolB.toString());
                        TestUtils.assertEquals(expected, cursor.getSymbolTable(1).valueOf(record.getInt(1)));
                        TestUtils.assertEquals(expected, cursor.newSymbolTable(1).valueOf(record.getInt(1)));
                        Assert.assertSame(record, cursor.getRecord());
                    }
                    Assert.assertFalse(cursor.hasNext());
                    cursor.toTop();
                }
            }
        });
    }

    @Test
    public void testWideValuesStreamWithoutCopying() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<String> expressions = new ObjList<>(
                    "rpad('v', 200_000, 'v')::STRING",
                    "rpad('v', 400_000, 'v')::VARCHAR",
                    "array_build(1, 50_000, 1.0)"
            );
            for (int i = 0; i < expressions.size(); i++) {
                final String expression = expressions.getQuick(i);
                final CountingFactory base = new CountingFactory(select("SELECT x, " + expression + " v FROM long_sequence(3)"));
                try (RecordCursorFactory factory = materialize(base, 131_072, 2);
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
                    for (int pass = 0; pass < 2; pass++) {
                        for (int row = 1; row <= 3; row++) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(row, record.getLong(0));
                            final int type = factory.getMetadata().getColumnType(1);
                            switch (ColumnType.tagOf(type)) {
                                case ColumnType.STRING -> Assert.assertEquals(200_000, record.getStrLen(1));
                                case ColumnType.VARCHAR -> Assert.assertEquals(400_000, record.getVarcharSize(1));
                                case ColumnType.ARRAY ->
                                        Assert.assertEquals(50_000, record.getArray(1, type).getFlatViewLength());
                                default -> Assert.fail("unexpected type");
                            }
                            Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
                            Assert.assertSame(record, cursor.getRecord());
                        }
                        Assert.assertFalse(cursor.hasNext());
                        cursor.toTop();
                    }
                    Assert.assertEquals(2, base.toTopCalls);
                }
            }
        });
    }

    @Test
    public void testCrossJoinConsumerSkipsAcrossFallback() throws Exception {
        assertMemoryLeak(() -> {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("m", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("s", ColumnType.LONG));
            final CountingFactory master = new CountingFactory(select("SELECT x FROM long_sequence(2)"));
            final RecordCursorFactory slave = materialize(new CountingFactory(select("SELECT x FROM long_sequence(5)")), 16, 1);
            try (RecordCursorFactory factory = new CrossJoinRecordCursorFactory(metadata, master, slave, 1)) {
                for (int pass = 0; pass < 2; pass++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final Record record = cursor.getRecord();
                        final RecordCursor.Counter count = new RecordCursor.Counter();
                        // A fresh skip calculates the slave size; a partial skip advances its current scan.
                        if (pass == 1) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(1, record.getLong(0));
                            Assert.assertEquals(1, record.getLong(1));
                        }
                        count.set(pass == 0 ? 4 : 3);
                        cursor.skipRows(count, RecordCursor.UNBOUNDED_ROW_COUNT);
                        Assert.assertEquals(0, count.get());
                        TestUtils.assertCursor("""
                                m\ts
                                1\t5
                                2\t1
                                2\t2
                                2\t3
                                2\t4
                                2\t5
                                """, cursor, metadata, true, sink);
                        Assert.assertSame(record, cursor.getRecord());
                        cursor.toTop();
                        TestUtils.assertCursor("""
                                m\ts
                                1\t1
                                1\t2
                                1\t3
                                1\t4
                                1\t5
                                2\t1
                                2\t2
                                2\t3
                                2\t4
                                2\t5
                                """, cursor, metadata, true, sink);
                    }
                }
            }
        });
    }

    @Test
    public void testFullJoinEmptyMasterAcrossFallback() throws Exception {
        assertMemoryLeak(() -> {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("m", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("s", ColumnType.LONG));
            final CountingFactory master = new CountingFactory(select("SELECT x FROM long_sequence(0)"));
            final RecordCursorFactory slave = materialize(new CountingFactory(select("SELECT x FROM long_sequence(5)")), 16, 1);
            try (RecordCursorFactory factory = new NestedLoopFullJoinRecordCursorFactory(
                    configuration, metadata, master, slave, 1, BooleanConstant.TRUE,
                    NullRecordFactory.getInstance(master.getMetadata()), NullRecordFactory.getInstance(slave.getMetadata())
            )) {
                assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().returns("""
                        m\ts
                        null\t1
                        null\t2
                        null\t3
                        null\t4
                        null\t5
                        """);
            }
        });
    }

    @Test
    public void testWrappedMaterializerKeepsJoinRecordAcrossFallback() throws Exception {
        assertMemoryLeak(() -> {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("m", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("s", ColumnType.LONG));
            final CountingFactory master = new CountingFactory(select("SELECT x FROM long_sequence(2)"));
            final RecordCursorFactory slave = new CountingFactory(materialize(
                    new CountingFactory(select("SELECT x FROM long_sequence(5)")), 16, 1
            ));
            try (RecordCursorFactory factory = new CrossJoinRecordCursorFactory(metadata, master, slave, 1)) {
                assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns("""
                        m\ts
                        1\t1
                        1\t2
                        1\t3
                        1\t4
                        1\t5
                        2\t1
                        2\t2
                        2\t3
                        2\t4
                        2\t5
                        """);
            }
        });
    }

    private static void assertRows(RecordCursor cursor, int lo, int hi) {
        for (int row = lo; row <= hi; row++) {
            Assert.assertTrue(cursor.hasNext());
            Assert.assertEquals(row, cursor.getRecord().getLong(0));
        }
    }

    private static RecordCursorFactory materialize(RecordCursorFactory base, int pageSize, int maxPages) {
        final EntityColumnFilter filter = new EntityColumnFilter();
        filter.of(base.getMetadata().getColumnCount());
        return new MaterializedRecordCursorFactory(new CairoConfigurationWrapper(configuration) {
            @Override
            public int getSqlHashJoinLightValuePageSize() {
                return pageSize;
            }

            @Override
            public int getSqlHashJoinValueMaxPages() {
                return maxPages;
            }

            @Override
            public int getSqlHashJoinValuePageSize() {
                return pageSize;
            }
        }, base, RecordSinkFactory.getInstance(configuration, new BytecodeAssembler(), base.getMetadata(), filter), false, true, false);
    }

    private static int countRuns(SqlCompiler compiler, SqlExecutionContext context, String sql) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
            final int openCount = TestMatchFunctionFactory.getOpenCounter();
            try (RecordCursor cursor = factory.getCursor(context)) {
                while (cursor.hasNext()) {
                    // drain the cursor
                }
            }
            return TestMatchFunctionFactory.getOpenCounter() - openCount;
        }
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

    private static String readRows(RecordCursorFactory factory, SqlExecutionContext context) throws SqlException {
        final StringSink sink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(context)) {
            CursorPrinter.println(cursor, factory.getMetadata(), sink);
        }
        return sink.toString();
    }

    private static RecordCursorFactory strictCopy(
            SqlCompiler compiler,
            SqlExecutionContext context,
            String sql,
            boolean isCopiedInBatches
    ) throws SqlException {
        final RecordCursorFactory base = compiler.compile(sql, context).getRecordCursorFactory();
        final RecordSink recordSink;
        try {
            final EntityColumnFilter columns = new EntityColumnFilter();
            columns.of(base.getMetadata().getColumnCount());
            recordSink = RecordSinkFactory.getInstance(configuration, new BytecodeAssembler(), base.getMetadata(), columns);
        } catch (Throwable th) {
            Misc.free(base);
            throw th;
        }
        return new MaterializedRecordCursorFactory(configuration, base, recordSink, true, false, isCopiedInBatches);
    }

    private static String[] withNegativeLimit(String[] subquery) {
        return new String[]{"SELECT * FROM (" + subquery[0] + ") LIMIT -6", "SELECT * FROM (" + subquery[1] + ") LIMIT -6", subquery[2]};
    }

    private void run(CustomisableRunnable runnable) throws Exception {
        assertMemoryLeak(() -> TestUtils.execute(workerCount == 0 ? null : new TestWorkerPool(workerCount), runnable, configuration, LOG));
    }

    private static class CountingFactory extends AbstractRecordCursorFactory {
        private final RecordCursorFactory base;
        private int calculateSizeCalls;
        private RuntimeException closeFailure;
        private CairoException getterFailure;
        private int hasNextCalls;
        private CairoException nextFailure;
        private int skipCalls;
        private int toTopCalls;

        private CountingFactory(RecordCursorFactory base) {
            super(base.getMetadata());
            this.base = base;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            // Query registration installs its own tracker; these tests supply the tracker directly.
            final RecordCursor cursor = (base instanceof QueryProgress ? base.getBaseFactory() : base).getCursor(executionContext);
            final DelegatingRecord record = new DelegatingRecord() {
                @Override
                public long getLong(int col) {
                    if (getterFailure != null) {
                        throw getterFailure;
                    }
                    return super.getLong(col);
                }
            };
            record.of(cursor.getRecord());
            return new NoRandomAccessRecordCursor() {
                @Override
                public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
                    calculateSizeCalls++;
                    cursor.calculateSize(circuitBreaker, counter);
                }

                @Override
                public void close() {
                    cursor.close();
                    if (closeFailure != null) {
                        throw closeFailure;
                    }
                }

                @Override
                public Record getRecord() {
                    return record;
                }

                @Override
                public SymbolTable getSymbolTable(int columnIndex) {
                    return cursor.getSymbolTable(columnIndex);
                }

                @Override
                public boolean hasNext() {
                    hasNextCalls++;
                    if (nextFailure != null) {
                        throw nextFailure;
                    }
                    return cursor.hasNext();
                }

                @Override
                public SymbolTable newSymbolTable(int columnIndex) {
                    return cursor.newSymbolTable(columnIndex);
                }

                @Override
                public long preComputedStateSize() {
                    return cursor.preComputedStateSize();
                }

                @Override
                public long size() {
                    return cursor.size();
                }

                @Override
                public void skipRows(Counter rowCount, long maxRowsAfterSkip) {
                    skipCalls++;
                    cursor.skipRows(rowCount, maxRowsAfterSkip);
                }

                @Override
                public void toTop() {
                    toTopCalls++;
                    cursor.toTop();
                }
            };
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            base.toPlan(sink);
        }

        @Override
        protected void _close() {
            base.close();
        }
    }
}
