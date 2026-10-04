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

package io.questdb.test.griffin.engine.window;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.window.AsyncWindowMinMaxFilterRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@code WHERE x = min|max(x) OVER (PARTITION BY k...)} plans as a parallel aggregation of the
 * partition keys followed by a parallel lookup filter ({@code Async Window Min/Max Filter}). Its
 * output must be the serial plan's (a cached window and a filter), row for row and value for value:
 * every test runs each query with the rewrite switched off and on and compares the two, through a
 * fresh compile, a rewind and a second execution of the same factory.
 * <p>
 * The page frames are shrunk to 64 rows, so that each partition spans many frames, and so many
 * workers' maps.
 */
public class WindowMinMaxFilterTest extends AbstractCairoTest {
    private static final String C = "ts, ex, sym, v, size, price, l, d, tt, x";

    @Override
    public void tearDown() throws Exception {
        AsyncWindowMinMaxFilterRecordCursorFactory.DEBUG_MAX_DENSE_SLOTS = -1;
        super.tearDown();
    }

    @Override
    public void setUp() {
        super.setUp();
        sqlExecutionContext.changePageFrameSizes(1, 64);
    }

    @Test
    public void testAggregateOverRewrite() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 3_000);
            assertSameAsSerial("select count(*), sum(price), min(x), max(x) from (" + taq70() + ")", true);
            assertSameAsSerial("select ex, count(*) from (" + taq59() + ") order by ex", true);
        });
    }

    @Test
    public void testAllNullPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table n as (select timestamp_sequence(0, 100000000) ts, rnd_symbol('A','B','C') k, " +
                    "case when x % 3 = 0 then null else x::double end p, " +
                    "case when x % 3 = 0 then null else x end l, x from long_sequence(600)) timestamp(ts) partition by day");
            // partition C: every value is NULL, so the window reads NULL there; NULL = NULL is true
            execute("update n set p = null, l = null where k = 'C'");
            assertSameAsSerial("select ts, k, p, x from (select ts, k, p, x, min(p) over (partition by k) mn from n) where p = mn", true);
            assertSameAsSerial("select ts, k, p, x, mn from (select ts, k, p, x, min(p) over (partition by k) mn from n) where mn is null", true);
            assertSameAsSerial("select ts, k, l, x, mx from (select ts, k, l, x, max(l) over (partition by k) mx from n) where l = mx or mx = null", true);
        });
    }

    @Test
    public void testCancelBeforeTheAggregation() throws Exception {
        assertCancellation(false);
    }

    @Test
    public void testCancelBeforeTheFilter() throws Exception {
        assertCancellation(true);
    }

    @Test
    public void testDenseAndMapLookupsAgree() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 5_000);
            // SYMBOL keys look up a dense array; a zero slot budget sends them to the map
            for (long budget : new long[]{-1, 0, 50}) {
                AsyncWindowMinMaxFilterRecordCursorFactory.DEBUG_MAX_DENSE_SLOTS = budget;
                for (String query : new String[]{taq56(), taq59(), taq60(), taq70(),
                        "select " + C + ", mn, mx from (select " + C + ", min(tt) over (partition by ex, sym) mn, max(l) over (partition by ex, sym) mx from t) where tt = mn or l = mx"}) {
                    assertSameAsSerial(query, true);
                    sqlExecutionContext.setParallelWindowMinMaxRewriteEnabled(true);
                    try (RecordCursorFactory factory = select(query)) {
                        final AsyncWindowMinMaxFilterRecordCursorFactory minMax = find(factory);
                        Assert.assertNotNull(minMax);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            print(cursor, factory);
                        }
                        // ex has 3 symbols and NULL, sym 40: 4 x 41 slots
                        final boolean dense = budget < 0 || (budget == 50 && !query.contains("partition by ex, sym"));
                        Assert.assertEquals(query + ", budget " + budget, dense ? 1 : 0, minMax.getDenseBuildCount());
                    }
                }
            }
        });
    }

    @Test
    public void testEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 0);
            assertSameAsSerialMaybeEmpty(taq59());
            assertSameAsSerialMaybeEmpty(taq70() + " limit -3");
        });
    }

    @Test
    public void testExplain() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 10);
            assertPlan(
                    taq59(),
                    """
                            SelectedRecord
                                Async Window Min/Max Filter workers: 1
                                  filter: size=min_size
                                  windows: [min(size) over (partition by [ex,sym])]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """
            );
            assertPlan(
                    "select " + C + ", mn, mx from (select " + C + ", min(price) over (partition by sym, v) mn, " +
                            "max(price) over (partition by sym, v) mx from t) where price = mn or price = mx limit 4",
                    """
                            Limit value: 4 skip-rows-max: 0 take-rows-max: 4
                                Async Window Min/Max Filter workers: 1
                                  filter: (price=mn or price=mx)
                                  windows: [min(price) over (partition by [sym,v]),max(price) over (partition by [sym,v])]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """
            );
            // switched off: the serial plan
            sqlExecutionContext.setParallelWindowMinMaxRewriteEnabled(false);
            assertPlan(
                    taq59(),
                    """
                            SelectedRecord
                                Filter filter: size=min_size
                                    CachedWindowLight
                                      unorderedFunctions: [min(size) over (partition by [ex,sym])]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testFilterShapes() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 3_000);
            final String w = "(select " + C + ", min(price) over (partition by sym) mn, max(price) over (partition by sym) mx from t)";
            assertSameAsSerial("select " + C + " from " + w + " where price = mn and size > 1", true);
            assertSameAsSerial("select " + C + " from " + w + " where price <= mn + 1", true);
            assertSameAsSerial("select " + C + " from " + w + " where price != mn", true);
            assertSameAsSerial("select " + C + ", mn, mx from " + w + " where price = mn or price = mx", true);
            assertSameAsSerial("select " + C + ", mx - mn r from " + w + " where mx - mn > 3 and ex = 'A'", true);
            // only the window column projected
            assertSameAsSerial("select mn, mx from " + w + " where price = mx", true);
        });
    }

    @Test
    public void testHugePartitionCount() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 20_000);
            // one partition per row, and nearly one per row
            assertSameAsSerial("select " + C + " from (select " + C + ", max(price) over (partition by x) mx from t) where price = mx", true);
            assertSameAsSerial("select " + C + " from (select " + C + ", min(size) over (partition by tt, ex) mn from t) where size = mn", true);
        });
    }

    @Test
    public void testIneligibleShapesStaySerial() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 2_000);
            // a cumulative window
            assertSameAsSerial("select " + C + " from (select " + C + ", min(price) over (partition by sym order by ts) mn from t) where price = mn", false);
            // another window function next to it
            assertSameAsSerial("select " + C + " from (select " + C + ", min(price) over (partition by sym) mn, " +
                    "row_number() over (partition by sym) rn from t) where price = mn and rn > 1", false);
            // two different partition keys
            assertSameAsSerial("select " + C + " from (select " + C + ", min(price) over (partition by sym) mn, " +
                    "max(price) over (partition by ex) mx from t) where price = mn or price = mx", false);
            // an argument that is not a column
            assertSameAsSerial("select " + C + " from (select " + C + ", min(price * 2) over (partition by sym) mn from t) where price * 2 = mn", false);
            // the whole result set
            assertSameAsSerial("select " + C + " from (select " + C + ", min(price) over () mn from t) where price = mn", false);
            // a filter before the window: its base has no page frames
            assertSameAsSerial("select " + C + " from (select " + C + ", min(price) over (partition by sym) mn from t where size > 1) where price = mn", false);
            // an INT argument, which the window reads widened
            assertSameAsSerial("select ts, k, x from (select ts, sym k, x, i, min(i) over (partition by sym) mn from t) where i = mn", false);
            // switched off
            sqlExecutionContext.setParallelWindowMinMaxRewriteEnabled(false);
            assertSameAsSerial(taq59(), false, false);
        });
    }

    @Test
    public void testLimit() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 3_000);
            assertSameAsSerial(taq70() + " limit 5", true);
            assertSameAsSerial(taq70() + " limit -3", true);
            assertSameAsSerial(taq70() + " limit 3, 7", true);
            assertSameAsSerial(taq70() + " limit 100000", true);
            assertSameAsSerial(taq60() + " limit 10", true);
        });
    }

    @Test
    public void testNearTiesReplayInScanOrder() throws Exception {
        assertMemoryLeak(() -> {
            // DOUBLE min compares with a 1e-10 tolerance: the window keeps the first of two values
            // the tolerance cannot tell apart, so its value depends on scan order
            execute("create table nt (ts timestamp, k symbol, p double, x long) timestamp(ts) partition by day");
            final double base = 1.0;
            final double[] ps = {
                    // A: three values, each within the tolerance of the next, smallest last
                    base + 1.2e-10, base + 0.6e-10, base, 5,
                    // B: smallest first
                    base, base + 0.6e-10, base + 1.2e-10, 5,
                    // C: zeroes of both signs
                    0.0, -0.0, 3, 0.0,
                    // D: no near tie
                    2, 1, 3, 1,
                    // E: infinities and NaN, which the window skips, and a near tie
                    Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN, 7 + 0.5e-10,
                    7, 7 + 0.5e-10, Double.NEGATIVE_INFINITY, 9,
                    // F: zeroes only, the negative one first
                    -0.0, 0.0, -0.0
            };
            final String[] ks = {"A", "A", "A", "A", "B", "B", "B", "B", "C", "C", "C", "C", "D", "D", "D", "D", "E", "E", "E", "E", "E", "E", "E", "E", "F", "F", "F"};
            // bind variables keep every value exact: a cast of 'Infinity' or an overflow reads NULL
            for (int i = 0; i < ps.length; i++) {
                bindVariableService.clear();
                bindVariableService.setTimestamp(0, i * 3_600_000_000L);
                bindVariableService.setStr(1, ks[i]);
                bindVariableService.setDouble(2, ps[i]);
                bindVariableService.setLong(3, i);
                execute("insert into nt values ($1, $2, $3, $4)");
            }
            bindVariableService.clear();
            // the infinities are stored: they compare greater than zero, which NULL does not
            final StringSink inf = new StringSink();
            printSql("select count() from nt where k = 'E' and (p > 0 or p < 0)", inf);
            TestUtils.assertEquals("count\n7\n", inf);
            final String query = "select ts, k, p, x, mn from (select ts, k, p, x, min(p) over (partition by k) mn from nt) where p = mn";
            final String q2 = "select ts, k, p, x, mn from (select ts, k, p, x, min(p) over (partition by k) mn from nt) where p > mn";
            final String q3 = "select ts, k, p, x, mn from (select ts, k, p, x, min(p) over (partition by k) mn from nt order by ts desc) where p >= mn";
            for (String q : new String[]{query, q2, q3}) {
                assertSameAsSerial(q, true);
                sqlExecutionContext.setParallelWindowMinMaxRewriteEnabled(true);
                try (RecordCursorFactory factory = select(q)) {
                    final AsyncWindowMinMaxFilterRecordCursorFactory minMax = find(factory);
                    Assert.assertNotNull(minMax);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        print(cursor, factory);
                    }
                    // A, B, C, E and F
                    Assert.assertEquals(1, minMax.getReplayRunCount());
                    Assert.assertEquals(5, minMax.getReplayedKeyCount());
                }
            }
            // max never replays: it orders by Double.compare, which has no tolerance
            final String max = "select ts, k, p, x, mx from (select ts, k, p, x, max(p) over (partition by k) mx from nt) where p = mx";
            assertSameAsSerial(max, true);
            try (RecordCursorFactory factory = select(max)) {
                final AsyncWindowMinMaxFilterRecordCursorFactory minMax = find(factory);
                Assert.assertNotNull(minMax);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    print(cursor, factory);
                }
                Assert.assertEquals(0, minMax.getReplayRunCount());
            }
        });
    }

    @Test
    public void testMergeKeepsTheTwoSmallestDistinctValues() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 10);
            try (
                    RecordCursorFactory factory = select(taq70());
                    io.questdb.griffin.engine.groupby.SimpleMapValue dest = new io.questdb.griffin.engine.groupby.SimpleMapValue(3);
                    io.questdb.griffin.engine.groupby.SimpleMapValue src = new io.questdb.griffin.engine.groupby.SimpleMapValue(3)
            ) {
                final AsyncWindowMinMaxFilterRecordCursorFactory minMax = find(factory);
                Assert.assertNotNull(minMax);
                final double[][] cases = {
                        // dest min, dest next, src min, src next, merged min, merged next
                        {1.0, Double.NaN, 1.0, 1.00000000005, 1.0, 1.00000000005},
                        {2.0, 3.0, 1.0, 2.5, 1.0, 2.0},
                        {1.0, 4.0, 2.0, 3.0, 1.0, 2.0},
                        {-0.0, Double.NaN, 0.0, Double.NaN, -0.0, 0.0},
                        {0.0, Double.NaN, -0.0, 5.0, -0.0, 0.0},
                        {1.0, 2.0, Double.NaN, Double.NaN, 1.0, 2.0},
                };
                for (double[] c : cases) {
                    dest.putDouble(0, c[0]);
                    dest.putDouble(1, c[1]);
                    dest.putDouble(2, Double.NaN);
                    src.putDouble(0, c[2]);
                    src.putDouble(1, c[3]);
                    src.putDouble(2, Double.NaN);
                    minMax.mergeForTesting(dest, src);
                    Assert.assertEquals(Double.doubleToRawLongBits(c[4]), Double.doubleToRawLongBits(dest.getDouble(0)));
                    Assert.assertEquals(Double.doubleToRawLongBits(c[5]), Double.doubleToRawLongBits(dest.getDouble(1)));
                }
            }
        });
    }

    @Test
    public void testNearTiesOnWorkerPool() throws Exception {
        // near ties spread over many frames, so that the workers' maps each see some of them and
        // the merge has to keep the two smallest distinct values of every partition
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            engine.execute("create table nt as (select timestamp_sequence(0, 10000000) ts, (x % 7)::symbol k, " +
                    "case when x % 7 = 0 then (case when x % 2 = 0 then 0.0 else -1 * 0.0 end) " +
                    "when x % 7 = 1 then 3.0 + x " +
                    "else 1.0 + (4 - (x / 700) % 5) * 4e-11 + (x % 7) end p, x " +
                    "from long_sequence(20000)) timestamp(ts) partition by day", ctx);
            final String query = "select ts, k, p, x, mn from (select ts, k, p, x, min(p) over (partition by k) mn from nt) where p = mn";
            final String expected = serial(engine, ctx, query);
            Assert.assertTrue(expected.length() > 100);
            ctx.setParallelWindowMinMaxRewriteEnabled(true);
            for (int run = 0; run < 3; run++) {
                try (RecordCursorFactory factory = engine.select(query, ctx)) {
                    final AsyncWindowMinMaxFilterRecordCursorFactory minMax = find(factory);
                    Assert.assertNotNull(minMax);
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        TestUtils.assertEquals(query, expected, print(cursor, factory));
                    }
                    // keys 0 and 2..6
                    Assert.assertEquals(6, minMax.getReplayedKeyCount());
                }
            }
        }));
    }

    @Test
    public void testOnWorkerPool() throws Exception {
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            createT(engine, ctx, 30_000);
            for (String query : new String[]{taq56(), taq59(), taq60(), taq70(), taq70() + " limit -7",
                    "select " + C + " from (select " + C + ", max(price) over (partition by x) mx from t) where price = mx"}) {
                final String expected = serial(engine, ctx, query);
                ctx.setParallelWindowMinMaxRewriteEnabled(true);
                for (int run = 0; run < 3; run++) {
                    try (RecordCursorFactory factory = engine.select(query, ctx)) {
                        Assert.assertNotNull(query, find(factory));
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            TestUtils.assertEquals(query, expected, print(cursor, factory));
                        }
                    }
                }
            }
        }));
    }

    @Test
    public void testParquetPartitions() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 3_000);
            execute("alter table t convert partition to parquet list '1970-01-01', '1970-01-03'");
            assertSameAsSerial(taq59(), true);
            assertSameAsSerial(taq60(), true);
            assertSameAsSerial(taq70() + " limit -5", true);
        });
    }

    @Test
    public void testRecordBAndRecordAt() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 3_000);
            final String query = "select " + C + ", mn from (select " + C + ", min(price) over (partition by sym) mn from t) where price = mn";
            assertSameAsSerial(query, true);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue(factory.recordCursorSupportsRandomAccess());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final io.questdb.std.LongList rowIds = new io.questdb.std.LongList();
                    final StringSink forward = new StringSink();
                    final io.questdb.cairo.sql.Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        rowIds.add(record.getRowId());
                        forward.put(record.getSymA(2)).put(',').put(record.getDouble(5)).put(',').put(record.getLong(9))
                                .put(',').put(record.getDouble(10)).put('\n');
                    }
                    Assert.assertTrue(rowIds.size() > 10);
                    // read back through recordB, in reverse, with recordA parked on the first row
                    cursor.recordAt(record, rowIds.getQuick(0));
                    final io.questdb.cairo.sql.Record recordB = cursor.getRecordB();
                    final String[] lines = forward.toString().split("\n");
                    for (int i = rowIds.size() - 1; i >= 0; i--) {
                        cursor.recordAt(recordB, rowIds.getQuick(i));
                        final StringSink line = new StringSink();
                        line.put(recordB.getSymA(2)).put(',').put(recordB.getDouble(5)).put(',').put(recordB.getLong(9))
                                .put(',').put(recordB.getDouble(10));
                        TestUtils.assertEquals(lines[i], line);
                    }
                    final StringSink first = new StringSink();
                    first.put(record.getSymA(2)).put(',').put(record.getDouble(5)).put(',').put(record.getLong(9))
                            .put(',').put(record.getDouble(10));
                    TestUtils.assertEquals(lines[0], first);
                }
            }
        });
    }

    @Test
    public void testSinglePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table s as (select timestamp_sequence(0, 1000000) ts, 'A'::symbol k, (x % 7)::double p, x " +
                    "from long_sequence(500)) timestamp(ts) partition by day");
            assertSameAsSerial("select ts, k, p, x from (select ts, k, p, x, min(p) over (partition by k) mn from s) where p = mn", true);
            assertSameAsSerial("select ts, k, p, x from (select ts, k, p, x, max(p) over (partition by k) mx from s) where p = mx", true);
        });
    }

    @Test
    public void testTaqShapes() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 5_000);
            assertSameAsSerial(taq56(), true);
            assertSameAsSerial(taq59(), true);
            assertSameAsSerial(taq60(), true);
            assertSameAsSerial(taq70(), true);
        });
    }

    @Test
    public void testTypesAndKeys() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 3_000);
            // LONG, DATE and TIMESTAMP arguments; VARCHAR, SYMBOL and LONG keys, NULL keys included
            assertSameAsSerial("select " + C + " from (select " + C + ", min(l) over (partition by v, ex) mn from t) where l = mn", true);
            assertSameAsSerial("select " + C + " from (select " + C + ", max(l) over (partition by v) mx from t) where l = mx", true);
            assertSameAsSerial("select " + C + " from (select " + C + ", max(d) over (partition by sym) mx from t) where d = mx", true);
            assertSameAsSerial("select " + C + " from (select " + C + ", min(tt) over (partition by ex, v) mn from t) where tt = mn", true);
            assertSameAsSerial("select " + C + " from (select " + C + ", max(size) over (partition by l) mx from t) where size = mx", true);
            assertSameAsSerial("select " + C + " from (select " + C + ", min(price) over (partition by size) mn from t) where price = mn", true);
            // aliased base columns
            assertSameAsSerial("select a, b, c from (select ex a, sym b, price c, max(price) over (partition by ex, sym) m from t) where c = m", true);
            // the window's own ORDER BY and an interval scan
            assertSameAsSerial("select " + C + " from (select " + C + ", min(price) over (partition by sym) mn from t where ts in '1970-01-02') where price = mn", true);
            assertSameAsSerial("select " + C + " from (select " + C + ", min(price) over (partition by sym) mn from t order by ts desc) where price = mn", true);
        });
    }

    private static void createT(CairoEngine engine, SqlExecutionContext ctx, int rows) throws Exception {
        engine.execute(
                "create table t as (select" +
                        " timestamp_sequence(0, 100000000) ts," +
                        " rnd_symbol('A', 'B', 'C', null) ex," +
                        " rnd_symbol(40, 1, 3, 5) sym," +
                        " rnd_varchar('p', 'q', 'r', null) v," +
                        " case when x % 37 = 0 then null else ((x * 7919) % 13)::float / 4 end size," +
                        " case when x % 41 = 0 then null else ((x * 31) % 17) / 3.0 end price," +
                        " case when x % 29 = 0 then null else (x * 13) % 23 end l," +
                        " case when x % 31 = 0 then null else ((x * 17) % 19 * 86400000)::date end d," +
                        " case when x % 43 = 0 then null else ((x * 11) % 7 * 1000000)::timestamp end tt," +
                        " (x % 5)::int i," +
                        " x" +
                        " from long_sequence(" + rows + ")) timestamp(ts) partition by day",
                ctx
        );
    }

    private static AsyncWindowMinMaxFilterRecordCursorFactory find(RecordCursorFactory factory) {
        while (factory != null) {
            if (factory instanceof AsyncWindowMinMaxFilterRecordCursorFactory minMax) {
                return minMax;
            }
            factory = factory.getBaseFactory();
        }
        return null;
    }

    private static String print(RecordCursor cursor, RecordCursorFactory factory) {
        final StringSink sink = new StringSink();
        CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
        return sink.toString();
    }

    private static String run(CairoEngine engine, SqlExecutionContext ctx, String query, boolean expectRewrite, boolean checkFactory) throws Exception {
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            if (checkFactory) {
                Assert.assertEquals(query, expectRewrite, find(factory) != null);
            }
            final String first;
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                first = print(cursor, factory);
                cursor.toTop();
                TestUtils.assertEquals(query, first, print(cursor, factory));
            }
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                TestUtils.assertEquals(query, first, print(cursor, factory));
            }
            return first;
        }
    }

    private static String serial(CairoEngine engine, SqlExecutionContext ctx, String query) throws Exception {
        ctx.setParallelWindowMinMaxRewriteEnabled(false);
        return run(engine, ctx, query, false, true);
    }

    private static String taq56() {
        return "select " + C + " from (select " + C + " from (select " + C + ", max(size) over (partition by ex) max_size from t) where size = max_size)";
    }

    private static String taq59() {
        return "select " + C + " from (select " + C + " from (select " + C + ", min(size) over (partition by ex, sym) min_size from t) where size = min_size)";
    }

    private static String taq60() {
        return "select " + C + " from (select " + C + " from (select " + C + ", min(size) over (partition by ex, sym) min_size from t) where size = min_size order by sym)";
    }

    private static String taq70() {
        return "select " + C + " from (select " + C + " from (select " + C + ", min(price) over (partition by sym) min_price from t) where price = min_price)";
    }

    private void assertSameAsSerial(String query, boolean expectRewrite) throws Exception {
        assertSameAsSerial(query, expectRewrite, true, false);
    }

    private void assertSameAsSerial(String query, boolean expectRewrite, boolean switchOn) throws Exception {
        assertSameAsSerial(query, expectRewrite, switchOn, false);
    }

    private void assertSameAsSerial(String query, boolean expectRewrite, boolean switchOn, boolean allowEmpty) throws Exception {
        final String expected = serial(engine, sqlExecutionContext, query);
        sqlExecutionContext.setParallelWindowMinMaxRewriteEnabled(switchOn);
        final String actual = run(engine, sqlExecutionContext, query, expectRewrite, true);
        TestUtils.assertEquals(query, expected, actual);
        // header plus at least one row, so that the comparison is not vacuous
        if (!allowEmpty) {
            Assert.assertTrue(query + " returned no rows", expected.indexOf('\n') < expected.length() - 1);
        }
    }

    private void assertCancellation(boolean afterAggregation) throws Exception {
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            createT(engine, ctx, 20_000);
            final String query = taq70();
            final String expected = serial(engine, ctx, query);
            final NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                    engine,
                    new DefaultSqlExecutionCircuitBreakerConfiguration()
            );
            try {
                ctx.with(ctx.getSecurityContext(), ctx.getBindVariableService(), ctx.getRandom(), ctx.getRequestFd(), circuitBreaker);
                ctx.setParallelWindowMinMaxRewriteEnabled(true);
                try (RecordCursorFactory factory = engine.select(query, ctx)) {
                    final AsyncWindowMinMaxFilterRecordCursorFactory minMax = find(factory);
                    Assert.assertNotNull(minMax);
                    if (!afterAggregation) {
                        circuitBreaker.cancel();
                    }
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        // the aggregation has run; no frame of the filter has been dispatched
                        circuitBreaker.cancel();
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                        }
                        Assert.fail("cancelled query ran to completion");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage(), e.isCancellation());
                    }
                    circuitBreaker.clearCancelSentinel();
                    circuitBreaker.resetTimer();
                    // the factory still produces the serial result
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        TestUtils.assertEquals(expected, print(cursor, factory));
                    }
                    Assert.assertEquals(0, minMax.getAcquiredSlotCount());
                }
            } finally {
                Misc.free(circuitBreaker);
            }
        }));
    }

    private void assertPlan(String query, String expected) throws Exception {
        final StringSink plan = new StringSink();
        printSql("explain " + query, plan);
        TestUtils.assertEquals(query, "QUERY PLAN\n" + expected, plan);
    }

    private void assertSameAsSerialMaybeEmpty(String query) throws Exception {
        assertSameAsSerial(query, true, true, true);
    }

    private void inPool(PoolTest test) throws Exception {
        final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
        TestUtils.execute(pool, (engine, compiler, context) -> {
            final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) context;
            ctx.changePageFrameSizes(1, 64);
            test.run(engine, ctx);
        }, configuration, LOG);
    }

    @FunctionalInterface
    private interface PoolTest {
        void run(CairoEngine engine, SqlExecutionContextImpl ctx) throws Exception;
    }
}
