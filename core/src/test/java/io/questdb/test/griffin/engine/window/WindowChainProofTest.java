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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.window.AsyncWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.AsyncWindowStage;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The planner's value proofs under a chained Async Window (see {@code SqlCodeGenerator}): that a
 * column is never negative, never NULL, or a whole number of known magnitude. A false claim does
 * not crash; it gives wrong rows. A running sum claimed non-negative is taken as non-decreasing,
 * which chains a streaming GROUP BY over it and elides the sort above; a group key claimed never
 * NULL is carried across tasks and sorted without its NULL group; a sum claimed exact is carried
 * instead of folded. Every query here runs serially and on the parallel window, and the two must
 * agree bit for bit; where a claim must hold, the plan must show it.
 * <p>
 * CASE comes in two layouts. A searched CASE ({@code CASE WHEN c THEN v ... ELSE e END}) and a
 * switch ({@code CASE x WHEN k THEN v ... END}, which the parser also makes of a searched CASE
 * whose every condition compares one column to a constant) keep their arguments in different
 * orders, so the proofs read the values a CASE returns through
 * {@link io.questdb.griffin.engine.functions.conditional.CaseBranches}, never by position.
 */
public class WindowChainProofTest extends AbstractCairoTest {
    // one key's rows, flags that are 0 or 1, a LONG of 3e9 or 0, over a lag: a chain whose last
    // window carries its running sum over tasks
    private static final String BASE = "WITH w0 AS (SELECT time, sym, price, size, lag(price) OVER (ORDER BY time) lp FROM t WHERE sym = 'A'), " +
            "ch AS (SELECT time, sym, price, size, CASE WHEN price > lp THEN 1 ELSE 0 END AS up, CASE WHEN price < lp THEN 1 ELSE 0 END AS dn, " +
            "CASE WHEN price > lp THEN 3000000000 ELSE 0 END AS big FROM w0) ";
    // one key's rows numbered: a chain of two carrying windows, which keeps keys whole
    private static final String BASE_RN = "WITH ch AS (SELECT time, sym, price, size, row_number() OVER (ORDER BY time) rn FROM t WHERE sym = 'A') ";

    @Test
    public void testCaseFormsNeverNegative() throws Exception {
        smallConfig();
        assertMemoryLeak(() -> {
            createRuns(engine, sqlExecutionContext, 4_000);
            // a negative value in any THEN or ELSE: the running sum goes down, no proof
            final String[][] refused = {
                    // the parser makes switches of these, laid out [then..., else, key]
                    {BASE, "CASE WHEN up = 1 THEN -1 ELSE 1 END"},
                    {BASE, "CASE WHEN up = 1 THEN 1 ELSE -1 END"},
                    {BASE, "CASE up WHEN 1 THEN -1 ELSE 1 END"},
                    {BASE, "CASE WHEN up = 0 THEN 1 WHEN up = 1 THEN -2 ELSE 1 END"},
                    {BASE, "CASE WHEN up = 0 THEN -2 WHEN up = 1 THEN 1 ELSE 1 END"},
                    {BASE_RN, "CASE rn WHEN 2 THEN -3 ELSE 1 END"},
                    {BASE_RN, "CASE WHEN rn = 2 THEN -3 WHEN rn = 5 THEN 2 ELSE 1 END"},
                    {BASE_RN, "CASE WHEN rn = 2 THEN 2 WHEN rn = 5 THEN -3 ELSE 1 END"},
                    // no ELSE: NULL there, which a sum skips, but the THEN values decide
                    {BASE, "CASE up WHEN 0 THEN 1 WHEN 1 THEN -1 END"},
                    {BASE, "CASE WHEN up = 1 THEN -1 END"},
                    // other key types: SYMBOL, DOUBLE, LONG
                    {BASE, "CASE sym WHEN 'A' THEN -1 ELSE 1 END"},
                    {BASE, "CASE sym WHEN 'A' THEN -1 WHEN 'Z' THEN 2 ELSE 1 END"},
                    {BASE, "CASE price WHEN 1.0 THEN -1 ELSE 1 END"},
                    {BASE, "CASE big WHEN 0 THEN 1 ELSE -1 END"},
                    // a searched CASE
                    {BASE, "CASE WHEN up > 0 THEN -1 ELSE 1 END"},
                    {BASE, "CASE WHEN up > 0 AND dn = 0 THEN 1 WHEN up > 0 THEN -2 ELSE 1 END"},
                    // nested, the negative value inside
                    {BASE, "CASE WHEN up = 1 THEN CASE WHEN dn = 0 THEN -1 ELSE 1 END ELSE 0 END"},
                    {BASE, "CASE WHEN up = 1 THEN 1 ELSE CASE dn WHEN 1 THEN -5 ELSE 2 END END"},
                    {BASE, "CASE WHEN up > 0 THEN CASE up WHEN 1 THEN -1 ELSE 1 END ELSE 1 END"},
                    // casts: of a negative value, and a narrowing one that wraps 3e9 below zero
                    {BASE, "CASE WHEN up = 1 THEN (-1)::long ELSE 1 END"},
                    {BASE, "CASE up WHEN 1 THEN -1 ELSE 10000000000 END"},
                    {BASE, "big::int"},
                    {BASE, "CASE WHEN up = 1 THEN big::int ELSE 0 END"},
            };
            for (String[] value : refused) {
                final String grouped = grouped(value);
                assertMatchesSerial(grouped);
                assertMatchesSerial(grouped + " ORDER BY s");
                Assert.assertNotEquals(value[1], AsyncWindowStage.KIND_GROUP_BY, lastStage(grouped));
            }
        });
    }

    @Test
    public void testCaseFormsNeverNull() throws Exception {
        smallConfig();
        assertMemoryLeak(() -> {
            createRuns(engine, sqlExecutionContext, 4_000);
            // values never negative, but NULL somewhere: GROUP BY chains, but its key may be NULL
            final String[][] nullable = {
                    {BASE, "CASE WHEN up = 1 THEN NULL ELSE 0 END"},
                    {BASE, "CASE WHEN up = 1 THEN 0 ELSE NULL END"},
                    {BASE, "CASE up WHEN 1 THEN NULL ELSE 1 END"},
                    {BASE, "CASE WHEN up = 0 THEN 2 WHEN up = 1 THEN NULL ELSE 1 END"},
                    {BASE_RN, "CASE rn WHEN 1 THEN NULL ELSE 1 END"},
                    {BASE_RN, "CASE WHEN rn = 1 THEN NULL WHEN rn = 9 THEN 2 ELSE 1 END"},
                    // no ELSE, of either layout
                    {BASE, "CASE up WHEN 1 THEN 1 END"},
                    {BASE, "CASE WHEN up = 1 THEN 1 END"},
                    {BASE, "CASE WHEN up > 0 THEN 1 END"},
                    {BASE, "CASE sym WHEN 'Z' THEN 1 END"},
                    {BASE, "CASE price WHEN 1.0 THEN NULL ELSE 1 END"},
                    {BASE, "CASE sym WHEN 'A' THEN NULL ELSE 1 END"},
                    {BASE, "CASE big WHEN 0 THEN 1 END"},
                    // nested and cast
                    {BASE, "CASE WHEN up = 1 THEN CASE WHEN dn = 0 THEN NULL ELSE 1 END ELSE 1 END"},
                    {BASE, "CASE WHEN up = 1 THEN CASE dn WHEN 1 THEN 2 END ELSE 1 END"},
                    {BASE, "CASE WHEN up = 1 THEN NULL::long ELSE 1 END"},
            };
            for (String[] value : nullable) {
                final String grouped = grouped(value);
                final String ordered = grouped + " ORDER BY s";
                assertMatchesSerial(grouped);
                assertMatchesSerial(ordered);
                // NULL groups sort first: the sort stays, and the group key is not carried
                Assert.assertFalse(value[1], isSortElided(ordered));
                Assert.assertFalse(value[1], Chars.contains(plan(grouped), "running carry"));
            }
        });
    }

    @Test
    public void testCaseFormsProven() throws Exception {
        smallConfig();
        assertMemoryLeak(() -> {
            createRuns(engine, sqlExecutionContext, 4_000);
            // never negative and never NULL, of every layout: the proofs must hold, or the
            // reader has been replaced by one that refuses everything
            final String[][] proven = {
                    {BASE, "CASE WHEN up > 0 AND dn = 0 THEN 1 ELSE 0 END"},
                    {BASE, "CASE WHEN up = 1 THEN 2 ELSE 1 END"},
                    {BASE, "CASE up WHEN 0 THEN 1 WHEN 1 THEN 0 ELSE 3 END"},
                    {BASE, "CASE WHEN up = 0 THEN 4 WHEN up = 1 THEN 0 ELSE 3 END"},
                    {BASE, "CASE sym WHEN 'A' THEN 1 ELSE 0 END"},
                    {BASE, "CASE sym WHEN 'A' THEN 1 WHEN 'Z' THEN 2 ELSE 0 END"},
                    {BASE, "CASE price WHEN 1.0 THEN 2 ELSE 1 END"},
                    {BASE, "CASE big WHEN 0 THEN 1 ELSE 2 END"},
                    {BASE, "CASE WHEN up = 1 THEN CASE WHEN dn = 0 THEN 4 ELSE 1 END ELSE 0 END"},
                    {BASE, "CASE WHEN up > 0 THEN CASE up WHEN 1 THEN 1 ELSE 2 END ELSE 1 END"},
                    {BASE, "CASE WHEN up = 1 THEN 1::long ELSE 0 END"},
                    {BASE, "CASE up WHEN 1 THEN 1 ELSE 3 END::long"},
                    {BASE, "up"},
                    {BASE, "up::long"},
                    {BASE_RN, "CASE rn WHEN 2 THEN 5 ELSE 1 END"},
                    {BASE_RN, "CASE WHEN rn = 1 THEN 0 WHEN rn = 9 THEN 2 ELSE 1 END"},
            };
            for (String[] value : proven) {
                final String grouped = grouped(value);
                final String ordered = grouped + " ORDER BY s";
                assertMatchesSerial(grouped);
                assertMatchesSerial(ordered);
                Assert.assertEquals(value[1], AsyncWindowStage.KIND_GROUP_BY, lastStage(grouped));
                Assert.assertTrue(value[1] + "\n" + plan(ordered), isSortElided(ordered));
                if (BASE.equals(value[0])) {
                    final String plan = plan(grouped);
                    Assert.assertTrue(value[1] + "\n" + plan, Chars.contains(plan, "keySplit: warmup 1 rows, running carry"));
                }
            }
        });
    }

    // The reviewer's probes at the default configuration: keys longer than a task, a table large
    // enough for min.rows. D5d: a +1/-1 running position; D5e: a carried group key that goes
    // NULL, then 0, in a task that continues the key; D5b: a NULL head group sorted.
    @Test
    public void testCaseSwitchAtDefaultConfig() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (time timestamp, sym symbol index, price double, size double) timestamp(time) partition by DAY");
            execute("insert into t select (x * 1_000_000L)::timestamp, case when x % 5 = 0 then 'Z' else 'A' end, ((x / 3) % 5)::double, (x % 9)::double from long_sequence(1000000)");
            final String flag = "WITH w0 AS (SELECT time, price, size, lag(price) OVER (ORDER BY time) lp FROM t WHERE sym = 'A'), " +
                    "ch AS (SELECT time, price, size, CASE WHEN price > lp THEN 1 ELSE 0 END AS up FROM w0) ";
            final String rn = "WITH w1 AS (SELECT time, size, row_number() OVER (ORDER BY time) rn FROM t WHERE sym = 'A') ";
            // D5d
            assertMatchesSerial(flag + ", runs AS (SELECT time, size, sum(CASE WHEN up = 1 THEN -1 ELSE 1 END) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) pos FROM ch) SELECT pos, count() c, sum(size) z FROM runs");
            // D5e
            assertMatchesSerial(flag + ", runs AS (SELECT time, size, sum(CASE WHEN up = 1 THEN NULL ELSE 0 END) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) seg FROM ch) SELECT seg, count() c, sum(size) z FROM runs");
            // D5b
            assertMatchesSerial(rn + ", w2 AS (SELECT time, size, sum(CASE WHEN rn = 1 THEN NULL ELSE 1 END) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s FROM w1) SELECT s, count() c, sum(size) z FROM w2 ORDER BY s");
            // controls: the negative value in the ELSE; a searched CASE (no equality); both refused
            assertMatchesSerial(flag + ", runs AS (SELECT time, size, sum(CASE WHEN up = 1 THEN 1 ELSE -1 END) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) pos FROM ch) SELECT pos, count() c, sum(size) z FROM runs");
            assertMatchesSerial(flag + ", runs AS (SELECT time, size, sum(CASE WHEN up > 0 THEN -1 ELSE 1 END) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) pos FROM ch) SELECT pos, count() c, sum(size) z FROM runs");
            assertMatchesSerial(rn + ", w2 AS (SELECT time, size, sum(CASE WHEN rn = 2 THEN -3 ELSE 1 END) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s FROM w1) SELECT s, count() c, sum(size) z FROM w2");
            // and the TAQ shape still chains its GROUP BY, its key carried
            final String taq = flag + ", runs AS (SELECT time, size, sum(up) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) g FROM ch) SELECT g, count() c, sum(size) z FROM runs ORDER BY g";
            assertMatchesSerial(taq);
            Assert.assertEquals(AsyncWindowStage.KIND_GROUP_BY, lastStage(taq));
        });
    }

    @Test
    public void testCaseSwitchOnWorkerPool() throws Exception {
        smallConfig();
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            createRuns(engine, ctx, 4_000);
            for (String[] value : new String[][]{{BASE, "CASE WHEN up = 1 THEN -1 ELSE 1 END"}, {BASE, "CASE WHEN up = 1 THEN NULL ELSE 0 END"}, {BASE_RN, "CASE rn WHEN 1 THEN NULL ELSE 1 END"}}) {
                assertMatchesSerial(engine, ctx, grouped(value));
                assertMatchesSerial(engine, ctx, grouped(value) + " ORDER BY s");
            }
        }));
    }

    @Test
    public void testCaseSwitchOverManyKeys() throws Exception {
        smallConfig();
        assertMemoryLeak(() -> {
            createRuns(engine, sqlExecutionContext, 4_000);
            final String src = "WITH w0 AS (SELECT sym, time, price, size, lag(price) OVER (PARTITION BY sym ORDER BY time) lp FROM t WHERE sym IN ('A', 'Z')), " +
                    "ch AS (SELECT sym, time, price, size, CASE WHEN price > lp THEN 1 ELSE 0 END AS up FROM w0) ";
            for (String value : new String[]{"CASE WHEN up = 1 THEN -1 ELSE 1 END", "CASE up WHEN 1 THEN NULL ELSE 0 END", "CASE WHEN up = 1 THEN 2 ELSE 1 END"}) {
                assertMatchesSerial(src + ", runs AS (SELECT sym, time, size, sum(" + value + ") OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) pos FROM ch) " +
                        "SELECT sym, pos, count() c, sum(size) z FROM runs ORDER BY sym, pos");
            }
        });
    }

    // the sort over the query's GROUP BY is elided: the Async Window is the plan's top
    private static boolean isSortElided(String query) throws Exception {
        return plan(query).startsWith("QUERY PLAN\nAsync Window");
    }

    private static String runs(String[] baseAndValue) {
        // the base's window column is read above, so that its windows stay: a value of constants
        // alone reads none of them
        return baseAndValue[0] + ", runs AS (SELECT time, size, " + baseColumn(baseAndValue) + ", sum(" + baseAndValue[1] + ") OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s FROM ch) ";
    }

    // the query's GROUP BY over the running sum s
    private static String grouped(String[] baseAndValue) {
        return runs(baseAndValue) + "SELECT s, count() c, sum(size) z, sum(" + baseColumn(baseAndValue) + ") u FROM runs";
    }

    private static String baseColumn(String[] baseAndValue) {
        return BASE_RN.equals(baseAndValue[0]) ? "rn" : "up";
    }

    private static void assertMatchesSerial(String query) throws Exception {
        assertMatchesSerial(engine, sqlExecutionContext, query);
    }

    // Serial, then parallel twice (toTop() between) and once more with a second cursor: all equal, bit for bit.
    private static void assertMatchesSerial(CairoEngine engine, SqlExecutionContext ctx, String query) throws Exception {
        ctx.setParallelWindowEnabled(false);
        final String expected;
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                expected = rawRows(cursor, factory.getMetadata());
            }
        }
        ctx.setParallelWindowEnabled(true);
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            for (int pass = 0; pass < 2; pass++) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertEquals(query, expected, rawRows(cursor, factory.getMetadata()));
                    cursor.toTop();
                    TestUtils.assertEquals(query, expected, rawRows(cursor, factory.getMetadata()));
                }
            }
        } finally {
            ctx.setParallelWindowEnabled(false);
        }
    }

    /**
     * Table {@code t}: key A holds 4 rows in 5, Z the rest; the price steps through 0..4 in runs of
     * three rows, so that it goes up, down and stays; sizes vary.
     */
    private static void createRuns(CairoEngine engine, SqlExecutionContext ctx, int rows) throws Exception {
        engine.execute("create table t (time timestamp, sym symbol index, price double, size double) timestamp(time) partition by DAY", ctx);
        engine.execute(
                "insert into t select (x * 1_000_000_000L)::timestamp, case when x % 5 = 0 then 'Z' else 'A' end, ((x / 3) % 5)::double, (x % 9)::double" +
                        " from long_sequence(" + rows + ")",
                ctx
        );
    }

    private static AsyncWindowRecordCursorFactory findAsyncFactory(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof AsyncWindowRecordCursorFactory async) {
                return async;
            }
        }
        throw new AssertionError("no Async Window in the factory tree");
    }

    private static void inPool(PoolTest test) throws Exception {
        final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
        TestUtils.execute(pool, (engine, compiler, context) -> {
            final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) context;
            ctx.changePageFrameSizes(1, 64);
            test.run(engine, ctx);
        }, configuration, LOG);
    }

    // The kind of the Async Window's last step, -1 without steps, -2 without an Async Window
    // below the plan's top.
    private static int lastStage(String query) throws Exception {
        sqlExecutionContext.setParallelWindowEnabled(true);
        try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
            for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
                if (f instanceof AsyncWindowRecordCursorFactory async) {
                    final ObjList<AsyncWindowStage> stages = async.getStages();
                    return stages.size() > 0 ? stages.getLast().getKind() : -1;
                }
            }
            return -2;
        } finally {
            sqlExecutionContext.setParallelWindowEnabled(false);
        }
    }

    private static String plan(String query) throws Exception {
        sqlExecutionContext.setParallelWindowEnabled(true);
        try {
            final StringSink plan = new StringSink();
            engine.print("explain " + query, plan, sqlExecutionContext);
            return plan.toString();
        } finally {
            sqlExecutionContext.setParallelWindowEnabled(false);
        }
    }

    // Every value of every row as raw bits: doubles by their bits, symbols by value.
    private static String rawRows(RecordCursor cursor, RecordMetadata metadata) {
        final StringSink sink = new StringSink();
        final Record record = cursor.getRecord();
        final int n = metadata.getColumnCount();
        while (cursor.hasNext()) {
            for (int c = 0; c < n; c++) {
                if (c > 0) {
                    sink.put('\t');
                }
                switch (ColumnType.tagOf(metadata.getColumnType(c))) {
                    case ColumnType.DOUBLE -> sink.put(Double.doubleToRawLongBits(record.getDouble(c)));
                    case ColumnType.FLOAT -> sink.put(Float.floatToRawIntBits(record.getFloat(c)));
                    case ColumnType.INT -> sink.put(record.getInt(c));
                    case ColumnType.LONG -> sink.put(record.getLong(c));
                    case ColumnType.TIMESTAMP -> sink.put(record.getTimestamp(c));
                    case ColumnType.SYMBOL -> sink.put(record.getSymA(c));
                    default -> throw new AssertionError("type " + ColumnType.nameOf(metadata.getColumnType(c)));
                }
            }
            sink.put('\n');
        }
        return sink.toString();
    }

    private static void smallConfig() {
        // keys longer than a task, tasks of 50 rows, a prefix of 100
        sqlExecutionContext.changePageFrameSizes(1, 64);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 50);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 400);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, 100);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, 200);
    }

    @FunctionalInterface
    private interface PoolTest {
        void run(CairoEngine engine, SqlExecutionContextImpl ctx) throws Exception;
    }
}
