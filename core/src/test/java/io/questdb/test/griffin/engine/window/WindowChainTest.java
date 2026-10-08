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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.window.AsyncWindowAtom;
import io.questdb.griffin.engine.window.AsyncWindowRecordCursor;
import io.questdb.griffin.engine.window.AsyncWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.AsyncWindowSplitPlan;
import io.questdb.griffin.engine.window.AsyncWindowStage;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Window chains of the NYSE TAQ benchmark on the shared workers: a window over one key, a running
 * sum, a whole-table lag with a filter over it, and price-change runs (lag, a CASE, a running sum
 * of it, then GROUP BY the run). The planner walks the scan key by key (a single key; an IN list
 * or the whole table when an ORDER BY above makes the order of the keys invisible), and chains the
 * projections, windows, filters and GROUP BY over the Async Window as steps its workers compute.
 * <p>
 * Every query runs with the parallel window switched off, which is the serial plan, and on; the
 * two must agree bit for bit, except the sums and averages of a plan that splits a key over tasks,
 * which may differ in their last bits, as documented for the parallel window. The tests also check
 * the plan they mean to test: the steps, the key-major scans, and that the sort over the GROUP BY
 * is elided where the groups come out in its order.
 */
@RunWith(Parameterized.class)
public class WindowChainTest extends AbstractCairoTest {
    private static final String CH_EXPR = "CASE WHEN (price IS NULL) != (lag(price) OVER (%s) IS NULL) OR price != lag(price) OVER (%s) THEN 1 ELSE 0 END";
    private static final long MAX_KEY_ROWS = 400;
    private static final long MIN_ROWS = 100;
    private static final long ROUND_ROWS = 200;
    private static final long TASK_ROWS = 50;
    private final String indexType;

    public WindowChainTest(String indexType) {
        this.indexType = indexType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"bitmap"},
                {"posting"},
        });
    }

    // idx 48: a 20-row rolling average over one key
    public static String q48(String sym) {
        return "SELECT time, avg(price) OVER (ORDER BY time ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS price FROM trade WHERE sym = '" + sym + "'";
    }

    // idx 52: a running sum over one key
    public static String q52(String sym) {
        return "SELECT time, sum(size) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS size FROM trade WHERE sym = '" + sym + "'";
    }

    // idx 55: a running sum over a filtered table
    public static String q55(String ex) {
        return "SELECT time, sum(size) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS size FROM trade WHERE ex = '" + ex + "'";
    }

    // idx 61, Index text: the lag inside an expression, filtered on it
    public static String q61Index() {
        return "SELECT sym, seq AS seqDecr FROM (SELECT sym, seq, seq - lag(seq) OVER (PARTITION BY sym ORDER BY time) AS seq_delta FROM trade) WHERE seq_delta < 0 ORDER BY sym, seqDecr";
    }

    // idx 61, Manual Opt text
    public static String q61ManualOpt() {
        return "SELECT sym, seq AS seqDecr FROM (SELECT sym, seq, lag(seq) OVER (PARTITION BY sym ORDER BY time) AS prev_seq FROM trade) WHERE seq < prev_seq ORDER BY sym, seqDecr";
    }

    // idx 72: price-change runs of one key
    public static String q72(String sym) {
        final String w = "ORDER BY time";
        return "WITH src AS (SELECT time, price, size FROM trade WHERE sym = '" + sym + "'), " +
                "ch AS (SELECT time, price, size, " + String.format(CH_EXPR, w, w) + " AS price_changed FROM src), " +
                "runs AS (SELECT time, price, size, sum(price_changed) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pricegroup FROM ch) " +
                "SELECT pricegroup, first(price) AS price, min(time) AS FirstTime, max(time) AS LastTime, count() AS cnt, sum(size::double) AS size FROM runs ORDER BY pricegroup";
    }

    // idx 73 and the Index text of 74: price-change runs of every key of a list
    public static String q73(String in) {
        final String w = "PARTITION BY sym ORDER BY time";
        return "WITH src AS (SELECT sym, time, price, size FROM trade WHERE sym IN (" + in + ")), " +
                "ch AS (SELECT sym, time, price, size, " + String.format(CH_EXPR, w, w) + " AS price_changed FROM src), " +
                "runs AS (SELECT sym, time, price, size, sum(price_changed) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pricegroup FROM ch) " +
                "SELECT sym, pricegroup, first(price) AS price, min(time) AS FirstTime, max(time) AS LastTime, count() AS cnt, sum(size::double) AS size FROM runs ORDER BY sym, pricegroup";
    }

    // idx 74, Manual Opt text: the scan ordered by the key, the windows without ORDER BY
    public static String q74ManualOpt(String in) {
        final String w = "PARTITION BY sym";
        return "WITH src AS (SELECT sym, time, price, size FROM trade WHERE sym IN (" + in + ") ORDER BY sym), " +
                "ch AS (SELECT sym, time, price, size, " + String.format(CH_EXPR, w, w) + " AS price_changed FROM src), " +
                "runs AS (SELECT sym, time, price, size, sum(price_changed) OVER (PARTITION BY sym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pricegroup FROM ch) " +
                "SELECT sym, pricegroup, first(price) AS price, min(time) AS FirstTime, max(time) AS LastTime, count() AS cnt, sum(size::double) AS size FROM runs ORDER BY sym, pricegroup";
    }

    @Override
    public void setUp() {
        super.setUp();
        // 64 rows per page frame: every key spans many frames
        sqlExecutionContext.changePageFrameSizes(1, 64);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, TASK_ROWS);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, MAX_KEY_ROWS);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, MIN_ROWS);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, ROUND_ROWS);
    }

    @Test
    public void testChainSwitchedOff() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_CHAIN_ENABLED, "false");
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            for (String query : new String[]{q61Index(), q61ManualOpt(), q72("BIG"), q73(allKeys()), q74ManualOpt(allKeys())}) {
                assertMatchesSerial(engine, sqlExecutionContext, query, null);
                sqlExecutionContext.setParallelWindowEnabled(true);
                final StringSink plan = new StringSink();
                engine.print("explain " + query, plan, sqlExecutionContext);
                sqlExecutionContext.setParallelWindowEnabled(false);
                Assert.assertFalse(query + "\n" + plan, Chars.contains(plan, " then: "));
            }
        });
    }

    @Test
    public void testExplain() throws Exception {
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            sqlExecutionContext.setParallelWindowEnabled(true);
            final String[] queries = {q48("BIG"), q52("BIG"), q61Index(), q61ManualOpt(), q72("BIG"), q73("'K1', 'BIG'"), q74ManualOpt("'K1', 'BIG'")};
            final String[] expected = {
                    "QUERY PLAN\n"
                            + "Async Window workers: 1\n"
                            + "  functions: [avg(price) over ( rows between 19 preceding and current row)]\n"
                            + "  keyShards: sym\n"
                            + "  keySplit: frame replayed\n"
                            + "    FilterOnValues symbolOrder: asc\n"
                            + "      keyMajor: true\n"
                            + "        Cursor-order scan\n"
                            + "            Index forward scan on: sym\n"
                            + "              filter: sym=1\n"
                            + "        Frame forward scan on: trade\n",
                    "QUERY PLAN\n"
                            + "Async Window workers: 1\n"
                            + "  functions: [sum(size) over (rows between unbounded preceding and current row)]\n"
                            + "  keyShards: sym\n"
                            + "  keySplit: running carry, folded\n"
                            + "    FilterOnValues symbolOrder: asc\n"
                            + "      keyMajor: true\n"
                            + "        Cursor-order scan\n"
                            + "            Index forward scan on: sym\n"
                            + "              filter: sym=1\n"
                            + "        Frame forward scan on: trade\n",
                    "QUERY PLAN\n"
                            + "Encode sort\n"
                            + "  keys: [sym, seqDecr]\n"
                            + "    SelectedRecord\n"
                            + "        Async Window workers: 1\n"
                            + "          functions: [lag(seq, 1, NULL) over (partition by [sym])]\n"
                            + "          keyShards: sym\n"
                            + "          keySplit: warmup 1 rows\n"
                            + "          then: project\n"
                            + "          functions: [sym,seq,seq-lag]\n"
                            + "          then: filter\n"
                            + "          filter: seq_delta<0\n"
                            + "            SortedSymbolIndex\n"
                            + "              keyMajor: true\n"
                            + "                Index forward scan on: sym\n"
                            + "                  symbolOrder: asc\n"
                            + "                Frame forward scan on: trade\n",
                    "QUERY PLAN\n"
                            + "Encode sort\n"
                            + "  keys: [sym, seqDecr]\n"
                            + "    SelectedRecord\n"
                            + "        Async Window workers: 1\n"
                            + "          functions: [lag(seq, 1, NULL) over (partition by [sym])]\n"
                            + "          keyShards: sym\n"
                            + "          keySplit: warmup 1 rows\n"
                            + "          then: filter\n"
                            + "          filter: seq<prev_seq\n"
                            + "            SortedSymbolIndex\n"
                            + "              keyMajor: true\n"
                            + "                Index forward scan on: sym\n"
                            + "                  symbolOrder: asc\n"
                            + "                Frame forward scan on: trade\n",
                    "QUERY PLAN\n"
                            + "Async Window workers: 1\n"
                            + "  functions: [lag(price, 1, NULL) over ()]\n"
                            + "  keyShards: sym\n"
                            + "  keySplit: warmup 1 rows, running carry\n"
                            + "  then: project\n"
                            + "  functions: [case([(price is null!=lag is null or price!=lag),1,0]),time,price,size]\n"
                            + "  then: window\n"
                            + "  functions: [sum(price_changed) over (rows between unbounded preceding and current row)]\n"
                            + "  then: group by\n"
                            + "  keys: [pricegroup]\n"
                            + "  values: [first(price),min(time),max(time),count(*),sum(size::double)]\n"
                            + "    SelectedRecord\n"
                            + "        FilterOnValues symbolOrder: asc\n"
                            + "          keyMajor: true\n"
                            + "            Cursor-order scan\n"
                            + "                Index forward scan on: sym\n"
                            + "                  filter: sym=1\n"
                            + "            Frame forward scan on: trade\n",
                    "QUERY PLAN\n"
                            + "Async Window workers: 1\n"
                            + "  functions: [lag(price, 1, NULL) over (partition by [sym])]\n"
                            + "  keyShards: sym\n"
                            + "  keySplit: warmup 1 rows, running carry\n"
                            + "  then: project\n"
                            + "  functions: [sym,case([(price is null!=lag is null or price!=lag),1,0]),time,price,size]\n"
                            + "  then: window\n"
                            + "  functions: [sum(price_changed) over (partition by [sym] rows between unbounded preceding and current row)]\n"
                            + "  then: group by\n"
                            + "  keys: [sym,pricegroup]\n"
                            + "  values: [first(price),min(time),max(time),count(*),sum(size::double)]\n"
                            + "    FilterOnValues symbolOrder: asc\n"
                            + "      keyMajor: true\n"
                            + "        Cursor-order scan\n"
                            + "            Index forward scan on: sym\n"
                            + "              filter: sym=1\n"
                            + "            Index forward scan on: sym deferred: true\n"
                            + "              filter: sym='K1'\n"
                            + "        Frame forward scan on: trade\n",
                    "QUERY PLAN\n"
                            + "Async Window workers: 1\n"
                            + "  functions: [lag(price, 1, NULL) over (partition by [sym])]\n"
                            + "  keyShards: sym\n"
                            + "  keySplit: warmup 1 rows, running carry\n"
                            + "  then: project\n"
                            + "  functions: [sym,case([(price is null!=lag is null or price!=lag),1,0]),price,time,size]\n"
                            + "  then: window\n"
                            + "  functions: [sum(price_changed) over (partition by [sym] rows between unbounded preceding and current row)]\n"
                            + "  then: group by\n"
                            + "  keys: [sym,pricegroup]\n"
                            + "  values: [first(price),min(time),max(time),count(*),sum(size::double)]\n"
                            + "    FilterOnValues symbolOrder: asc\n"
                            + "      keyMajor: true\n"
                            + "        Cursor-order scan\n"
                            + "            Index forward scan on: sym\n"
                            + "              filter: sym=1\n"
                            + "            Index forward scan on: sym deferred: true\n"
                            + "              filter: sym='K1'\n"
                            + "        Frame forward scan on: trade\n"
            };
            for (int i = 0; i < queries.length; i++) {
                final StringSink plan = new StringSink();
                engine.print("explain " + queries[i], plan, sqlExecutionContext);
                TestUtils.assertEquals(queries[i], expected[i], plan);
            }
            sqlExecutionContext.setParallelWindowEnabled(false);
        });
    }

    @Test
    public void testIdx48And52SingleKey() throws Exception {
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            for (String sym : new String[]{"BIG", "K3", "NOPE"}) {
                assertMatchesSerial(engine, sqlExecutionContext, q48(sym), null);
                assertMatchesSerial(engine, sqlExecutionContext, q52(sym), null);
            }
            // keys whole: no split, so every value is the serial one, bit for bit
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 100_000);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 100_000);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, 100_000);
            assertMatchesSerial(engine, sqlExecutionContext, q48("BIG"), null);
            assertMatchesSerial(engine, sqlExecutionContext, q52("BIG"), null);
        });
    }

    @Test
    public void testIdx55RowSlices() throws Exception {
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            final String[] queries = {
                    q55("T"),
                    q55("NOPE"),
                    "SELECT time, sum(size) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s," +
                            " count() OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c," +
                            " row_number() OVER () AS rn FROM trade",
                    q55("T") + " LIMIT 13",
                    q55("Q") + " LIMIT -7",
            };
            for (String query : queries) {
                assertMatchesSerial(engine, sqlExecutionContext, query, null);
            }
            sqlExecutionContext.setParallelWindowEnabled(true);
            try (RecordCursorFactory factory = engine.select(q55("T"), sqlExecutionContext)) {
                Assert.assertTrue(findAsyncFactory(factory).isSliceMode());
            } finally {
                sqlExecutionContext.setParallelWindowEnabled(false);
            }
        });
    }

    @Test
    public void testIdx61WholeTable() throws Exception {
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            for (String query : new String[]{q61Index(), q61ManualOpt()}) {
                assertMatchesSerial(engine, sqlExecutionContext, query, AsyncWindowStage.KIND_FILTER);
                assertMatchesSerial(engine, sqlExecutionContext, query + " LIMIT 7", AsyncWindowStage.KIND_FILTER);
                assertMatchesSerial(engine, sqlExecutionContext, query + " LIMIT -3", AsyncWindowStage.KIND_FILTER);
            }
            // without an ORDER BY over it, the order of the rows is the table's: no key-major walk
            final String unordered = "SELECT sym, seq FROM (SELECT sym, seq, lag(seq) OVER (PARTITION BY sym ORDER BY time) AS prev_seq FROM trade) WHERE seq < prev_seq";
            assertMatchesSerial(engine, sqlExecutionContext, unordered, -1);
            // ordered by a column that leaves ties, which a key-major walk would reorder
            final String ties = "SELECT sym, seq, time FROM (SELECT sym, seq, time, lag(seq) OVER (PARTITION BY sym ORDER BY time) AS prev_seq FROM trade) WHERE seq < prev_seq ORDER BY sym";
            assertMatchesSerial(engine, sqlExecutionContext, ties, -1);
        });
    }

    @Test
    public void testIdx72SingleKeyRuns() throws Exception {
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            for (String sym : new String[]{"BIG", "K3", "NOPE"}) {
                assertMatchesSerial(engine, sqlExecutionContext, q72(sym), AsyncWindowStage.KIND_GROUP_BY);
                assertMatchesSerial(engine, sqlExecutionContext, withLast(q72(sym)), AsyncWindowStage.KIND_GROUP_BY);
            }
            // BIG within a task: computed by a task rather than streamed by the query's thread
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 100_000);
            assertMatchesSerial(engine, sqlExecutionContext, q72("BIG"), AsyncWindowStage.KIND_GROUP_BY);
            assertMatchesSerial(engine, sqlExecutionContext, q72("BIG") + " LIMIT 5", AsyncWindowStage.KIND_GROUP_BY);
        });
    }

    @Test
    public void testIdx73And74Runs() throws Exception {
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            final String[] lists = {allKeys(), "'K1', 'K2', 'BIG', 'K7'", "'K5', 'NOPE'", "'K9'", "null, 'K1', 'BIG'", "'K3', null"};
            for (String in : lists) {
                assertMatchesSerial(engine, sqlExecutionContext, q73(in), AsyncWindowStage.KIND_GROUP_BY);
                assertMatchesSerial(engine, sqlExecutionContext, q74ManualOpt(in), AsyncWindowStage.KIND_GROUP_BY);
                assertMatchesSerial(engine, sqlExecutionContext, withLast(q73(in)), AsyncWindowStage.KIND_GROUP_BY);
                assertMatchesSerial(engine, sqlExecutionContext, withLast(q74ManualOpt(in)), AsyncWindowStage.KIND_GROUP_BY);
                assertMatchesSerial(engine, sqlExecutionContext, q73(in) + " LIMIT 11", AsyncWindowStage.KIND_GROUP_BY);
            }
            // whole keys within tasks: BIG computed by a task too
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 100_000);
            assertMatchesSerial(engine, sqlExecutionContext, q73(allKeys()), AsyncWindowStage.KIND_GROUP_BY);
            assertMatchesSerial(engine, sqlExecutionContext, q74ManualOpt(allKeys()), AsyncWindowStage.KIND_GROUP_BY);
        });
    }

    @Test
    public void testGroupsSpanningTasks() throws Exception {
        // runs of one price over hundreds of rows: a group spans many tasks of 50 rows, so tasks
        // that continue it, end it, and hold nothing but it are all met
        assertMemoryLeak(() -> {
            createLongRuns(engine, sqlExecutionContext);
            for (String query : new String[]{
                    q72("R"), q72("S"), q73("'R', 'S', 'T'"), q73("'T'"), q74ManualOpt("'R', 'S', 'T'"),
                    // last() keeps the row with the highest row id: a group's rows from several
                    // tasks must come in rising row ids
                    withLast(q72("R")), withLast(q73("'R', 'S', 'T'")), withLast(q74ManualOpt("'R', 'S', 'T'"))
            }) {
                assertMatchesSerial(engine, sqlExecutionContext, query, AsyncWindowStage.KIND_GROUP_BY);
                sqlExecutionContext.setParallelWindowEnabled(true);
                try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
                    Assert.assertNotEquals(query, AsyncWindowSplitPlan.MODE_NONE, findAsyncFactory(factory).getSplitPlan().getMode());
                } finally {
                    sqlExecutionContext.setParallelWindowEnabled(false);
                }
            }
        });
    }

    @Test
    public void testGroupsSpanningTasksOnWorkerPool() throws Exception {
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            createLongRuns(engine, ctx);
            long workerTasks = 0;
            for (String query : new String[]{q72("R"), q73("'R', 'S', 'T'"), q74ManualOpt("'R', 'S', 'T'"), withLast(q72("R")), withLast(q73("'R', 'S', 'T'"))}) {
                for (int run = 0; run < 3; run++) {
                    workerTasks += assertMatchesSerial(engine, ctx, query, AsyncWindowStage.KIND_GROUP_BY);
                }
            }
            Assert.assertTrue(workerTasks > 0);
        }));
    }

    @Test
    public void testHashShardsWithoutIndex() throws Exception {
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000, false);
            for (String query : new String[]{q61Index(), q61ManualOpt(), q61ManualOpt() + " LIMIT 5", q61Index() + " LIMIT -4"}) {
                assertMatchesSerial(engine, sqlExecutionContext, query, AsyncWindowStage.KIND_FILTER);
                sqlExecutionContext.setParallelWindowEnabled(true);
                try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
                    Assert.assertTrue(query, findAsyncFactory(factory).isShardMode());
                } finally {
                    sqlExecutionContext.setParallelWindowEnabled(false);
                }
            }
            // switched off: the plain scan stays, and so does the serial window
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_SHARD_ENABLED, "false");
            assertMatchesSerial(engine, sqlExecutionContext, q61ManualOpt(), -1);
        });
    }

    @Test
    public void testHashShardsWithoutIndexOnWorkerPool() throws Exception {
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            createTrade(engine, ctx, 20_000, false);
            long workerTasks = 0;
            for (int run = 0; run < 3; run++) {
                workerTasks += assertMatchesSerial(engine, ctx, q61Index(), AsyncWindowStage.KIND_FILTER);
                workerTasks += assertMatchesSerial(engine, ctx, q61ManualOpt(), AsyncWindowStage.KIND_FILTER);
            }
            Assert.assertTrue(workerTasks > 0);
        }));
    }

    @Test
    public void testKeyMajorCopiesWithoutPartitionMaps() throws Exception {
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            // lag cannot compute key runs: the workers' copies drop the PARTITION BY
            assertKeyStartReset(engine, sqlExecutionContext, q61ManualOpt(), true);
            assertKeyStartReset(engine, sqlExecutionContext, q73(allKeys()), true);
            // a bounded average computes key runs, which keep their state in fields already
            assertKeyStartReset(
                    engine,
                    sqlExecutionContext,
                    "SELECT sym, s FROM (SELECT sym, avg(price) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) s FROM trade) ORDER BY sym, s",
                    false
            );
            // key runs switched off: the copies drop the partitions
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_KEY_RUNS_ENABLED, "false");
            assertKeyStartReset(engine, sqlExecutionContext, q61ManualOpt(), false);
        });
    }

    @Test
    public void testOnWorkerPool() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 200);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 5_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, 1_000);
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            createTrade(engine, ctx, 20_000);
            final String[] queries = {q48("BIG"), q52("BIG"), q55("T"), q61Index(), q61ManualOpt(), q72("BIG"), q73(allKeys()), q74ManualOpt(allKeys())};
            long workerTasks = 0;
            for (String query : queries) {
                for (int run = 0; run < 3; run++) {
                    workerTasks += assertMatchesSerial(engine, ctx, query, null);
                }
            }
            Assert.assertTrue(workerTasks > 0);
        }));
    }

    @Test
    public void testCancelMidQuery() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 5_000);
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            createTrade(engine, ctx, 20_000);
            assertCancelMidQuery(engine, ctx, q73(allKeys()));
        }));
    }

    @Test
    public void testCancelMidQueryHashShards() throws Exception {
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            createTrade(engine, ctx, 20_000, false);
            assertCancelMidQuery(engine, ctx, q61Index());
        }));
    }

    @Test
    public void testCancelMidQueryRowSlices() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 200);
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            createTrade(engine, ctx, 20_000);
            assertCancelMidQuery(engine, ctx, q55("T"));
        }));
    }

    @Test
    public void testHashShardsOverParquet() throws Exception {
        // a plan over a plain scan refuses a changed partition format, so only new plans meet Parquet
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 64);
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000, false);
            execute("alter table trade convert partition to parquet list '1970-01-01'");
            for (String query : new String[]{q61Index(), q61ManualOpt(), q55("T"), q61ManualOpt() + " LIMIT 5"}) {
                assertMatchesSerial(engine, sqlExecutionContext, query, null);
            }
        });
    }

    private static void assertCancelMidQuery(CairoEngine engine, SqlExecutionContextImpl ctx, String query) throws Exception {
        final String expected = serial(engine, ctx, query);
        final NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                engine,
                new DefaultSqlExecutionCircuitBreakerConfiguration()
        );
        try {
            ctx.with(ctx.getSecurityContext(), ctx.getBindVariableService(), ctx.getRandom(), ctx.getRequestFd(), circuitBreaker);
            ctx.setParallelWindowEnabled(true);
            try (RecordCursorFactory factory = engine.select(query, ctx)) {
                final AsyncWindowRecordCursorFactory async = findAsyncFactory(factory);
                try (RecordCursor cursor = async.getCursor(ctx)) {
                    for (int i = 0; i < 50; i++) {
                        Assert.assertTrue(cursor.hasNext());
                    }
                    circuitBreaker.cancel();
                    try {
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                        }
                        Assert.fail("cancelled query ran to completion");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage(), e.isCancellation());
                    }
                    circuitBreaker.clearCancelSentinel();
                    circuitBreaker.resetTimer();
                }
                assertSlotsReleased(factory);
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    TestUtils.assertEquals(expected, rawRows(cursor, factory.getMetadata()));
                }
            } finally {
                ctx.setParallelWindowEnabled(false);
            }
        } finally {
            Misc.free(circuitBreaker);
        }
    }

    @Test
    public void testParquetPartition() throws Exception {
        // a Parquet partition keeps the serial plan's scan, or runs serially if planned before
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 64);
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            final String[] queries = {q48("BIG"), q61ManualOpt(), q72("BIG"), q73(allKeys())};
            final ObjList<RecordCursorFactory> cached = new ObjList<>();
            final ObjList<String> expected = new ObjList<>();
            try {
                for (String query : queries) {
                    expected.add(serial(engine, sqlExecutionContext, query));
                    sqlExecutionContext.setParallelWindowEnabled(true);
                    cached.add(engine.select(query, sqlExecutionContext));
                    sqlExecutionContext.setParallelWindowEnabled(false);
                }
                execute("alter table trade convert partition to parquet list '1970-01-01'");
                for (int i = 0; i < queries.length; i++) {
                    // the plans made over native partitions
                    sqlExecutionContext.setParallelWindowEnabled(true);
                    try (RecordCursor cursor = cached.getQuick(i).getCursor(sqlExecutionContext)) {
                        assertRowsMatch(queries[i], cached.getQuick(i), expected.getQuick(i), rawRows(cursor, cached.getQuick(i).getMetadata()));
                    } finally {
                        sqlExecutionContext.setParallelWindowEnabled(false);
                    }
                    // and new plans over a Parquet partition
                    assertMatchesSerial(engine, sqlExecutionContext, queries[i], null);
                }
            } finally {
                Misc.freeObjList(cached);
            }
        });
    }

    @Test
    public void testRerunsAndRewinds() throws Exception {
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            for (String query : new String[]{q61ManualOpt(), q72("BIG"), q73(allKeys())}) {
                final String expected = serial(engine, sqlExecutionContext, query);
                sqlExecutionContext.setParallelWindowEnabled(true);
                try (RecordCursorFactory factory = select(query)) {
                    for (int run = 0; run < 3; run++) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            for (int i = 0; i < 37 && cursor.hasNext(); i++) {
                                // a partial pass
                            }
                            cursor.toTop();
                            assertRowsMatch(query, factory, expected, rawRows(cursor, factory.getMetadata()));
                            cursor.toTop();
                            assertRowsMatch(query, factory, expected, rawRows(cursor, factory.getMetadata()));
                        }
                    }
                    assertSlotsReleased(factory);
                } finally {
                    sqlExecutionContext.setParallelWindowEnabled(false);
                }
            }
        });
    }

    @Test
    public void testTableOrderKeptWhereVisible() throws Exception {
        // each query sees the table's order somewhere, so the key-major walk must not be used
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            final String rn = "SELECT sym, time, row_number() OVER (PARTITION BY sym ORDER BY time) rn FROM trade";
            final String[] queries = {
                    // a GROUP BY without the key: first() reads the earliest row of the table
                    "SELECT rn, first(sym) f, count() c FROM (" + rn + ") ORDER BY rn",
                    // a window above that is not partitioned by the key numbers the rows in table order
                    "SELECT sym, rn, g FROM (SELECT sym, rn, sum(rn) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) g FROM (" + rn + ")) ORDER BY sym, rn, g",
                    // descending orders, which the key-major walk's groups do not come in
                    q73(allKeys()).replace("ORDER BY sym, pricegroup", "ORDER BY sym DESC, pricegroup"),
                    q73(allKeys()).replace("ORDER BY sym, pricegroup", "ORDER BY sym, pricegroup DESC"),
            };
            for (String query : queries) {
                assertMatchesSerial(engine, sqlExecutionContext, query, null);
            }
        });
    }

    @Test
    public void testUnprovenGroupKeysStaySerial() throws Exception {
        assertMemoryLeak(() -> {
            createTrade(engine, sqlExecutionContext, 3_000);
            final String w = "PARTITION BY sym ORDER BY time";
            // a CASE that can be negative: its running sum may come back to an earlier value
            final String negative = "WITH ch AS (SELECT sym, time, price, size, CASE WHEN price > lag(price) OVER (" + w + ") THEN 1 ELSE -1 END AS d FROM trade WHERE sym IN (" + allKeys() + ")), " +
                    "runs AS (SELECT sym, time, price, size, sum(d) OVER (" + w + " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS g FROM ch) " +
                    "SELECT sym, g, count() cnt, sum(size::double) s FROM runs ORDER BY sym, g";
            assertMatchesSerial(engine, sqlExecutionContext, negative, AsyncWindowStage.KIND_WINDOW);
            // a group key that is not the scan's key nor the running sum
            final String byPrice = "WITH runs AS (SELECT sym, time, price, size, sum(size) OVER (" + w + " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS g FROM trade WHERE sym IN (" + allKeys() + ")) " +
                    "SELECT sym, price, count() cnt FROM runs ORDER BY sym, price";
            assertMatchesSerial(engine, sqlExecutionContext, byPrice, null);
        });
    }

    @Test
    public void testRangeFramesOverKeyMajorWalks() throws Exception {
        // a RANGE frame reads the designated timestamp, which a walk of several keys no longer
        // declares: the window must read the walk's own ascending timestamp
        assertMemoryLeak(() -> {
            execute("create table t (time timestamp, sym symbol index type " + indexType + ", v double) timestamp(time) partition by DAY");
            execute("insert into t select ((x / 2) * 1_000_000_000L)::timestamp, 'K' || (x % 5), (x % 17)::double from long_sequence(3000)");
            final String[] queries = {
                    "SELECT sym, time, v, s FROM (SELECT sym, time, v, sum(v) OVER (PARTITION BY sym ORDER BY time RANGE BETWEEN 10 SECONDS PRECEDING AND CURRENT ROW) s FROM t) ORDER BY sym, time, v, s",
                    "SELECT sym, time, v, a, l FROM (SELECT sym, time, v, avg(v) OVER (PARTITION BY sym ORDER BY time RANGE BETWEEN 1 MINUTE PRECEDING AND CURRENT ROW) a, lag(v) OVER (PARTITION BY sym ORDER BY time) l FROM t) ORDER BY sym, time, v, a, l",
                    "SELECT sym, time, v, s FROM (SELECT sym, time, v, sum(v) OVER (PARTITION BY sym ORDER BY time) s FROM t) ORDER BY sym, time, v, s",
                    "SELECT sym, time, v, r, a FROM (SELECT sym, time, v, rank() OVER (PARTITION BY sym ORDER BY time) r, avg(v) OVER (PARTITION BY sym ORDER BY time RANGE BETWEEN 1 MINUTE PRECEDING AND CURRENT ROW) a FROM t) ORDER BY sym, time, v, r, a",
                    "SELECT sym, time, v, a FROM (SELECT sym, time, v, avg(v) OVER (PARTITION BY sym ORDER BY time RANGE BETWEEN 1 MINUTE PRECEDING AND CURRENT ROW) a FROM t WHERE sym IN ('K1', 'K2', 'K3')) ORDER BY sym, time, v, a",
                    "SELECT sym, time, c FROM (SELECT sym, time, count(*) OVER (PARTITION BY sym ORDER BY time RANGE BETWEEN 30 SECONDS PRECEDING AND CURRENT ROW) c FROM t WHERE sym IN ('K1', 'K4')) ORDER BY sym, time, c",
                    "SELECT time, v, c FROM (SELECT time, v, count(*) OVER (ORDER BY time RANGE BETWEEN 1 MINUTE PRECEDING AND CURRENT ROW) c FROM t WHERE sym = 'K1')",
                    // a RANGE frame chained over another window of the walk
                    "SELECT sym, time, l, s FROM (SELECT sym, time, l, sum(l) OVER (PARTITION BY sym ORDER BY time RANGE BETWEEN 20 SECONDS PRECEDING AND CURRENT ROW) s FROM (SELECT sym, time, lag(v) OVER (PARTITION BY sym ORDER BY time) l FROM t)) ORDER BY sym, time, l, s",
                    // a key-major walk under a window that stays serial
                    "SELECT sym, time, v, m FROM (SELECT sym, time, v, min(v) OVER (PARTITION BY sym) m FROM t) WHERE v > m ORDER BY sym, time, v, m",
            };
            for (String query : queries) {
                assertMatchesSerial(engine, sqlExecutionContext, query, null);
            }
            // the walk is key-major, and the window runs on it
            assertAsyncWindow(engine, sqlExecutionContext, queries[0]);
            assertAsyncWindow(engine, sqlExecutionContext, queries[4]);
        });
    }

    @Test
    public void testPartitionFinerThanKeyUnderGroupBy() throws Exception {
        // row_number and a running count partitioned by (key, side) restart within a key: a group
        // of their values is not contiguous along the walk, so a GROUP BY on them is not streamed
        assertMemoryLeak(() -> {
            execute("create table t (time timestamp, sym symbol index type " + indexType + ", side symbol, price double) timestamp(time) partition by DAY");
            execute("insert into t select (x * 1_000_000_000L)::timestamp, case when x % 7 = 0 then 'Z' else 'A' end," +
                    " case when x % 3 = 0 then 'S' else 'B' end, (x % 5)::double from long_sequence(2000)");
            final String w2 = "WITH src AS (SELECT time, sym, side, price FROM t WHERE sym = 'A'), " +
                    "w1 AS (SELECT time, sym, side, price, lag(price) OVER (ORDER BY time) lp FROM src), " +
                    "w1p AS (SELECT time, sym, side, price, lp + 1 AS lp1 FROM w1), " +
                    "w2 AS (SELECT time, sym, side, lp1, row_number() OVER (PARTITION BY sym, side) rn FROM w1p) ";
            final String w3 = w2.replace("row_number() OVER (PARTITION BY sym, side)", "count() OVER (PARTITION BY sym, side ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");
            final String w4 = "WITH src AS (SELECT time, sym, side, price FROM t WHERE sym IN ('A', 'Z') ORDER BY sym), " +
                    "w1 AS (SELECT time, sym, side, price, lag(price) OVER (PARTITION BY sym) lp FROM src), " +
                    "w1p AS (SELECT time, sym, side, price, lp + 1 AS lp1 FROM w1), " +
                    "w2 AS (SELECT time, sym, side, lp1, row_number() OVER (PARTITION BY sym, side) rn FROM w1p) ";
            final String[] queries = {
                    w2 + "SELECT rn, count() c, sum(lp1) s FROM w2",
                    w2 + "SELECT sym, rn, count() c, sum(lp1) s FROM w2",
                    w2 + "SELECT rn, count() c, sum(lp1) s FROM w2 ORDER BY rn",
                    w3 + "SELECT rn, count() c, sum(lp1) s FROM w2",
                    w4 + "SELECT sym, rn, count() c, sum(lp1) s FROM w2 ORDER BY sym, rn",
            };
            for (String query : queries) {
                assertMatchesSerial(engine, sqlExecutionContext, query, null);
            }
            // partitioned by the key alone, the same GROUP BY streams, its groups in order
            assertMatchesSerial(engine, sqlExecutionContext, w2.replace("PARTITION BY sym, side", "PARTITION BY sym") + "SELECT rn, count() c, sum(lp1) s FROM w2 ORDER BY rn", AsyncWindowStage.KIND_GROUP_BY);
        });
    }

    @Test
    public void testCarriedGroupKeyNullThenZero() throws Exception {
        // a CASE without ELSE is NULL between its cases: a task's own running sum of it goes NULL,
        // then 0, both of which the carry turns into the same group key
        assertMemoryLeak(() -> {
            execute("create table t (time timestamp, sym symbol index type " + indexType + ", price double, size double) timestamp(time) partition by DAY");
            execute("insert into t select (x * 1_000_000_000L)::timestamp, case when x % 5 = 0 then 'Z' else 'A' end," +
                    " ((x / 3) % 5)::double, (x % 9)::double from long_sequence(4000)");
            final String w = "ORDER BY time";
            final String q = "WITH src AS (SELECT time, price, size FROM t WHERE sym = 'A'), " +
                    "ch AS (SELECT time, price, size, CASE WHEN price > lag(price) OVER (" + w + ") THEN 1 WHEN price < lag(price) OVER (" + w + ") THEN 0 END AS f FROM src), " +
                    "runs AS (SELECT time, price, size, sum(f) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS seg FROM ch) ";
            final String w2 = "PARTITION BY sym ORDER BY time";
            final String q2 = "WITH src AS (SELECT sym, time, price, size FROM t WHERE sym IN ('A', 'Z')), " +
                    "ch AS (SELECT sym, time, price, size, CASE WHEN price > lag(price) OVER (" + w2 + ") THEN 1 WHEN price < lag(price) OVER (" + w2 + ") THEN 0 END AS f FROM src), " +
                    "runs AS (SELECT sym, time, price, size, sum(f) OVER (" + w2 + " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS seg FROM ch) ";
            final String[] queries = {
                    q + "SELECT seg, count() cnt, sum(size) s, last(size) l FROM runs",
                    q + "SELECT seg, count() cnt, sum(size) s FROM runs ORDER BY seg",
                    q2 + "SELECT sym, seg, count() cnt, sum(size) s FROM runs ORDER BY sym, seg",
                    // with an ELSE the flag is never NULL, and the carry may split the key
                    q.replace("THEN 0 END", "THEN 0 ELSE 0 END") + "SELECT seg, count() cnt, sum(size) s FROM runs",
            };
            for (String query : queries) {
                assertMatchesSerial(engine, sqlExecutionContext, query, null);
            }
        });
    }

    @Test
    public void testNullableDoubleGroupKeyOrder() throws Exception {
        // a running DOUBLE sum whose first rows are NULL starts with a NaN group along the walk,
        // which ORDER BY ranks last: the sort must stay
        assertMemoryLeak(() -> {
            execute("create table t (time timestamp, sym symbol index type " + indexType + ", price double, size double) timestamp(time) partition by DAY");
            execute("insert into t select (x * 1_000_000_000L)::timestamp, 'K' || (x % 4)," +
                    " case when x < 400 then 5.0 else (x / 37)::double end, (x % 9)::double from long_sequence(3000)");
            final String w = "PARTITION BY sym ORDER BY time";
            final String runs = "WITH src AS (SELECT sym, time, price, size FROM t WHERE sym IN ('K0','K1','K2','K3')), w0 AS (SELECT sym, time, price, size, lag(price) OVER (" + w + ") lp FROM src), " +
                    "ch AS (SELECT sym, time, price, size, CASE WHEN price > 10 AND lp > 0 THEN 1 END AS f FROM w0), " +
                    "runs AS (SELECT sym, time, price, size, sum(f) OVER (" + w + " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS seg FROM ch) ";
            final String runs1 = "WITH src AS (SELECT time, price, size FROM t WHERE sym = 'K1'), w0 AS (SELECT time, price, size, lag(price) OVER (ORDER BY time) lp FROM src), " +
                    "ch AS (SELECT time, price, size, CASE WHEN price > 10 AND lp > 0 THEN 1 END AS f FROM w0), " +
                    "runs AS (SELECT time, price, size, sum(f) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS seg FROM ch) ";
            final String runs2 = "WITH ch AS (SELECT time, price, size, CASE WHEN price > 10 THEN 1 END AS f FROM t WHERE sym = 'K1'), " +
                    "runs AS (SELECT time, price, size, sum(f) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS seg FROM (SELECT time, price, size, f, lag(price) OVER (ORDER BY time) lp FROM ch)) ";
            final String[] queries = {
                    runs + "SELECT sym, seg, count() c, sum(size) s FROM runs ORDER BY sym, seg",
                    runs1 + "SELECT seg, count() c, sum(size) s FROM runs ORDER BY seg",
                    runs2 + "SELECT seg, count() c, sum(size) s FROM runs ORDER BY seg",
            };
            for (String query : queries) {
                assertMatchesSerial(engine, sqlExecutionContext, query, null);
            }
        });
    }

    @Test
    public void testStepsOverFoldWithoutPrefix() throws Exception {
        // without a prefix on the query's thread, tasks compute the key from its first row; a
        // step chained over a folded running sum must not see the workers' stand-in for it
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_PREFIX_ROWS, 0);
        assertMemoryLeak(() -> {
            assertStepsOverFold();
            // a gate of no rows has no prefix either
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_PREFIX_ROWS, 16384);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, 0);
            assertStepsOverFold();
        });
    }

    @Test
    public void testInexactDoublesBitExact() throws Exception {
        assertMemoryLeak(() -> assertInexactDoubles(engine, sqlExecutionContext));
    }

    @Test
    public void testInexactDoublesBitExactOnWorkerPool() throws Exception {
        assertMemoryLeak(() -> inPool(this::assertInexactDoubles));
    }

    /**
     * Doubles no sum of which is exact, so that adding them in another order shows: random
     * fractions, spikes of 1e9, pairs of +-1e16 that cancel, -0.0, NULLs, tiny magnitudes; a
     * column with infinities and sums that overflow; and an INT. Every window over them must match
     * the serial one bit for bit, but the running DOUBLE sums a carry adds under PARTITION BY,
     * within the bound of their data.
     */
    private void assertInexactDoubles(CairoEngine engine, SqlExecutionContext ctx) throws Exception {
        final int rows = 6_000;
        engine.execute(
                "create table d (time timestamp, sym symbol index type " + indexType + ", v double, x double, i int)" +
                        " timestamp(time) partition by DAY",
                ctx
        );
        engine.execute(
                "insert into d select" +
                        " ((x / 2) * " + (3 * 86_400_000_000L / rows) + "L)::timestamp," +
                        " case when x % 5 < 2 then 'BIG' when x % 13 = 0 then null else 'K' || (x % 10) end," +
                        " case when x % 37 = 0 then null when x % 31 = 0 then -0.0 when x % 29 = 0 then 1e16 when x % 29 = 1 then -1e16" +
                        " when x % 23 = 0 then 1e-300 when x % 13 = 0 then 1e9 + rnd_double() else rnd_double() * 1000.0 / 7.0 - 50.0 end," +
                        " case when x % 97 = 0 then cast('Infinity' as double) when x % 89 = 0 then cast('-Infinity' as double)" +
                        " when x % 83 = 0 then 1e308 else rnd_double() / 3.0 end," +
                        " (x % 1000 - 500)::int" +
                        " from long_sequence(" + rows + ")",
                ctx
        );
        engine.execute("create table u as (select * from d) timestamp(time) partition by DAY", ctx);
        // n * 2^-52 * the sum of the magnitudes: the bound of a sum's rounding in any order
        final double tolerance = rows * 0x1p-52 * (rows * 1e16);
        final String big = "FROM d WHERE sym = 'BIG'";
        final String[] singleKey = {
                // a bounded frame's avg and sum, replayed (idx 48)
                "SELECT time, avg(v) OVER (ORDER BY time ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) a " + big,
                "SELECT time, sum(v) OVER (ORDER BY time ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) a " + big,
                "SELECT time, sum(v) OVER (ORDER BY time ROWS BETWEEN 5 PRECEDING AND 2 PRECEDING) a " + big,
                "SELECT time, avg(x) OVER (ORDER BY time ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) a, sum(x) OVER (ORDER BY time ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) s " + big,
                // replayed next to warm-up rows
                "SELECT time, avg(v) OVER (ORDER BY time ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) a, lag(v) OVER (ORDER BY time) l " + big,
                // a running sum, folded (idx 52)
                "SELECT time, sum(v) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s " + big,
                "SELECT time, sum(x) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s " + big,
                // a running sum chained over a window and a projection, folded
                "SELECT time, l1, sum(l1) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s FROM (SELECT time, l + 1 l1 FROM (SELECT time, lag(v) OVER (ORDER BY time) l " + big + "))",
                // warm-up rows rebuild these exactly
                "SELECT time, min(v) OVER (ORDER BY time ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) mn, max(v) OVER (ORDER BY time ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) mx " + big,
                "SELECT time, first_value(v) OVER (ORDER BY time ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) f, count(v) OVER (ORDER BY time ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) c " + big,
                "SELECT time, avg(i) OVER (ORDER BY time ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) a, sum(i) OVER (ORDER BY time ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) s " + big,
                // a GROUP BY over runs whose groups span tasks: its sums add in walk order
                "WITH ch AS (SELECT time, v, CASE WHEN v > lag(v) OVER (ORDER BY time) THEN 1 ELSE 0 END AS up " + big + ")," +
                        " runs AS (SELECT time, v, sum(up) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS g FROM ch)" +
                        " SELECT g, count() c, sum(v) s, avg(v) a, first(v) f, last(v) l, min(v) mn, max(v) mx FROM runs ORDER BY g",
        };
        for (String query : singleKey) {
            assertMatchesSerial(engine, ctx, query, null);
        }
        // the plans split the key, so the values are the fold's, the replay's and the warm-up's
        assertSplitOp(engine, ctx, singleKey[0], AsyncWindowSplitPlan.OP_REPLAY);
        assertSplitOp(engine, ctx, singleKey[4], AsyncWindowSplitPlan.OP_REPLAY);
        assertSplitOp(engine, ctx, singleKey[5], AsyncWindowSplitPlan.OP_FOLD);
        assertSplitOp(engine, ctx, singleKey[7], AsyncWindowSplitPlan.OP_FOLD);
        for (String table : new String[]{"d", "u"}) {
            final String[] manyKeys = {
                    // a bounded frame of DOUBLE over several keys keeps its keys whole
                    "SELECT sym, time, a FROM (SELECT sym, time, avg(v) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) a FROM " + table + ") ORDER BY sym, time, a",
                    "SELECT sym, time, a, l FROM (SELECT sym, time, sum(v) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) a, lag(v) OVER (PARTITION BY sym ORDER BY time) l FROM " + table + " WHERE sym IN ('BIG', 'K1', 'K2')) ORDER BY sym, time, a, l",
                    "SELECT sym, time, mn, mx FROM (SELECT sym, time, min(v) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) mn, max(v) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) mx FROM " + table + ") ORDER BY sym, time, mn, mx",
                    "SELECT sym, time, l FROM (SELECT sym, time, lag(v) OVER (PARTITION BY sym ORDER BY time) l FROM " + table + ") WHERE l < 0 ORDER BY sym, time, l",
            };
            for (String query : manyKeys) {
                assertMatchesSerial(engine, ctx, query, null);
            }
            // the one documented exception: a running DOUBLE sum under PARTITION BY
            assertMatchesSerial(
                    engine,
                    ctx,
                    "SELECT sym, time, s FROM (SELECT sym, time, sum(v) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s FROM " + table + " WHERE sym IN ('BIG', 'K1', 'K2')) ORDER BY sym, time",
                    null,
                    tolerance
            );
            // a sum that overflowed stays infinite, also across a carry
            assertMatchesSerial(
                    engine,
                    ctx,
                    "SELECT sym, time, s FROM (SELECT sym, time, sum(x) OVER (PARTITION BY sym ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s FROM " + table + " WHERE sym IN ('BIG', 'K1', 'K2')) ORDER BY sym, time",
                    null,
                    tolerance
            );
        }
        // slices of the whole table, a running sum folded
        assertMatchesSerial(engine, ctx, "SELECT time, sum(v) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s FROM u WHERE i > 0", null);
        assertMatchesSerial(engine, ctx, "SELECT time, sum(x) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s FROM u", null);
        engine.execute("drop table d", ctx);
        engine.execute("drop table u", ctx);
    }

    private void assertSplitOp(CairoEngine engine, SqlExecutionContext ctx, String query, int op) throws Exception {
        ctx.setParallelWindowEnabled(true);
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            final AsyncWindowSplitPlan plan = findAsyncFactory(factory).getSplitPlan();
            boolean found = false;
            for (int i = 0, n = plan.getPrefixCount(); i < n; i++) {
                found |= plan.getPrefixOp(i) == op;
            }
            Assert.assertTrue(query, found);
        } finally {
            ctx.setParallelWindowEnabled(false);
        }
    }

    private void assertStepsOverFold() throws Exception {
        execute("create table t (time timestamp, sym symbol index type " + indexType + ", size double) timestamp(time) partition by DAY");
        execute("insert into t select (x * 1_000_000_000L)::timestamp, case when x % 5 = 0 then 'Z' else 'A' end, (x % 9)::double from long_sequence(300)");
        final String q52 = "SELECT time, size, sum(size) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM t WHERE sym = 'A'";
        final String[] queries = {
                q52,
                "SELECT time, size, s FROM (" + q52 + ") WHERE s > 10",
                "SELECT time, s, lag(s) OVER (ORDER BY time) ls FROM (" + q52 + ")",
                "SELECT time, s + 1 s1 FROM (" + q52 + ")",
                "SELECT size, max(s) m, count() c FROM (" + q52 + ")",
        };
        for (String query : queries) {
            assertMatchesSerial(engine, sqlExecutionContext, query, null);
        }
        execute("drop table t");
    }

    private void assertAsyncWindow(CairoEngine engine, SqlExecutionContext ctx, String query) throws Exception {
        ctx.setParallelWindowEnabled(true);
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            findAsyncFactory(factory);
        } finally {
            ctx.setParallelWindowEnabled(false);
        }
    }

    private void assertKeyStartReset(CairoEngine engine, SqlExecutionContext ctx, String query, boolean expected) throws Exception {
        assertMatchesSerial(engine, ctx, query, null);
        ctx.setParallelWindowEnabled(true);
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            Assert.assertEquals(query, expected, ((AsyncWindowAtom) findAsyncFactory(factory).getAtom()).isKeyStartReset());
        } finally {
            ctx.setParallelWindowEnabled(false);
        }
    }

    /**
     * Table {@code trade} of price runs over hundreds of rows: keys R (half of the rows), S and
     * T, interleaved; the price of each key changes every 400 of its rows, sizes vary, and one
     * size in thirteen is NULL.
     */
    private void createLongRuns(CairoEngine engine, SqlExecutionContext ctx) throws Exception {
        engine.execute(
                "create table trade (time timestamp, sym symbol index type " + indexType + ", ex symbol, price float, size float, seq int)" +
                        " timestamp(time) partition by DAY",
                ctx
        );
        engine.execute(
                "insert into trade select" +
                        " (x * 30_000_000L)::timestamp," +
                        " case when x % 4 < 2 then 'R' when x % 4 = 2 then 'S' else 'T' end," +
                        " 'Q'," +
                        " (100 + (x / 1_600) % 3)::float," +
                        " case when x % 13 = 0 then null else (x % 11 + 0.5)::float end," +
                        " (x % 31)::int" +
                        " from long_sequence(9_000)",
                ctx
        );
    }

    // The query with aggregates that keep a group's last row, which compare row ids, and that
    // skip NULLs.
    private static String withLast(String query) {
        final String first = "first(price) AS price, ";
        Assert.assertTrue(query, query.contains(first));
        return query.replace(first, first + "last(price) AS lp, last_not_null(price) AS lnn, last(size) AS ls, first_not_null(size) AS fnn, last(time) AS lt, ");
    }

    private static String allKeys() {
        final StringSink sink = new StringSink();
        sink.put("'BIG'");
        for (int i = 0; i < 20; i++) {
            sink.put(", 'K").put(i).put('\'');
        }
        return sink.toString();
    }

    private static void assertRowsMatch(String query, RecordCursorFactory factory, String expected, String actual) {
        assertRowsMatch(query, factory, expected, actual, 0);
    }

    /**
     * Compares every value bit for bit, but for the one documented exception: a running DOUBLE
     * sum combined with a carry (OP_ADD) may differ from the serial sum by the rounding of adding
     * its values in another order, at most {@code tolerance}, in its own columns and nowhere else.
     * A test passes the bound of its data, n * 2^-52 * the sum of the values' magnitudes, or 0
     * when no such sum may differ at all.
     */
    private static void assertRowsMatch(String query, RecordCursorFactory factory, String expected, String actual, double tolerance) {
        final AsyncWindowRecordCursorFactory async = findAsyncFactoryOrNull(factory);
        final RecordMetadata metadata = factory.getMetadata();
        final boolean[] mayDiffer = new boolean[metadata.getColumnCount()];
        boolean any = false;
        if (async != null && tolerance > 0) {
            final AsyncWindowSplitPlan plan = async.getSplitPlan();
            for (int i = 0, n = plan.getPrefixCount(); i < n; i++) {
                if (plan.getPrefixOp(i) == AsyncWindowSplitPlan.OP_ADD && ColumnType.tagOf(plan.getPrefixType(i)) == ColumnType.DOUBLE) {
                    // the carried column, by its name, wherever the output puts it
                    final int column = metadata.getColumnIndexQuiet(async.getMetadata().getColumnName(plan.getPrefixColumn(i)));
                    if (column > -1) {
                        mayDiffer[column] = true;
                        any = true;
                    }
                }
            }
        }
        if (!any) {
            TestUtils.assertEquals(query, expected, actual);
            return;
        }
        final String[] e = expected.split("\n");
        final String[] a = actual.split("\n");
        Assert.assertEquals(query, e.length, a.length);
        for (int i = 0; i < e.length; i++) {
            if (e[i].equals(a[i])) {
                continue;
            }
            final String[] ev = e[i].split("\t", -1);
            final String[] av = a[i].split("\t", -1);
            Assert.assertEquals(query, ev.length, av.length);
            for (int c = 0; c < ev.length; c++) {
                if (ev[c].equals(av[c])) {
                    continue;
                }
                final String message = query + " row " + i + " column " + c + ": expected " + ev[c] + " but was " + av[c];
                Assert.assertTrue(message, mayDiffer[c]);
                final double x = Double.longBitsToDouble(Long.parseLong(ev[c]));
                final double y = Double.longBitsToDouble(Long.parseLong(av[c]));
                Assert.assertTrue(message, Math.abs(x - y) <= tolerance);
            }
        }
    }

    private static void assertSlotsReleased(RecordCursorFactory factory) {
        final PerWorkerLocks locks = TestUtils.findPerWorkerLocks(factory, "async window");
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
    }

    private static AsyncWindowRecordCursorFactory findAsyncFactory(RecordCursorFactory factory) {
        final AsyncWindowRecordCursorFactory async = findAsyncFactoryOrNull(factory);
        Assert.assertNotNull("no Async Window in the factory tree", async);
        return async;
    }

    private static AsyncWindowRecordCursorFactory findAsyncFactoryOrNull(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof AsyncWindowRecordCursorFactory async) {
                return async;
            }
        }
        return null;
    }

    // Every value of every row as raw bits: doubles and floats by their bits, symbols by value.
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

    private static String serial(CairoEngine engine, SqlExecutionContext ctx, String query) throws Exception {
        ctx.setParallelWindowEnabled(false);
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            Assert.assertNull(query, findAsyncFactoryOrNull(factory));
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                return rawRows(cursor, factory.getMetadata());
            }
        }
    }

    /**
     * Runs the query serially and on the parallel window, compares the rows, and returns the
     * tasks the worker threads computed.
     *
     * @param lastStage the kind of the Async Window's last step the plan must have, -1 for no
     *                  Async Window at all, null for any plan
     */
    private long assertMatchesSerial(CairoEngine engine, SqlExecutionContext ctx, String query, Integer lastStage) throws Exception {
        return assertMatchesSerial(engine, ctx, query, lastStage, 0);
    }

    /**
     * {@link #assertMatchesSerial(CairoEngine, SqlExecutionContext, String, Integer)}, letting the
     * running DOUBLE sums a carry adds to differ by at most {@code tolerance}, see
     * {@link #assertRowsMatch(String, RecordCursorFactory, String, String, double)}.
     */
    private long assertMatchesSerial(CairoEngine engine, SqlExecutionContext ctx, String query, Integer lastStage, double tolerance) throws Exception {
        final String expected = serial(engine, ctx, query);
        ctx.setParallelWindowEnabled(true);
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            final AsyncWindowRecordCursorFactory async = findAsyncFactoryOrNull(factory);
            if (lastStage != null) {
                if (lastStage == -1) {
                    Assert.assertNull(query, async);
                } else {
                    Assert.assertNotNull(query, async);
                    final ObjList<AsyncWindowStage> stages = async.getStages();
                    Assert.assertTrue(query, stages.size() > 0);
                    Assert.assertEquals(query, (int) lastStage, stages.getLast().getKind());
                }
            }
            for (int pass = 0; pass < 2; pass++) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    assertRowsMatch(query, factory, expected, rawRows(cursor, factory.getMetadata()), tolerance);
                    cursor.toTop();
                    assertRowsMatch(query, factory, expected, rawRows(cursor, factory.getMetadata()), tolerance);
                }
            }
            if (async == null) {
                return 0;
            }
            assertSlotsReleased(factory);
            return ((AsyncWindowAtom) async.getAtom()).getWorkerThreadTaskCount();
        } finally {
            ctx.setParallelWindowEnabled(false);
        }
    }

    /**
     * A {@code trade}-like table: key BIG holds 40% of the rows, K0..K19 the rest, less one in
     * thirteen that is NULL; two rows share each timestamp; price repeats in runs of a few rows and
     * is NULL one row in eleven; size is NULL one row in seventeen; seq goes down now and then.
     * Three DAY partitions.
     */
    private void createTrade(CairoEngine engine, SqlExecutionContext ctx, int rows) throws Exception {
        createTrade(engine, ctx, rows, true);
    }

    private void createTrade(CairoEngine engine, SqlExecutionContext ctx, int rows, boolean indexed) throws Exception {
        engine.execute(
                "create table trade (time timestamp, sym symbol" + (indexed ? " index type " + indexType : "") + ", ex symbol, price float, size float, seq int)" +
                        " timestamp(time) partition by DAY",
                ctx
        );
        engine.execute(
                "insert into trade select" +
                        " ((x / 2) * " + (3 * 86_400_000_000L / rows) + "L)::timestamp," +
                        " case when x % 5 < 2 then 'BIG' when x % 13 = 0 then null else 'K' || (x % 20) end," +
                        " case when x % 3 = 0 then 'T' else 'Q' end," +
                        " case when x % 11 = 0 then null else (100 + (x / 7) % 5)::float end," +
                        " case when x % 17 = 0 then null else (x % 9 + 0.25)::float end," +
                        " (x % 97 - (x % 7) * 3)::int" +
                        " from long_sequence(" + rows + ")",
                ctx
        );
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
