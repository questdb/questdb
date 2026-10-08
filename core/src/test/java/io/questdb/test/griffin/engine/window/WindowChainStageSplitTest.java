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
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkSPI;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.window.SumDoubleWindowFunctionFactory;
import io.questdb.griffin.engine.window.AsyncWindowChainSplit;
import io.questdb.griffin.engine.window.AsyncWindowFoldEcho;
import io.questdb.griffin.engine.window.AsyncWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.AsyncWindowSplitPlan;
import io.questdb.griffin.engine.window.AsyncWindowStage;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * How a window chained over an Async Window (see {@code SqlCodeGenerator.tryChainAsyncWindow})
 * splits keys over tasks. The workers of a folded running DOUBLE sum output its argument, a
 * stand-in ({@link AsyncWindowFoldEcho}) that only the query thread's fold turns into the sum. So
 * a stand-in may only be put in where the plan the chain finally follows folds that column: when
 * an earlier window carries, keeps its keys whole, or the warm-up rows add up past half a task,
 * the chain splits no key, nothing is folded, and the workers must compute the sum itself. Every
 * query here runs serially and on the parallel window, and the two must agree bit for bit.
 */
public class WindowChainStageSplitTest extends AbstractCairoTest {
    private static final String P = " OVER (PARTITION BY sym ORDER BY time";
    private static final String P1 = " OVER (ORDER BY time";
    private static final String RUN = " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)";
    // primary windows of every kind, by name and expression without its OVER clause's frame:
    // {name, function, frame}; the frame is appended to the PARTITION BY or the single key's OVER
    private static final String[][] PRIMARIES = {
            {"row_number", "row_number()", ")"},
            {"running count", "count(bid)", RUN},
            {"running DOUBLE sum", "sum(bid)", RUN},
            {"running whole sum", "sum(CASE WHEN bid > 5 THEN 1 ELSE 0 END)", RUN},
            {"running INT sum", "sum(i)", RUN},
            {"running DOUBLE max", "max(bid)", RUN},
            {"running DOUBLE min", "min(bid)", RUN},
            {"running INT max", "max(i)", RUN},
            {"running INT min", "min(i)", RUN},
            {"running first_value", "first_value(bid)", RUN},
            {"lag", "lag(bid)", ")"},
            {"lag 3", "lag(bid, 3)", ")"},
            {"lead", "lead(bid)", ")"},
            {"bounded DOUBLE avg", "avg(bid)", " ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)"},
            {"bounded DOUBLE sum", "sum(bid)", " ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)"},
            {"bounded DOUBLE min", "min(bid)", " ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)"},
            {"bounded count", "count(bid)", " ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)"},
            {"bounded INT max", "max(i)", " ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)"},
            {"bounded whole sum", "sum(sh)", " ROWS BETWEEN 7 PRECEDING AND CURRENT ROW)"},
    };

    // A fold stand-in in a step whose plan has no fold for it: withStage refuses it.
    @Test
    public void testFoldStandInWithoutFoldRefused() throws Exception {
        smallConfig();
        assertMemoryLeak(() -> {
            createSkew(engine, sqlExecutionContext, 2_000);
            sqlExecutionContext.setParallelWindowEnabled(true);
            try (RecordCursorFactory factory = engine.select("SELECT time, v, lag(v) OVER (ORDER BY time) l FROM s WHERE sym = 'K0'", sqlExecutionContext)) {
                final AsyncWindowRecordCursorFactory async = WindowChainProofTest.findAsyncFactory(factory);
                Assert.assertEquals(0, async.getStages().size());
                final GenericRecordMetadata metadata = GenericRecordMetadata.copyOf(async.getMetadata());
                final ObjList<AsyncWindowStage> workerStages = new ObjList<>();
                for (int i = 1, n = Math.max(2, async.getWorkerSlotCount()); i < n; i++) {
                    workerStages.add(AsyncWindowStage.window(standIns(metadata.getColumnCount()), null));
                }
                final AsyncWindowStage ownerStage = AsyncWindowStage.window(columns(metadata.getColumnCount()), null);
                try {
                    async.withStage(ownerStage, workerStages, metadata, NO_SINK, AsyncWindowSplitPlan.NONE, -1);
                    Assert.fail("a stand-in without its fold");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "internal error: a fold stand-in without its fold");
                }
                // the stages were freed, and the factory is unchanged
                Assert.assertEquals(0, workerStages.size());
                Assert.assertEquals(0, async.getStages().size());
                // a plan that folds the column, but at another stage than this one
                final IntList columns = new IntList();
                columns.add(0);
                final IntList ops = new IntList();
                ops.add(AsyncWindowSplitPlan.OP_FOLD);
                final IntList types = new IntList();
                types.add(io.questdb.cairo.ColumnType.DOUBLE);
                final AsyncWindowSplitPlan fold = new AsyncWindowSplitPlan(AsyncWindowSplitPlan.MODE_PREFIX, 0, columns, ops, types);
                for (int i = 1, n = Math.max(2, async.getWorkerSlotCount()); i < n; i++) {
                    workerStages.add(AsyncWindowStage.window(standIns(metadata.getColumnCount()), null));
                }
                try {
                    async.withStage(AsyncWindowStage.window(columns(metadata.getColumnCount()), null), workerStages, metadata, NO_SINK, fold, -1);
                    Assert.fail("a stand-in folded at another stage");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "internal error: a fold stand-in without its fold [column=0]");
                }
                Assert.assertEquals(0, workerStages.size());
                Assert.assertEquals(0, async.getStages().size());
            } finally {
                sqlExecutionContext.setParallelWindowEnabled(false);
            }
        });
    }

    // H1 of the review: an earlier window that carries (row_number), one that keeps its keys whole
    // (a running DOUBLE max), or warm-up rows past half a task (lag 20, then lag 10, with tasks of
    // 50 rows) leave the chain splitting no key; a chained running DOUBLE sum must not have its
    // workers output its argument then. With a lag 2 and a lag 1, the warm-up rows fit a task but
    // the chained window has its own warm-up rows, which the chain cannot give a fold.
    @Test
    public void testFoldStandInsWithoutFold() throws Exception {
        smallConfig();
        assertMemoryLeak(() -> {
            createSkew(engine, sqlExecutionContext, 6_000);
            final String in = " FROM s WHERE sym IN ('K0', 'K1', 'K2')";
            final String a = "WITH w0 AS (SELECT sym, time, v, bid, row_number()" + P + ") rn" + in + "), ch AS (SELECT sym, time, v, rn, bid * 1.5 + rn d FROM w0) ";
            assertMatchesSerial(a + "SELECT sym, time, d, x FROM (SELECT sym, time, d, sum(d)" + P + RUN + " x FROM ch) ORDER BY sym, time, d, x");
            final String b = "WITH w0 AS (SELECT sym, time, v, bid, max(bid)" + P + RUN + " mb" + in + "), ch AS (SELECT sym, time, v, bid - mb + v d FROM w0) ";
            assertMatchesSerial(b + "SELECT sym, time, d, x FROM (SELECT sym, time, d, sum(d)" + P + RUN + " x FROM ch) ORDER BY sym, time, d, x");
            final String c = "WITH w0 AS (SELECT sym, time, v, bid, lag(bid, 20)" + P + ") lb" + in + "), ch AS (SELECT sym, time, v, bid - lb + v d FROM w0) ";
            assertMatchesSerial(c + "SELECT sym, time, d, x, l FROM (SELECT sym, time, d, sum(d)" + P + RUN + " x, lag(d, 10)" + P + ") l FROM ch) ORDER BY sym, time, d, x, l");
            final String d = "WITH w0 AS (SELECT sym, time, v, bid, lag(bid, 2)" + P + ") lb" + in + "), ch AS (SELECT sym, time, v, bid - lb + v d FROM w0) ";
            final String own = d + "SELECT sym, time, d, x, l FROM (SELECT sym, time, d, sum(d)" + P + RUN + " x, lag(d)" + P + ") l FROM ch) ORDER BY sym, time, d, x, l";
            assertMatchesSerial(own);
            Assert.assertFalse(plan(own), Chars.contains(plan(own), "keySplit"));
            // the whole table
            assertMatchesSerial(a.replace(in, " FROM s") + "SELECT sym, time, d, x FROM (SELECT sym, time, d, sum(d)" + P + RUN + " x FROM ch) ORDER BY sym, time, d, x");
            assertMatchesSerial(b.replace(in, " FROM s") + "SELECT sym, time, d, x FROM (SELECT sym, time, d, sum(d)" + P + RUN + " x FROM ch) ORDER BY sym, time, d, x");
            // control: a lag 1 splits the chain, and the chained sum is folded
            final String ok = "WITH w0 AS (SELECT sym, time, v, bid, lag(bid)" + P + ") lb" + in + "), ch AS (SELECT sym, time, v, bid - lb + v d FROM w0) " +
                    "SELECT sym, time, d, x FROM (SELECT sym, time, d, sum(d)" + P + RUN + " x FROM ch) ORDER BY sym, time, d, x";
            assertMatchesSerial(ok);
            TestUtils.assertContains(plan(ok), "keySplit: warmup 1 rows, running carry, folded");
        });
    }

    // the same at the default configuration, where every key but the walk's first, which the
    // query's thread streams as the prefix, is computed whole by a worker
    @Test
    public void testFoldStandInsAtDefaultConfig() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table d (time timestamp, sym symbol index, v double, bid double) timestamp(time) partition by DAY");
            execute("insert into d select (x * 100_000L)::timestamp," +
                    " case when x % 10 < 6 then 'A' when x % 10 < 9 then 'B' else 'C' || (x % 7) end," +
                    " case when x % 31 = 0 then null when x % 997 = 0 then cast('Infinity' as double) when x % 19 = 0 then -0.0 else (rnd_double() - 0.4) * power(10.0, (x % 13) - 2) end," +
                    " rnd_double() * 10 from long_sequence(1_200_000)");
            final String in = " FROM d WHERE sym IN ('A', 'B', 'C3')";
            final String a = "WITH w0 AS (SELECT sym, time, v, bid, row_number()" + P + ") rn" + in + "), ch AS (SELECT sym, time, rn, bid * 1.5 + rn x FROM w0) ";
            assertMatchesSerial(a + "SELECT sym, time, x, s FROM (SELECT sym, time, x, sum(x)" + P + RUN + " s FROM ch) ORDER BY sym, time, x, s");
            final String b = "WITH w0 AS (SELECT sym, time, v, bid, max(bid)" + P + RUN + " mb" + in + "), ch AS (SELECT sym, time, bid - mb x FROM w0) ";
            assertMatchesSerial(b + "SELECT sym, time, x, s FROM (SELECT sym, time, x, sum(x)" + P + RUN + " s FROM ch) ORDER BY sym, time, x, s");
            final String a1 = "WITH w0 AS (SELECT time, v, bid, row_number()" + P1 + ") rn FROM d WHERE sym = 'A'), ch AS (SELECT time, rn, bid * 1.5 + rn x FROM w0) ";
            assertMatchesSerial(a1 + "SELECT time, x, sum(x)" + P1 + RUN + " s FROM ch");
            final String b1 = "WITH w0 AS (SELECT time, v, bid, max(bid)" + P1 + RUN + " mb FROM d WHERE sym = 'A'), ch AS (SELECT time, bid - mb x FROM w0) ";
            assertMatchesSerial(b1 + "SELECT time, x, sum(x)" + P1 + RUN + " s FROM ch");
        });
    }

    // H2 of the review: a single key with no prefix reaches the workers, whatever its size
    @Test
    public void testFoldStandInsSingleKeyNoPrefix() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_PREFIX_ROWS, 0);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 100_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 5_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, 20_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, 100);
        sqlExecutionContext.changePageFrameSizes(1, 64);
        assertMemoryLeak(() -> {
            createSkew(engine, sqlExecutionContext, 6_000);
            final String a1 = "WITH w0 AS (SELECT time, v, bid, row_number()" + P1 + ") rn FROM s WHERE sym = 'K0'), ch AS (SELECT time, v, rn, bid * 1.5 + rn d FROM w0) ";
            assertMatchesSerial(a1 + "SELECT time, d, sum(d)" + P1 + RUN + " x FROM ch");
            final String b1 = "WITH w0 AS (SELECT time, v, bid, max(bid)" + P1 + RUN + " mb FROM s WHERE sym = 'K0'), ch AS (SELECT time, v, bid - mb + v d FROM w0) ";
            assertMatchesSerial(b1 + "SELECT time, d, sum(d)" + P1 + RUN + " x FROM ch");
        });
    }

    @Test
    public void testEveryPrimaryThenChainedSum() throws Exception {
        smallConfig();
        assertMemoryLeak(() -> {
            createSkew(engine, sqlExecutionContext, 4_000);
            everyPrimaryThenChainedSum(engine, sqlExecutionContext, "s", "('K0', 'K1', 'K2')", "K0", true);
        });
    }

    @Test
    public void testEveryPrimaryThenChainedSumAtDefaultConfig() throws Exception {
        assertMemoryLeak(() -> {
            createSkew(engine, sqlExecutionContext, 300_000);
            everyPrimaryThenChainedSum(engine, sqlExecutionContext, "s", "('K0', 'K1', 'K2')", "K1", false);
        });
    }

    @Test
    public void testEveryPrimaryThenChainedSumNoPrefix() throws Exception {
        smallConfig();
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_PREFIX_ROWS, 0);
        assertMemoryLeak(() -> {
            createSkew(engine, sqlExecutionContext, 3_000);
            everyPrimaryThenChainedSum(engine, sqlExecutionContext, "s", "('K0', 'K1', 'K2', 'K3')", "K0", false);
        });
    }

    @Test
    public void testEveryPrimaryThenChainedSumOnWorkerPool() throws Exception {
        smallConfig();
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            createSkew(engine, ctx, 4_000);
            everyPrimaryThenChainedSum(engine, ctx, "s", "('K0', 'K1', 'K2')", "K0", false);
        }));
    }

    // A chained window over a window that keeps its keys whole computes its running sum itself:
    // the chain's plan has no fold, and no worker copy has a stand-in.
    @Test
    public void testNoSplitChainComputesItsSum() throws Exception {
        smallConfig();
        assertMemoryLeak(() -> {
            createSkew(engine, sqlExecutionContext, 4_000);
            final String q = "WITH w0 AS (SELECT sym, time, v, bid, max(bid)" + P + RUN + " mb FROM s WHERE sym IN ('K0', 'K1')), ch AS (SELECT sym, time, v, bid - mb + v d FROM w0) " +
                    "SELECT sym, time, d, x FROM (SELECT sym, time, d, sum(d)" + P + RUN + " x FROM ch) ORDER BY sym, time, d, x";
            sqlExecutionContext.setParallelWindowEnabled(true);
            try (RecordCursorFactory factory = engine.select(q, sqlExecutionContext)) {
                final AsyncWindowRecordCursorFactory async = WindowChainProofTest.findAsyncFactory(factory);
                // the projection and the window are chained, and nothing splits
                Assert.assertEquals(2, async.getStages().size());
                Assert.assertEquals(AsyncWindowSplitPlan.MODE_NONE, async.getSplitPlan().getMode());
            } finally {
                sqlExecutionContext.setParallelWindowEnabled(false);
            }
            assertMatchesSerial(q);
        });
    }

    // m1 of the review: a warm-up as large as half of all longs, whose double overflows, is no
    // warm-up a task can hold, and neither is half a task or more
    @Test
    public void testHugeWarmupNotSplit() throws Exception {
        smallConfig();
        assertMemoryLeak(() -> {
            createSkew(engine, sqlExecutionContext, 2_000);
            // (the serial plan's lag allocates by its offset: some offsets this large fail there)
            for (String k : new String[]{"4611686018427387904", "100000", "25"}) {
                final String q = "SELECT sym, time, x FROM (SELECT sym, time, lag(v, " + k + ")" + P + ") x FROM s WHERE sym IN ('K0', 'K1')) ORDER BY sym, time, x";
                assertMatchesSerial(q);
                Assert.assertFalse(plan(q), Chars.contains(plan(q), "keySplit"));
                // and a lag chained over it (a lag of such an offset over a chained, cached window
                // fails in the serial plan itself, in its memory)
                final String chained = "WITH w0 AS (SELECT sym, time, v, lag(v, " + k + ")" + P + ") lv FROM s WHERE sym IN ('K0', 'K1')), ch AS (SELECT sym, time, v + lv d FROM w0) " +
                        "SELECT sym, time, d, x FROM (SELECT sym, time, d, lag(d)" + P + ") x FROM ch) ORDER BY sym, time, d, x";
                assertMatchesSerial(chained);
                Assert.assertFalse(plan(chained), Chars.contains(plan(chained), "keySplit"));
            }
            // with tasks of 50 rows, 24 warm-up rows split keys, 25 do not
            final String split = "SELECT sym, time, x FROM (SELECT sym, time, lag(v, 24)" + P + ") x FROM s WHERE sym IN ('K0', 'K1')) ORDER BY sym, time, x";
            assertMatchesSerial(split);
            TestUtils.assertContains(plan(split), "keySplit: warmup 24 rows");
        });
    }

    // the warm-up rows of a chain add up without overflowing, and stop at half a task
    @Test
    public void testChainWarmupRowsAddUp() {
        final AsyncWindowChainSplit split = AsyncWindowChainSplit.of(warmup(Long.MAX_VALUE - 1)).thenWindow(warmup(Long.MAX_VALUE - 1), 0);
        Assert.assertSame(AsyncWindowSplitPlan.NONE, split.toPlan(Long.MAX_VALUE));
        Assert.assertSame(AsyncWindowSplitPlan.NONE, AsyncWindowChainSplit.of(warmup(3)).toPlan(6));
        Assert.assertEquals(3, AsyncWindowChainSplit.of(warmup(3)).toPlan(7).getWarmupRows());
        Assert.assertEquals(3, AsyncWindowChainSplit.of(warmup(3)).toPlan(8).getWarmupRows());
        Assert.assertEquals(5, AsyncWindowChainSplit.of(warmup(2)).thenWindow(warmup(3), 0).toPlan(11).getWarmupRows());
        Assert.assertSame(AsyncWindowSplitPlan.NONE, AsyncWindowChainSplit.of(warmup(2)).thenWindow(warmup(3), 0).toPlan(10));
        // warm-up rows past any task, then a carry: an overflowed count must not read as none
        final IntList columns = new IntList();
        columns.add(0);
        final IntList ops = new IntList();
        ops.add(AsyncWindowSplitPlan.OP_ADD);
        final IntList types = new IntList();
        types.add(io.questdb.cairo.ColumnType.LONG);
        final AsyncWindowSplitPlan carry = new AsyncWindowSplitPlan(AsyncWindowSplitPlan.MODE_PREFIX, 0, columns, ops, types);
        Assert.assertSame(AsyncWindowSplitPlan.NONE, AsyncWindowChainSplit.of(warmup(1)).thenWindow(warmup(Long.MAX_VALUE), 0).thenWindow(carry, 1).toPlan(100));
        // control: warm-up rows within a task, then a carry
        final AsyncWindowSplitPlan carried = AsyncWindowChainSplit.of(warmup(1)).thenWindow(warmup(2), 0).thenWindow(carry, 1).toPlan(100);
        Assert.assertEquals(AsyncWindowSplitPlan.MODE_WARMUP, carried.getMode());
        Assert.assertEquals(3, carried.getWarmupRows());
    }

    // an unsplittable chain stays so, whatever a later window allows on its own
    @Test
    public void testUnsplittableChainStaysUnsplittable() {
        final IntList columns = new IntList();
        columns.add(1);
        final IntList ops = new IntList();
        ops.add(AsyncWindowSplitPlan.OP_FOLD);
        final IntList types = new IntList();
        types.add(io.questdb.cairo.ColumnType.DOUBLE);
        final AsyncWindowSplitPlan fold = new AsyncWindowSplitPlan(AsyncWindowSplitPlan.MODE_PREFIX, 0, columns, ops, types);
        final AsyncWindowChainSplit split = AsyncWindowChainSplit.of(AsyncWindowSplitPlan.NONE).thenWindow(fold, 0);
        Assert.assertSame(AsyncWindowSplitPlan.NONE, split.toPlan(1_000));
        Assert.assertEquals(-1, split.getCarryStage());
        // a chained window with warm-up rows of its own and a fold: the fold cannot be carried
        final AsyncWindowSplitPlan warmupFold = new AsyncWindowSplitPlan(AsyncWindowSplitPlan.MODE_WARMUP, 1, columns, ops, types);
        Assert.assertSame(AsyncWindowSplitPlan.NONE, AsyncWindowChainSplit.of(warmup(1)).thenWindow(warmupFold, 0).toPlan(1_000));
        // control: a fold after warm-up rows is the chain's carry
        final AsyncWindowChainSplit carried = AsyncWindowChainSplit.of(warmup(1)).thenWindow(fold, 0);
        Assert.assertEquals(0, carried.getCarryStage());
        Assert.assertTrue(carried.toPlan(1_000).hasFold());
    }

    // m2 of the review: a window ordered by an alias of the designated timestamp still walks the
    // keys one by one; a column aliased with the timestamp's name does not
    @Test
    public void testAliasedTimestampWalksKeyMajor() throws Exception {
        smallConfig();
        assertMemoryLeak(() -> {
            createSkew(engine, sqlExecutionContext, 4_000);
            final String[] walked = {
                    "SELECT sym, t2, x FROM (SELECT sym, t2, sum(v)" + P.replace("time", "t2") + RUN + " x FROM (SELECT sym, time t2, v FROM s WHERE sym IN ('K0', 'K1'))) ORDER BY sym, t2, x",
                    "SELECT sym, t3, x FROM (SELECT sym, t3, sum(v)" + P.replace("time", "t3") + RUN + " x FROM (SELECT sym, t2 t3, v FROM (SELECT sym, time t2, v FROM s WHERE sym IN ('K0', 'K1')))) ORDER BY sym, t3, x",
                    "SELECT sym, time, x FROM (SELECT sym, time, sum(v)" + P.replace("time", "s.time") + RUN + " x FROM s WHERE sym IN ('K0', 'K1')) ORDER BY sym, time, x",
                    "SELECT sym, time, x FROM (SELECT sym, time, sum(v)" + P.replace("time", "TIME") + RUN + " x FROM s WHERE sym IN ('K0', 'K1')) ORDER BY sym, time, x",
            };
            for (String q : walked) {
                assertMatchesSerial(q);
                TestUtils.assertContains(plan(q), "keyMajor: true");
            }
            // "time" is v here: ordered by another column, the window keeps the serial plan's scan
            final String shadowed = "SELECT sym, time, x FROM (SELECT sym, time, sum(bid)" + P + RUN + " x FROM (SELECT sym, v time, bid FROM s WHERE sym IN ('K0', 'K1'))) ORDER BY sym, time, x";
            assertMatchesSerial(shadowed);
            Assert.assertFalse(plan(shadowed), Chars.contains(plan(shadowed), "keyMajor: true"));
        });
    }

    // m3 of the review: the serial frame's running sum holds at most the frame's rows and the one
    // coming in, N + 2 values for N PRECEDING AND CURRENT ROW (frame rows + 1); whole numbers of
    // a bound B keep it exact, and its warm-up rows rebuild it, only while (N + 2) * B <= 2^53
    @Test
    public void testExactFrameBound() throws Exception {
        smallConfig();
        assertMemoryLeak(() -> {
            createSkew(engine, sqlExecutionContext, 4_000);
            // 9 PRECEDING: 11 values; 2^53 / 11 = 818836295885544.7
            final String lagged = "WITH w0 AS (SELECT time, bid, lag(bid) OVER (ORDER BY time) lb FROM s WHERE sym = 'K0') ";
            final String within = lagged + "SELECT time, sum(CASE WHEN bid > lb THEN 818836295885544 ELSE -818836295885544 END)" + P1 + " ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) a FROM w0";
            final String past = lagged + "SELECT time, sum(CASE WHEN bid > lb THEN 818836295885545 ELSE -818836295885545 END)" + P1 + " ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) a FROM w0";
            assertMatchesSerial(within);
            assertMatchesSerial(past);
            TestUtils.assertContains(plan(within), "keySplit: warmup 10 rows");
            // past the bound, a chained frame keeps its keys whole
            Assert.assertFalse(plan(past), Chars.contains(plan(past), "keySplit"));
            // and a frame of the window's own functions is replayed
            final String ownPast = "SELECT time, sum(CASE WHEN bid > 5 THEN 818836295885545 ELSE -818836295885545 END)" + P1 + " ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) a FROM s WHERE sym = 'K0'";
            final String ownWithin = "SELECT time, sum(CASE WHEN bid > 5 THEN 818836295885544 ELSE -818836295885544 END)" + P1 + " ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) a FROM s WHERE sym = 'K0'";
            assertMatchesSerial(ownPast);
            assertMatchesSerial(ownWithin);
            TestUtils.assertContains(plan(ownWithin), "keySplit: warmup 9 rows");
            TestUtils.assertContains(plan(ownPast), "keySplit: frame replayed");
        });
    }

    private static final RecordSink NO_SINK = new RecordSink() {
        @Override
        public void copy(Record r, RecordSinkSPI w) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setFunctions(ObjList<Function> keyFunctions) {
        }
    };

    private static void assertMatchesSerial(String query) throws Exception {
        WindowChainProofTest.assertMatchesSerial(engine, sqlExecutionContext, query);
    }

    private static ObjList<Function> columns(int count) {
        final ObjList<Function> functions = new ObjList<>(count);
        for (int i = 0; i < count; i++) {
            functions.add(new DoubleConstant(i));
        }
        return functions;
    }

    /**
     * Table {@code s}: skewed keys K0 (6 rows in 10), K1 (2 in 10), K2..K6, a NULL key and K7;
     * {@code v} over 15 orders of magnitude with signs, NULL, both infinities and -0.0; INT, SHORT.
     */
    private static void createSkew(CairoEngine engine, SqlExecutionContext ctx, int rows) throws Exception {
        engine.execute("create table s (time timestamp, sym symbol index, v double, bid double, i int, sh short) timestamp(time) partition by HOUR", ctx);
        engine.execute(
                "insert into s select (x * 500_000L)::timestamp," +
                        " case when x % 10 < 6 then 'K0' when x % 10 < 8 then 'K1' when x % 10 = 8 then 'K' || (2 + (x / 10) % 5) when x % 50 = 9 then null else 'K7' end," +
                        " case when x % 31 = 0 then null when x % 97 = 0 then cast('Infinity' as double) when x % 89 = 0 then cast('-Infinity' as double) when x % 19 = 0 then -0.0" +
                        "   else (rnd_double() - 0.3) * power(10.0, (x % 15) - 3) end," +
                        " rnd_double() * 10," +
                        " rnd_int(-2000000000, 2000000000, 2)," +
                        " rnd_short()" +
                        " from long_sequence(" + rows + ")",
                ctx
        );
    }

    // Each primary window, projected with a non-whole value, then a chained running DOUBLE sum
    // under the key's PARTITION BY (and, when asked, with a chained lag of its own), over an IN
    // list, the whole table and a single key.
    private static void everyPrimaryThenChainedSum(CairoEngine engine, SqlExecutionContext ctx, String table, String inList, String key, boolean withOwnLag) throws Exception {
        for (String[] primary : PRIMARIES) {
            for (String where : new String[]{" WHERE sym IN " + inList, ""}) {
                final String ch = "WITH w0 AS (SELECT sym, time, v, bid, " + primary[1] + P + primary[2] + " p FROM " + table + where + "), " +
                        "ch AS (SELECT sym, time, v, p, p * 1.5 + v x FROM w0) ";
                WindowChainProofTest.assertMatchesSerial(engine, ctx, ch + "SELECT sym, time, x, s FROM (SELECT sym, time, x, sum(x)" + P + RUN + " s FROM ch) ORDER BY sym, time, x, s");
                if (withOwnLag) {
                    WindowChainProofTest.assertMatchesSerial(engine, ctx, ch + "SELECT sym, time, x, s, l FROM (SELECT sym, time, x, sum(x)" + P + RUN + " s, lag(x)" + P + ") l FROM ch) ORDER BY sym, time, x, s, l");
                }
            }
            final String ch1 = "WITH w0 AS (SELECT time, v, bid, " + primary[1] + P1 + primary[2] + " p FROM " + table + " WHERE sym = '" + key + "'), " +
                    "ch AS (SELECT time, v, p, p * 1.5 + v x FROM w0) ";
            WindowChainProofTest.assertMatchesSerial(engine, ctx, ch1 + "SELECT time, x, sum(x)" + P1 + RUN + " s FROM ch");
            if (withOwnLag) {
                WindowChainProofTest.assertMatchesSerial(engine, ctx, ch1 + "SELECT time, x, sum(x)" + P1 + RUN + " s, lag(x)" + P1 + ") l FROM ch");
            }
        }
    }

    private static void inPool(PoolTest test) throws Exception {
        final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
        TestUtils.execute(pool, (engine, compiler, context) -> {
            final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) context;
            ctx.changePageFrameSizes(1, 64);
            test.run(engine, ctx);
        }, configuration, LOG);
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

    private static void smallConfig() {
        // keys longer than a task, tasks of 50 rows, a prefix of 100
        sqlExecutionContext.changePageFrameSizes(1, 64);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 50);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 400);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, 100);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, 200);
    }

    // the stand-in of a running DOUBLE sum in every column
    private static ObjList<Function> standIns(int count) {
        final ObjList<Function> functions = new ObjList<>(count);
        for (int i = 0; i < count; i++) {
            functions.add(new AsyncWindowFoldEcho(new SumDoubleWindowFunctionFactory.SumOverUnboundedRowsFrameFunction(new DoubleConstant(i))));
        }
        return functions;
    }

    private static AsyncWindowSplitPlan warmup(long rows) {
        return new AsyncWindowSplitPlan(AsyncWindowSplitPlan.MODE_WARMUP, rows, new IntList(), new IntList(), new IntList());
    }

    @FunctionalInterface
    private interface PoolTest {
        void run(CairoEngine engine, SqlExecutionContextImpl ctx) throws Exception;
    }
}
