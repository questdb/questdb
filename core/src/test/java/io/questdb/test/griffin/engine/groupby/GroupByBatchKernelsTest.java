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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.columns.FloatColumn;
import io.questdb.griffin.engine.functions.groupby.CovarSampleGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.SumDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.VarSampleGroupByFunctionFactory;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByBatchKernels;
import io.questdb.griffin.engine.table.AsyncGroupByNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Column-wise batch kernels of the parallel GROUP BY ({@link GroupByBatchKernels}) against the row
 * path, which is the same query compiled with {@code cairo.sql.parallel.groupby.batch.kernels.enabled}
 * set to false.
 * <p>
 * <b>Tolerance policy.</b>
 * <ul>
 *     <li>Without a worker pool the query's own thread reduces every frame, in frame order, on both
 *     paths. Every result must then match the row path <b>bit for bit</b>: the raw bits of every
 *     DOUBLE and FLOAT (so -0.0, and which NaN), and every integer. There is no tolerance; this is
 *     what most tests here check, through {@link #assertMatchesRowPath}.</li>
 *     <li>With a worker pool, which worker reduces which frame, and so the order the partial results
 *     are merged in, changes from run to run on <b>both</b> paths. Integer results, counts, min and
 *     max must still match exactly. Sums, averages and the Welford/Chan states (stddev, variance,
 *     corr, covar) may differ in the last bits; they are compared with a relative tolerance of
 *     1e-9 (absolute 1e-9 near zero), the run-to-run variation parallel GROUP BY already has
 *     without the kernels. The kernels change neither the order rows are accumulated in within a
 *     frame nor merge(), so they add no variation of their own. The worker-pool data avoids large
 *     offsets, on which the Chan merge is poorly conditioned on either path.</li>
 * </ul>
 */
public class GroupByBatchKernelsTest extends AbstractCairoTest {
    // every argument shape the kernels cover; each lands in all the aggregates of AGGREGATES
    private static final String[] ARGS = {
            // columns of every type
            "d", "f", "i", "l", "s", "b",
            // arithmetic per type: INT and LONG wrap on overflow and return NULL for a NULL operand
            // or a zero divisor; FLOAT and DOUBLE divide to NaN when the quotient is not finite
            "i + j", "i - j", "i * j", "i / j",
            "l + m", "l - m", "l * m", "l / m",
            "f + g", "f - g", "f * g", "f / g",
            "d + e", "d - e", "d * e", "d / e",
            // mixed types: implicit conversions through the base classes' getters
            "i * f", "l * d", "s + b", "i + l", "f * d", "b * f", "s * l", "(l + m) * f", "(i - j) * g", "s * f", "b * l",
            // casts
            "i::double", "i::float", "i::long", "f::double", "f::int", "f::long",
            "l::double", "l::int", "l::float", "d::float", "d::int", "d::long",
            "s::double", "b::int", "s::long", "b::float",
            // constants
            "i * 2", "d / 3.0", "f - 1.5", "l + 7", "2 - i",
            // the shapes of NYSE TAQ idx 20/21/34-46/68/69
            "(j * f + i * g) / (j + i)", "(i * g)::double", "g - f", "(i + j)::double",
            // nesting
            "((i + j) * (l - 3)) / 2", "(d * e + f) / (s + 1)", "((f + g) * 2.5)::float",
    };
    // kernels in every query (weighted_avg, corr, covar, stddev/var) plus sum/avg/min/max, which
    // have a kernel for an expression or a column of another type, and count(), which has none
    private static final String AGGREGATES = "sum(%1$s) s1, avg(%1$s) a1, min(%1$s) mn, max(%1$s) mx, " +
            "stddev_samp(%1$s) sd, var_pop(%1$s) vp, stddev(%1$s) sdd, var_samp(%1$s) vs, variance(%1$s) va, stddev_pop(%1$s) sdp, " +
            "weighted_avg(%1$s, j) w1, weighted_avg(d, %1$s) w2, corr(%1$s, e) c1, covar_samp(g, %1$s) cs, covar_pop(%1$s, l) cp, " +
            "count(%1$s) n";
    // random values with NULL, NaN, +-Infinity, +-0, extremes and overflow-prone values
    private static final int MODE_LARGE_OFFSETS = 1;
    private static final int MODE_SPECIALS = 0;
    // moderate values and NULLs only: sums that do not depend on the merge order beyond rounding
    private static final int MODE_TAME = 2;
    private static final int ROW_COUNT = 4_000;

    @Override
    public void setUp() {
        super.setUp();
        // small frames and batches, so groups, batches and kernel chunks cross frame boundaries
        sqlExecutionContext.changePageFrameSizes(1, 97);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_BATCH_SIZE, 61);
    }

    @Test
    public void testBindVariablesAreReadPerExecution() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t", ROW_COUNT, 11, MODE_SPECIALS);
            final String sql = "select k, weighted_avg(d * $1, i + $2) w, stddev_samp(f - $1) sd, sum(i * $2) s from t group by k order by k";
            for (int round = 0; round < 3; round++) {
                bindVariableService.clear();
                bindVariableService.setDouble(0, 1.5 + round);
                bindVariableService.setInt(1, round == 2 ? Numbers.INT_NULL : 3 - round);
                assertMatchesRowPath(sql, true);
            }
        });
    }

    @Test
    public void testColumnTops() throws Exception {
        // frames before the added columns have no buffer for them: those batches take the row
        // path, later ones the kernel
        assertMemoryLeak(() -> {
            createTable("t", ROW_COUNT, 3, MODE_SPECIALS);
            execute("alter table t add column x double");
            execute("alter table t add column y int");
            execute("insert into t (k, i, j, l, m, f, g, d, e, s, b, ts, x, y) " +
                    "select 'K' || (x % 13), x::int, (x % 7)::int, x, x * 3, (x / 3.0)::float, (x % 11)::float, x / 7.0, x % 5, (x % 100)::short, (x % 50)::byte, " +
                    "('1970-01-06'::timestamp + x * 1000000L), " +
                    "case when x % 9 = 0 then null else x * 1.25 end, case when x % 8 = 0 then null else (x * 3)::int end " +
                    "from long_sequence(900)");
            final String[] queries = {
                    "select k, weighted_avg(x, y) w, stddev_pop(x + d) sd, corr(x, y) c, sum(y + i) s, max(x * 2) mx from t group by k order by k",
                    "select weighted_avg(x, y) w, stddev_pop(x + d) sd, corr(x, y) c, sum(y + i) s, max(x * 2) mx from t",
                    "select k, var_samp(y * 2) v, avg(x - 1) a from t where i > 100 group by k order by k",
                    "select var_samp(y * 2) v, avg(x - 1) a from t where i > 100",
            };
            for (String sql : queries) {
                final long[] counts = assertMatchesRowPath(sql, true);
                Assert.assertTrue(sql, counts[0] > 0);
                Assert.assertTrue(sql, counts[1] > 0);
            }
        });
    }

    @Test
    public void testEmptyAndSingleRowGroups() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (k symbol, i int, j int, l long, m long, f float, g float, d double, e double, s short, b byte, ts timestamp) timestamp(ts) partition by day bypass wal");
            // empty table
            assertMatchesRowPath("select weighted_avg(d, i) w, stddev_samp(d + e) sd, sum(i + j) s, corr(f, g) c from t", true);
            assertMatchesRowPath("select k, weighted_avg(d, i) w, stddev_samp(d + e) sd, sum(i + j) s from t group by k order by k", true);
            try (TableWriter w = getWriter("t")) {
                long ts = 0;
                // a single row
                row(w, ts++, "single", 3, 4, 5, 6, 1.5f, 2.5f, 7.25, 8.5, (short) 1, (byte) 2);
                // groups whose every argument is NULL: empty aggregates
                row(w, ts++, "nulls", Numbers.INT_NULL, Numbers.INT_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Float.NaN, Float.NaN, Double.NaN, Double.NaN, (short) 0, (byte) 0);
                row(w, ts++, "nulls", Numbers.INT_NULL, Numbers.INT_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Float.NaN, Float.NaN, Double.NaN, Double.NaN, (short) 0, (byte) 0);
                // a group whose first row is NULL and whose later rows are not: computeFirst() of a NULL
                row(w, ts++, "late", Numbers.INT_NULL, 1, Numbers.LONG_NULL, 2, Float.NaN, 1f, Double.NaN, 1, (short) 3, (byte) 4);
                row(w, ts++, "late", 5, 6, 7, 8, 2f, 3f, 9.5, 10.5, (short) 5, (byte) 6);
                row(w, ts++, "late", 9, 10, 11, 12, 4f, 5f, 13.5, 14.5, (short) 7, (byte) 8);
                // infinities: not finite, so skipped by weighted_avg/stddev/corr, kept by sum/min/max
                row(w, ts++, "inf", 1, 2, 3, 4, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, (short) 1, (byte) 1);
                row(w, ts++, "inf", 2, 3, 4, 5, 1f, 2f, 3, 4, (short) 1, (byte) 1);
                row(w, ts++, "inf", 2, 0, 4, 0, 0f, 0f, 0, -0.0, (short) 1, (byte) 1);
                // signed zeros
                row(w, ts++, "zero", 0, 0, 0, 0, -0.0f, 0f, -0.0, -0.0, (short) 0, (byte) 0);
                row(w, ts++, "zero", 0, 0, 0, 0, -0.0f, -0.0f, -0.0, 0.0, (short) 0, (byte) 0);
                w.commit();
            }
            for (String arg : new String[]{"d", "d + e", "f / g", "i / j", "l / m", "i + j", "f::double", "d * 0"}) {
                assertMatchesRowPath("select k, " + String.format(AGGREGATES, arg) + " from t group by k order by k", true);
                assertMatchesRowPath("select " + String.format(AGGREGATES, arg) + " from t", true);
                assertMatchesRowPath("select " + String.format(AGGREGATES, arg) + " from t where k = 'late'", true);
                assertMatchesRowPath("select " + String.format(AGGREGATES, arg) + " from t where k = 'single'", true);
                assertMatchesRowPath("select " + String.format(AGGREGATES, arg) + " from t where k = 'none'", true);
            }
        });
    }

    @Test
    public void testExplain() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t", 100, 1, MODE_SPECIALS);
            String sql = "select k, weighted_avg(f, i) w from t group by k order by k";
            String expected = rowPathResult(sql);
            setKernels(true);
            assertQuery(sql)
                    .withPlanContaining("Async Group By workers: 1", "batchKernels: true")
                    .inferRandomAccess()
                    .expectSize()
                    .returns(expected);

            sql = "select stddev(d + e) sd from t";
            expected = rowPathResult(sql);
            setKernels(true);
            assertQuery(sql)
                    .withPlanContaining("Async Group By workers: 1", "vectorized: false", "batchKernels: true")
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns(expected);

            // a direct column of its own type keeps the existing loops of sum/avg/min/max
            sql = "select k, sum(d) s, avg(d) a, min(f) mn, max(f) mx from t group by k order by k";
            expected = rowPathResult(sql);
            setKernels(true);
            assertQuery(sql)
                    .withPlanNotContaining("batchKernels")
                    .inferRandomAccess()
                    .expectSize()
                    .returns(expected);

            // an argument the evaluator has no loop for (abs) keeps the row path
            sql = "select k, weighted_avg(abs(d), i) w, stddev(abs(f) + 1) r from t group by k order by k";
            expected = rowPathResult(sql);
            setKernels(true);
            assertQuery(sql)
                    .withPlanNotContaining("batchKernels")
                    .inferRandomAccess()
                    .expectSize()
                    .returns(expected);

            // the switch
            sql = "select k, weighted_avg(f, i) w from t group by k order by k";
            expected = rowPathResult(sql);
            assertQuery(sql)
                    .withPlanNotContaining("batchKernels")
                    .inferRandomAccess()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testFiltered() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t", ROW_COUNT, 5, MODE_SPECIALS);
            final String[] filters = {
                    "f > 0",
                    "k in ('K1', 'K2', 'K3', 'K5', 'K8', 'K13', 'K21', 'K34', 'K4', 'K6', 'K7', 'K9')",
                    "i % 3 = 0 and d < 100",
                    "k = 'K7'",
                    "k = 'missing'",
            };
            for (String filter : filters) {
                for (String arg : new String[]{"d", "f", "i * j", "(j * f + i * g) / (j + i)", "l::double", "d / e", "g - f"}) {
                    assertMatchesRowPath("select k, " + String.format(AGGREGATES, arg) + " from t where " + filter + " group by k order by k", true);
                    assertMatchesRowPath("select " + String.format(AGGREGATES, arg) + " from t where " + filter, true);
                }
            }
        });
    }

    @Test
    public void testHugeKeyCountWithoutWorkers() throws Exception {
        // every row its own group: every batch entry is new, and every row's argument value shows
        // in the result on its own, not folded into a group already NULL from another row. Past the
        // sharding threshold (1000 groups in the test configuration) the remaining rows of the
        // query take the sharded reduce on the row path, so this also checks that a kernel-then-
        // sharded mix within one query is exact; testShardedHeavy() covers the mostly sharded case.
        assertMemoryLeak(() -> {
            createTable("t", ROW_COUNT, 17, MODE_SPECIALS);
            for (String arg : new String[]{
                    "d + e", "f * g", "i - j", "(j * f + i * g) / (j + i)",
                    "d::float", "d::int", "d::long", "f::int", "f::long", "l::int", "l::float", "i::float",
                    "i / j", "l / m", "f / g", "d / e", "i * j", "l * m", "s + b", "b::float"
            }) {
                final long[] counts = assertMatchesRowPath("select ts, " + String.format(AGGREGATES, arg) + " from t group by ts order by ts", true);
                Assert.assertTrue(arg + ": kernel rows " + counts[3], counts[3] > 0 && counts[3] < ROW_COUNT);
                assertMatchesRowPath("select i, l, " + String.format(AGGREGATES, arg) + " from t group by i, l order by i, l", true);
            }
        });
    }

    @Test
    public void testKeyed() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t", ROW_COUNT, 42, MODE_SPECIALS);
            for (String arg : ARGS) {
                final String sql = "select k, " + String.format(AGGREGATES, arg) + " from t group by k order by k";
                Assert.assertTrue(sql, assertMatchesRowPath(sql, true)[0] > 0);
            }
        });
    }

    @Test
    public void testKeyedLargeOffsets() throws Exception {
        // epoch-scale values, on which the Welford states are poorly conditioned: the kernels must
        // reproduce the row path's rounding exactly, not improve or worsen it
        assertMemoryLeak(() -> {
            createTable("t", ROW_COUNT, 7, MODE_LARGE_OFFSETS);
            for (String arg : new String[]{"d", "e", "d + e", "d - e", "l::double", "d * 1.000001"}) {
                assertMatchesRowPath("select k, " + String.format(AGGREGATES, arg) + " from t group by k order by k", true);
                assertMatchesRowPath("select " + String.format(AGGREGATES, arg) + " from t", true);
            }
        });
    }

    @Test
    public void testLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t", ROW_COUNT, 9, MODE_SPECIALS);
            final String agg = String.format(AGGREGATES, "(j * f + i * g) / (j + i)");
            for (String limit : new String[]{"1", "5", "-3", "2, 6", "100"}) {
                assertMatchesRowPath("select k, " + agg + " from t group by k order by k limit " + limit, true);
                assertMatchesRowPath("select " + agg + " from t limit " + limit, true);
            }
        });
    }

    @Test
    public void testMixedWithVectorizedNotKeyed() throws Exception {
        // no filter, with aggregates of direct columns: the vectorized reducer runs computeBatch()
        // for those, the kernels for the rest, and the row path for the remaining ones
        assertMemoryLeak(() -> {
            createTable("t", ROW_COUNT, 23, MODE_SPECIALS);
            final String sql = "select sum(d) s, count(*) c, max(l) mxl, weighted_avg(f, i) w, stddev(d + e) sd, " +
                    "first(d) fd, last(i * j) li, count_distinct(i % 10) cd, sum(i - j) si from t";
            final String expected = rowPathResult(sql);
            setKernels(true);
            assertQuery(sql)
                    .withPlanContaining("vectorized: true", "batchKernels: true")
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns(expected);
            assertMatchesRowPath(sql, true);
            // only kernels and vectorized aggregates
            assertMatchesRowPath("select sum(d) s, count(*) c, weighted_avg(f, i) w, corr(d, e) c2 from t", true);
        });
    }

    @Test
    public void testNotKeyed() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t", ROW_COUNT, 43, MODE_SPECIALS);
            for (String arg : ARGS) {
                final String sql = "select " + String.format(AGGREGATES, arg) + " from t";
                Assert.assertTrue(sql, assertMatchesRowPath(sql, true)[0] > 0);
            }
        });
    }

    @Test
    public void testParquetFrames() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t", ROW_COUNT, 31, MODE_SPECIALS);
            execute("alter table t convert partition to parquet where ts < '1970-01-04'");
            for (String arg : new String[]{"d", "f", "d + e", "i * j", "(j * f + i * g) / (j + i)", "l::double", "s + b"}) {
                assertMatchesRowPath("select k, " + String.format(AGGREGATES, arg) + " from t group by k order by k", true);
                assertMatchesRowPath("select " + String.format(AGGREGATES, arg) + " from t", true);
                assertMatchesRowPath("select k, " + String.format(AGGREGATES, arg) + " from t where f > 0 group by k order by k", true);
                assertMatchesRowPath("select " + String.format(AGGREGATES, arg) + " from t where f > 0", true);
            }
        });
    }

    @Test
    public void testQueryTimeout() throws Exception {
        // a timeout between frames aborts a query that runs the kernels, without leaking memory
        assertMemoryLeak(() -> {
            final AtomicLong ticks = new AtomicLong();
            final long[] tripAfter = {Long.MAX_VALUE};
            final DefaultSqlExecutionCircuitBreakerConfiguration cbConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
                @Override
                @NotNull
                public MillisecondClock getClock() {
                    return () -> ticks.incrementAndGet() < tripAfter[0] ? 0 : Long.MAX_VALUE;
                }

                @Override
                public long getQueryTimeout() {
                    return 1;
                }
            };
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, context) -> {
                final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) context;
                ctx.changePageFrameSizes(1, 97);
                createTable(engine, ctx, "t", 100_000, 19, MODE_TAME);
                final String[] queries = {
                        "select k, weighted_avg(f, i) w, stddev_samp(d + e) sd from t group by k order by k",
                        "select weighted_avg(f, i) w, stddev_samp(d + e) sd from t",
                };
                for (String sql : queries) {
                    final NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine, cbConfiguration);
                    try {
                        ctx.with(ctx.getSecurityContext(), ctx.getBindVariableService(), ctx.getRandom(), ctx.getRequestFd(), circuitBreaker);
                        tripAfter[0] = Long.MAX_VALUE;
                        try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
                            Assert.assertNotNull(sql, kernelCountsOrNull(factory));
                            ticks.set(0);
                            tripAfter[0] = 2;
                            circuitBreaker.resetTimer();
                            try (RecordCursor cursor = factory.getCursor(ctx)) {
                                while (cursor.hasNext()) {
                                    // drain
                                }
                                Assert.fail("expected a timeout: " + sql);
                            } catch (Throwable e) {
                                TestUtils.assertContains(e.getMessage(), "timeout, query aborted");
                            }
                        }
                    } finally {
                        Misc.free(circuitBreaker);
                    }
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testSubclassOverridingRowMethodsHasNoKernel() {
        final VarSampleGroupByFunctionFactory.VarSampleGroupByFunction plain =
                new VarSampleGroupByFunctionFactory.VarSampleGroupByFunction(DoubleColumn.newInstance(0));
        Assert.assertTrue(GroupByBatchKernels.supportsKernel(plain));
        final VarSampleGroupByFunctionFactory.VarSampleGroupByFunction overriding =
                new VarSampleGroupByFunctionFactory.VarSampleGroupByFunction(DoubleColumn.newInstance(0)) {
                    @Override
                    public void computeNext(MapValue mapValue, Record record, long rowId) {
                        super.computeNext(mapValue, record, rowId);
                    }
                };
        Assert.assertFalse(GroupByBatchKernels.supportsKernel(overriding));
    }

    @Test
    public void testWorkerPool() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, context) -> {
                final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) context;
                ctx.changePageFrameSizes(1, 97);
                createTable(engine, ctx, "t", 20_000, 29, MODE_TAME);
                final String[] queries = {
                        "select k, " + String.format(AGGREGATES, "((j * f + i * g) / (j + i))::double") + " from t group by k order by k",
                        "select k, " + String.format(AGGREGATES, "d - e") + " from t where f > 0 group by k order by k",
                        "select " + String.format(AGGREGATES, "i * j") + " from t",
                        "select " + String.format(AGGREGATES, "f::double * 2") + " from t where k in ('K1', 'K2', 'K3')",
                        // many keys: past the sharding threshold the reduce goes per row through the shards
                        "select i % 5000 key, " + String.format(AGGREGATES, "d * 2") + " from t group by key order by key",
                };
                for (String sql : queries) {
                    setKernels(false);
                    Assert.assertFalse(sql, runToString(engine, compiler, ctx, "explain " + sql).contains("batchKernels"));
                    final String expected = runToString(engine, compiler, ctx, sql);
                    setKernels(true);
                    TestUtils.assertContains(runToString(engine, compiler, ctx, "explain " + sql), "batchKernels: true");
                    for (int run = 0; run < 3; run++) {
                        assertWithinTolerance(sql, expected, runToString(engine, compiler, ctx, sql));
                    }
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testDefaultFrameAndBatchSizes() throws Exception {
        // every other test uses 97-row frames and 61-row batches
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(configuration.getSqlPageFrameMinRows(), configuration.getSqlPageFrameMaxRows());
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_BATCH_SIZE, configuration.getGroupByBatchSize());
            createTable("t", 150_000, 53, MODE_SPECIALS);
            final String aggregates = "sum(%1$s) s1, avg(%1$s) a1, min(%1$s) mn, max(%1$s) mx, " +
                    "stddev_samp(%1$s) sd, var_pop(%1$s) vp, weighted_avg(%1$s, j) w1, weighted_avg(d, %1$s) w2, " +
                    "corr(%1$s, e) c1, covar_samp(g, %1$s) cs, covar_pop(%1$s, l) cp";
            for (String arg : new String[]{"d + e", "i * j", "(j * f + i * g) / (j + i)", "l::double", "s + b", "f * 2"}) {
                final String agg = String.format(aggregates, arg);
                assertMatchesRowPath("select k, " + agg + " from t group by k order by k", true);
                assertMatchesRowPath("select " + agg + " from t", true);
                assertMatchesRowPath("select " + agg + " from t where i > 0", true);
                assertMatchesRowPath("select k, " + agg + " from t where i > 0 group by k order by k", true);
            }
        });
    }

    @Test
    public void testOddArgumentShapes() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t", ROW_COUNT, 59, MODE_SPECIALS);
            final String[] queries = {
                    // one argument read with different getters
                    "select k, sum(i + j) a, avg(i + j) b, weighted_avg(i + j, i + j) c, max((i + j)::double) d2, min((i + j)::long) e2, stddev(i + j) f2 from t group by k order by k",
                    // the key column as the argument
                    "select j, sum(j * 2) a, avg(j + 0.5) b, stddev(j) c from t group by j order by j",
                    // constant-only and NULL-constant arguments
                    "select k, sum(1 + 2) a, avg(3.5) b, weighted_avg(d, 2) c, stddev(d + null) sd, sum(i + null) sn, max(f * null) mf from t group by k order by k",
                    "select sum(1 + 2) a, avg(3.5) b, weighted_avg(d, 2) c, stddev(d + null) sd, sum(i + null) sn from t",
                    // overflow and division by zero
                    "select k, sum(l * 3037000500) a, sum(i * 46341) b, sum(l / 0) c, avg(d / 0) dd, sum(i / 0) e2, max(l - l) mx from t group by k order by k",
                    // SHORT x BYTE
                    "select k, corr(s, b) c, covar_pop(b, s) cp, weighted_avg(s::double, b) w, sum(s * b) sm, sum(b::long + 1) sb from t group by k order by k",
                    // the same expression under a filter, next to a direct column
                    "select k, max(d) filler, max(d * 1) a, min(d * 1) b, sum(d * 1) c from t where d > 0 group by k order by k",
            };
            for (String sql : queries) {
                assertMatchesRowPath(sql, true);
            }
            // negation has no loop: the row path, bit for bit
            assertMatchesRowPath("select k, sum(-i) a, sum(i - -j) b, max(-d) c, min(-(f)) mn from t group by k order by k", false);
        });
    }

    @Test
    public void testParquetLateMaterializedFramesTakeRowPath() throws Exception {
        // a selective filter over Parquet frames: late materialization decodes the remaining columns
        // for the filtered rows only, and such frames keep the row path (counted per frame)
        assertMemoryLeak(() -> {
            createTable("t", ROW_COUNT, 61, MODE_SPECIALS);
            execute("alter table t convert partition to parquet where ts < '1970-01-04'");
            for (String arg : new String[]{"d + e", "i * j", "l::double"}) {
                final String agg = String.format(AGGREGATES, arg);
                for (String sql : new String[]{
                        "select k, " + agg + " from t where k = 'K3' group by k order by k",
                        "select " + agg + " from t where k = 'K3'",
                }) {
                    final long[] counts = assertMatchesRowPath(sql, true);
                    Assert.assertTrue(sql + ": late-materialized frames " + counts[2], counts[2] > 0);
                    // the native partitions run the kernels
                    Assert.assertTrue(sql, counts[0] > 0);
                }
            }
        });
    }

    @Test
    public void testParquetTypeCastFramesTakeRowPath() throws Exception {
        // a VARCHAR column converted to DOUBLE after its partitions went to Parquet: those frames
        // decode the VARCHAR and convert per row, so the column has no readable buffer there and the
        // aggregate takes the row path for those batches (counted); the native partitions do not
        assertMemoryLeak(() -> {
            createTable("t", ROW_COUNT, 67, MODE_TAME);
            execute("alter table t add column v varchar");
            execute("update t set v = case when i % 7 = 0 then null else (i / 8.0)::varchar end");
            execute("alter table t convert partition to parquet where ts < '1970-01-04'");
            execute("alter table t alter column v type double");
            for (String arg : new String[]{"v", "v * 2", "v + d", "(v - e)::long"}) {
                final String agg = String.format(AGGREGATES, arg);
                for (String sql : new String[]{
                        "select k, " + agg + " from t group by k order by k",
                        "select " + agg + " from t",
                        "select k, " + agg + " from t where i > 0 group by k order by k",
                        "select " + agg + " from t where i > 0",
                }) {
                    final long[] counts = assertMatchesRowPath(sql, true);
                    // per batch, or per frame in the vectorized non-keyed reduce (sum(v) and the
                    // other direct-column aggregates make the unfiltered non-keyed query vectorized)
                    Assert.assertTrue(sql + ": row-path batches " + counts[1] + ", frames " + counts[2], counts[1] > 0 || counts[2] > 0);
                    Assert.assertTrue(sql + ": kernel batches " + counts[0], counts[0] > 0);
                }
            }
        });
    }

    @Test
    public void testRebindSameFactory() throws Exception {
        // one compiled factory executed again with new bind values: the kernels read the binds
        // through their getters per batch, not once per compile
        assertMemoryLeak(() -> {
            createTable("t", ROW_COUNT, 71, MODE_SPECIALS);
            final String[] queries = {
                    "select k, weighted_avg(d * $1, i + $2) w, stddev_samp(f - $1) sd, sum(i * $2) s, max(l + $2) mx from t group by k order by k",
                    "select weighted_avg(d * $1, i + $2) w, stddev_samp(f - $1) sd, sum(i * $2) s, max(l + $2) mx from t",
            };
            for (String sql : queries) {
                bindVariableService.clear();
                bindVariableService.setDouble(0, 1.5);
                bindVariableService.setInt(1, 3);
                setKernels(false);
                try (RecordCursorFactory rowPath = select(sql)) {
                    Assert.assertNull(sql, kernelCountsOrNull(rowPath));
                    setKernels(true);
                    try (RecordCursorFactory kernels = select(sql)) {
                        Assert.assertNotNull(sql, kernelCountsOrNull(kernels));
                        for (int round = 0; round < 4; round++) {
                            bindVariableService.setDouble(0, round == 3 ? Double.NaN : 1.5 + round);
                            bindVariableService.setInt(1, round == 2 ? Numbers.INT_NULL : 3 - round);
                            TestUtils.assertEquals(sql + " round " + round, bits(rowPath, sqlExecutionContext), bits(kernels, sqlExecutionContext));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testScratchCapFallsBackToRowPath() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t", 500, 73, MODE_SPECIALS);
            final String sql = "select k, weighted_avg(d * 2, i + j) w, stddev(d - e) sd from t group by k order by k";
            final long saved = GroupByBatchKernels.getMaxScratchBytes();
            try {
                setKernels(true);
                final long need;
                try (RecordCursorFactory factory = select(sql)) {
                    final GroupByBatchKernels kernels = findAsync(factory, AsyncGroupByRecordCursorFactory.class).getAtom().getBatchKernels(-1);
                    Assert.assertNotNull(kernels);
                    need = kernels.getProgram().getScratchBytes();
                    Assert.assertTrue(need > 0);
                }
                // just below what the owner and the workers would need together
                GroupByBatchKernels.setMaxScratchBytes(need - 1);
                assertMatchesRowPath(sql, false);
                GroupByBatchKernels.setMaxScratchBytes(need * 1024);
                assertMatchesRowPath(sql, true);
            } finally {
                GroupByBatchKernels.setMaxScratchBytes(saved);
            }
        });
    }

    @Test
    public void testScratchIsNativeAndFreedOnCursorClose() throws Exception {
        // the per-slot argument buffers are native memory, allocated on first use, charged to the
        // tag (so assertMemoryLeak sees them), and freed when the cursor closes even though the
        // factory, as in a cache, stays open
        assertMemoryLeak(() -> {
            createTable("t", ROW_COUNT, 79, MODE_SPECIALS);
            setKernels(true);
            for (String sql : new String[]{
                    "select k, weighted_avg(d * 2, i + j) w, stddev(d - e) sd, sum(i * j) s from t group by k",
                    "select weighted_avg(d * 2, i + j) w, stddev(d - e) sd, sum(i * j) s from t",
                    "select weighted_avg(d * 2, i + j) w, stddev(d - e) sd, sum(i * j) s from t where i > 0",
            }) {
                try (RecordCursorFactory factory = select(sql)) {
                    final GroupByBatchKernels kernels = ownerKernels(factory);
                    Assert.assertNotNull(sql, kernels);
                    Assert.assertEquals(sql, 0, kernels.getAllocatedScratchBytes());
                    for (int run = 0; run < 2; run++) {
                        final long tagBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(sql, cursor.hasNext());
                            final long allocated = kernels.getAllocatedScratchBytes();
                            Assert.assertEquals(sql, kernels.getProgram().getScratchBytes(), allocated);
                            Assert.assertTrue(sql, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION) - tagBefore >= allocated);
                        }
                        Assert.assertEquals(sql, 0, kernels.getAllocatedScratchBytes());
                    }
                }
            }
        });
    }

    @Test
    public void testShardedHeavy() throws Exception {
        // every row its own group, far past the sharding threshold: most rows go through the
        // sharded reduce, which keeps the row path, after the first frames ran the kernels
        assertMemoryLeak(() -> {
            final int rows = 20_000;
            createTable("t", rows, 83, MODE_SPECIALS);
            Assert.assertTrue(configuration.getGroupByShardingThreshold() < rows / 10);
            for (String arg : new String[]{"d + e", "i * j", "f::double", "l / m"}) {
                final long[] counts = assertMatchesRowPath("select ts, " + String.format(AGGREGATES, arg) + " from t group by ts order by ts", true);
                Assert.assertTrue(arg + ": kernel rows " + counts[3], counts[3] > 0);
                Assert.assertTrue(arg + ": kernel rows " + counts[3], counts[3] < rows / 5);
            }
        });
    }

    @Test
    public void testStructureMismatchHasNoKernels() {
        // a slot's aggregates must have the shape the program was compiled from
        final ObjList<GroupByFunction> owner = new ObjList<>();
        owner.add(new VarSampleGroupByFunctionFactory.VarSampleGroupByFunction(DoubleColumn.newInstance(0)));
        final GroupByBatchKernels.Program program = GroupByBatchKernels.compile(owner, 64, 1);
        Assert.assertNotNull(program);
        Assert.assertNotNull(program.newInstance(owner));
        final ObjList<GroupByFunction> otherColumn = new ObjList<>();
        otherColumn.add(new VarSampleGroupByFunctionFactory.VarSampleGroupByFunction(DoubleColumn.newInstance(1)));
        Assert.assertNull(program.newInstance(otherColumn));
        final ObjList<GroupByFunction> otherClass = new ObjList<>();
        otherClass.add(new SumDoubleGroupByFunction(DoubleColumn.newInstance(0)));
        Assert.assertNull(program.newInstance(otherClass));
        Assert.assertNull(program.newInstance(new ObjList<>()));
    }

    @Test
    public void testSubclassOverridingAggregateHasNoKernel() {
        // the kernels inline aggregate(): a subclass with its own keeps the row path
        final VarSampleGroupByFunctionFactory.VarSampleGroupByFunction variance =
                new VarSampleGroupByFunctionFactory.VarSampleGroupByFunction(DoubleColumn.newInstance(0)) {
                    @Override
                    protected void aggregate(MapValue mapValue, double value) {
                        super.aggregate(mapValue, value);
                    }
                };
        Assert.assertFalse(GroupByBatchKernels.supportsKernel(variance));
        final CovarSampleGroupByFunctionFactory.CovarSampleGroupByFunction covar =
                new CovarSampleGroupByFunctionFactory.CovarSampleGroupByFunction(DoubleColumn.newInstance(0), DoubleColumn.newInstance(1)) {
                    @Override
                    protected void aggregate(MapValue mapValue, double y, double x) {
                        super.aggregate(mapValue, y, x);
                    }
                };
        Assert.assertFalse(GroupByBatchKernels.supportsKernel(covar));
        Assert.assertTrue(GroupByBatchKernels.supportsKernel(
                new CovarSampleGroupByFunctionFactory.CovarSampleGroupByFunction(DoubleColumn.newInstance(0), DoubleColumn.newInstance(1)) {
                }
        ));
    }

    @Test
    public void testSubclassOverridingComputeKeyedBatchHasNoKernel() {
        // the keyed kernel mirrors computeKeyedBatch(), the keyed row path: a subclass with its own
        // keeps the row path
        final VarSampleGroupByFunctionFactory.VarSampleGroupByFunction variance =
                new VarSampleGroupByFunctionFactory.VarSampleGroupByFunction(DoubleColumn.newInstance(0)) {
                    @Override
                    public void computeKeyedBatch(PageFrameMemoryRecord record, FlyweightPackedMapValue mapValue, long baseValueAddr, long batchAddr, long rowCount, long baseRowId) {
                        super.computeKeyedBatch(record, mapValue, baseValueAddr, batchAddr, rowCount, baseRowId);
                    }
                };
        Assert.assertFalse(GroupByBatchKernels.supportsKernel(variance));
        final SumDoubleGroupByFunction sum = new SumDoubleGroupByFunction(FloatColumn.newInstance(0)) {
            @Override
            public void computeKeyedBatch(PageFrameMemoryRecord record, FlyweightPackedMapValue mapValue, long baseValueAddr, long batchAddr, long rowCount, long baseRowId) {
                super.computeKeyedBatch(record, mapValue, baseValueAddr, batchAddr, rowCount, baseRowId);
            }
        };
        Assert.assertFalse(GroupByBatchKernels.supportsKernel(sum));
        // a FLOAT column read as DOUBLE: sum(double) has a kernel for it
        Assert.assertTrue(GroupByBatchKernels.supportsKernel(new SumDoubleGroupByFunction(FloatColumn.newInstance(0))));
    }

    @Test
    public void testWorkersShareOneProgram() throws Exception {
        // the kernels compile once per factory: every slot runs the owner's program, and a worker's
        // evaluator is made on the slot's first use
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, context) -> {
                final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) context;
                createTable(engine, ctx, "t", 1_000, 89, MODE_TAME);
                setKernels(true);
                for (String sql : new String[]{
                        "select k, weighted_avg((j * f + i * g) / (j + i), j + i) w, stddev_samp(d - e) sd, corr(d - e, f * g) c, sum(i * $1) s from t group by k",
                        "select weighted_avg((j * f + i * g) / (j + i), j + i) w, stddev_samp(d - e) sd, corr(d - e, f * g) c, sum(i * $1) s from t",
                        // count_distinct is not thread-safe: the workers get their own aggregates
                        "select k, count_distinct(i) cd, stddev_samp(d - e + $1) sd from t group by k",
                        "select count_distinct(i) cd, stddev_samp(d - e + $1) sd from t",
                }) {
                    ctx.getBindVariableService().clear();
                    ctx.getBindVariableService().setInt(0, 3);
                    try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
                        final GroupByBatchKernels owner = ownerKernels(factory);
                        Assert.assertNotNull(sql, owner);
                        final GroupByBatchKernels.PerWorker perWorker = perWorkerKernels(factory);
                        final int workerCount = perWorker.size();
                        Assert.assertTrue(sql, workerCount > 0);
                        // compiling made no worker evaluator: each is made on its slot's first use
                        for (int i = 0; i < workerCount; i++) {
                            Assert.assertNull(sql, perWorker.getIfMade(i));
                        }
                        for (int i = 0; i < workerCount; i++) {
                            final GroupByBatchKernels worker = workerKernels(factory, i);
                            Assert.assertNotNull(sql, worker);
                            Assert.assertNotSame(sql, owner, worker);
                            Assert.assertSame(sql, owner.getProgram(), worker.getProgram());
                        }
                    }
                }
            }, configuration, LOG);
        });
    }

    private static void assertWithinTolerance(String sql, String expected, String actual) {
        final String[] e = expected.split("\n");
        final String[] a = actual.split("\n");
        Assert.assertEquals(sql, e.length, a.length);
        for (int r = 0; r < e.length; r++) {
            final String[] ec = e[r].split("\t", -1);
            final String[] ac = a[r].split("\t", -1);
            Assert.assertEquals(sql, ec.length, ac.length);
            for (int c = 0; c < ec.length; c++) {
                if (ec[c].equals(ac[c])) {
                    continue;
                }
                final double x;
                final double y;
                try {
                    x = Double.parseDouble(ec[c]);
                    y = Double.parseDouble(ac[c]);
                } catch (NumberFormatException ex) {
                    Assert.fail(sql + ": row " + r + " col " + c + ": " + ec[c] + " vs " + ac[c]);
                    return;
                }
                final double diff = Math.abs(x - y);
                Assert.assertTrue(
                        sql + ": row " + r + " col " + c + ": " + ec[c] + " vs " + ac[c],
                        diff <= 1e-9 || diff <= 1e-9 * Math.max(Math.abs(x), Math.abs(y))
                );
            }
        }
    }

    // raw bits of every value, so that -0.0 differs from 0.0 and NaNs are compared exactly
    private static String bits(RecordCursorFactory factory, SqlExecutionContext ctx) throws SqlException {
        final StringSink sink = new StringSink();
        final RecordMetadata metadata = factory.getMetadata();
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                for (int c = 0, n = metadata.getColumnCount(); c < n; c++) {
                    switch (ColumnType.tagOf(metadata.getColumnType(c))) {
                        case ColumnType.DOUBLE:
                            sink.put(Long.toHexString(Double.doubleToRawLongBits(record.getDouble(c))));
                            break;
                        case ColumnType.FLOAT:
                            sink.put(Integer.toHexString(Float.floatToRawIntBits(record.getFloat(c))));
                            break;
                        case ColumnType.INT:
                            sink.put(record.getInt(c));
                            break;
                        case ColumnType.LONG:
                            sink.put(record.getLong(c));
                            break;
                        case ColumnType.TIMESTAMP:
                            sink.put(record.getTimestamp(c));
                            break;
                        case ColumnType.SHORT:
                            sink.put(record.getShort(c));
                            break;
                        case ColumnType.BYTE:
                            sink.put(record.getByte(c));
                            break;
                        case ColumnType.SYMBOL:
                            sink.put(record.getSymA(c));
                            break;
                        default:
                            sink.put("type:").put(ColumnType.nameOf(metadata.getColumnType(c)));
                            break;
                    }
                    sink.put('\t');
                }
                sink.put('\n');
            }
        }
        return sink.toString();
    }

    private static void createTable(CairoEngine engine, SqlExecutionContext ctx, String name, int rows, long seed, int mode) throws SqlException {
        engine.execute(
                "create table " + name + " (k symbol, i int, j int, l long, m long, f float, g float, d double, e double, s short, b byte, ts timestamp)" +
                        " timestamp(ts) partition by day bypass wal",
                ctx
        );
        final Rnd rnd = new Rnd(seed, seed * 31 + 7);
        final long step = 5L * 86_400_000_000L / rows;
        try (TableWriter w = getWriter(engine, name)) {
            for (int r = 0; r < rows; r++) {
                final TableWriter.Row row = w.newRow(r * step);
                if (rnd.nextInt(30) == 0) {
                    row.putSym(0, null);
                } else {
                    row.putSym(0, "K" + rnd.nextInt(37));
                }
                row.putInt(1, anInt(rnd, mode));
                row.putInt(2, anInt(rnd, mode));
                row.putLong(3, aLong(rnd, mode));
                row.putLong(4, aLong(rnd, mode));
                row.putFloat(5, aFloat(rnd, mode));
                row.putFloat(6, aFloat(rnd, mode));
                row.putDouble(7, aDouble(rnd, mode));
                row.putDouble(8, aDouble(rnd, mode));
                row.putShort(9, aShort(rnd));
                row.putByte(10, aByte(rnd));
                row.append();
            }
            w.commit();
        }
    }

    private static void createTable(String name, int rows, long seed, int mode) throws SqlException {
        createTable(engine, sqlExecutionContext, name, rows, seed, mode);
    }

    private static <T> T findAsync(RecordCursorFactory factory, Class<T> clazz) {
        RecordCursorFactory f = factory;
        while (f != null) {
            if (clazz.isInstance(f)) {
                return clazz.cast(f);
            }
            f = f.getBaseFactory();
        }
        return null;
    }

    private static long[] kernelCountsOrNull(RecordCursorFactory factory) {
        final AsyncGroupByRecordCursorFactory keyed = findAsync(factory, AsyncGroupByRecordCursorFactory.class);
        if (keyed != null) {
            return keyed.getAtom().getBatchKernels(-1) != null ? keyed.getAtom().getBatchKernelCounts() : null;
        }
        final AsyncGroupByNotKeyedRecordCursorFactory notKeyed = findAsync(factory, AsyncGroupByNotKeyedRecordCursorFactory.class);
        return notKeyed != null && notKeyed.getAtom().getBatchKernels(-1) != null ? notKeyed.getAtom().getBatchKernelCounts() : null;
    }

    private static long[] kernelCounts(RecordCursorFactory factory) {
        final AsyncGroupByRecordCursorFactory keyed = findAsync(factory, AsyncGroupByRecordCursorFactory.class);
        if (keyed != null) {
            return keyed.getAtom().getBatchKernelCounts();
        }
        final AsyncGroupByNotKeyedRecordCursorFactory notKeyed = findAsync(factory, AsyncGroupByNotKeyedRecordCursorFactory.class);
        Assert.assertNotNull("no parallel GROUP BY in the plan", notKeyed);
        return notKeyed.getAtom().getBatchKernelCounts();
    }

    private static GroupByBatchKernels ownerKernels(RecordCursorFactory factory) {
        final AsyncGroupByRecordCursorFactory keyed = findAsync(factory, AsyncGroupByRecordCursorFactory.class);
        if (keyed != null) {
            return keyed.getAtom().getBatchKernels(-1);
        }
        final AsyncGroupByNotKeyedRecordCursorFactory notKeyed = findAsync(factory, AsyncGroupByNotKeyedRecordCursorFactory.class);
        Assert.assertNotNull("no parallel GROUP BY in the plan", notKeyed);
        return notKeyed.getAtom().getBatchKernels(-1);
    }

    private static String runToString(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext ctx, String sql) throws SqlException {
        final StringSink sink = new StringSink();
        try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                CursorPrinter.println(cursor, factory.getMetadata(), sink);
            }
        }
        return sink.toString();
    }

    private static void row(TableWriter w, long ts, String k, int i, int j, long l, long m, float f, float g, double d, double e, short s, byte b) {
        final TableWriter.Row row = w.newRow(ts);
        row.putSym(0, k);
        row.putInt(1, i);
        row.putInt(2, j);
        row.putLong(3, l);
        row.putLong(4, m);
        row.putFloat(5, f);
        row.putFloat(6, g);
        row.putDouble(7, d);
        row.putDouble(8, e);
        row.putShort(9, s);
        row.putByte(10, b);
        row.append();
    }

    private static void setKernels(boolean enabled) {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_BATCH_KERNELS_ENABLED, String.valueOf(enabled));
    }

    private static int anInt(Rnd rnd, int mode) {
        if (mode == MODE_TAME) {
            return rnd.nextInt(20) == 0 ? Numbers.INT_NULL : rnd.nextInt(2000) - 1000;
        }
        switch (rnd.nextInt(14)) {
            case 0:
                return Numbers.INT_NULL;
            case 1:
                return Integer.MAX_VALUE;
            case 2:
                return Integer.MIN_VALUE + 1;
            case 3:
                return 0;
            case 4:
                return 1 << 30;
            case 5:
                return 46341;
            case 6:
                return rnd.nextInt();
            default:
                return rnd.nextInt(2000) - 1000;
        }
    }

    private static long aLong(Rnd rnd, int mode) {
        if (mode == MODE_TAME) {
            return rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : rnd.nextInt(2_000_000) - 1_000_000;
        }
        switch (rnd.nextInt(14)) {
            case 0:
                return Numbers.LONG_NULL;
            case 1:
                return Long.MAX_VALUE;
            case 2:
                return Long.MIN_VALUE + 1;
            case 3:
                return 0;
            case 4:
                return 3037000500L;
            case 5:
                return 1L << 62;
            case 6:
                return rnd.nextLong();
            default:
                return rnd.nextInt(2_000_000) - 1_000_000;
        }
    }

    private static float aFloat(Rnd rnd, int mode) {
        if (mode == MODE_TAME) {
            return rnd.nextInt(20) == 0 ? Float.NaN : rnd.nextFloat() * 200 - 100;
        }
        switch (rnd.nextInt(18)) {
            case 0:
                return Float.NaN;
            case 1:
                return Float.POSITIVE_INFINITY;
            case 2:
                return Float.NEGATIVE_INFINITY;
            case 3:
                return -0.0f;
            case 4:
                return 0f;
            case 5:
                return Float.MAX_VALUE;
            case 6:
                return Float.MIN_VALUE;
            case 7:
                return 3e9f;
            default:
                return rnd.nextFloat() * 200 - 100;
        }
    }

    private static double aDouble(Rnd rnd, int mode) {
        if (mode == MODE_LARGE_OFFSETS) {
            return rnd.nextInt(20) == 0 ? Double.NaN : 4e15 + rnd.nextDouble() * 1000;
        }
        if (mode == MODE_TAME) {
            return rnd.nextInt(20) == 0 ? Double.NaN : rnd.nextDouble() * 2000 - 1000;
        }
        switch (rnd.nextInt(18)) {
            case 0:
                return Double.NaN;
            case 1:
                return Double.POSITIVE_INFINITY;
            case 2:
                return Double.NEGATIVE_INFINITY;
            case 3:
                return -0.0;
            case 4:
                return 0;
            case 5:
                return Double.MAX_VALUE;
            case 6:
                return Double.MIN_VALUE;
            case 7:
                return 1e300;
            case 8:
                return 3e10;
            default:
                return rnd.nextDouble() * 2000 - 1000;
        }
    }

    private static short aShort(Rnd rnd) {
        switch (rnd.nextInt(10)) {
            case 0:
                return Short.MIN_VALUE;
            case 1:
                return Short.MAX_VALUE;
            case 2:
                return 0;
            default:
                return rnd.nextShort();
        }
    }

    private static byte aByte(Rnd rnd) {
        switch (rnd.nextInt(10)) {
            case 0:
                return Byte.MIN_VALUE;
            case 1:
                return Byte.MAX_VALUE;
            case 2:
                return 0;
            default:
                return (byte) rnd.nextInt();
        }
    }

    private static GroupByBatchKernels.PerWorker perWorkerKernels(RecordCursorFactory factory) {
        final AsyncGroupByRecordCursorFactory keyed = findAsync(factory, AsyncGroupByRecordCursorFactory.class);
        if (keyed != null) {
            return keyed.getAtom().getPerWorkerBatchKernels();
        }
        return findAsync(factory, AsyncGroupByNotKeyedRecordCursorFactory.class).getAtom().getPerWorkerBatchKernels();
    }

    private static GroupByBatchKernels workerKernels(RecordCursorFactory factory, int slotId) {
        final AsyncGroupByRecordCursorFactory keyed = findAsync(factory, AsyncGroupByRecordCursorFactory.class);
        if (keyed != null) {
            return keyed.getAtom().getBatchKernels(slotId);
        }
        return findAsync(factory, AsyncGroupByNotKeyedRecordCursorFactory.class).getAtom().getBatchKernels(slotId);
    }

    /**
     * Runs the query on the row path, then with the kernels, and asserts the same text through the
     * fluent assertion and the same raw bits value by value. With {@code expectKernels}, the plan
     * must show them and they must have run.
     *
     * @return the counts of the kernel run, see {@code getBatchKernelCounts()}: [kernel batches,
     * row-path batches, late-materialized frames kept on the row path, rows in kernel batches]
     */
    private long[] assertMatchesRowPath(String sql, boolean expectKernels) throws Exception {
        final String expected = rowPathResult(sql);
        final String expectedBits;
        try (RecordCursorFactory factory = select(sql)) {
            expectedBits = bits(factory, sqlExecutionContext);
        }
        setKernels(true);
        final QueryAssertion assertion = assertQuery(sql).inferRandomAccess().inferTimestamp().sizeMayVary();
        if (expectKernels) {
            assertion.withPlanContaining("batchKernels: true");
        } else {
            assertion.withPlanNotContaining("batchKernels");
        }
        assertion.returns(expected);
        try (RecordCursorFactory factory = select(sql)) {
            TestUtils.assertEquals(sql, expectedBits, bits(factory, sqlExecutionContext));
            return kernelCounts(factory);
        }
    }

    private String rowPathResult(String sql) throws SqlException {
        setKernels(false);
        final StringSink sink = new StringSink();
        try (RecordCursorFactory factory = select(sql)) {
            final StringSink plan = new StringSink();
            printSql("explain " + sql, plan);
            Assert.assertFalse(sql, plan.toString().contains("batchKernels"));
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                CursorPrinter.println(cursor, factory.getMetadata(), sink);
            }
        }
        return sink.toString();
    }
}
