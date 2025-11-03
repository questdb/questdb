/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetVersion;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.jit.JitUtil;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Files;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT;

public class ParallelFilterTest extends AbstractCairoTest {
    private static final int PAGE_FRAME_COUNT = 4; // also used to set queue size, so must be a power of 2
    private static final int PAGE_FRAME_MAX_ROWS = 100;
    private static final int ROW_COUNT = PAGE_FRAME_COUNT * PAGE_FRAME_MAX_ROWS;
    private static final String expectedNegativeLimit = """
            v
            4039070554630775695
            3424747151763089683
            4290477379978201771
            3794031607724753525
            3745605868208843008
            3464609208866088600
            3513816850827798261
            3542505137180114151
            4169687421984608700
            3433721896286859656
            """;
    private static final String expectedPositiveLimit = """
            v
            3394168647660478011
            4086802474270249591
            3958193676455060057
            3619114107112892010
            3705833798044144433
            4238042693748641409
            3518554007419864093
            4014104627539596639
            3393210801760647293
            4099611147050818391
            """;
    private static final String expectedSymbolInCursorQueryLimit = """
            v\ts
            -9200716729349404576\tC
            -9199187235976552080\tC
            -9128506055317587235\tB
            -8960406850507339854\tC
            -8955092533521658248\tC
            -8930904012891908076\tC
            -8906871108655466881\tB
            -8889930662239044040\tC
            -8888027247206813045\tC
            -8845171937548005347\tC
            """;
    private static final String expectedSymbolNoLimit = """
            v
            3393210801760647293
            3394168647660478011
            3424747151763089683
            3433721896286859656
            3464609208866088600
            3513816850827798261
            3518554007419864093
            3542505137180114151
            3619114107112892010
            3669882909701240516
            3705833798044144433
            3745605868208843008
            3771494396743411509
            3794031607724753525
            3820631780839257855
            3944678179613436885
            3958193676455060057
            4014104627539596639
            4039070554630775695
            4086802474270249591
            4099611147050818391
            4160567228070722087
            4169687421984608700
            4238042693748641409
            4290477379978201771
            """;
    private static final String expectedVarcharNoLimit = """
            l\tv
            3342946432339528961\t^̈RɗT\uDBCE\uDF3F\u008E
            3349345766605440862\t
            3394168647660478011\t)|1u%2uL>gG8#3Ts
            3401443869949416748\t
            3433721896286859656\t?[麛P\uD9E8\uDEDE\uD931\uDF48ҽ\uDA01\uDE60
            3523446414305966840\t7yXx>K%H[ g0nHS\\
            3547449704743013886\tfT)D>XP?dYL
            3570175762165818271\t97hVO
            3571824131493518678\tA왋G&ُܵ9}\uD91F\uDCE8+\uDAAF\uDC59\uDAC8\uDE3B亲
            3585172908882940409\t
            3682423623919780100\tF~)xNm\\~Fwz;gR.G
            3739186870210598690\tB&LA.&g
            3780956794407111569\t
            3921912097533020222\tmH%/###`3g?
            3927079694554322589\tg\uECF9J9漫\uDBDB\uDDDB1fÄ}o輖NI
            4171842711013652287\t
            4290056275098552124\t=ܼDdjvsoߛ)*EB
            """;
    private static final String symbolInCursorQueryLimit = "select v, s from x where s::string::symbol in (select rnd_varchar('C', 'B') from long_sequence(100)) order by v limit 10";
    private static final String symbolQueryNegativeLimit = "select v from x where v > 3326086085493629941L and v < 4326086085493629941L limit -10";
    private static final String symbolQueryNoLimit = "select v from x where v > 3326086085493629941L and v < 4326086085493629941L order by v";
    private static final String symbolQueryPositiveLimit = "select v from x where v > 3326086085493629941L and v < 4326086085493629941L limit 10";
    private static final String symbolStaticInCursorQueryLimit = "select v, s from x where s in (select rnd_varchar('C', 'B') from long_sequence(100)) order by v limit 10";
    private static final String varcharQueryNoLimit = "select l, v from x where l > 3326086085493629941L and l < 4326086085493629941L order by l";
    private final boolean convertToParquet;

    public ParallelFilterTest() {
        this.convertToParquet = TestUtils.generateRandom(LOG).nextBoolean();
    }

    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, PAGE_FRAME_MAX_ROWS);
        // We intentionally use small values for shard count and reduce
        // queue capacity to exhibit various edge cases.
        setProperty(CAIRO_PAGE_FRAME_SHARD_COUNT, 2);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, PAGE_FRAME_COUNT);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        super.setUp();
        inputRoot = root;
    }

    @Test
    public void testArrayFilter() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "CREATE TABLE x (" +
                                    "  ts TIMESTAMP," +
                                    "  a double[][], " +
                                    "  b double[][] " +
                                    ") timestamp (ts) PARTITION BY DAY;",
                            sqlExecutionContext
                    );
                    engine.execute("insert into x select x::timestamp, rnd_double_array(2), rnd_double_array(2) from long_sequence(50000)", sqlExecutionContext);
                    engine.execute("insert into x values (50001, ARRAY[[1,1],[2,2]], ARRAY[[1,1],[2,2]])", sqlExecutionContext);

                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "select count() from x where a = b",
                            sink,
                            "count\n1\n"
                    );
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testAsyncOffloadNegativeLimitTimeoutWithJitEnabled() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(SqlJitMode.JIT_MODE_ENABLED));
        testAsyncOffloadTimeout("select s from x where v > 3326086085493629941L and v < 4326086085493629941L and s = 'A' limit -100");
    }

    @Test
    public void testAsyncOffloadNegativeLimitTimeoutWithoutJit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(SqlJitMode.JIT_MODE_DISABLED));
        testAsyncOffloadTimeout("select s from x where v > 3326086085493629941L and v < 4326086085493629941L and s = 'A' limit -100");
    }

    @Test
    public void testAsyncOffloadTimeoutWithJitEnabled() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(SqlJitMode.JIT_MODE_ENABLED));
        testAsyncOffloadTimeout("x where v > 3326086085493629941L and v < 4326086085493629941L and s = 'A' order by v");
    }

    @Test
    public void testAsyncOffloadTimeoutWithoutJit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(SqlJitMode.JIT_MODE_DISABLED));
        testAsyncOffloadTimeout("x where v > 3326086085493629941L and v < 4326086085493629941L and s = 'A' order by v");
    }

    @Test
    public void testAsyncSubQueryWithFilterOnIndexedSymbol() throws Exception {
        testAsyncSubQueryWithFilter(
                "SELECT count(*) " +
                        "FROM price " +
                        "WHERE type IN (SELECT id FROM mapping WHERE ext in ('s1', 's2') and substring(ext,2,1) = '1')"
        );
    }

    @Test
    public void testAsyncSubQueryWithFilterOnLatestBy() throws Exception {
        testAsyncSubQueryWithFilter(
                "SELECT count(*) " +
                        "FROM price " +
                        "WHERE type IN (SELECT id FROM mapping WHERE ext in ('s1'))"
        );
    }

    @Test
    public void testAsyncSubQueryWithFilterOnSymbol() throws Exception {
        testAsyncSubQueryWithFilter(
                "SELECT count(*) " +
                        "FROM price " +
                        "WHERE type IN (SELECT id FROM mapping WHERE ext in ('s1'))"
        );
    }

    @Test
    public void testAsyncTimestampSubQueryWithEqFilter() throws Exception {
        String query = "select * from x where ts2 = (select min(ts) from x)";
        String expected = """
                ts\tts2\tid
                1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000001Z\t1
                """;

        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "CREATE TABLE x (" +
                                    "  ts TIMESTAMP," +
                                    "  ts2 TIMESTAMP," +
                                    "  id long" +
                                    ") timestamp (ts) PARTITION BY DAY;",
                            sqlExecutionContext
                    );
                    engine.execute("insert into x select x::timestamp, x::timestamp, x from long_sequence(100000)", sqlExecutionContext);

                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            query,
                            sink,
                            expected
                    );
                },
                configuration,
                LOG
        );

    }

    @Test
    public void testEarlyCursorClose() throws Exception {
        // This scenario used to lead to an NPE on `circuitBreaker.cancelledFlag` access in PageFrameReduceJob.
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "CREATE TABLE x (" +
                                    "  ts TIMESTAMP," +
                                    "  id long" +
                                    ") timestamp (ts) PARTITION BY DAY;",
                            sqlExecutionContext
                    );
                    // We want a lot of page frames, so that reduce jobs have enough tasks to do.
                    engine.execute("insert into x select x::timestamp, x from long_sequence(" + (1000 * PAGE_FRAME_MAX_ROWS) + ")", sqlExecutionContext);

                    // A special CB is needed to be able to track NPEs since otherwise the exception will come unnoticed.
                    final NpeCountingAtomicBooleanCircuitBreaker npeCountingCircuitBreaker = new NpeCountingAtomicBooleanCircuitBreaker(engine);
                    ((SqlExecutionContextImpl) sqlExecutionContext).with(npeCountingCircuitBreaker);

                    try (RecordCursorFactory factory = compiler.compile("x where id != -1;", sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            // Trigger parallel filter and call it a day. The cursor will be closed early.
                            Assert.assertTrue(cursor.hasNext());
                        }
                    }

                    Assert.assertEquals(0, npeCountingCircuitBreaker.npeCounter.get());
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testEqStrFunctionFactory() throws Exception {
        final int threadCount = 4;
        final int workerCount = 4;

        WorkerPool pool = new WorkerPool(() -> workerCount);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "CREATE TABLE 'test1' " +
                                    "(column1 SYMBOL capacity 256 CACHE index capacity 256, timestamp TIMESTAMP) " +
                                    "timestamp (timestamp) PARTITION BY HOUR",
                            sqlExecutionContext
                    );

                    final int numOfRows = 2000;
                    for (int i = 0; i < numOfRows; i++) {
                        final int seconds = i % 60;
                        CharSequence insertSql = "INSERT INTO test1 (column1, timestamp) " +
                                "VALUES ('0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c', '2022-08-28T06:25:"
                                + (seconds < 10 ? "0" + seconds : String.valueOf(seconds)) + "Z')";
                        engine.execute(insertSql, sqlExecutionContext);
                    }

                    if (convertToParquet) {
                        engine.execute(
                                "alter table test1 convert partition to parquet where timestamp >= 0",
                                sqlExecutionContext
                        );
                    }

                    final String query = "SELECT column1 FROM test1 WHERE to_lowercase(column1) = '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'";

                    final RecordCursorFactory[] factories = new RecordCursorFactory[threadCount];
                    for (int i = 0; i < threadCount; i++) {
                        // Each factory should use a dedicated compiler instance, so that they don't
                        // share the same reduce task local pool in the SqlCodeGenerator.
                        factories[i] = engine.select(query, sqlExecutionContext);
                    }

                    final AtomicInteger errors = new AtomicInteger();
                    final CyclicBarrier barrier = new CyclicBarrier(threadCount);
                    final SOCountDownLatch haltLatch = new SOCountDownLatch(threadCount);
                    for (int i = 0; i < threadCount; i++) {
                        final int finalI = i;
                        new Thread(() -> {
                            TestUtils.await(barrier);

                            final RecordCursorFactory factory = factories[finalI];
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                Assert.assertTrue(factory.recordCursorSupportsRandomAccess());
                                cursor.toTop();
                                final Record record = cursor.getRecord();
                                int rowCount = 0;
                                while (cursor.hasNext()) {
                                    rowCount++;
                                    TestUtils.assertEquals("0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", record.getSymA(0));
                                }
                                Assert.assertEquals(numOfRows, rowCount);
                            } catch (Throwable e) {
                                e.printStackTrace();
                                errors.incrementAndGet();
                            } finally {
                                haltLatch.countDown();
                            }
                        }).start();
                    }

                    haltLatch.await();

                    Misc.free(factories);
                    Assert.assertEquals(0, errors.get());
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testHammerWorkStealing() throws Exception {
        // Here we're stress-testing work stealing, so no shared workers and no reduce jobs.
        final int threadCount = 4;
        final int iterations = 1000;

        assertMemoryLeak(() -> {
            engine.execute(
                    "CREATE TABLE 'x' (ts timestamp, id long) TIMESTAMP(ts) PARTITION BY DAY;",
                    sqlExecutionContext
            );
            // We want tasks from different queries to interleave within the queue,
            // so generate only `PAGE_FRAME_COUNT / 2` page frames.
            engine.execute(
                    "insert into x select x::timestamp, x from long_sequence(" + ((PAGE_FRAME_COUNT / 2) * PAGE_FRAME_MAX_ROWS) + ")",
                    sqlExecutionContext
            );

            final String query = "SELECT count() FROM x WHERE id > 42;";

            final RecordCursorFactory[] factories = new RecordCursorFactory[threadCount];
            for (int i = 0; i < threadCount; i++) {
                // Each factory should use a dedicated compiler instance, so that they don't
                // share the same reduce task local pool in the SqlCodeGenerator.
                factories[i] = engine.select(query, sqlExecutionContext);
            }

            final AtomicInteger errors = new AtomicInteger();
            final CyclicBarrier barrier = new CyclicBarrier(threadCount);
            final SOCountDownLatch haltLatch = new SOCountDownLatch(threadCount);
            for (int i = 0; i < threadCount; i++) {
                final int finalI = i;
                new Thread(() -> {
                    TestUtils.await(barrier);

                    final RecordCursorFactory factory = factories[finalI];
                    try (SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine)) {
                        // Make sure to use atomic CB instead of the default test no-op.
                        ctx.setUseSimpleCircuitBreaker(true);
                        for (int j = 0; j < iterations; j++) {
                            try (RecordCursor cursor = factory.getCursor(ctx)) {
                                final Record record = cursor.getRecord();
                                Assert.assertTrue(cursor.hasNext());
                                Assert.assertEquals(158, record.getLong(0));
                                Assert.assertFalse(cursor.hasNext());
                            } catch (Throwable e) {
                                e.printStackTrace();
                                errors.incrementAndGet();
                            }
                        }
                    } finally {
                        Path.clearThreadLocals();
                        haltLatch.countDown();
                    }
                }).start();
            }

            haltLatch.await();

            Misc.free(factories);
            Assert.assertEquals(0, errors.get());
        });
    }

    @Test
    public void testInAndInTimestampJitDisabled() throws Exception {
        testInAndInTimestamp(SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testInAndInTimestampJitEnabled() throws Exception {
        testInAndInTimestamp(SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testInJitDisabled() throws Exception {
        testIn(SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testInJitEnabled() throws Exception {
        testIn(SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testInTimestampJitDisabled() throws Exception {
        testInTimestamp(SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testInTimestampJitEnabled() throws Exception {
        testInTimestamp(SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testParallelStressSymbolEnablePreTouch() throws Exception {
        testParallelStressSymbol(
                "select /*+ ENABLE_PRE_TOUCH(x) */ * from x where v > 3326086085493629941L and v < 4326086085493629941L and s ~ 'A' order by v",
                """
                        v\ts
                        3393210801760647293\tA
                        3433721896286859656\tA
                        3619114107112892010\tA
                        3669882909701240516\tA
                        3820631780839257855\tA
                        4039070554630775695\tA
                        4290477379978201771\tA
                        """,
                4,
                1,
                SqlJitMode.JIT_MODE_DISABLED
        );
    }

    @Test
    public void testParallelStressSymbolMultipleThreadsMultipleWorkersJitDisabled() throws Exception {
        testParallelStressSymbol(symbolQueryNoLimit, expectedSymbolNoLimit, 4, 4, SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testParallelStressSymbolMultipleThreadsMultipleWorkersJitEnabled() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());

        testParallelStressSymbol(symbolQueryNoLimit, expectedSymbolNoLimit, 4, 4, SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testParallelStressSymbolMultipleThreadsMultipleWorkersNegativeLimitJitDisabled() throws Exception {
        testParallelStressSymbol(symbolQueryNegativeLimit, expectedNegativeLimit, 4, 4, SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testParallelStressSymbolMultipleThreadsMultipleWorkersNegativeLimitJitEnabled() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());

        testParallelStressSymbol(symbolQueryNegativeLimit, expectedNegativeLimit, 4, 4, SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testParallelStressSymbolMultipleThreadsMultipleWorkersNonStaticSymbolInVarcharCursorJitDisabled() throws Exception {
        testParallelStressSymbol(symbolInCursorQueryLimit, expectedSymbolInCursorQueryLimit, 4, 4, SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testParallelStressSymbolMultipleThreadsMultipleWorkersPositiveLimitJitDisabled() throws Exception {
        testParallelStressSymbol(symbolQueryPositiveLimit, expectedPositiveLimit, 4, 4, SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testParallelStressSymbolMultipleThreadsMultipleWorkersPositiveLimitJitEnabled() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());

        testParallelStressSymbol(symbolQueryPositiveLimit, expectedPositiveLimit, 4, 4, SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testParallelStressSymbolMultipleThreadsMultipleWorkersSymbolInVarcharCursorJitDisabled() throws Exception {
        testParallelStressSymbol(symbolStaticInCursorQueryLimit, expectedSymbolInCursorQueryLimit, 4, 4, SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testParallelStressSymbolMultipleThreadsSingleWorkerJitDisabled() throws Exception {
        testParallelStressSymbol(symbolQueryNoLimit, expectedSymbolNoLimit, 1, 4, SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testParallelStressSymbolMultipleThreadsSingleWorkerJitEnabled() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());

        testParallelStressSymbol(symbolQueryNoLimit, expectedSymbolNoLimit, 1, 4, SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testParallelStressSymbolSingleThreadMultipleWorkersJitDisabled() throws Exception {
        testParallelStressSymbol(symbolQueryNoLimit, expectedSymbolNoLimit, 4, 1, SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testParallelStressSymbolSingleThreadMultipleWorkersJitEnabled() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());

        testParallelStressSymbol(symbolQueryNoLimit, expectedSymbolNoLimit, 4, 1, SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testParallelStressSymbolSingleThreadMultipleWorkersSymbolValueFilter() throws Exception {
        testParallelStressSymbol(
                "x where v > 3326086085493629941L and v < 4326086085493629941L and s ~ 'A' order by v",
                """
                        v\ts
                        3393210801760647293\tA
                        3433721896286859656\tA
                        3619114107112892010\tA
                        3669882909701240516\tA
                        3820631780839257855\tA
                        4039070554630775695\tA
                        4290477379978201771\tA
                        """,
                4,
                1,
                SqlJitMode.JIT_MODE_DISABLED
        );
    }

    @Test
    public void testParallelStressVarcharMultipleThreadsMultipleWorkersJitDisabled() throws Exception {
        testParallelStressVarchar(varcharQueryNoLimit, expectedVarcharNoLimit, 4, SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testParallelStressVarcharMultipleThreadsMultipleWorkersJitEnabled() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());

        testParallelStressVarchar(varcharQueryNoLimit, expectedVarcharNoLimit, 4, SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testParallelStressVarcharSingleThreadMultipleWorkersJitDisabled() throws Exception {
        testParallelStressVarchar(varcharQueryNoLimit, expectedVarcharNoLimit, 1, SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testParallelStressVarcharSingleThreadMultipleWorkersJitEnabled() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());

        testParallelStressVarchar(varcharQueryNoLimit, expectedVarcharNoLimit, 1, SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testParallelStressVarcharSingleThreadMultipleWorkersSymbolValueFilter() throws Exception {
        testParallelStressVarchar(
                "x where l > 3326086085493629941L and l < 4326086085493629941L and v = 'A왋G&ُܵ9}\uD91F\uDCE8+\uDAAF\uDC59\uDAC8\uDE3B亲' order by l",
                """
                        l\tv
                        3571824131493518678\tA왋G&ُܵ9}\uD91F\uDCE8+\uDAAF\uDC59\uDAC8\uDE3B亲
                        """,
                1,
                SqlJitMode.JIT_MODE_DISABLED
        );
    }

    @Test
    public void testReadParquet() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "CREATE TABLE price (" +
                                    "  ts TIMESTAMP," +
                                    "  type SYMBOL," +
                                    "  value DOUBLE" +
                                    ") timestamp (ts) PARTITION BY DAY;",
                            sqlExecutionContext
                    );
                    engine.execute("insert into price select x::timestamp, 't' || (x%5), rnd_double() from long_sequence(50000)", sqlExecutionContext);

                    try (
                            Path path = new Path();
                            PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                            TableReader reader = engine.getReader("price")
                    ) {
                        path.of(root).concat("price.parquet");
                        PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                        PartitionEncoder.encodeWithOptions(
                                partitionDescriptor,
                                path,
                                ParquetCompression.COMPRESSION_UNCOMPRESSED,
                                true,
                                true,
                                PAGE_FRAME_MAX_ROWS,
                                0,
                                ParquetVersion.PARQUET_VERSION_V1
                        );
                        Assert.assertTrue(Files.exists(path.$()));

                        TestUtils.assertSql(
                                engine,
                                sqlExecutionContext,
                                "select count() from read_parquet('price.parquet') where value > 0.5",
                                sink,
                                "count\n25139\n"
                        );
                    }
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testStrBindVariable() throws Exception {
        testStrBindVariable("STRING", SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testSymbolBindVariableJitDisabled() throws Exception {
        testStrBindVariable("SYMBOL", SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testSymbolBindVariableJitEnabled() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());

        testStrBindVariable("SYMBOL", SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testVarcharBindVariable() throws Exception {
        testStrBindVariable("VARCHAR", SqlJitMode.JIT_MODE_ENABLED);
    }

    private static boolean assertCursor(
            CharSequence expected,
            RecordCursorFactory factory,
            SqlExecutionContext sqlExecutionContext,
            StringSink sink,
            LongList rows
    ) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue("supports random access", factory.recordCursorSupportsRandomAccess());
            if (
                    assertCursor(
                            expected,
                            true,
                            false,
                            true,
                            cursor,
                            factory.getMetadata(),
                            sink,
                            rows,
                            factory.fragmentedSymbolTables()
                    )
            ) {
                return true;
            }
        }
        return false;
    }

    private static void assertQuery(
            CharSequence expected,
            RecordCursorFactory factory,
            SqlExecutionContext sqlExecutionContext
    ) throws Exception {
        StringSink sink = new StringSink();
        LongList rows = new LongList();
        if (
                assertCursor(
                        expected,
                        factory,
                        sqlExecutionContext,
                        sink,
                        rows
                )
        ) {
            return;
        }
        // make sure we get the same outcome when we get factory to create new cursor
        assertCursor(
                expected,
                factory,
                sqlExecutionContext,
                sink,
                rows
        );
    }

    private void testAsyncOffloadTimeout(String query) throws Exception {
        final int rowCount = 10 * ROW_COUNT;
        // The test is very sensitive to page frame sizes.
        Assert.assertEquals(40, rowCount / configuration.getSqlPageFrameMaxRows());
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(AbstractCairoTest.LOG);
            // We want the timeout to happen in reduce.
            // Page frame count is 40.
            final long tripWhenTicks = Math.max(10, rnd.nextLong(39));

            circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
                private final AtomicLong ticks = new AtomicLong();

                @Override
                @NotNull
                public MillisecondClock getClock() {
                    return () -> {
                        if (ticks.incrementAndGet() < tripWhenTicks) {
                            return 0;
                        }
                        return Long.MAX_VALUE;
                    };
                }

                @Override
                public long getQueryTimeout() {
                    return 1;
                }
            };

            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
                        final NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine, circuitBreakerConfiguration, MemoryTag.NATIVE_DEFAULT);
                        try {
                            engine.execute(
                                    "create table x ( " +
                                            "v long, " +
                                            "s symbol capacity 4 cache " +
                                            ")",
                                    sqlExecutionContext
                            );
                            engine.execute(
                                    "insert into x select rnd_long() v, rnd_symbol('A','B','C') s from long_sequence(" + rowCount + ")",
                                    sqlExecutionContext
                            );

                            context.with(
                                    context.getSecurityContext(),
                                    context.getBindVariableService(),
                                    context.getRandom(),
                                    context.getRequestFd(),
                                    circuitBreaker
                            );

                            TestUtils.assertSql(compiler, context, query, sink, "");
                            Assert.fail();
                        } catch (CairoException ex) {
                            TestUtils.assertContains(ex.getFlyweightMessage(), "timeout, query aborted");
                        } finally {
                            Misc.free(circuitBreaker);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testAsyncSubQueryWithFilter(String query) throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "CREATE TABLE price (" +
                                    "  ts TIMESTAMP," +
                                    "  type SYMBOL," +
                                    "  value DOUBLE" +
                                    ") timestamp (ts) PARTITION BY DAY;",
                            sqlExecutionContext
                    );
                    engine.execute("insert into price select x::timestamp, 't' || (x%5), rnd_double() from long_sequence(100000)", sqlExecutionContext);
                    engine.execute("CREATE TABLE mapping (id SYMBOL, ext SYMBOL, ext_in SYMBOL, ts timestamp) timestamp(ts)", sqlExecutionContext);
                    engine.execute("insert into mapping select 't' || x, 's' || x, 's' || x, x::timestamp  from long_sequence(5)", sqlExecutionContext);
                    if (convertToParquet) {
                        execute(
                                compiler,
                                "alter table price convert partition to parquet where ts >= 0",
                                sqlExecutionContext
                        );
                    }

                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            query,
                            sink,
                            "count\n20000\n"
                    );
                },
                configuration,
                LOG
        );
    }

    private void testIn(int jitMode) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(jitMode));

        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "CREATE TABLE tab (\n" +
                                    "  ts TIMESTAMP," +
                                    "  type INT," +
                                    "  value SYMBOL) timestamp (ts) PARTITION BY DAY;",
                            sqlExecutionContext
                    );
                    engine.execute("insert into tab select x::timestamp, x%10, 't' || (x%10) from long_sequence(10)", sqlExecutionContext);
                    if (convertToParquet) {
                        execute(
                                compiler,
                                "alter table tab convert partition to parquet where ts >= 0",
                                sqlExecutionContext
                        );
                    }

                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "select * from tab where type IN (2, 3, 4, 6) limit 10",
                            sink,
                            """
                                    ts\ttype\tvalue
                                    1970-01-01T00:00:00.000002Z\t2\tt2
                                    1970-01-01T00:00:00.000003Z\t3\tt3
                                    1970-01-01T00:00:00.000004Z\t4\tt4
                                    1970-01-01T00:00:00.000006Z\t6\tt6
                                    """
                    );
                },
                configuration,
                LOG
        );
    }

    private void testInAndInTimestamp(int jitMode) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(jitMode));

        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "CREATE TABLE tab (\n" +
                                    "  ts TIMESTAMP," +
                                    "  preciseTs TIMESTAMP," +
                                    "  type INT," +
                                    "  value SYMBOL) timestamp (ts) PARTITION BY DAY;",
                            sqlExecutionContext
                    );
                    engine.execute("insert into tab select (x * 1000 * 1000 * 60)::timestamp, (x * 1000 * 1000 * 60)::timestamp, x%10, 't' || (x%10) from long_sequence(10000)", sqlExecutionContext);
                    if (convertToParquet) {
                        execute(
                                compiler,
                                "alter table tab convert partition to parquet where ts >= 0",
                                sqlExecutionContext
                        );
                    }

                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "select * from tab where preciseTs in '1970-01-01T00:00:00;3m;1d;5' and value IN ('t1', 't3') limit 10",
                            sink,
                            """
                                    ts\tpreciseTs\ttype\tvalue
                                    1970-01-01T00:01:00.000000Z\t1970-01-01T00:01:00.000000Z\t1\tt1
                                    1970-01-01T00:03:00.000000Z\t1970-01-01T00:03:00.000000Z\t3\tt3
                                    1970-01-02T00:01:00.000000Z\t1970-01-02T00:01:00.000000Z\t1\tt1
                                    1970-01-02T00:03:00.000000Z\t1970-01-02T00:03:00.000000Z\t3\tt3
                                    1970-01-03T00:01:00.000000Z\t1970-01-03T00:01:00.000000Z\t1\tt1
                                    1970-01-03T00:03:00.000000Z\t1970-01-03T00:03:00.000000Z\t3\tt3
                                    1970-01-04T00:01:00.000000Z\t1970-01-04T00:01:00.000000Z\t1\tt1
                                    1970-01-04T00:03:00.000000Z\t1970-01-04T00:03:00.000000Z\t3\tt3
                                    1970-01-05T00:01:00.000000Z\t1970-01-05T00:01:00.000000Z\t1\tt1
                                    1970-01-05T00:03:00.000000Z\t1970-01-05T00:03:00.000000Z\t3\tt3
                                    """
                    );
                },
                configuration,
                LOG
        );
    }

    private void testInTimestamp(int jitMode) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(jitMode));

        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "CREATE TABLE tab (\n" +
                                    "  ts TIMESTAMP," +
                                    "  preciseTs TIMESTAMP," +
                                    "  type INT," +
                                    "  value SYMBOL) timestamp (ts) PARTITION BY DAY;",
                            sqlExecutionContext
                    );
                    engine.execute("insert into tab select (x * 1000 * 1000 * 60)::timestamp, (x * 1000 * 1000 * 60)::timestamp, x%10, 't' || (x%10) from long_sequence(10000)", sqlExecutionContext);
                    if (convertToParquet) {
                        execute(
                                compiler,
                                "alter table tab convert partition to parquet where ts >= 0",
                                sqlExecutionContext
                        );
                    }

                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "select * from tab where preciseTs in '1970-01-01T00:00:00;3m;1d;5' and value = 't3' limit 10",
                            sink,
                            """
                                    ts\tpreciseTs\ttype\tvalue
                                    1970-01-01T00:03:00.000000Z\t1970-01-01T00:03:00.000000Z\t3\tt3
                                    1970-01-02T00:03:00.000000Z\t1970-01-02T00:03:00.000000Z\t3\tt3
                                    1970-01-03T00:03:00.000000Z\t1970-01-03T00:03:00.000000Z\t3\tt3
                                    1970-01-04T00:03:00.000000Z\t1970-01-04T00:03:00.000000Z\t3\tt3
                                    1970-01-05T00:03:00.000000Z\t1970-01-05T00:03:00.000000Z\t3\tt3
                                    """
                    );
                },
                configuration,
                LOG
        );
    }

    private void testParallelStressSymbol(String query, String expected, int workerCount, int threadCount, int jitMode) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(jitMode));

        WorkerPool pool = new WorkerPool(() -> workerCount);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "create table x ( " +
                                    " v long, " +
                                    " s symbol capacity 4 cache " +
                                    ")",
                            sqlExecutionContext
                    );
                    engine.execute("insert into x select rnd_long() v, rnd_symbol('A','B','C') s from long_sequence(" + ROW_COUNT + ")", sqlExecutionContext);

                    RecordCursorFactory[] factories = new RecordCursorFactory[threadCount];

                    for (int i = 0; i < threadCount; i++) {
                        // Each factory should use a dedicated compiler instance, so that they don't
                        // share the same reduce task local pool in the SqlCodeGenerator.
                        factories[i] = engine.select(query, sqlExecutionContext);
                        Assert.assertEquals(jitMode != SqlJitMode.JIT_MODE_DISABLED, factories[i].usesCompiledFilter());
                    }

                    final AtomicInteger errors = new AtomicInteger();
                    final CyclicBarrier barrier = new CyclicBarrier(threadCount);
                    final SOCountDownLatch haltLatch = new SOCountDownLatch(threadCount);

                    for (int i = 0; i < threadCount; i++) {
                        int finalI = i;
                        new Thread(() -> {
                            TestUtils.await(barrier);
                            try (SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine, workerCount)) {
                                RecordCursorFactory factory = factories[finalI];
                                assertQuery(expected, factory, ctx);
                            } catch (Throwable e) {
                                e.printStackTrace();
                                errors.incrementAndGet();
                            } finally {
                                haltLatch.countDown();
                            }
                        }).start();
                    }

                    haltLatch.await();

                    Misc.free(factories);
                    Assert.assertEquals(0, errors.get());
                },
                configuration,
                LOG
        );
    }

    private void testParallelStressVarchar(String query, String expected, int threadCount, int jitMode) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(jitMode));

        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "create table x ( " +
                                    " l long, " +
                                    " v varchar " +
                                    ")",
                            sqlExecutionContext
                    );
                    engine.execute("insert into x select rnd_long() v, rnd_varchar(4,16,5) s from long_sequence(" + ROW_COUNT + ")", sqlExecutionContext);

                    RecordCursorFactory[] factories = new RecordCursorFactory[threadCount];

                    for (int i = 0; i < threadCount; i++) {
                        // Each factory should use a dedicated compiler instance, so that they don't
                        // share the same reduce task local pool in the SqlCodeGenerator.
                        factories[i] = engine.select(query, sqlExecutionContext);
                        Assert.assertEquals(jitMode != SqlJitMode.JIT_MODE_DISABLED, factories[i].usesCompiledFilter());
                    }

                    final AtomicInteger errors = new AtomicInteger();
                    final CyclicBarrier barrier = new CyclicBarrier(threadCount);
                    final SOCountDownLatch haltLatch = new SOCountDownLatch(threadCount);

                    for (int i = 0; i < threadCount; i++) {
                        int finalI = i;
                        new Thread(() -> {
                            TestUtils.await(barrier);
                            try (SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine, 4)) {
                                RecordCursorFactory factory = factories[finalI];
                                assertQuery(expected, factory, ctx);
                            } catch (Throwable e) {
                                e.printStackTrace();
                                errors.incrementAndGet();
                            } finally {
                                haltLatch.countDown();
                            }
                        }).start();
                    }

                    haltLatch.await();

                    Misc.free(factories);
                    Assert.assertEquals(0, errors.get());
                },
                configuration,
                LOG
        );
    }

    private void testStrBindVariable(String columnType, int jitMode) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(jitMode));

        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "CREATE TABLE price (\n" +
                                    "  ts TIMESTAMP," +
                                    "  type " + columnType + "," +
                                    "  value DOUBLE) timestamp (ts) PARTITION BY DAY;",
                            sqlExecutionContext
                    );
                    engine.execute("insert into price select x::timestamp, 't' || (x%5), rnd_double()  from long_sequence(100000)", sqlExecutionContext);
                    if (convertToParquet) {
                        execute(
                                compiler,
                                "alter table price convert partition to parquet where ts >= 0",
                                sqlExecutionContext
                        );
                    }

                    sqlExecutionContext.getBindVariableService().clear();
                    sqlExecutionContext.getBindVariableService().setStr(0, "t3");
                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "select * from price where type = $1 limit 10",
                            sink,
                            """
                                    ts\ttype\tvalue
                                    1970-01-01T00:00:00.000003Z\tt3\t0.08486964232560668
                                    1970-01-01T00:00:00.000008Z\tt3\t0.9856290845874263
                                    1970-01-01T00:00:00.000013Z\tt3\t0.5243722859289777
                                    1970-01-01T00:00:00.000018Z\tt3\t0.6778564558839208
                                    1970-01-01T00:00:00.000023Z\tt3\t0.3288176907679504
                                    1970-01-01T00:00:00.000028Z\tt3\t0.6381607531178513
                                    1970-01-01T00:00:00.000033Z\tt3\t0.6761934857077543
                                    1970-01-01T00:00:00.000038Z\tt3\t0.7664256753596138
                                    1970-01-01T00:00:00.000043Z\tt3\t0.05048190020054388
                                    1970-01-01T00:00:00.000048Z\tt3\t0.8001121139739173
                                    """
                    );
                },
                configuration,
                LOG
        );
    }

    private static class NpeCountingAtomicBooleanCircuitBreaker extends AtomicBooleanCircuitBreaker {
        final AtomicInteger npeCounter = new AtomicInteger();

        public NpeCountingAtomicBooleanCircuitBreaker(CairoEngine engine) {
            super(engine);
        }

        @Override
        public int getState() {
            if (cancelledFlag == null) {
                npeCounter.incrementAndGet();
            }
            return super.getState();
        }
    }
}
