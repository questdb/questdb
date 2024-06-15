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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.jit.JitUtil;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.datetime.millitime.MillisecondClock;
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

import static io.questdb.PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT;

public class ParallelFilterTest extends AbstractCairoTest {

    private static final int PAGE_FRAME_COUNT = 4; // also used to set queue size, so must be a power of 2
    private static final int PAGE_FRAME_MAX_ROWS = 100;
    private static final int ROW_COUNT = PAGE_FRAME_COUNT * PAGE_FRAME_MAX_ROWS;
    private static final String expectedNegativeLimit = "v\n" +
            "4039070554630775695\n" +
            "3424747151763089683\n" +
            "4290477379978201771\n" +
            "3794031607724753525\n" +
            "3745605868208843008\n" +
            "3464609208866088600\n" +
            "3513816850827798261\n" +
            "3542505137180114151\n" +
            "4169687421984608700\n" +
            "3433721896286859656\n";
    private static final String expectedPositiveLimit = "v\n" +
            "3394168647660478011\n" +
            "4086802474270249591\n" +
            "3958193676455060057\n" +
            "3619114107112892010\n" +
            "3705833798044144433\n" +
            "4238042693748641409\n" +
            "3518554007419864093\n" +
            "4014104627539596639\n" +
            "3393210801760647293\n" +
            "4099611147050818391\n";
    private static final String expectedSymbolNoLimit = "v\n" +
            "3393210801760647293\n" +
            "3394168647660478011\n" +
            "3424747151763089683\n" +
            "3433721896286859656\n" +
            "3464609208866088600\n" +
            "3513816850827798261\n" +
            "3518554007419864093\n" +
            "3542505137180114151\n" +
            "3619114107112892010\n" +
            "3669882909701240516\n" +
            "3705833798044144433\n" +
            "3745605868208843008\n" +
            "3771494396743411509\n" +
            "3794031607724753525\n" +
            "3820631780839257855\n" +
            "3944678179613436885\n" +
            "3958193676455060057\n" +
            "4014104627539596639\n" +
            "4039070554630775695\n" +
            "4086802474270249591\n" +
            "4099611147050818391\n" +
            "4160567228070722087\n" +
            "4169687421984608700\n" +
            "4238042693748641409\n" +
            "4290477379978201771\n";
    private static final String expectedVarcharNoLimit = "l\tv\n" +
            "3342946432339528961\t^̈RɗT\uDBCE\uDF3F\u008E\n" +
            "3349345766605440862\t\n" +
            "3394168647660478011\t)|1u%2uL>gG8#3Ts\n" +
            "3401443869949416748\t\n" +
            "3433721896286859656\t?[麛P\uD9E8\uDEDE\uD931\uDF48ҽ\uDA01\uDE60\n" +
            "3523446414305966840\t7yXx>K%H[ g0nHS\\\n" +
            "3547449704743013886\tfT)D>XP?dYL\n" +
            "3570175762165818271\t97hVO\n" +
            "3571824131493518678\tA왋G&ُܵ9}\uD91F\uDCE8+\uDAAF\uDC59\uDAC8\uDE3B亲\n" +
            "3585172908882940409\t\n" +
            "3682423623919780100\tF~)xNm\\~Fwz;gR.G\n" +
            "3739186870210598690\tB&LA.&g\n" +
            "3780956794407111569\t\n" +
            "3921912097533020222\tmH%/###`3g?\n" +
            "3927079694554322589\tg\uECF9J9漫\uDBDB\uDDDB1fÄ}o輖NI\n" +
            "4171842711013652287\t\n" +
            "4290056275098552124\t=ܼDdjvsoߛ)*EB\n";
    private static final String symbolQueryNegativeLimit = "select v from x where v > 3326086085493629941L and v < 4326086085493629941L limit -10";
    private static final String symbolQueryNoLimit = "select v from x where v > 3326086085493629941L and v < 4326086085493629941L order by v";
    private static final String symbolQueryPositiveLimit = "select v from x where v > 3326086085493629941L and v < 4326086085493629941L limit 10";
    private static final String varcharQueryNoLimit = "select l, v from x where l > 3326086085493629941L and l < 4326086085493629941L order by l";

    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, PAGE_FRAME_MAX_ROWS);
        // We intentionally use small values for shard count and reduce
        // queue capacity to exhibit various edge cases.
        setProperty(CAIRO_PAGE_FRAME_SHARD_COUNT, 2);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, PAGE_FRAME_COUNT);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        super.setUp();
    }

    @Test
    public void testAsyncOffloadNegativeLimitTimeoutWithJitEnabled() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(SqlJitMode.JIT_MODE_ENABLED));
        testAsyncOffloadNegativeLimitTimeout();
    }

    @Test
    public void testAsyncOffloadNegativeLimitTimeoutWithoutJit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(SqlJitMode.JIT_MODE_DISABLED));
        testAsyncOffloadNegativeLimitTimeout();
    }

    @Test
    public void testAsyncOffloadTimeoutWithJitEnabled() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(SqlJitMode.JIT_MODE_ENABLED));
        testAsyncOffloadTimeout();
    }

    @Test
    public void testAsyncOffloadTimeoutWithoutJit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(SqlJitMode.JIT_MODE_DISABLED));
        testAsyncOffloadTimeout();
    }

    @Test
    public void testAsyncSubQueryWithFilterOnIndexedSymbol() throws Exception {
        testAsyncSubQueryWithFilter("SELECT count(*) " +
                "FROM price " +
                "WHERE type IN (SELECT id FROM mapping WHERE ext in ('s1', 's2') and substring(ext,2,1) = '1')");
    }

    @Test
    public void testAsyncSubQueryWithFilterOnLatestBy() throws Exception {
        testAsyncSubQueryWithFilter("SELECT count(*) " +
                "FROM price " +
                "WHERE type IN (SELECT id FROM mapping WHERE ext in ('s1'))");
    }

    @Test
    public void testAsyncSubQueryWithFilterOnSymbol() throws Exception {
        testAsyncSubQueryWithFilter("SELECT count(*) " +
                "FROM price " +
                "WHERE type IN (SELECT id FROM mapping WHERE ext in ('s1'))");
    }

    @Test
    public void testEqStrFunctionFactory() throws Exception {
        final int threadCount = 4;
        final int workerCount = 4;

        WorkerPool pool = new WorkerPool((() -> workerCount));
        TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    engine.ddl(
                            "CREATE TABLE 'test1' " +
                                    "(column1 SYMBOL capacity 256 CACHE index capacity 256, timestamp TIMESTAMP) " +
                                    "timestamp (timestamp) PARTITION BY HOUR",
                            sqlExecutionContext
                    );

                    final int numOfRows = 2000;
                    for (int i = 0; i < numOfRows; i++) {
                        final int seconds = i % 60;
                        engine.insert(
                                "INSERT INTO test1 (column1, timestamp) " +
                                        "VALUES ('0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c', '2022-08-28T06:25:"
                                        + (seconds < 10 ? "0" + seconds : String.valueOf(seconds)) + "Z')",
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
                "v\ts\n" +
                        "3393210801760647293\tA\n" +
                        "3433721896286859656\tA\n" +
                        "3619114107112892010\tA\n" +
                        "3669882909701240516\tA\n" +
                        "3820631780839257855\tA\n" +
                        "4039070554630775695\tA\n" +
                        "4290477379978201771\tA\n",
                4,
                1,
                SqlJitMode.JIT_MODE_DISABLED
        );
    }

    @Test
    public void testParallelStressVarcharMultipleThreadsMultipleWorkersJitDisabled() throws Exception {
        testParallelStressVarchar(varcharQueryNoLimit, expectedVarcharNoLimit, 4, 4, SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testParallelStressVarcharMultipleThreadsMultipleWorkersJitEnabled() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());

        testParallelStressVarchar(varcharQueryNoLimit, expectedVarcharNoLimit, 4, 4, SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testParallelStressVarcharSingleThreadMultipleWorkersJitDisabled() throws Exception {
        testParallelStressVarchar(varcharQueryNoLimit, expectedVarcharNoLimit, 4, 1, SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testParallelStressVarcharSingleThreadMultipleWorkersJitEnabled() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());

        testParallelStressVarchar(varcharQueryNoLimit, expectedVarcharNoLimit, 4, 1, SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testParallelStressVarcharSingleThreadMultipleWorkersSymbolValueFilter() throws Exception {
        testParallelStressVarchar(
                "x where l > 3326086085493629941L and l < 4326086085493629941L and v = 'A왋G&ُܵ9}\uD91F\uDCE8+\uDAAF\uDC59\uDAC8\uDE3B亲' order by l",
                "l\tv\n" +
                        "3571824131493518678\tA왋G&ُܵ9}\uD91F\uDCE8+\uDAAF\uDC59\uDAC8\uDE3B亲\n",
                4,
                1,
                SqlJitMode.JIT_MODE_DISABLED
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

    private void testAsyncOffloadNegativeLimitTimeout() throws Exception {
        assertMemoryLeak(() -> {
            SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
            currentMicros = 0;
            NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                    new DefaultSqlExecutionCircuitBreakerConfiguration() {
                        @Override
                        @NotNull
                        public MillisecondClock getClock() {
                            return () -> Long.MAX_VALUE;
                        }

                        @Override
                        public long getQueryTimeout() {
                            return 1;
                        }
                    },
                    MemoryTag.NATIVE_DEFAULT
            );

            try {
                ddl(
                        "create table x ( " +
                                "v long, " +
                                "s symbol capacity 4 cache " +
                                ")"
                );
                insert("insert into x select rnd_long() v, rnd_symbol('A','B','C') s from long_sequence(" + ROW_COUNT + ")");

                context.with(
                        context.getSecurityContext(),
                        context.getBindVariableService(),
                        context.getRandom(),
                        context.getRequestFd(),
                        circuitBreaker
                );

                assertSql(
                        "s\n" +
                                "A\n" +
                                "A\n" +
                                "A\n" +
                                "A\n" +
                                "A\n",
                        "select s from x where v > 3326086085493629941L and v < 4326086085493629941L and s = 'A' limit -5"
                );
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "timeout, query aborted");
            } finally {
                context.with(
                        context.getSecurityContext(),
                        context.getBindVariableService(),
                        context.getRandom(),
                        context.getRequestFd(),
                        null
                );
                Misc.free(circuitBreaker);
            }
        });
    }

    private void testAsyncOffloadTimeout() throws Exception {
        assertMemoryLeak(() -> {
            SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
            currentMicros = 0;
            NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                    new DefaultSqlExecutionCircuitBreakerConfiguration() {
                        @Override
                        @NotNull
                        public MillisecondClock getClock() {
                            return () -> Long.MAX_VALUE;
                        }

                        @Override
                        public long getQueryTimeout() {
                            return 1;
                        }
                    },
                    MemoryTag.NATIVE_DEFAULT
            );

            try {
                ddl(
                        "create table x ( " +
                                "v long, " +
                                "s symbol capacity 4 cache " +
                                ")"
                );
                insert("insert into x select rnd_long() v, rnd_symbol('A','B','C') s from long_sequence(" + ROW_COUNT + ")");

                context.with(
                        context.getSecurityContext(),
                        context.getBindVariableService(),
                        context.getRandom(),
                        context.getRequestFd(),
                        circuitBreaker
                );

                assertSql(
                        "v\ts\n" +
                                "3393210801760647293\tA\n" +
                                "3433721896286859656\tA\n" +
                                "3619114107112892010\tA\n" +
                                "3669882909701240516\tA\n" +
                                "3820631780839257855\tA\n" +
                                "4039070554630775695\tA\n" +
                                "4290477379978201771\tA\n",
                        "x where v > 3326086085493629941L and v < 4326086085493629941L and s = 'A' order by v"
                );
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "timeout, query aborted");
            } finally {
                context.with(
                        context.getSecurityContext(),
                        context.getBindVariableService(),
                        context.getRandom(),
                        context.getRequestFd(),
                        null
                );
                Misc.free(circuitBreaker);
            }
        });
    }

    private void testAsyncSubQueryWithFilter(String query) throws Exception {
        WorkerPool pool = new WorkerPool((() -> 4));
        TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    ddl(compiler, "CREATE TABLE price (\n" +
                            "  ts TIMESTAMP," +
                            "  type SYMBOL," +
                            "  value DOUBLE ) timestamp (ts) PARTITION BY DAY;", sqlExecutionContext);
                    insert(compiler, "insert into price select x::timestamp,  't' || (x%5), rnd_double()  from long_sequence(100000)", sqlExecutionContext);
                    ddl(compiler, "CREATE TABLE mapping ( id SYMBOL, ext SYMBOL, ext_in SYMBOL, ts timestamp ) timestamp(ts)", sqlExecutionContext);
                    ddl(compiler, "insert into mapping select 't' || x, 's' || x, 's' || x, x::timestamp  from long_sequence(5)", sqlExecutionContext);

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

    private void testParallelStressSymbol(String query, String expected, int workerCount, int threadCount, int jitMode) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(jitMode));

        WorkerPool pool = new WorkerPool(() -> workerCount);
        TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    engine.ddl(
                            "create table x ( " +
                                    " v long, " +
                                    " s symbol capacity 4 cache " +
                                    ")",
                            sqlExecutionContext
                    );
                    engine.insert(
                            "insert into x select rnd_long() v, rnd_symbol('A','B','C') s from long_sequence(" + ROW_COUNT + ")",
                            sqlExecutionContext
                    );

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
                            try {
                                RecordCursorFactory factory = factories[finalI];
                                assertQuery(expected, factory, sqlExecutionContext);
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

    private void testParallelStressVarchar(String query, String expected, int workerCount, int threadCount, int jitMode) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(jitMode));

        WorkerPool pool = new WorkerPool(() -> workerCount);
        TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    engine.ddl(
                            "create table x ( " +
                                    " l long, " +
                                    " v varchar " +
                                    ")",
                            sqlExecutionContext
                    );
                    engine.insert(
                            "insert into x select rnd_long() v, rnd_varchar(4,16,5) s from long_sequence(" + ROW_COUNT + ")",
                            sqlExecutionContext
                    );

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
                            try {
                                RecordCursorFactory factory = factories[finalI];
                                assertQuery(expected, factory, sqlExecutionContext);
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

        WorkerPool pool = new WorkerPool((() -> 4));
        TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    ddl(compiler, "CREATE TABLE price (\n" +
                            "  ts TIMESTAMP," +
                            "  type " + columnType + "," +
                            "  value DOUBLE) timestamp (ts) PARTITION BY DAY;", sqlExecutionContext);
                    insert(compiler, "insert into price select x::timestamp, 't' || (x%5), rnd_double()  from long_sequence(100000)", sqlExecutionContext);

                    sqlExecutionContext.getBindVariableService().clear();
                    sqlExecutionContext.getBindVariableService().setStr(0, "t3");
                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "select * from price where type = $1 limit 10",
                            sink,
                            "ts\ttype\tvalue\n" +
                                    "1970-01-01T00:00:00.000003Z\tt3\t0.08486964232560668\n" +
                                    "1970-01-01T00:00:00.000008Z\tt3\t0.9856290845874263\n" +
                                    "1970-01-01T00:00:00.000013Z\tt3\t0.5243722859289777\n" +
                                    "1970-01-01T00:00:00.000018Z\tt3\t0.6778564558839208\n" +
                                    "1970-01-01T00:00:00.000023Z\tt3\t0.3288176907679504\n" +
                                    "1970-01-01T00:00:00.000028Z\tt3\t0.6381607531178513\n" +
                                    "1970-01-01T00:00:00.000033Z\tt3\t0.6761934857077543\n" +
                                    "1970-01-01T00:00:00.000038Z\tt3\t0.7664256753596138\n" +
                                    "1970-01-01T00:00:00.000043Z\tt3\t0.05048190020054388\n" +
                                    "1970-01-01T00:00:00.000048Z\tt3\t0.8001121139739173\n"
                    );
                },
                configuration,
                LOG
        );
    }
}
