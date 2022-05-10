/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import io.questdb.Metrics;
import io.questdb.WorkerPoolAwareConfiguration;
import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.RecordCursorPrinter;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.jit.JitUtil;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncOffloadTest extends AbstractGriffinTest {

    private static final int PAGE_FRAME_MAX_ROWS = 100;
    private static final int PAGE_FRAME_COUNT = 4; // also used to set queue size, so must be a power of 2
    private static final int ROW_COUNT = PAGE_FRAME_COUNT * PAGE_FRAME_MAX_ROWS;

    private static final String queryNoLimit = "select v from x where v > 3326086085493629941L and v < 4326086085493629941L order by v";
    private static final String expectedNoLimit = "v\n" +
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

    private static final String queryPositiveLimit = "select v from x where v > 3326086085493629941L and v < 4326086085493629941L limit 10";
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

    private static final String queryNegativeLimit = "select v from x where v > 3326086085493629941L and v < 4326086085493629941L limit -10";
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

    @BeforeClass
    public static void setUpStatic() {
        pageFrameMaxRows = PAGE_FRAME_MAX_ROWS;
        // We intentionally use small values for shard count and reduce
        // queue capacity to exhibit various edge cases.
        pageFrameReduceShardCount = 2;
        pageFrameReduceQueueCapacity = PAGE_FRAME_COUNT;

        AbstractGriffinTest.setUpStatic();
    }

    @Test
    public void testParallelStressMultipleThreadsMultipleWorkersJitDisabled() throws Exception {
        testParallelStress(queryNoLimit, expectedNoLimit, 4, 4, SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testParallelStressMultipleThreadsMultipleWorkersJitEnabled() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());

        testParallelStress(queryNoLimit, expectedNoLimit, 4, 4, SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testParallelStressSingleThreadMultipleWorkersJitDisabled() throws Exception {
        testParallelStress(queryNoLimit, expectedNoLimit, 4, 1, SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testParallelStressSingleThreadMultipleWorkersJitEnabled() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());

        testParallelStress(queryNoLimit, expectedNoLimit, 4, 1, SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testParallelStressMultipleThreadsSingleWorkerJitDisabled() throws Exception {
        testParallelStress(queryNoLimit, expectedNoLimit, 1, 4, SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testParallelStressMultipleThreadsSingleWorkerJitEnabled() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());

        testParallelStress(queryNoLimit, expectedNoLimit, 1, 4, SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testParallelStressMultipleThreadsMultipleWorkersPositiveLimitJitDisabled() throws Exception {
        testParallelStress(queryPositiveLimit, expectedPositiveLimit, 4, 4, SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testParallelStressMultipleThreadsMultipleWorkersPositiveLimitJitEnabled() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());

        testParallelStress(queryPositiveLimit, expectedPositiveLimit, 4, 4, SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testParallelStressMultipleThreadsMultipleWorkersNegativeLimitJitDisabled() throws Exception {
        testParallelStress(queryNegativeLimit, expectedNegativeLimit, 4, 4, SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testParallelStressMultipleThreadsMultipleWorkersNegativeLimitJitEnabled() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());

        testParallelStress(queryNegativeLimit, expectedNegativeLimit, 4, 4, SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testParallelStressSingleThreadMultipleWorkersSymbolValueFilter() throws Exception {
        testParallelStress(
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

    private void testParallelStress(String query, String expected, int workerCount, int threadCount, int jitMode) throws Exception {
        AbstractCairoTest.jitMode = jitMode;

        final int[] affinity = new int[workerCount];
        for (int i = 0; i < workerCount; i++) {
            affinity[i] = -1;
        }

        WorkerPool pool = new WorkerPool(
                new WorkerPoolAwareConfiguration() {
                    @Override
                    public int[] getWorkerAffinity() {
                        return affinity;
                    }

                    @Override
                    public int getWorkerCount() {
                        return workerCount;
                    }

                    @Override
                    public boolean haltOnError() {
                        return false;
                    }

                    @Override
                    public boolean isEnabled() {
                        return true;
                    }
                },
                Metrics.disabled()
        );

        TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    compiler.compile("create table x ( " +
                                    "v long, " +
                                    "s symbol capacity 4 cache " +
                                    ")",
                            sqlExecutionContext
                    );
                    compiler.compile("insert into x select rnd_long() v, rnd_symbol('A','B','C') s from long_sequence(" + ROW_COUNT + ")",
                            sqlExecutionContext
                    );

                    SqlCompiler[] compilers = new SqlCompiler[threadCount];
                    RecordCursorFactory[] factories = new RecordCursorFactory[threadCount];

                    for (int i = 0; i < threadCount; i++) {
                        // Each factory should use a dedicated compiler instance, so that they don't
                        // share the same reduce task local pool in the SqlCodeGenerator.
                        compilers[i] = new SqlCompiler(engine);
                        factories[i] = compilers[i].compile(query, sqlExecutionContext).getRecordCursorFactory();
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

                    Misc.free(compilers);
                    Misc.free(factories);

                    Assert.assertEquals(0, errors.get());
                },
                configuration
        );
    }

    private static void assertQuery(
            CharSequence expected,
            RecordCursorFactory factory,
            SqlExecutionContext sqlExecutionContext
    ) throws Exception {
        StringSink sink = new StringSink();
        RecordCursorPrinter printer = new RecordCursorPrinter();
        LongList rows = new LongList();
        if (
                assertCursor(
                        expected,
                        factory,
                        sqlExecutionContext,
                        sink,
                        printer,
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
                printer,
                rows
        );
    }

    private static boolean assertCursor(
            CharSequence expected,
            RecordCursorFactory factory,
            SqlExecutionContext sqlExecutionContext,
            StringSink sink,
            RecordCursorPrinter printer,
            LongList rows
    ) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue("supports random access", factory.recordCursorSupportsRandomAccess());
            if (
                    assertCursor(
                            expected,
                            true,
                            true,
                            false,
                            true,
                            cursor,
                            factory.getMetadata(),
                            sink,
                            printer,
                            rows,
                            factory.fragmentedSymbolTables()
                    )
            ) {
                return true;
            }
        }
        return false;
    }
}
