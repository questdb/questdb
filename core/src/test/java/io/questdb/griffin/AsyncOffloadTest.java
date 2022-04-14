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
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.jit.JitUtil;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncOffloadTest extends AbstractGriffinTest {

    private static final String queryNoLimit = "x where v > 3326086085493629941L and v < 4326086085493629941L order by v";
    private static final String expectedNoLimit = "v\n" +
            "3352215237270276085\n" +
            "3387658305631292205\n" +
            "3393210801760647293\n" +
            "3394168647660478011\n" +
            "3396017735551392340\n" +
            "3400331104999099445\n" +
            "3424747151763089683\n" +
            "3433721896286859656\n" +
            "3446015290144635451\n" +
            "3461453140026724919\n" +
            "3464609208866088600\n" +
            "3499854656700247548\n" +
            "3513816850827798261\n" +
            "3516348260509090745\n" +
            "3518554007419864093\n" +
            "3525232847069179869\n" +
            "3527911398466283309\n" +
            "3528688207779231649\n" +
            "3542505137180114151\n" +
            "3564031921719748904\n" +
            "3614738589890112276\n" +
            "3619114107112892010\n" +
            "3658246407068171204\n" +
            "3669882909701240516\n" +
            "3705833798044144433\n" +
            "3745605868208843008\n" +
            "3771494396743411509\n" +
            "3792128300541831563\n" +
            "3794031607724753525\n" +
            "3820631780839257855\n" +
            "3843127285248668146\n" +
            "3860877990849202595\n" +
            "3861637258207773908\n" +
            "3944678179613436885\n" +
            "3958193676455060057\n" +
            "3987576220753016999\n" +
            "4014104627539596639\n" +
            "4031101756856736238\n" +
            "4034325504592033197\n" +
            "4039070554630775695\n" +
            "4086802474270249591\n" +
            "4092568845903588572\n" +
            "4099611147050818391\n" +
            "4107109535030235684\n" +
            "4160567228070722087\n" +
            "4167328623064065836\n" +
            "4169687421984608700\n" +
            "4180749328631778250\n" +
            "4238042693748641409\n" +
            "4270881260749377500\n" +
            "4290477379978201771\n";

    private static final String queryPositiveLimit = "x where v > 3326086085493629941L and v < 4326086085493629941L limit 10";
    private static final String expectedPositiveLimit = "v\n" +
            "3614738589890112276\n" +
            "3394168647660478011\n" +
            "4086802474270249591\n" +
            "3958193676455060057\n" +
            "3619114107112892010\n" +
            "3705833798044144433\n" +
            "4238042693748641409\n" +
            "3518554007419864093\n" +
            "4014104627539596639\n" +
            "4167328623064065836\n";

    private static final String queryNegativeLimit = "x where v > 3326086085493629941L and v < 4326086085493629941L limit 10";
    private static final String expectedNegativeLimit = "v\n" +
            "3614738589890112276\n" +
            "3394168647660478011\n" +
            "4086802474270249591\n" +
            "3958193676455060057\n" +
            "3619114107112892010\n" +
            "3705833798044144433\n" +
            "4238042693748641409\n" +
            "3518554007419864093\n" +
            "4014104627539596639\n" +
            "4167328623064065836\n";

    @BeforeClass
    public static void setUpStatic() {
        jitMode = SqlJitMode.JIT_MODE_DISABLED;
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
    public void testParallelStressMultipleThreadsMultipleWorkersPositiveLimitJitEnabled() throws Exception {
        testParallelStress(queryPositiveLimit, expectedPositiveLimit, 4, 4, SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testParallelStressMultipleThreadsMultipleWorkersPositiveLimitJitDisabled() throws Exception {
        testParallelStress(queryPositiveLimit, expectedPositiveLimit, 4, 4, SqlJitMode.JIT_MODE_DISABLED);
    }

    @Test
    public void testParallelStressMultipleThreadsMultipleWorkersNegativeLimitJitEnabled() throws Exception {
        testParallelStress(queryNegativeLimit, expectedNegativeLimit, 4, 4, SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testParallelStressMultipleThreadsMultipleWorkersNegativeLimitJitDisabled() throws Exception {
        testParallelStress(queryNegativeLimit, expectedNegativeLimit, 4, 4, SqlJitMode.JIT_MODE_DISABLED);
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
                    compiler.compile("create table x as (" +
                                    "select rnd_long() v from long_sequence(1000)" +
                                    ")",
                            sqlExecutionContext
                    );

                    RecordCursorFactory[] factories = new RecordCursorFactory[threadCount];

                    for (int i = 0; i < threadCount; i++) {
                        factories[i] = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory();
                        Assert.assertEquals(jitMode != SqlJitMode.JIT_MODE_DISABLED, factories[i].usesCompiledFilter());
                    }

                    final AtomicInteger errors = new AtomicInteger();
                    final CyclicBarrier barrier = new CyclicBarrier(threadCount);
                    final SOCountDownLatch haltLatch = new SOCountDownLatch(threadCount);

                    for (int i = 0; i < threadCount; i++) {
                        int th = i;
                        new Thread(() -> {
                            TestUtils.await(barrier);
                            try {
                                RecordCursorFactory factory = factories[th];
                                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                    TestUtils.assertCursor(
                                            expected,
                                            cursor,
                                            factory.getMetadata(),
                                            true,
                                            new StringSink() // sink is transient and used internally by printer
                                    );
                                }
                            } catch (Throwable e) {
                                e.printStackTrace();
                                errors.incrementAndGet();
                            } finally {
                                haltLatch.countDown();
                            }
                        }).start();
                    }

                    haltLatch.await();
                    Assert.assertEquals(0, errors.get());
                },
                configuration
        );
    }
}
