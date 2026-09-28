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

package io.questdb.test;

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.MemoryConfiguration;
import io.questdb.Metrics;
import io.questdb.PublicPassthroughConfiguration;
import io.questdb.ServerConfiguration;
import io.questdb.WorkerPoolManager;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.metrics.MetricsConfiguration;
import io.questdb.metrics.WorkerMetrics;
import io.questdb.mp.Job;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class WorkerPoolManagerTest {
    private static final Log LOG = LogFactory.getLog(WorkerPoolManagerTest.class);

    private static final String END_MESSAGE = "run is over";

    @Before
    public void setUp() throws Exception {
        Metrics.ENABLED.clear();
    }

    @Test
    public void testAssignFailureRetainsCloneOwnership() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final RuntimeException failure = new RuntimeException("hash");
            final WorkerPool pool = new WorkerPool(createServerConfig(2).getSharedWorkerPoolWriteConfiguration());
            try {
                pool.assign(new ThrowingHashNativeMemoryJob(failure, false));
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(failure, e);
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testBoundedHaltApiCompatibility() throws Throwable {
        final MethodHandles.Lookup lookup = MethodHandles.publicLookup();
        final MethodHandle managerHalt = lookup.findVirtual(
                WorkerPoolManager.class,
                "halt",
                MethodType.methodType(void.class, long.class)
        );
        final MethodHandle poolHaltBy = lookup.findVirtual(
                WorkerPool.class,
                "haltBy",
                MethodType.methodType(boolean.class, long.class)
        );

        final AtomicBoolean poolResourceClosed = new AtomicBoolean();
        final WorkerPool pool = new WorkerPool(() -> 0);
        pool.freeOnExit(closeFlagJob(poolResourceClosed));
        try {
            Assert.assertTrue((boolean) poolHaltBy.invokeExact(
                    pool,
                    System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
            ));
        } finally {
            pool.halt();
        }
        Assert.assertTrue(poolResourceClosed.get());

        final AtomicBoolean managerResourceClosed = new AtomicBoolean();
        final WorkerPoolManager workerPoolManager = createWorkerPoolManager(0);
        workerPoolManager.getSharedPoolNetwork().freeOnExit(closeFlagJob(managerResourceClosed));
        try {
            managerHalt.invokeExact(
                    workerPoolManager,
                    System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
            );
        } finally {
            workerPoolManager.halt();
        }
        Assert.assertTrue(managerResourceClosed.get());
    }

    @Test
    public void testBoundedHaltRetainsResourcesWhenWorkerWedged() {
        final AtomicBoolean release = new AtomicBoolean(false);
        final AtomicInteger closeOrder = new AtomicInteger();
        final AtomicBoolean resourceClosed = new AtomicBoolean(false);
        final SOCountDownLatch jobEntered = new SOCountDownLatch(1);
        final WorkerPool pool = TestWorkerPool.createWithRandomMode(TestUtils.generateRandom(LOG), new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "wedged";
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }

            @Override
            public boolean isDaemonPool() {
                // Daemon so the wedged worker cannot keep the JVM alive after the test returns.
                return true;
            }
        });
        pool.assign(new Job() {
            @Override
            public void closeInstance() {
                resourceClosed.set(true);
            }

            @Override
            public boolean run(Job.WorkerContext workerContext) {
                jobEntered.countDown();
                // Spin until released so the worker never reaches halted.countDown() within the bound.
                while (!release.get()) {
                    Os.pause();
                }
                return false;
            }
        });
        pool.freeOnExit(new OrderedCloseJob(closeOrder, 0));
        try {
            pool.start(null);
            Assert.assertTrue("worker job never started", jobEntered.await(TimeUnit.SECONDS.toNanos(30L)));

            final long budgetNanos = TimeUnit.MILLISECONDS.toNanos(200L);
            final long start = System.nanoTime();
            Assert.assertFalse(pool.haltBy(start + budgetNanos));
            final long elapsed = System.nanoTime() - start;

            // halt retained the live pool and returned close to the budget rather than blocking
            // forever. Allow generous slack for CI scheduling jitter.
            Assert.assertTrue(
                    "bounded halt returned too fast, budget not honoured [elapsedMs=" + (elapsed / 1_000_000) + "]",
                    elapsed >= budgetNanos - TimeUnit.MILLISECONDS.toNanos(50L)
            );
            Assert.assertTrue(
                    "bounded halt did not respect its timeout [elapsedMs=" + (elapsed / 1_000_000) + "]",
                    elapsed < TimeUnit.SECONDS.toNanos(10L)
            );
            Assert.assertEquals(0, closeOrder.get());
            Assert.assertFalse(resourceClosed.get());
        } finally {
            release.set(true);
            Assert.assertTrue(pool.haltBy(System.nanoTime() + TimeUnit.SECONDS.toNanos(30)));
        }
        Assert.assertEquals(1, closeOrder.get());
        Assert.assertTrue(resourceClosed.get());
    }

    @Test
    public void testConstructor() {
        final int workerCount = 2;
        final AtomicInteger counter = new AtomicInteger(0);
        final WorkerPoolManager workerPoolManager = createWorkerPoolManager(workerCount, sharedPool -> counter.incrementAndGet());
        Assert.assertEquals(1, counter.get());
        Assert.assertNotNull(workerPoolManager.getSharedPoolNetwork());
        Assert.assertEquals(workerCount, workerPoolManager.getSharedQueryWorkerCount());
    }

    @Test
    public void testConstructorFailureRollsBackPools() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final RuntimeException cleanupFailure = new RuntimeException("cleanup");
            final RuntimeException primaryFailure = new RuntimeException("configure");
            try {
                new WorkerPoolManager(createServerConfig(2)) {
                    @Override
                    protected void configureWorkerPools(WorkerPool sharedPoolQuery, WorkerPool sharedPoolWrite) {
                        final WorkerPool dedicatedPool = getSharedPoolNetwork(
                                workerPoolConfiguration("dedicated", 1),
                                WorkerPoolManager.Requester.OTHER
                        );
                        dedicatedPool.freeOnExit(new ThrowingCloseJob(cleanupFailure));
                        getSharedPoolNetwork().assign(new NativeMemoryJob());
                        sharedPoolQuery.assign(new NativeMemoryJob());
                        sharedPoolWrite.assign(new NativeMemoryJob());
                        throw primaryFailure;
                    }
                };
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(primaryFailure, e);
                Assert.assertEquals(1, e.getSuppressed().length);
                Assert.assertSame(cleanupFailure, e.getSuppressed()[0]);
            }
        });
    }

    @Test
    public void testGetInstanceDedicatedPool() {
        final int workerCount = 2;
        final String poolName = "pool";
        final WorkerPoolManager workerPoolManager = createWorkerPoolManager(workerCount);
        WorkerPool networkSharedPool = workerPoolManager.getSharedPoolNetwork(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return poolName;
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }
        }, WorkerPoolManager.Requester.OTHER);
        Assert.assertNotSame(workerPoolManager.getSharedPoolNetwork(), networkSharedPool);
        Assert.assertEquals(workerCount, networkSharedPool.getWorkerCount());
        Assert.assertEquals(poolName, networkSharedPool.getPoolName());
    }

    @Test
    public void testGetInstanceDedicatedPoolGetAgain() {
        final int workerCount = 2;
        final String poolName = "pool";
        final WorkerPoolManager workerPoolManager = createWorkerPoolManager(workerCount);
        final WorkerPoolConfiguration workerPoolConfiguration = new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return poolName;
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }
        };
        WorkerPool networkSharedPool0 = workerPoolManager.getSharedPoolNetwork(workerPoolConfiguration, WorkerPoolManager.Requester.OTHER);
        Assert.assertNotSame(workerPoolManager.getSharedPoolNetwork(), networkSharedPool0);
        WorkerPool networkSharedPool1 = workerPoolManager.getSharedPoolNetwork(workerPoolConfiguration, WorkerPoolManager.Requester.OTHER);
        Assert.assertSame(networkSharedPool0, networkSharedPool1);
        Assert.assertEquals(workerCount, networkSharedPool0.getWorkerCount());
        Assert.assertEquals(poolName, networkSharedPool0.getPoolName());
        Assert.assertEquals(workerCount, networkSharedPool1.getWorkerCount());
        Assert.assertEquals(poolName, networkSharedPool1.getPoolName());
    }

    @Test
    public void testGetInstanceDefaultPool() {
        final int workerCount = 2;
        final WorkerPoolManager workerPoolManager = createWorkerPoolManager(workerCount);
        WorkerPool networkSharedPool = workerPoolManager.getSharedPoolNetwork(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "pool";
            }

            @Override
            public int getWorkerCount() {
                return 0; // No workers, will result in returning the shared pool
            }
        }, WorkerPoolManager.Requester.OTHER);
        Assert.assertSame(workerPoolManager.getSharedPoolNetwork(), networkSharedPool);
        Assert.assertEquals(workerCount, networkSharedPool.getWorkerCount());
        Assert.assertEquals("worker", networkSharedPool.getPoolName());
    }

    @Test
    public void testGetInstanceFailsAsStartAllWasCalled() {
        final WorkerPoolManager workerPoolManager = createWorkerPoolManager(1);
        workerPoolManager.start(null);
        try {
            workerPoolManager.getSharedPoolNetwork(new WorkerPoolConfiguration() {
                @Override
                public String getPoolName() {
                    return null;
                }

                @Override
                public int getWorkerCount() {
                    return 0;
                }
            }, WorkerPoolManager.Requester.OTHER);
            Assert.fail();
        } catch (IllegalStateException e) {
            TestUtils.assertContains(e.getMessage(), "can only get instance before start");
        } finally {
            workerPoolManager.halt();
        }
    }

    @Test
    public void testHaltAttemptsEveryFreeOnExitResourceAfterFailure() {
        final AtomicInteger closeOrder = new AtomicInteger();
        final RuntimeException failure = new RuntimeException("close");
        final WorkerPool pool = new TestWorkerPool(workerPoolConfiguration("pool", 1));
        pool.freeOnExit(new ThrowingCloseJob(failure));
        pool.freeOnExit(new OrderedCloseJob(closeOrder, 0));
        try {
            pool.halt();
            Assert.fail();
        } catch (RuntimeException e) {
            Assert.assertSame(failure, e);
        }
        Assert.assertEquals(1, closeOrder.get());
        Assert.assertTrue(pool.haltWithin(TimeUnit.SECONDS.toNanos(30)));
    }

    @Test
    public void testLineTcpPoolsHaltProducerBeforeWriter() {
        assertLineTcpHaltOrder(0, 0);
        assertLineTcpHaltOrder(0, 1);
        assertLineTcpHaltOrder(1, 0);
        assertLineTcpHaltOrder(1, 1);
    }

    @Test
    public void testLineTcpPoolsWithSameNameHaltOnceInDependencyOrder() {
        final AtomicInteger closeOrder = new AtomicInteger();
        final WorkerPoolManager workerPoolManager = createWorkerPoolManager(1);
        final WorkerPool ioPool = workerPoolManager.getSharedPoolNetwork(
                workerPoolConfiguration("line", 1),
                WorkerPoolManager.Requester.LINE_TCP_IO
        );
        final WorkerPool writerPool = workerPoolManager.getSharedPoolWrite(
                workerPoolConfiguration("line", 1),
                WorkerPoolManager.Requester.LINE_TCP_WRITER
        );
        Assert.assertSame(ioPool, writerPool);
        ioPool.freeOnExit(new OrderedCloseJob(closeOrder, 0));
        writerPool.freeOnExit(new OrderedCloseJob(closeOrder, 1));
        Assert.assertTrue(workerPoolManager.haltAndReportCompletion());
        Assert.assertEquals(2, closeOrder.get());
    }

    @Test
    public void testManagerBoundedHaltTimesOutAndStaysRetryable() {
        final AtomicBoolean release = new AtomicBoolean(false);
        final SOCountDownLatch jobEntered = new SOCountDownLatch(1);
        final AtomicInteger closeOrder = new AtomicInteger();
        final WorkerPoolManager workerPoolManager = createWorkerPoolManager(1, pool -> {
            pool.assign(workerContext -> {
                jobEntered.countDown();
                while (!release.get()) {
                    Os.pause();
                }
                return false;
            });
            pool.freeOnExit(new OrderedCloseJob(closeOrder, 0));
        });
        try {
            workerPoolManager.start(null);
            Assert.assertTrue("worker job never started", jobEntered.await(TimeUnit.SECONDS.toNanos(30L)));

            final long budgetNanos = TimeUnit.MILLISECONDS.toNanos(200L);
            final long start = System.nanoTime();
            Assert.assertFalse(workerPoolManager.haltWithin(budgetNanos));
            final long elapsed = System.nanoTime() - start;
            Assert.assertTrue(
                    "bounded manager halt did not respect its budget [elapsedMs=" + (elapsed / 1_000_000) + "]",
                    elapsed < TimeUnit.SECONDS.toNanos(10L)
            );
            Assert.assertNull(workerPoolManager.getHaltFailure());
            Assert.assertEquals(0, closeOrder.get());
        } finally {
            release.set(true);
            Assert.assertTrue(workerPoolManager.haltWithin(TimeUnit.SECONDS.toNanos(30L)));
        }
        Assert.assertEquals(1, closeOrder.get());
    }

    @Test
    public void testManagerHaltContinuesAfterPoolCleanupFailure() {
        final AtomicInteger closeOrder = new AtomicInteger();
        final RuntimeException failure = new RuntimeException("close");
        final WorkerPoolManager workerPoolManager = createWorkerPoolManager(1);
        workerPoolManager.getSharedPoolNetwork().freeOnExit(new ThrowingCloseJob(failure));
        workerPoolManager.getSharedPoolWrite(
                workerPoolConfiguration("shared", 0),
                WorkerPoolManager.Requester.OTHER
        ).freeOnExit(new OrderedCloseJob(closeOrder, 0));
        Assert.assertTrue(workerPoolManager.haltAndReportCompletion());
        Assert.assertSame(failure, workerPoolManager.getHaltFailure());
        Assert.assertEquals(1, closeOrder.get());
        Assert.assertTrue(workerPoolManager.haltAndReportCompletion());
        Assert.assertSame(failure, workerPoolManager.getHaltFailure());
        Assert.assertEquals(1, closeOrder.get());
    }

    @Test
    public void testManagerHaltRetainsEveryPoolCleanupFailure() {
        final RuntimeException networkFailure = new RuntimeException("network");
        final RuntimeException writeFailure = new RuntimeException("write");
        final WorkerPoolManager workerPoolManager = createWorkerPoolManager(1);
        workerPoolManager.getSharedPoolNetwork().freeOnExit(new ThrowingCloseJob(networkFailure));
        workerPoolManager.getSharedPoolWrite(
                workerPoolConfiguration("shared", 0),
                WorkerPoolManager.Requester.OTHER
        ).freeOnExit(new ThrowingCloseJob(writeFailure));

        Assert.assertTrue(workerPoolManager.haltAndReportCompletion());
        Assert.assertSame(networkFailure, workerPoolManager.getHaltFailure());
        Assert.assertArrayEquals(new Throwable[]{writeFailure}, networkFailure.getSuppressed());
    }

    @Test
    public void testPublicWorkerPoolApiCompatibility() throws Throwable {
        final MethodHandles.Lookup lookup = MethodHandles.publicLookup();
        final MethodHandle isLegacy = lookup.findVirtual(
                WorkerPoolConfiguration.class,
                "isLegacy",
                MethodType.methodType(boolean.class)
        );
        final MethodHandle poolHalt = lookup.findVirtual(
                WorkerPool.class,
                "halt",
                MethodType.methodType(void.class, long.class)
        );
        final MethodHandle managerHalt = lookup.findVirtual(
                WorkerPoolManager.class,
                "halt",
                MethodType.methodType(void.class)
        );

        final WorkerPoolConfiguration legacyConfiguration = workerPoolConfiguration(WorkerPoolMode.LEGACY);
        final WorkerPoolConfiguration fiberConfiguration = workerPoolConfiguration(WorkerPoolMode.FIBER_HOST);
        Assert.assertTrue((boolean) isLegacy.invokeExact(legacyConfiguration));
        Assert.assertFalse((boolean) isLegacy.invokeExact(fiberConfiguration));

        final AtomicInteger poolCloseOrder = new AtomicInteger();
        final WorkerPool pool = new WorkerPool(legacyConfiguration);
        pool.freeOnExit(new OrderedCloseJob(poolCloseOrder, 0));
        try {
            poolHalt.invokeExact(pool, TimeUnit.SECONDS.toNanos(1));
        } finally {
            pool.halt();
        }
        Assert.assertEquals(1, poolCloseOrder.get());

        final AtomicInteger managerCloseOrder = new AtomicInteger();
        final WorkerPoolManager workerPoolManager = createWorkerPoolManager(0);
        workerPoolManager.getSharedPoolNetwork().freeOnExit(new OrderedCloseJob(managerCloseOrder, 0));
        try {
            managerHalt.invokeExact(workerPoolManager);
        } finally {
            workerPoolManager.halt();
        }
        Assert.assertEquals(1, managerCloseOrder.get());
    }

    @Test
    public void testScrapeWorkerMetrics() {
        int events = 20;
        AtomicInteger count = new AtomicInteger();
        SOCountDownLatch endLatch = new SOCountDownLatch(events);
        AtomicReference<DirectUtf8Sink> sink = new AtomicReference<>(new DirectUtf8Sink(32));

        final ServerConfiguration config = createServerConfig(1); // shared pool
        final WorkerPoolManager workerPoolManager = new WorkerPoolManager(config) {
            @Override
            protected void configureWorkerPools(final WorkerPool sharedPoolR, final WorkerPool sharedPoolW) {
                sharedPoolW.assign(scrapeIntoPrometheusJob(sink));
            }
        };
        WorkerPool p0 = workerPoolManager.getSharedPoolNetwork(
                workerPoolConfiguration("UP", 30L),
                WorkerPoolManager.Requester.OTHER
        );
        WorkerPool p1 = workerPoolManager.getSharedPoolNetwork(
                workerPoolConfiguration("DOWN", 10L),
                WorkerPoolManager.Requester.OTHER
        );
        p0.assign(slowCountUpJob(count));
        p1.assign(fastCountDownJob(endLatch));
        final WorkerMetrics metrics = Metrics.ENABLED.workerMetrics();
        metrics.clear();
        final long startNanos = System.nanoTime();
        workerPoolManager.start(null);
        if (!endLatch.await(TimeUnit.SECONDS.toNanos(60L))) {
            Assert.fail("timeout");
        }
        workerPoolManager.halt();
        final long wallMicros = (System.nanoTime() - startNanos) / 1000;

        Assert.assertEquals(0, endLatch.getCount());
        long min = metrics.getMinElapsedMicros();
        long max = metrics.getMaxElapsedMicros();
        Assert.assertTrue(min > 0L);
        Assert.assertTrue(max > min);
        Assert.assertTrue(
                "job start age is not in microseconds [max=" + max + ", wall=" + wallMicros + ']',
                max <= wallMicros
        );
        String metricsAsStr = sink.get().toString();
        TestUtils.assertContains(metricsAsStr, "questdb_workers_job_start_micros_min");
        TestUtils.assertContains(metricsAsStr, "questdb_workers_job_start_micros_max");
    }

    @Test
    public void testStartHaltAreOneOff() {
        final WorkerPoolManager workerPoolManager = createWorkerPoolManager(1);
        workerPoolManager.start(null);
        workerPoolManager.start(null);
        workerPoolManager.halt();
        workerPoolManager.halt();
    }

    private static void assertLineTcpHaltOrder(int ioWorkerCount, int writerWorkerCount) {
        final AtomicInteger closeOrder = new AtomicInteger();
        final WorkerPoolManager workerPoolManager = createWorkerPoolManager(1);
        final WorkerPool ioPool = workerPoolManager.getSharedPoolNetwork(
                workerPoolConfiguration("line-io", ioWorkerCount),
                WorkerPoolManager.Requester.LINE_TCP_IO
        );
        final WorkerPool writerPool = workerPoolManager.getSharedPoolWrite(
                workerPoolConfiguration("line-writer", writerWorkerCount),
                WorkerPoolManager.Requester.LINE_TCP_WRITER
        );
        ioPool.freeOnExit(new OrderedCloseJob(closeOrder, 0));
        writerPool.freeOnExit(new OrderedCloseJob(closeOrder, 1));
        workerPoolManager.halt();
        Assert.assertEquals(2, closeOrder.get());
    }

    private static ServerConfiguration createServerConfig(int workerCount) {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        return new ServerConfiguration() {
            @Override
            public CairoConfiguration getCairoConfiguration() {
                return null;
            }

            @Override
            public WorkerPoolConfiguration getExportPoolConfiguration() {
                return () -> workerCount;
            }

            @Override
            public FactoryProvider getFactoryProvider() {
                return DefaultFactoryProvider.INSTANCE;
            }

            @Override
            public HttpServerConfiguration getHttpMinServerConfiguration() {
                return null;
            }

            @Override
            public HttpFullFatServerConfiguration getHttpServerConfiguration() {
                return null;
            }

            @Override
            public LineTcpReceiverConfiguration getLineTcpReceiverConfiguration() {
                return null;
            }

            @Override
            public LineUdpReceiverConfiguration getLineUdpReceiverConfiguration() {
                return null;
            }

            @Override
            public WorkerPoolConfiguration getLiveViewRefreshPoolConfiguration() {
                return null;
            }

            @Override
            public WorkerPoolConfiguration getMatViewRefreshPoolConfiguration() {
                return null;
            }

            @Override
            public MemoryConfiguration getMemoryConfiguration() {
                return null;
            }

            @Override
            public Metrics getMetrics() {
                return Metrics.ENABLED;
            }

            @Override
            public MetricsConfiguration getMetricsConfiguration() {
                return null;
            }

            @Override
            public PGConfiguration getPGWireConfiguration() {
                return null;
            }

            @Override
            public PublicPassthroughConfiguration getPublicPassthroughConfiguration() {
                return null;
            }

            @Override
            public WorkerPoolConfiguration getSharedWorkerPoolNetworkConfiguration() {
                return TestWorkerPool.withRandomMode(rnd, () -> workerCount);
            }

            @Override
            public WorkerPoolConfiguration getSharedWorkerPoolQueryConfiguration() {
                return TestWorkerPool.withRandomMode(rnd, () -> workerCount);
            }

            @Override
            public WorkerPoolConfiguration getSharedWorkerPoolWriteConfiguration() {
                return TestWorkerPool.withRandomMode(rnd, () -> workerCount);
            }

            @Override
            public WorkerPoolConfiguration getViewCompilerPoolConfiguration() {
                return TestWorkerPool.withRandomMode(rnd, () -> workerCount);
            }

            @Override
            public WorkerPoolConfiguration getWalApplyPoolConfiguration() {
                return null;
            }
        };
    }

    private static WorkerPoolManager createWorkerPoolManager(int workerCount, Consumer<WorkerPool> call) {
        return new WorkerPoolManager(createServerConfig(workerCount)) {
            @Override
            protected void configureWorkerPools(final WorkerPool sharedPoolR, final WorkerPool sharedPoolW) {
                if (call != null) {
                    call.accept(sharedPoolR);
                }
            }
        };
    }

    private static WorkerPoolManager createWorkerPoolManager(int workerCount) {
        return createWorkerPoolManager(workerCount, null);
    }

    private static WorkerPoolConfiguration fiberWorkerPoolConfiguration(String poolName, int workerCount) {
        return new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return poolName;
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public WorkerPoolMode getWorkerPoolMode() {
                return WorkerPoolMode.FIBER_HOST;
            }
        };
    }

    private static Job closeFlagJob(AtomicBoolean closed) {
        return new CloseFlagJob(closed);
    }

    private static Job fastCountDownJob(SOCountDownLatch endLatch) {
        return workerContext -> {
            endLatch.countDown();
            if (endLatch.getCount() < 1) {
                throw new RuntimeException(END_MESSAGE);
            }
            return false; // not eager
        };
    }

    private static Job scrapeIntoPrometheusJob(AtomicReference<DirectUtf8Sink> sink) {
        return workerContext -> {
            final DirectUtf8Sink s = sink.get();
            s.clear();
            Metrics.ENABLED.scrapeIntoPrometheus(s);
            return false; // not eager
        };
    }

    private static Job slowCountUpJob(AtomicInteger count) {
        return workerContext -> {
            count.incrementAndGet();
            return false; // not eager
        };
    }

    private static WorkerPoolConfiguration workerPoolConfiguration(String poolName, int workerCount) {
        return TestWorkerPool.withRandomMode(TestUtils.generateRandom(LOG), new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return poolName;
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }
        });
    }

    private static WorkerPoolConfiguration workerPoolConfiguration(String poolName, long sleepMillis) {
        return TestWorkerPool.withRandomMode(TestUtils.generateRandom(LOG), new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return poolName;
            }

            @Override
            public long getSleepThreshold() {
                return 1L;
            }

            @Override
            public long getSleepTimeout() {
                return sleepMillis;
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }

            @Override
            public boolean haltOnError() {
                return true;
            }
        });
    }

    private static WorkerPoolConfiguration workerPoolConfiguration(WorkerPoolMode workerPoolMode) {
        return new WorkerPoolConfiguration() {
            @Override
            public int getWorkerCount() {
                return 0;
            }

            @Override
            public WorkerPoolMode getWorkerPoolMode() {
                return workerPoolMode;
            }
        };
    }

    private static final class CloseFlagJob implements Job, Closeable {
        private final AtomicBoolean closed;

        private CloseFlagJob(AtomicBoolean closed) {
            this.closed = closed;
        }

        @Override
        public void close() {
            closed.set(true);
        }

        @Override
        public boolean run(Job.WorkerContext workerContext) {
            return false;
        }
    }

    private static final class NativeMemoryJob implements Job {
        private static final int MEMORY_SIZE = Long.BYTES;
        private long address = Unsafe.malloc(MEMORY_SIZE, MemoryTag.NATIVE_DEFAULT);

        @Override
        public Job cloneInstance() {
            return new NativeMemoryJob();
        }

        @Override
        public void closeInstance() {
            address = Unsafe.free(address, MEMORY_SIZE, MemoryTag.NATIVE_DEFAULT);
        }

        @Override
        public boolean run(Job.WorkerContext workerContext) {
            return false;
        }
    }

    private static final class OrderedCloseJob implements Job, Closeable {
        private final AtomicInteger closeOrder;
        private final int expectedOrder;

        private OrderedCloseJob(AtomicInteger closeOrder, int expectedOrder) {
            this.closeOrder = closeOrder;
            this.expectedOrder = expectedOrder;
        }

        @Override
        public void close() {
            if (!closeOrder.compareAndSet(expectedOrder, expectedOrder + 1)) {
                closeOrder.set(Integer.MIN_VALUE);
            }
        }

        @Override
        public boolean run(Job.WorkerContext workerContext) {
            return false;
        }
    }

    private static final class ThrowingCloseJob implements Job, Closeable {
        private final RuntimeException failure;

        private ThrowingCloseJob(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public void close() {
            throw failure;
        }

        @Override
        public boolean run(Job.WorkerContext workerContext) {
            return false;
        }
    }

    private static final class ThrowingHashNativeMemoryJob implements Job {
        private static final int MEMORY_SIZE = Long.BYTES;
        private long address = Unsafe.malloc(MEMORY_SIZE, MemoryTag.NATIVE_DEFAULT);
        private final RuntimeException failure;
        private final boolean isHashFailure;

        private ThrowingHashNativeMemoryJob(RuntimeException failure, boolean isHashFailure) {
            this.failure = failure;
            this.isHashFailure = isHashFailure;
        }

        @Override
        public Job cloneInstance() {
            return new ThrowingHashNativeMemoryJob(failure, true);
        }

        @Override
        public void closeInstance() {
            address = Unsafe.free(address, MEMORY_SIZE, MemoryTag.NATIVE_DEFAULT);
        }

        @Override
        public int hashCode() {
            if (isHashFailure) {
                throw failure;
            }
            return super.hashCode();
        }

        @Override
        public boolean run(Job.WorkerContext workerContext) {
            return false;
        }
    }
}
