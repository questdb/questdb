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
import io.questdb.cutlass.pgwire.PGWireConfiguration;
import io.questdb.metrics.MetricsConfiguration;
import io.questdb.metrics.WorkerMetrics;
import io.questdb.mp.Job;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class WorkerPoolManagerTest {

    private static final String END_MESSAGE = "run is over";

    @Before
    public void setUp() throws Exception {
        Metrics.ENABLED.clear();
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
    public void testGetInstanceDedicatedPool() {
        final int workerCount = 2;
        final String poolName = "pool";
        final WorkerPoolManager workerPoolManager = createWorkerPoolManager(workerCount);
        WorkerPool networkSharedPool = workerPoolManager.getSharedNetworkPool(new WorkerPoolConfiguration() {
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
        WorkerPool networkSharedPool0 = workerPoolManager.getSharedNetworkPool(workerPoolConfiguration, WorkerPoolManager.Requester.OTHER);
        Assert.assertNotSame(workerPoolManager.getSharedPoolNetwork(), networkSharedPool0);
        WorkerPool networkSharedPool1 = workerPoolManager.getSharedNetworkPool(workerPoolConfiguration, WorkerPoolManager.Requester.OTHER);
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
        WorkerPool networkSharedPool = workerPoolManager.getSharedNetworkPool(new WorkerPoolConfiguration() {
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
            workerPoolManager.getSharedNetworkPool(new WorkerPoolConfiguration() {
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
        WorkerPool p0 = workerPoolManager.getSharedNetworkPool(
                workerPoolConfiguration("UP", 30L),
                WorkerPoolManager.Requester.OTHER
        );
        WorkerPool p1 = workerPoolManager.getSharedNetworkPool(
                workerPoolConfiguration("DOWN", 10L),
                WorkerPoolManager.Requester.OTHER
        );
        p0.assign(slowCountUpJob(count));
        p1.assign(fastCountDownJob(endLatch));
        workerPoolManager.start(null);
        if (!endLatch.await(TimeUnit.SECONDS.toNanos(60L))) {
            Assert.fail("timeout");
        }
        workerPoolManager.halt();

        Assert.assertEquals(0, endLatch.getCount());
        WorkerMetrics metrics = Metrics.ENABLED.workerMetrics();
        long min = metrics.getMinElapsedMicros();
        long max = metrics.getMaxElapsedMicros();
        Assert.assertTrue(min > 0L);
        Assert.assertTrue(max > min);
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

    private static ServerConfiguration createServerConfig(int workerCount) {
        return new ServerConfiguration() {
            @Override
            public CairoConfiguration getCairoConfiguration() {
                return null;
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
            public PGWireConfiguration getPGWireConfiguration() {
                return null;
            }

            @Override
            public PublicPassthroughConfiguration getPublicPassthroughConfiguration() {
                return null;
            }

            @Override
            public WorkerPoolConfiguration getWalApplyPoolConfiguration() {
                return null;
            }

            @Override
            public WorkerPoolConfiguration getNetworkWorkerPoolConfiguration() {
                return () -> workerCount;
            }

            @Override
            public WorkerPoolConfiguration getQueryWorkerPoolConfiguration() {
                return () -> workerCount;
            }

            @Override
            public WorkerPoolConfiguration getWriteWorkerPoolConfiguration() {
                return () -> workerCount;
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

    private static Job fastCountDownJob(SOCountDownLatch endLatch) {
        return (workerId, runStatus) -> {
            endLatch.countDown();
            if (endLatch.getCount() < 1) {
                throw new RuntimeException(END_MESSAGE);
            }
            return false; // not eager
        };
    }

    private static Job scrapeIntoPrometheusJob(AtomicReference<DirectUtf8Sink> sink) {
        return (workerId, runStatus) -> {
            final DirectUtf8Sink s = sink.get();
            s.clear();
            Metrics.ENABLED.scrapeIntoPrometheus(s);
            return false; // not eager
        };
    }

    private static Job slowCountUpJob(AtomicInteger count) {
        return (workerId, runStatus) -> {
            count.incrementAndGet();
            return false; // not eager
        };
    }

    private static WorkerPoolConfiguration workerPoolConfiguration(String poolName, long sleepMillis) {
        return new WorkerPoolConfiguration() {
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
        };
    }
}
