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

package io.questdb.mp;

import io.questdb.Metrics;
import io.questdb.ServerConfiguration;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cutlass.http.HttpMinServerConfiguration;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.pgwire.PGWireConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.metrics.MetricsConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class WorkerPoolManagerTest {

    private static final Metrics METRICS = Metrics.disabled();


    @Test
    public void testConstructor() throws SqlException {
        final int workerCount = 2;
        final AtomicInteger counter = new AtomicInteger(0);
        final WorkerPoolManager workerPoolManager = createWorkerPoolManager(workerCount, sharedPool -> counter.incrementAndGet());
        Assert.assertEquals(1, counter.get());
        Assert.assertNotNull(workerPoolManager.getSharedPool());
        Assert.assertEquals(workerCount, workerPoolManager.getSharedWorkerCount());
    }

    @Test
    public void testGetInstanceDefaultPool() throws SqlException {
        final int workerCount = 2;
        final WorkerPoolManager workerPoolManager = createWorkerPoolManager(workerCount);
        WorkerPool workerPool = workerPoolManager.getInstance(new WorkerPoolConfiguration() {
            @Override
            public int getWorkerCount() {
                return 0; // No workers, will result in returning the shared pool
            }

            @Override
            public String getPoolName() {
                return "pool";
            }
        }, METRICS);
        Assert.assertTrue(workerPoolManager.getSharedPool() == workerPool);
        Assert.assertEquals(workerCount, workerPool.getWorkerCount());
        Assert.assertEquals("worker", workerPool.getPoolName());
    }

    @Test
    public void testGetInstanceDedicatedPool() throws SqlException {
        final int workerCount = 2;
        final String poolName = "pool";
        final WorkerPoolManager workerPoolManager = createWorkerPoolManager(workerCount);
        WorkerPool workerPool = workerPoolManager.getInstance(new WorkerPoolConfiguration() {
            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public String getPoolName() {
                return poolName;
            }
        }, METRICS);
        Assert.assertFalse(workerPoolManager.getSharedPool() == workerPool);
        Assert.assertEquals(workerCount, workerPool.getWorkerCount());
        Assert.assertEquals(poolName, workerPool.getPoolName());
    }

    @Test
    public void testGetInstanceDedicatedPoolGetAgain() throws SqlException {
        final int workerCount = 2;
        final String poolName = "pool";
        final WorkerPoolManager workerPoolManager = createWorkerPoolManager(workerCount);
        final WorkerPoolConfiguration workerPoolConfiguration = new WorkerPoolConfiguration() {
            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public String getPoolName() {
                return poolName;
            }
        };
        WorkerPool workerPool0 = workerPoolManager.getInstance(workerPoolConfiguration, METRICS);
        Assert.assertFalse(workerPoolManager.getSharedPool() == workerPool0);
        WorkerPool workerPool1 = workerPoolManager.getInstance(workerPoolConfiguration, METRICS);
        Assert.assertTrue(workerPool0 == workerPool1);
        Assert.assertEquals(workerCount, workerPool0.getWorkerCount());
        Assert.assertEquals(poolName, workerPool0.getPoolName());
        Assert.assertEquals(workerCount, workerPool1.getWorkerCount());
        Assert.assertEquals(poolName, workerPool1.getPoolName());
    }

    @Test
    public void testGetInstanceFailsAsStartAllWasCalled() throws SqlException {
        final WorkerPoolManager workerPoolManager = createWorkerPoolManager(1);
        workerPoolManager.startAll(null);
        try {
            workerPoolManager.getInstance(new WorkerPoolConfiguration() {
                @Override
                public int getWorkerCount() {
                    return 0;
                }

                @Override
                public String getPoolName() {
                    return null;
                }
            }, METRICS);
            Assert.fail();
        } catch (IllegalStateException err) {
            TestUtils.assertContains("can only get instance before start", err.getMessage());
        } finally {
            workerPoolManager.closeAll();
        }
    }

    @Test
    public void testStartCloseAreOneOff() throws SqlException {
        final WorkerPoolManager workerPoolManager = createWorkerPoolManager(1);
        workerPoolManager.startAll(null);
        workerPoolManager.startAll(null);
        workerPoolManager.closeAll();
        workerPoolManager.closeAll();
    }

    private static WorkerPoolManager createWorkerPoolManager(int workerCount) throws SqlException {
        return createWorkerPoolManager(workerCount, null);
    }

    private static WorkerPoolManager createWorkerPoolManager(int workerCount, Consumer<WorkerPool> call) throws SqlException {
        return new WorkerPoolManager(createServerConfig(workerCount), METRICS) {
            @Override
            protected void configureSharedPool(WorkerPool sharedPool) {
                if (call != null) {
                    call.accept(getSharedPool());
                }
            }
        };
    }

    private static ServerConfiguration createServerConfig(int workerCount) {
        return new ServerConfiguration() {
            @Override
            public CairoConfiguration getCairoConfiguration() {
                return null;
            }

            @Override
            public HttpServerConfiguration getHttpServerConfiguration() {
                return null;
            }

            @Override
            public HttpMinServerConfiguration getHttpMinServerConfiguration() {
                return null;
            }

            @Override
            public LineUdpReceiverConfiguration getLineUdpReceiverConfiguration() {
                return null;
            }

            @Override
            public LineTcpReceiverConfiguration getLineTcpReceiverConfiguration() {
                return null;
            }

            @Override
            public WorkerPoolConfiguration getWorkerPoolConfiguration() {
                return () -> workerCount;
            }

            @Override
            public PGWireConfiguration getPGWireConfiguration() {
                return null;
            }

            @Override
            public MetricsConfiguration getMetricsConfiguration() {
                return null;
            }
        };
    }
}
