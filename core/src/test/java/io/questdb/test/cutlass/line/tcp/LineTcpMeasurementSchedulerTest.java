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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.Metrics;
import io.questdb.cairo.CairoException;
import io.questdb.cutlass.line.tcp.DefaultLineTcpReceiverConfiguration;
import io.questdb.cutlass.line.tcp.LineTcpConnectionContext;
import io.questdb.cutlass.line.tcp.LineTcpMeasurementScheduler;
import io.questdb.cutlass.line.tcp.NetworkIOJob;
import io.questdb.cutlass.line.tcp.SymbolCache;
import io.questdb.cutlass.line.tcp.TableUpdateDetails;
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.network.IODispatcher;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Pool;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import org.junit.Assert;
import org.junit.Test;

public class LineTcpMeasurementSchedulerTest extends AbstractCairoTest {

    @Test
    public void testConstructorFailureClosesCreatedJobsAndPreservesPrimaryFailure() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException cleanupFailure = new RuntimeException("cleanup");
            final RuntimeException primaryFailure = new RuntimeException("register");
            final TrackingNetworkIOJob networkIOJob = new TrackingNetworkIOJob(cleanupFailure);
            final WorkerPool networkPool = new TestWorkerPool(1) {
                @Override
                public void freeOnExit(Job job) {
                    throw primaryFailure;
                }
            };
            final WorkerPool writePool = new TestWorkerPool(1);
            try (LineTcpMeasurementScheduler ignored = new LineTcpMeasurementScheduler(
                    new DefaultLineTcpReceiverConfiguration(configuration),
                    engine,
                    networkPool,
                    null,
                    writePool
            ) {
                @Override
                protected NetworkIOJob createNetworkIOJob(
                        IODispatcher<LineTcpConnectionContext> dispatcher,
                        int workerId
                ) {
                    Assert.assertEquals(0, workerId);
                    return networkIOJob;
                }
            }) {
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(primaryFailure, e);
                Assert.assertEquals(1, e.getSuppressed().length);
                Assert.assertSame(cleanupFailure, e.getSuppressed()[0]);
                Assert.assertEquals(1, networkIOJob.getCloseCount());
            } finally {
                networkPool.halt();
                writePool.halt();
            }
        });
    }

    @Test
    public void testSharedPoolsUseTwoIlpJobs() throws Exception {
        assertMemoryLeak(() -> {
            assertSharedPoolsUseExpectedIlpJobs(8, 2);
            assertSharedPoolsUseExpectedIlpJobs(1, 1);
        });
    }

    private void assertSharedPoolsUseExpectedIlpJobs(int workerCount, int expectedJobCount) {
        CapturingWorkerPool networkPool = null;
        CapturingWorkerPool writePool = null;
        LineTcpMeasurementScheduler scheduler = null;
        Throwable failure = null;
        try {
            // FIBER_HOST on both pools: ILP jobs are ordinary blocking jobs, so the shared-pool
            // job count must not depend on the host pool's mode.
            networkPool = new CapturingWorkerPool("ilp-network-test", workerCount, WorkerPoolMode.FIBER_HOST);
            writePool = new CapturingWorkerPool("ilp-write-test", workerCount, WorkerPoolMode.FIBER_HOST);

            scheduler = new LineTcpMeasurementScheduler(
                    new DefaultLineTcpReceiverConfiguration(configuration),
                    engine,
                    networkPool,
                    null,
                    writePool
            );
            Assert.assertEquals(expectedJobCount, networkPool.getAssignedJobCount());
            Assert.assertEquals(expectedJobCount, writePool.getAssignedJobCount());
        } catch (Throwable th) {
            failure = th;
        } finally {
            failure = Misc.freeBestEffort(failure, networkPool);
            failure = Misc.freeBestEffort(failure, writePool);
            failure = Misc.freeBestEffort(failure, scheduler);
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    private static final class CapturingWorkerPool extends TestWorkerPool {
        private int assignedJobCount;

        private CapturingWorkerPool(String poolName, int workerCount, WorkerPoolMode mode) {
            super(poolName, workerCount, Metrics.DISABLED, mode);
        }

        @Override
        public void assign(int worker, Job job) {
            super.assign(worker, job);
            assignedJobCount++;
        }

        private int getAssignedJobCount() {
            return assignedJobCount;
        }
    }

    private static final class TrackingNetworkIOJob implements NetworkIOJob {
        private static final int MEMORY_SIZE = Long.BYTES;
        private long address = Unsafe.malloc(MEMORY_SIZE, MemoryTag.NATIVE_DEFAULT);
        private final RuntimeException cleanupFailure;
        private int closeCount;

        private TrackingNetworkIOJob(RuntimeException cleanupFailure) {
            this.cleanupFailure = cleanupFailure;
        }

        @Override
        public void addTableUpdateDetails(Utf8String tableNameUtf8, TableUpdateDetails tableUpdateDetails) {
        }

        @Override
        public void close() {
            address = Unsafe.free(address, MEMORY_SIZE, MemoryTag.NATIVE_DEFAULT);
            closeCount++;
            throw cleanupFailure;
        }

        @Override
        public TableUpdateDetails getLocalTableDetails(DirectUtf8Sequence tableNameUtf8) {
            return null;
        }

        @Override
        public Pool<SymbolCache> getSymbolCachePool() {
            return null;
        }

        @Override
        public int getWorkerId() {
            return 0;
        }

        @Override
        public void releaseWalTableDetails() {
        }

        @Override
        public boolean run(Job.WorkerContext workerContext) {
            return false;
        }

        private int getCloseCount() {
            return closeCount;
        }
    }
}
