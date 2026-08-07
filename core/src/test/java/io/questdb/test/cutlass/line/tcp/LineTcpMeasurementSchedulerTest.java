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

import io.questdb.cutlass.line.tcp.DefaultLineTcpReceiverConfiguration;
import io.questdb.cutlass.line.tcp.LineTcpConnectionContext;
import io.questdb.cutlass.line.tcp.LineTcpMeasurementScheduler;
import io.questdb.cutlass.line.tcp.NetworkIOJob;
import io.questdb.cutlass.line.tcp.SymbolCache;
import io.questdb.cutlass.line.tcp.TableUpdateDetails;
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.IODispatcher;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
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
            final CapturingWorkerPool networkPool = new CapturingWorkerPool(8);
            final CapturingWorkerPool writePool = new CapturingWorkerPool(8);
            LineTcpMeasurementScheduler scheduler = null;
            try {
                scheduler = new LineTcpMeasurementScheduler(
                        new DefaultLineTcpReceiverConfiguration(configuration),
                        engine,
                        networkPool,
                        null,
                        writePool
                );
                Assert.assertEquals(2, networkPool.getAssignedJobCount());
                Assert.assertEquals(2, writePool.getAssignedJobCount());
            } finally {
                networkPool.halt();
                writePool.halt();
                Misc.free(scheduler);
            }
        });
    }

    private static final class CapturingWorkerPool extends WorkerPool {
        private final ObjList<Job> assignedJobs = new ObjList<>();

        private CapturingWorkerPool(int workerCount) {
            super(new TestWorkerPoolConfiguration(workerCount));
        }

        @Override
        public void assign(int worker, Job job) {
            super.assign(worker, job);
            assignedJobs.extendAndSet(worker, job);
        }

        private int getAssignedJobCount() {
            return assignedJobs.size();
        }
    }

    private static final class TestWorkerPoolConfiguration implements WorkerPoolConfiguration {
        private final int workerCount;

        private TestWorkerPoolConfiguration(int workerCount) {
            this.workerCount = workerCount;
        }

        @Override
        public int getWorkerCount() {
            return workerCount;
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
