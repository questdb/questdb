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

package io.questdb.test.mp;

import io.questdb.Metrics;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogLevel;
import io.questdb.log.LogRecordUtf8Sink;
import io.questdb.log.LogWriter;
import io.questdb.log.LogWriterConfig;
import io.questdb.metrics.MetricsRegistryImpl;
import io.questdb.mp.EagerThreadSetup;
import io.questdb.mp.Job;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.Worker;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(Parameterized.class)
public class WorkerErrorMetricsTest {
    private final WorkerPoolMode workerPoolMode;

    public WorkerErrorMetricsTest(WorkerPoolMode workerPoolMode) {
        this.workerPoolMode = workerPoolMode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<WorkerPoolMode> data() {
        return List.of(WorkerPoolMode.LEGACY, WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testFatalJobErrorCount() throws Exception {
        assertErrorCount(false, true, false);
        assertErrorCount(false, true, true);
    }

    @Test
    public void testJobToStringFailureDoesNotKillWorkerOrLeakLogSlot() throws Exception {
        // Worker logs an unhandled job error with the job's toString(). Calling
        // toString() at that point could throw (e.g. OutOfMemoryError right after
        // the job itself ran out of memory), leak the reserved log queue slot and
        // kill the worker. Worker materializes the name at construction instead,
        // so a toString() that throws once the job runs must not matter.
        TestUtils.assertMemoryLeak(() -> {
            final AtomicReference<SCSequence> consumerSequence = new AtomicReference<>();
            final AtomicReference<RingQueue<LogRecordUtf8Sink>> consumerRing = new AtomicReference<>();
            try (LogFactory factory = new LogFactory()) {
                factory.add(new LogWriterConfig(LogLevel.CRITICAL, (ring, seq, level) -> {
                    consumerSequence.set(seq);
                    consumerRing.set(ring);
                    return new LogWriter() {
                        @Override
                        public void bindProperties(LogFactory factory) {
                        }

                        @Override
                        public boolean run(@NotNull WorkerContext workerContext) {
                            return false;
                        }
                    };
                }));
                factory.bind();
                final Log log = factory.create("worker-error-log-test");

                final ErrorJob job = new ErrorJob(false) {
                    private volatile boolean isToStringArmed;

                    @Override
                    public boolean run(WorkerContext workerContext) {
                        isToStringArmed = true;
                        return super.run(workerContext);
                    }

                    @Override
                    public String toString() {
                        if (isToStringArmed) {
                            throw new IllegalStateException("test toString failure");
                        }
                        return "error-job";
                    }
                };
                final CountDownLatch workerStopped = new CountDownLatch(1);
                try (TestWorkerPool pool = new TestWorkerPool(new WorkerPoolConfiguration() {
                    @Override
                    public Metrics getMetrics() {
                        return Metrics.DISABLED;
                    }

                    @Override
                    public String getPoolName() {
                        return "worker-error-log-test";
                    }

                    @Override
                    public int getWorkerCount() {
                        return 1;
                    }

                    @Override
                    public WorkerPoolMode getWorkerPoolMode() {
                        return workerPoolMode;
                    }

                    @Override
                    public boolean haltOnError() {
                        return false;
                    }

                    @Override
                    public boolean isDaemonPool() {
                        return true;
                    }
                })) {
                    pool.assign(job);
                    pool.assignThreadLocalCleaner(0, workerStopped::countDown);
                    pool.start(log);
                    Assert.assertTrue("worker did not stop", workerStopped.await(10, TimeUnit.SECONDS));
                }
                // the worker survived the failed error report and ran the job again
                Assert.assertEquals(2, job.runCount);

                final SCSequence sequence = consumerSequence.get();
                final RingQueue<LogRecordUtf8Sink> ring = consumerRing.get();
                final StringSink sink = new StringSink();
                int recordCount = 0;
                long cursor;
                while ((cursor = sequence.next()) >= 0) {
                    sink.put((Sinkable) ring.get(cursor));
                    sequence.done(cursor);
                    recordCount++;
                }
                Assert.assertEquals(sink.toString(), 1, recordCount);
                TestUtils.assertContains(sink, "unhandled error [job=error-job, ex=java.lang.IllegalStateException: test job failure");
                TestUtils.assertNotContains(sink, "ABANDONED");
            }
        });
    }

    @Test
    public void testNonFatalJobErrorCount() throws Exception {
        assertErrorCount(false, false, false);
        assertErrorCount(false, false, true);
    }

    @Test
    public void testSetupErrorCount() throws Exception {
        assertErrorCount(true, false, false);
        assertErrorCount(true, false, true);
        assertErrorCount(true, true, false);
        assertErrorCount(true, true, true);
    }

    private void assertErrorCount(boolean isSetupFailure, boolean isHaltOnError, boolean isMetricsEnabled) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Metrics metrics = new Metrics(isMetricsEnabled, new MetricsRegistryImpl());
            final ErrorJob job = new ErrorJob(isSetupFailure);
            final CountDownLatch workerStopped = new CountDownLatch(1);
            try (TestWorkerPool pool = new TestWorkerPool(new WorkerPoolConfiguration() {
                @Override
                public Metrics getMetrics() {
                    return metrics;
                }

                @Override
                public String getPoolName() {
                    return "worker-error-metrics-test";
                }

                @Override
                public int getWorkerCount() {
                    return 1;
                }

                @Override
                public WorkerPoolMode getWorkerPoolMode() {
                    return workerPoolMode;
                }

                @Override
                public boolean haltOnError() {
                    return isHaltOnError;
                }

                @Override
                public boolean isDaemonPool() {
                    return true;
                }
            })) {
                pool.assign(job);
                pool.assignThreadLocalCleaner(0, workerStopped::countDown);
                pool.start();
                Assert.assertTrue("worker did not stop", workerStopped.await(10, TimeUnit.SECONDS));
            }
            Assert.assertEquals(1, job.setupCount);
            Assert.assertEquals(isSetupFailure ? 0 : (isHaltOnError ? 1 : 2), job.runCount);
            Assert.assertEquals(isMetricsEnabled ? 1 : 0, metrics.healthMetrics().unhandledErrorsCount());
        });
    }

    private static class ErrorJob implements Job, EagerThreadSetup {
        private final boolean isSetupFailure;
        private int runCount;
        private int setupCount;

        private ErrorJob(boolean isSetupFailure) {
            this.isSetupFailure = isSetupFailure;
        }

        @Override
        public boolean run(WorkerContext workerContext) {
            if (++runCount == 1) {
                throw new IllegalStateException("test job failure");
            }
            Objects.requireNonNull(Worker.current()).halt();
            return false;
        }

        @Override
        public void setup() {
            setupCount++;
            if (isSetupFailure) {
                throw new IllegalStateException("test setup failure");
            }
        }
    }
}
