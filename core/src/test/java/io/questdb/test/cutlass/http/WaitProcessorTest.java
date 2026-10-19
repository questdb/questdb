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

package io.questdb.test.cutlass.http;

import io.questdb.cutlass.http.HttpException;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.HttpRequestProcessorSelector;
import io.questdb.cutlass.http.RescheduleContext;
import io.questdb.cutlass.http.Retry;
import io.questdb.cutlass.http.RetryAttemptAttributes;
import io.questdb.cutlass.http.WaitProcessor;
import io.questdb.cutlass.http.WaitProcessorConfiguration;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;

public class WaitProcessorTest {

    private final HttpRequestProcessorSelector emptySelector = createEmptySelector();
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(10 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();
    private long currentTimeMs;
    private int job1Attempts = 0;

    @Test
    public void testCloseFreesOnlyCurrentRetryIncarnation() {
        final WaitProcessor processor = createProcessor();
        final int[] closed = {0};
        final long[] currentIncarnation = {1};
        final Retry retry = new Retry() {
            private final RetryAttemptAttributes attemptAttributes = new RetryAttemptAttributes();

            @Override
            public void close() {
                closed[0]++;
            }

            @Override
            public void fail(HttpRequestProcessorSelector selector, HttpException e) {
                throw new UnsupportedOperationException();
            }

            @Override
            public RetryAttemptAttributes getAttemptDetails() {
                return attemptAttributes;
            }

            @Override
            public boolean isRetryCurrent(long taskIncarnation) {
                return currentIncarnation[0] == taskIncarnation;
            }

            @Override
            public boolean tryRerun(HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext) {
                throw new UnsupportedOperationException();
            }
        };

        processor.reschedule(retry, currentIncarnation[0]);
        Assert.assertTrue(processor.runSerially());
        currentIncarnation[0]++;
        currentTimeMs += 10;
        processor.reschedule(retry, currentIncarnation[0]);
        Assert.assertTrue(processor.runSerially());
        processor.close();

        Assert.assertEquals(1, closed[0]);
    }

    @Test
    public void testCloseFreesRetryStrandedInOutQueue() {
        WaitProcessor processor = createProcessor();
        final int[] closed = {0};
        Retry retry = new Retry() {
            private final RetryAttemptAttributes attemptAttributes = new RetryAttemptAttributes();

            @Override
            public void close() {
                closed[0]++;
            }

            @Override
            public void fail(HttpRequestProcessorSelector selector, HttpException e) {
                throw new UnsupportedOperationException();
            }

            @Override
            public RetryAttemptAttributes getAttemptDetails() {
                return attemptAttributes;
            }

            @Override
            public boolean tryRerun(HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext) {
                throw new UnsupportedOperationException("retry must not run; it is stranded in the out queue");
            }
        };

        processor.reschedule(retry);
        // inQueue -> nextRerun; the first retry is due at now + 2ms.
        processor.runSerially();
        // Advance past that delay so the next runSerially() promotes it nextRerun -> outQueue.
        currentTimeMs += 10;
        processor.runSerially();
        // Deliberately skip runReruns(): the retry now sits in the out queue, the exact state
        // a shutdown catches once the worker pool has halted and no longer drains it.
        Assert.assertEquals("retry must still be parked, not yet freed", 0, closed[0]);

        processor.close();

        Assert.assertEquals("close() must free the retry stranded in the out queue", 1, closed[0]);
    }

    @Test
    public void testMultipleRetriesExecutedSameCountOverSamePeriod() {
        WaitProcessor processor = createProcessor();
        int[] jobAttempts = new int[10];

        for (int i = 0; i < jobAttempts.length; i++) {
            int index = i;

            processor.reschedule(
                    new Retry() {
                        private final RetryAttemptAttributes attemptAttributes = new RetryAttemptAttributes();

                        @Override
                        public void close() {
                        }

                        @Override
                        public void fail(HttpRequestProcessorSelector selector, HttpException e) {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public RetryAttemptAttributes getAttemptDetails() {
                            return attemptAttributes;
                        }

                        @Override
                        public boolean tryRerun(HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext) {
                            jobAttempts[index]++;
                            return false;
                        }
                    });
        }

        // Do not move currentTimeMs, all calls happens at same ms
        for (int i = 0; i < 5000; i++) {
            currentTimeMs++;
            processor.runReruns(emptySelector);
            processor.runSerially();
        }

        int attempt0 = jobAttempts[0];
        Assert.assertTrue(attempt0 > 0);

        for (int i = 1; i < jobAttempts.length; i++) {
            Assert.assertEquals(attempt0, jobAttempts[i]);
        }
    }

    @Test
    public void testPublishedRetryCannotLaunchReopenedTask() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WaitProcessor processor = createProcessor();
            final FiberRuntime runtime = new FiberRuntime(1);
            try {
                final PublishedRetryTask task = new PublishedRetryTask(processor);
                final long publishedIncarnation = task.getIncarnation();

                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
                Assert.assertEquals(1, runtime.drain(8));
                Assert.assertTrue(task.isCancelled());
                Assert.assertEquals(1, task.runCount);

                task.reopen();
                Assert.assertEquals(publishedIncarnation + 1, task.getIncarnation());

                Assert.assertTrue(processor.runSerially());
                task.getAttemptDetails().nextRunTimestamp = Long.MAX_VALUE;
                currentTimeMs += 10;
                Assert.assertTrue(processor.runSerially());

                final LaunchResult[] launchResult = {null};
                Assert.assertTrue(processor.launchReruns(runtime, (fiber, reservationEpoch, retry, taskIncarnation) -> {
                    Assert.assertSame(task, retry);
                    Assert.assertEquals(publishedIncarnation, taskIncarnation);
                    launchResult[0] = runtime.launchReserved(
                            fiber,
                            reservationEpoch,
                            task,
                            taskIncarnation
                    );
                }));
                Assert.assertEquals(LaunchResult.STALE_INCARNATION, launchResult[0]);
                Assert.assertEquals(FiberTask.STATE_IDLE, task.getScheduleState());
                Assert.assertEquals(1, task.runCount);
            } finally {
                processor.close();
                close(runtime);
            }
        });
    }

    @Test
    public void testRetryLaunchFailureReleasesFiberAndRetry() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WaitProcessor processor = createProcessor();
            final FiberRuntime runtime = new FiberRuntime(1);
            final boolean[] isClosed = {false};
            final Retry retry = new Retry() {
                private final RetryAttemptAttributes attemptAttributes = new RetryAttemptAttributes();

                @Override
                public void close() {
                    isClosed[0] = true;
                }

                @Override
                public void fail(HttpRequestProcessorSelector selector, HttpException e) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public RetryAttemptAttributes getAttemptDetails() {
                    return attemptAttributes;
                }

                @Override
                public boolean tryRerun(
                        HttpRequestProcessorSelector selector,
                        RescheduleContext rescheduleContext
                ) {
                    throw new UnsupportedOperationException();
                }
            };
            try {
                processor.reschedule(retry, 1);
                Assert.assertTrue(processor.runSerially());
                currentTimeMs += 10;
                Assert.assertTrue(processor.runSerially());

                final OutOfMemoryError failure = new OutOfMemoryError("test launch failure");
                try {
                    processor.launchReruns(runtime, (_, _, _, _) -> {
                        throw failure;
                    });
                    Assert.fail("expected launch failure");
                } catch (OutOfMemoryError expected) {
                    Assert.assertSame(failure, expected);
                }

                Assert.assertTrue(isClosed[0]);
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            } finally {
                processor.close();
                close(runtime);
            }
        });
    }

    @Test
    public void testSaturationLeavesRetryInOutQueue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final WaitProcessor processor = createProcessor();
            final FiberRuntime runtime = new FiberRuntime(1);
            Fiber heldFiber = null;
            long heldFiberEpoch = 0;
            try {
                final Retry retry = createRetry();
                processor.reschedule(retry, 1);
                Assert.assertTrue(processor.runSerially());
                currentTimeMs += 10;
                Assert.assertTrue(processor.runSerially());

                heldFiber = runtime.tryReserveFiber();
                Assert.assertNotNull(heldFiber);
                heldFiberEpoch = heldFiber.getReservationEpoch();
                Assert.assertFalse(
                        processor.launchReruns(
                                runtime,
                                (fiber, reservationEpoch, queuedRetry, taskIncarnation) -> {
                                    throw new AssertionError("saturated retry must remain queued");
                                }
                        )
                );

                runtime.releaseReservedFiber(heldFiber, heldFiberEpoch);
                heldFiber = null;
                final int[] launchCount = {0};
                Assert.assertTrue(processor.launchReruns(runtime, (fiber, reservationEpoch, queuedRetry, taskIncarnation) -> {
                    Assert.assertSame(retry, queuedRetry);
                    Assert.assertEquals(1, taskIncarnation);
                    launchCount[0]++;
                    runtime.releaseReservedFiber(fiber, reservationEpoch);
                }));
                Assert.assertEquals(1, launchCount[0]);
            } finally {
                if (heldFiber != null) {
                    runtime.releaseReservedFiber(heldFiber, heldFiberEpoch);
                }
                processor.close();
                close(runtime);
            }
        });
    }

    @Test
    public void testRescheduleHappensInFirstSecond() {
        WaitProcessor processor = createProcessor();
        job1Attempts = 0;
        processor.reschedule(createRetry());

        // Do not move currentTimeMs, all calls happens at same ms
        for (int i = 0; i < 5000; i++) {
            currentTimeMs++;
            processor.runReruns(emptySelector);
            processor.runSerially();
        }

        Assert.assertEquals("Job runs expected to be 10 but are: " + job1Attempts, 10, job1Attempts);
    }

    @Test
    public void testRescheduleNotHappensImmediately() {

        WaitProcessor processor = createProcessor();
        job1Attempts = 0;

        processor.reschedule(createRetry());

        // Do not move currentTimeMs, all calls happens at same ms
        for (int i = 0; i < 10; i++) {
            processor.runReruns(emptySelector);
            processor.runSerially();
        }
        Assert.assertEquals(0, job1Attempts);
    }

    private static void close(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(64);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
    }

    private static HttpRequestProcessorSelector createEmptySelector() {
        return new HttpRequestProcessorSelector() {

            @Override
            public void close() {
            }

            @Override
            public HttpRequestProcessor select(HttpRequestHeader header) {
                return null;
            }

            @Override
            public HttpRequestProcessor resolveProcessorById(int handlerId, HttpRequestHeader header) {
                return null;
            }
        };
    }

    @NotNull
    private WaitProcessor createProcessor() {
        return new WaitProcessor(new WaitProcessorConfiguration() {
            @Override
            public MillisecondClock getClock() {
                return () -> currentTimeMs;
            }

            @Override
            public double getExponentialWaitMultiplier() {
                return 2.0;
            }

            @Override
            public int getInitialWaitQueueSize() {
                return 64;
            }

            @Override
            public int getMaxProcessingQueueSize() {
                return 4096;
            }

            @Override
            public long getMaxWaitCapMs() {
                return 1000;
            }
        }, null);
    }

    @NotNull
    private Retry createRetry() {
        return new Retry() {
            private final RetryAttemptAttributes attemptAttributes = new RetryAttemptAttributes();

            @Override
            public void close() {
            }

            @Override
            public void fail(HttpRequestProcessorSelector selector, HttpException e) {
                throw new UnsupportedOperationException();
            }

            @Override
            public RetryAttemptAttributes getAttemptDetails() {
                return attemptAttributes;
            }

            @Override
            public boolean tryRerun(HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext) {
                job1Attempts++;
                return false;
            }
        };
    }

    private static class PublishedRetryTask extends FiberTask implements Retry {
        private final RetryAttemptAttributes attemptAttributes = new RetryAttemptAttributes();
        private final WaitProcessor processor;
        private int runCount;

        private PublishedRetryTask(WaitProcessor processor) {
            this.processor = processor;
        }

        @Override
        public void close() {
        }

        @Override
        public void fail(HttpRequestProcessorSelector selector, HttpException e) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RetryAttemptAttributes getAttemptDetails() {
            return attemptAttributes;
        }

        @Override
        public boolean tryRerun(HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void onParked() {
            processor.reschedule(this, getIncarnation());
            Assert.assertTrue(signalAxisA(SIGNAL_DISCONNECT));
        }

        @Override
        protected boolean runStep() {
            runCount++;
            return false;
        }
    }
}
