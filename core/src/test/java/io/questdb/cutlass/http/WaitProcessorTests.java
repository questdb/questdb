/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cutlass.http;

import io.questdb.std.datetime.millitime.MillisecondClock;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

public class WaitProcessorTests {
    private long currentTimeMs;
    private int job1Attempts = 0;
    private final HttpRequestProcessorSelector emptySelector = createEmptySelector();

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

        System.out.println("Rerun attempts: " + job1Attempts);
        Assert.assertEquals("Job runs expected to be 10 but are: " + job1Attempts, 10, job1Attempts);
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
                        public boolean tryRerun(HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext) {
                            jobAttempts[index]++;
                            return false;
                        }

                        @Override
                        public RetryAttemptAttributes getAttemptDetails() {
                            return attemptAttributes;
                        }

                        @Override
                        public void fail(HttpRequestProcessorSelector selector, HttpException e) {
                            throw new UnsupportedOperationException();
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
        System.out.println("Rerun attempts: " + attempt0);
        Assert.assertTrue(attempt0 > 0);

        for (int i = 1; i < jobAttempts.length; i++) {
            Assert.assertEquals(attempt0, jobAttempts[0]);
        }
    }

    @NotNull
    private Retry createRetry() {
        return new Retry() {
            private final RetryAttemptAttributes attemptAttributes = new RetryAttemptAttributes();

            @Override
            public boolean tryRerun(HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext) {
                job1Attempts++;
                return false;
            }

            @Override
            public RetryAttemptAttributes getAttemptDetails() {
                return attemptAttributes;
            }

            @Override
            public void fail(HttpRequestProcessorSelector selector, HttpException e) {
                throw new UnsupportedOperationException();
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
            public long getMaxWaitCapMs() {
                return 1000;
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
        });
    }

    private static HttpRequestProcessorSelector createEmptySelector() {
        return new HttpRequestProcessorSelector() {

            @Override
            public HttpRequestProcessor select(CharSequence url) {
                return null;
            }

            @Override
            public HttpRequestProcessor getDefaultProcessor() {
                return null;
            }

            @Override
            public void close() {

            }
        };
    }
}
