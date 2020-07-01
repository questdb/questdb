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

import io.questdb.mp.Job;
import io.questdb.mp.JobRunner;
import io.questdb.std.time.MillisecondClock;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

public class WaitProcessorTests {
    private long currentTimeMs;
    private int job1Attempts = 0;
    private HttpRequestProcessorSelector emptySelector = createEmptySelector();

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
                        public boolean tryRerun(HttpRequestProcessorSelector selector) {
                            jobAttempts[index]++;
                            return false;
                        }

                        @Override
                        public RetryAttemptAttributes getAttemptDetails() {
                            return attemptAttributes;
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
            public boolean tryRerun(HttpRequestProcessorSelector selector) {
                job1Attempts++;
                return false;
            }

            @Override
            public RetryAttemptAttributes getAttemptDetails() {
                return attemptAttributes;
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

    public class TestWorkerPool implements JobRunner {
        private List<Job> jobs = new ArrayList<Job>();

        public void runJobs() {
            for (Job job : jobs) {
                job.run(0);
            }
        }

        @Override
        public void assign(Job job) {
            jobs.add(job);
        }

        @Override
        public void assign(int worker, Job job) {
            jobs.add(job);
        }

        @Override
        public void assign(int worker, Closeable cleaner) {
        }

        @Override
        public int getWorkerCount() {
            return 4;
        }
    }
}
