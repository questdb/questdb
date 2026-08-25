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
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPoolConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Back-off must depend on how OFTEN a worker finds work, not on whether it ever
 * does.
 * <p>
 * The worker loop used to reset its back-off ticker to zero whenever any job
 * reported work, so a worker handed a scrap more often than once per
 * {@code napThreshold} passes could never reach a nap -- with the shipped
 * default that meant 6,990 CONSECUTIVE idle passes. A {@code SynchronizedJob}
 * assigned to every worker (an IODispatcher, say) rotates its trylock winner,
 * so a trickle of work that only one worker handles at a time kept every worker
 * in the pool spinning at full rate.
 * <p>
 * Both tests here are comparative -- they assert a ratio between two arms
 * measured on the same machine in the same run -- so they do not encode any
 * absolute throughput expectation.
 */
public class WorkerBackoffDecayTest {
    private static final long NAP_THRESHOLD = 200;
    private static final long RUN_MILLIS = 500;
    private static final long SLEEP_THRESHOLD = 400;
    // Work once per this many passes: far rarer than the credit below, so a
    // decaying ticker still climbs, while a resetting one is wiped every time.
    private static final int RARE_WORK_PERIOD = 100;
    private static final long CREDIT = 8;

    @Test
    public void testAlwaysBusyWorkerStaysHot() throws Exception {
        // Guards the other side of the trade: the decay must not throttle a
        // worker that genuinely has work on every pass.
        long busy = runFor(1, CREDIT);
        long rare = runFor(RARE_WORK_PERIOD, CREDIT);

        Assert.assertTrue(
                "an always-busy worker was throttled by the back-off decay"
                        + " (busy=" + busy + " passes, rare=" + rare + " passes)",
                busy > 10 * rare
        );
    }

    @Test
    public void testRareWorkStillReachesBackoff() throws Exception {
        long decay = runFor(RARE_WORK_PERIOD, CREDIT);
        // A credit >= sleepThreshold can never leave the ticker above zero after
        // work, so this arm reproduces the historical reset-to-zero behaviour.
        long reset = runFor(RARE_WORK_PERIOD, SLEEP_THRESHOLD);

        Assert.assertTrue(
                "worker fed one scrap per " + RARE_WORK_PERIOD + " passes never backed off"
                        + " (decay=" + decay + " passes, reset-equivalent=" + reset + " passes)",
                reset > 10 * decay
        );
    }

    /**
     * Runs a single-worker pool for a fixed wall-clock window and returns how
     * many loop passes the worker managed. A worker that backs off makes orders
     * of magnitude fewer passes than one that spins.
     */
    private static long runFor(int workPeriod, long backoffCredit) throws Exception {
        final PeriodicWorkJob job = new PeriodicWorkJob(workPeriod);
        final TestWorkerPool pool = new TestWorkerPool(new WorkerPoolConfiguration() {
            @Override
            public long getBackoffCredit() {
                return backoffCredit;
            }

            @Override
            public Metrics getMetrics() {
                return Metrics.DISABLED;
            }

            @Override
            public long getNapThreshold() {
                return NAP_THRESHOLD;
            }

            @Override
            public long getNapTimeout() {
                return 1;
            }

            @Override
            public String getPoolName() {
                return "backoff-decay-test";
            }

            @Override
            public long getSleepThreshold() {
                return SLEEP_THRESHOLD;
            }

            @Override
            public long getSleepTimeout() {
                return 5;
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }

            @Override
            public long getYieldThreshold() {
                return 10;
            }
        });
        pool.assign(job);
        try {
            pool.start();
            Thread.sleep(RUN_MILLIS);
            return job.passes.get();
        } finally {
            pool.halt();
        }
    }

    /**
     * Reports work on every {@code period}-th call, standing in for a worker's
     * share of a rotating {@code SynchronizedJob}. A period of 1 is a job that
     * always has work.
     */
    private static final class PeriodicWorkJob implements Job {
        final AtomicLong passes = new AtomicLong();
        private final int period;
        // Read and written only by the single worker thread running this job.
        private int calls;

        PeriodicWorkJob(int period) {
            this.period = period;
        }

        @Override
        public boolean run(@NotNull WorkerContext workerContext) {
            passes.incrementAndGet();
            return period <= 1 || ++calls % period == 0;
        }
    }
}
