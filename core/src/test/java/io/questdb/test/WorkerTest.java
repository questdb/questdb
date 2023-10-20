/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.*;
import io.questdb.metrics.MetricsRegistryImpl;
import io.questdb.metrics.WorkerMetricsImpl;
import io.questdb.mp.*;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Os;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerTest {

    private static final String END_MESSAGE = "run is over";
    private static final Metrics METRICS = new Metrics(true, new MetricsRegistryImpl());

    @Test
    public void testJobs() {
        ObjHashSet<Job> jobs = new ObjHashSet<>();

        int events = 20;

        AtomicInteger count = new AtomicInteger();
        jobs.add(slowCountUp(count));

        AtomicInteger endLatch = new AtomicInteger(events);
        jobs.add(fastCountDown(endLatch)); // counts down, on 0 it halts the worker

        SOCountDownLatch workerHaltLatch = new SOCountDownLatch(1);
        Worker worker = new Worker(
                "test_pool",
                0,
                Worker.NO_THREAD_AFFINITY,
                jobs,
                workerHaltLatch,
                ex -> Assert.assertEquals(END_MESSAGE, ex.getMessage()),
                true,
                10000L,
                5L,
                20L,
                METRICS,
                null
        );
        worker.start();
        if (!workerHaltLatch.await(TimeUnit.SECONDS.toNanos(60L))) {
            Assert.fail();
        }
        Assert.assertEquals(0, endLatch.get());
        Assert.assertEquals(events, count.get());
        WorkerMetricsImpl metrics = (WorkerMetricsImpl) METRICS.workerMetrics();
        long min = metrics.getMinElapsed(worker.getName());
        long max = metrics.getMaxElapsed(worker.getName());
        Assert.assertTrue(min < max);
        Assert.assertTrue(min > 0L);
    }

    private static Job fastCountDown(AtomicInteger endLatch) {
        return (workerId, runStatus) -> {
            if (endLatch.decrementAndGet() < 1) {
                throw new RuntimeException(END_MESSAGE);
            }
            Os.sleep(10L);
            return false; // not eager
        };
    }

    private static Job slowCountUp(AtomicInteger count) {
        return (workerId, runStatus) -> {
            count.incrementAndGet();
            Os.sleep(30L);
            return false; // not eager
        };
    }
}
