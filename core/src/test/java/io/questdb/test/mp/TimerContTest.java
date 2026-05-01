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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.TimerCont;
import io.questdb.mp.TimerShards;
import io.questdb.mp.WorkerContinuation;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class TimerContTest {
    private static final Log LOG = LogFactory.getLog(TimerContTest.class);

    @Test
    public void testScheduleAfterFiresInBoundedTime() throws InterruptedException {
        TimerShards shards = new TimerShards(1, "test-timer", LOG);
        shards.start();
        try {
            CountDownLatch resumed = new CountDownLatch(1);
            AtomicReference<TimerCont> timerRef = new AtomicReference<>();
            AtomicBoolean parked = new AtomicBoolean();
            WorkerContinuation cont = new WorkerContinuation(() -> {
                TimerCont t = TimerCont.scheduleAfter(shards, 50);
                timerRef.set(t);
                if (WorkerContinuation.suspend()) {
                    parked.set(true);
                    resumed.countDown();
                }
            }, c -> {
                // Resume sink: simply remount on the test thread.
                new Thread(c::run).start();
            });
            cont.run();
            Assert.assertTrue("did not resume in time", resumed.await(2, TimeUnit.SECONDS));
            Assert.assertTrue(parked.get());
            Assert.assertNotNull(timerRef.get());
        } finally {
            shards.shutdown();
        }
    }

    @Test
    public void testShutdownInterruptsSleep() throws InterruptedException {
        TimerShards shards = new TimerShards(1, "test-timer", LOG);
        shards.start();
        CountDownLatch resumed = new CountDownLatch(1);
        AtomicBoolean wasShutdown = new AtomicBoolean();
        WorkerContinuation cont = new WorkerContinuation(() -> {
            TimerCont t = TimerCont.scheduleAfter(shards, 60_000);
            if (WorkerContinuation.suspend()) {
                wasShutdown.set(t.isShuttingDown());
                resumed.countDown();
            }
        }, c -> new Thread(c::run).start());
        cont.run();
        // Body parked with a 60s deadline. Shutdown should drain it within ms.
        long start = System.nanoTime();
        shards.shutdown();
        Assert.assertTrue("shutdown did not resume in time",
                resumed.await(2, TimeUnit.SECONDS));
        Assert.assertTrue("cont should observe isShuttingDown", wasShutdown.get());
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        Assert.assertTrue("shutdown should be quick, took " + elapsedMs + "ms", elapsedMs < 1_500);
    }
}
