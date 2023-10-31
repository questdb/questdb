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

package io.questdb.test.mp;

import io.questdb.mp.SOCountDownLatch;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class SOCountDownLatchTest {

    @Test
    public void testAwait() {
        SOCountDownLatch latch = new SOCountDownLatch(1);
        Assert.assertEquals(1, latch.getCount());
        latch.countDown();
        Assert.assertEquals(0, latch.getCount());
        latch.await(); // should return immediately
    }

    @Test
    public void testAwait_concurrentCountDown() {
        int concLevel = 200;

        SOCountDownLatch latch = new SOCountDownLatch(concLevel);
        CyclicBarrier barrier = new CyclicBarrier(concLevel);
        AtomicInteger countDownCounter = new AtomicInteger();
        AtomicInteger errorCounter = new AtomicInteger();
        for (int i = 0; i < concLevel; i++) {
            new Thread(() -> {
                try {
                    barrier.await();
                    countDownCounter.incrementAndGet();
                    latch.countDown();
                } catch (InterruptedException | BrokenBarrierException e) {
                    errorCounter.incrementAndGet();
                    throw new RuntimeException(e);
                }
            }).start();
        }

        latch.await(); // make sure we don't get stuck on await()
        Assert.assertEquals("await() returned prematurely", concLevel, countDownCounter.get());
        Assert.assertEquals(0, latch.getCount());
        Assert.assertEquals(0, errorCounter.get());
    }

    @Test
    public void testAwaitTimeout_timingOut() {
        SOCountDownLatch latch = new SOCountDownLatch();
        latch.setCount(2);
        new Thread(latch::countDown).start();
        Assert.assertFalse(latch.await(TimeUnit.MILLISECONDS.toNanos(100)));
    }

    @Test
    public void testConcurrentCountDown_countIsNeverNegative() throws Exception {
        int count = 50;
        int concLevel = 2 * count; // intentionally more than count

        SOCountDownLatch latch = new SOCountDownLatch(count);
        CyclicBarrier barrier = new CyclicBarrier(concLevel + 1);
        for (int i = 0; i < concLevel; i++) {
            new Thread(() -> {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
                latch.countDown();
            }).start();
        }

        barrier.await();
        latch.await();
        Assert.assertEquals(0, latch.getCount());
    }

    @Test
    public void testAwaitTimeout() {
        int concLevel = 200;

        SOCountDownLatch latch = new SOCountDownLatch();
        latch.setCount(concLevel);
        for (int i = 0; i < concLevel; i++) {
            new Thread(latch::countDown).start();
        }
        Assert.assertTrue(latch.await(TimeUnit.SECONDS.toNanos(30)));

        // now we have 0 count, so await() should return immediately, we still wait for a bit
        // to prevent false negative due to OS/JVM hiccups
        Assert.assertTrue(latch.await(TimeUnit.SECONDS.toNanos(5)));
    }

    @Test
    public void testAwaitWithTimeout_spuriousWakeups() {
        // a thread may receive spurious wakeups at any time
        // this test is to make sure that SOCountDownLatch.await(long) still respects the timeout
        // even in the presence of frequent spurious wakeups

        long awaitTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(500);
        long spuriousWakeupsMaxDurationNanos = TimeUnit.SECONDS.toNanos(60);

        SOCountDownLatch latch = new SOCountDownLatch(1);

        Thread awaitingThread = Thread.currentThread();
        long wakerDeadline = System.nanoTime() + spuriousWakeupsMaxDurationNanos;
        AtomicBoolean stopWaker = new AtomicBoolean(false);
        new Thread(() -> {
            while (System.nanoTime() < wakerDeadline && !stopWaker.get()) {
                LockSupport.unpark(awaitingThread);
            }
        }).start();

        long start = System.nanoTime();
        boolean awaitResult = latch.await(awaitTimeoutNanos);
        long elapsed = System.nanoTime() - start;
        stopWaker.set(true); // stop the waker thread

        Assert.assertFalse(awaitResult);

        long maxElapsed = 10 * awaitTimeoutNanos; // we are fairly lenient here, to compensate for JVM/OS scheduling delays
        Assert.assertTrue("Elapsed time should be close to timeout. " +
                        "Expected=" + awaitTimeoutNanos + "ns, actual = " + elapsed + "ns",
                elapsed <= maxElapsed);
    }
}
