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

import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.SimpleWaitingLock;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class InterruptedWaitTest {

    @Test
    public void testCountDownLatchTimedAwait() throws Exception {
        final SOCountDownLatch latch = new SOCountDownLatch(1);
        assertInterruptedWait(
                () -> Assert.assertTrue(latch.await(TimeUnit.SECONDS.toNanos(30))),
                latch::countDown
        );
    }

    @Test
    public void testCountDownLatchUntimedAwait() throws Exception {
        final SOCountDownLatch latch = new SOCountDownLatch(1);
        assertInterruptedWait(latch::await, latch::countDown);
    }

    @Test
    public void testSimpleWaitingLock() throws Exception {
        final SimpleWaitingLock lock = new SimpleWaitingLock();
        Assert.assertTrue(lock.tryLock());
        assertInterruptedWait(() -> {
            Assert.assertTrue(lock.tryLock(30, TimeUnit.SECONDS));
            lock.unlock();
        }, lock::unlock);
    }

    @Test
    public void testUnboundedCountDownLatch() throws Exception {
        final SOUnboundedCountDownLatch latch = new SOUnboundedCountDownLatch();
        assertInterruptedWait(() -> latch.await(1), latch::countDown);
    }

    private static void assertInterruptedWait(Runnable wait, Runnable release) throws Exception {
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicBoolean isInterruptedAfter = new AtomicBoolean();
        final AtomicBoolean isWaitComplete = new AtomicBoolean();
        final Thread waiter = new Thread(() -> {
            Thread.currentThread().interrupt();
            try {
                wait.run();
                isWaitComplete.set(true);
            } catch (Throwable th) {
                failure.set(th);
            } finally {
                isInterruptedAfter.set(Thread.currentThread().isInterrupted());
            }
        }, "interrupted-waiter");
        waiter.setDaemon(true);
        waiter.start();

        try {
            TestUtils.assertEventually(
                    () -> Assert.assertEquals(Thread.State.TIMED_WAITING, waiter.getState()),
                    5
            );
        } finally {
            release.run();
            waiter.join(TimeUnit.SECONDS.toMillis(5));
        }

        Assert.assertFalse("interrupted waiter did not stop", waiter.isAlive());
        if (failure.get() != null) {
            throw new AssertionError("interrupted waiter failed", failure.get());
        }
        Assert.assertTrue(isWaitComplete.get());
        Assert.assertTrue(isInterruptedAfter.get());
    }
}
