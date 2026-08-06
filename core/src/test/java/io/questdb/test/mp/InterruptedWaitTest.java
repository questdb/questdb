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

public class InterruptedWaitTest {

    @Test
    public void testCountDownLatchTimedAwait() throws Exception {
        final SOCountDownLatch latch = new SOCountDownLatch(1);
        TestUtils.assertInterruptedWaitDoesNotSpin(
                "timed SOCountDownLatch await",
                () -> Assert.assertTrue(latch.await(TimeUnit.SECONDS.toNanos(30))),
                latch::countDown
        );

        final SOCountDownLatch nonInterruptedLatch = new SOCountDownLatch(1);
        TestUtils.assertNonInterruptedWaitDoesNotSpin(
                "non-interrupted timed SOCountDownLatch await",
                () -> Assert.assertTrue(nonInterruptedLatch.await(TimeUnit.SECONDS.toNanos(30))),
                nonInterruptedLatch::countDown
        );
    }

    @Test
    public void testCountDownLatchUntimedAwait() throws Exception {
        final SOCountDownLatch latch = new SOCountDownLatch(1);
        TestUtils.assertInterruptedWaitDoesNotSpin("SOCountDownLatch await", latch::await, latch::countDown);

        final SOCountDownLatch nonInterruptedLatch = new SOCountDownLatch(1);
        TestUtils.assertNonInterruptedWaitDoesNotSpin(
                "non-interrupted SOCountDownLatch await",
                nonInterruptedLatch::await,
                nonInterruptedLatch::countDown
        );
    }

    @Test
    public void testSimpleWaitingLock() throws Exception {
        final SimpleWaitingLock lock = new SimpleWaitingLock();
        Assert.assertTrue(lock.tryLock());
        TestUtils.assertInterruptedWaitDoesNotSpin("SimpleWaitingLock tryLock", () -> {
            Assert.assertTrue(lock.tryLock(30, TimeUnit.SECONDS));
            lock.unlock();
        }, lock::unlock);

        final SimpleWaitingLock nonInterruptedLock = new SimpleWaitingLock();
        Assert.assertTrue(nonInterruptedLock.tryLock());
        TestUtils.assertNonInterruptedWaitDoesNotSpin("non-interrupted SimpleWaitingLock tryLock", () -> {
            Assert.assertTrue(nonInterruptedLock.tryLock(30, TimeUnit.SECONDS));
            nonInterruptedLock.unlock();
        }, nonInterruptedLock::unlock);
    }

    @Test
    public void testUnboundedCountDownLatch() throws Exception {
        final SOUnboundedCountDownLatch latch = new SOUnboundedCountDownLatch();
        TestUtils.assertInterruptedWaitDoesNotSpin(
                "SOUnboundedCountDownLatch await",
                () -> latch.await(1),
                latch::countDown
        );

        final SOUnboundedCountDownLatch nonInterruptedLatch = new SOUnboundedCountDownLatch();
        TestUtils.assertNonInterruptedWaitDoesNotSpin(
                "non-interrupted SOUnboundedCountDownLatch await",
                () -> nonInterruptedLatch.await(1),
                nonInterruptedLatch::countDown
        );
    }
}
