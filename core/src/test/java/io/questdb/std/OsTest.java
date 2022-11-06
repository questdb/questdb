/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.std;

import io.questdb.mp.SOCountDownLatch;
import io.questdb.test.tools.TestUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class OsTest {
    @Test
    public void testAffinity() throws Exception {
        if (Os.type != Os.OSX_ARM64) {
            Assert.assertEquals(0, Os.setCurrentThreadAffinity(0));

            AtomicInteger result = new AtomicInteger(-1);
            CountDownLatch threadHalt = new CountDownLatch(1);

            new Thread(() -> {
                result.set(Os.setCurrentThreadAffinity(1));
                threadHalt.countDown();
            }).start();

            Assert.assertTrue(threadHalt.await(1, TimeUnit.SECONDS));
            Assert.assertEquals(0, result.get());

            Assert.assertEquals(0, Os.setCurrentThreadAffinity(-1));
        }
    }

    @Test
    public void testCurrentTimeMicros() {
        long reference = System.currentTimeMillis();
        long actual = Os.currentTimeMicros();
        long delta = actual / 1000 - reference;
        Assert.assertTrue(delta < 200);
    }

    @Test
    public void testCurrentTimeNanos() {
        long reference = System.currentTimeMillis();
        long actual = Os.currentTimeNanos();
        Assert.assertTrue(actual > 0);
        long delta = actual / 1_000_000 - reference;
        Assert.assertTrue(delta < 200);
    }

    @Test
    public void testSleepEnds() {
        SOCountDownLatch doneLatch = new SOCountDownLatch(1);
        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread t = new Thread(() -> {
            TestUtils.await(barrier);
            Os.sleep(1000);
            doneLatch.countDown();
        });

        long time = System.currentTimeMillis();
        t.start();

        TestUtils.await(barrier);
        t.interrupt();
        Assert.assertTrue(doneLatch.await(10_000_000_000L));
        Assert.assertTrue(System.currentTimeMillis() - time >= 1000);
    }

    @Test
    public void testGetRss() {
        assertThat(Os.getRss(), not(equalTo(0)));
    }
}
