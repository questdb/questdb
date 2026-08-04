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

package io.questdb.test.std;

import com.sun.management.ThreadMXBean;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Os;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

public class OsTest {

    @Test
    public void rustSmokeTest() {
        Assert.assertEquals(42, Os.smokeTest(0, 42));
    }

    @Test
    public void testAffinity() throws Exception {
        if (Os.arch != Os.ARCH_AARCH64 || Os.type != Os.DARWIN) {
            Assert.assertEquals(0, Os.setCurrentThreadAffinity(0));

            AtomicInteger result = new AtomicInteger(-1);
            CountDownLatch threadHalt = new CountDownLatch(1);

            new Thread(() -> {
                result.set(Os.setCurrentThreadAffinity(1));
                threadHalt.countDown();
            }).start();

            assertTrue(threadHalt.await(1, TimeUnit.SECONDS));
            Assert.assertEquals(0, result.get());

            Assert.assertEquals(0, Os.setCurrentThreadAffinity(-1));
        }
    }

    @Test
    public void testCurrentTimeMicros() {
        long reference = System.currentTimeMillis();
        long actual = Os.currentTimeMicros();
        long delta = actual / 1000 - reference;
        assertTrue(delta < 200);
    }

    @Test
    public void testCurrentTimeNanos() {
        long reference = System.currentTimeMillis();
        long actual = Os.currentTimeNanos();
        assertTrue(actual > 0);
        long delta = actual / 1_000_000 - reference;
        assertTrue(delta < 200);
    }

    @Test
    public void testGetRss() {
        Assert.assertNotEquals(0, Os.getRss());
    }

    @Test
    public void testPauseDoesNotAllocate() {
        ThreadMXBean bean = threadAllocationBean();
        for (int i = 0; i < 10_000; i++) {
            Os.pause();
        }
        long before = bean.getCurrentThreadAllocatedBytes();
        for (int i = 0; i < 50_000; i++) {
            Os.pause();
        }
        long allocated = bean.getCurrentThreadAllocatedBytes() - before;
        // Thread.sleep(0), which pause() used to call, allocates a 40-byte
        // jdk.internal.event.ThreadSleepEvent per call on JDK 25 -- 2 MB for this loop.
        Assert.assertTrue(String.valueOf(allocated), allocated < 64 * 1024);
    }

    @Test
    public void testSleepDoesNotAllocate() {
        ThreadMXBean bean = threadAllocationBean();
        // The first calls resolve the downcall handle and run the method handle on the
        // interpreted LambdaForm path, both of which allocate; warm past them.
        for (int i = 0; i < 128; i++) {
            Os.sleep(1);
        }
        long before = bean.getCurrentThreadAllocatedBytes();
        for (int i = 0; i < 128; i++) {
            Os.sleep(1);
        }
        long allocated = bean.getCurrentThreadAllocatedBytes() - before;
        // Thread.sleep, which sleep() used to loop on, allocates 40 bytes per call
        // on JDK 25 -- 5120 for this loop.
        Assert.assertTrue(String.valueOf(allocated), allocated < 1280);
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
        assertTrue(doneLatch.await(10_000_000_000L));
        long sleepTime = System.currentTimeMillis() - time;
        assertTrue(String.valueOf(sleepTime), sleepTime >= 1000);
    }

    @Test
    public void testSystemMemoryByMXBean() {
        long fromMXBean = Os.getMemorySizeFromMXBean();
        assertTrue("Could not obtain memory size from OperatingSystemMXBean",
                fromMXBean > 0 && fromMXBean < (1L << 48));
    }

    private static ThreadMXBean threadAllocationBean() {
        java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        Assume.assumeTrue("thread allocation measurement not supported", bean instanceof ThreadMXBean);
        ThreadMXBean sunBean = (ThreadMXBean) bean;
        Assume.assumeTrue(sunBean.isThreadAllocatedMemorySupported() && sunBean.isThreadAllocatedMemoryEnabled());
        return sunBean;
    }
}
