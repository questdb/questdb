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
import io.questdb.std.Os;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;

public class OsTest {

    @Test
    public void rustSmokeTest() {
        Assert.assertEquals(42, Os.smokeTest(0, 42));
    }

    @Test
    public void testAffinity() throws Exception {
        if (Os.arch != Os.ARCH_AARCH64 || Os.type != Os.DARWIN) {
            AtomicInteger cpu0Result = new AtomicInteger(-1);
            AtomicInteger cpu1Result = new AtomicInteger(-1);
            AtomicInteger noAffinityResult = new AtomicInteger(-1);

            // Run on a spawned thread: pinning the JUnit runner thread would serialize
            // every subsequent test in this fork onto one CPU.
            Thread t = new Thread(() -> {
                noAffinityResult.set(Os.setCurrentThreadAffinity(-1));
                cpu0Result.set(Os.setCurrentThreadAffinity(0));
                cpu1Result.set(Os.setCurrentThreadAffinity(1));
            });
            t.start();
            t.join(TimeUnit.SECONDS.toMillis(10));

            Assert.assertFalse(t.isAlive());
            Assert.assertEquals(0, noAffinityResult.get());
            Assert.assertEquals(0, cpu0Result.get());
            Assert.assertEquals(0, cpu1Result.get());
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
    public void testParkPreservesInterruptFlag() {
        Thread.currentThread().interrupt();
        try {
            Os.park();
            assertTrue("park() cleared the interrupt flag", Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void testPauseAndSleepPreserveInterruptFlag() {
        Thread.currentThread().interrupt();
        try {
            Os.pause();
            assertTrue("pause() cleared the interrupt flag", Thread.currentThread().isInterrupted());
            Os.sleep(1);
            assertTrue("sleep() cleared the interrupt flag", Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void testPauseDoesNotAllocate() {
        try (TestUtils.ThreadMetricsScope<ThreadMXBean> scope = TestUtils.threadAllocationScope()) {
            ThreadMXBean bean = scope.getBean();
            for (int i = 0; i < 128; i++) {
                Os.pause();
            }
            long before = bean.getCurrentThreadAllocatedBytes();
            for (int i = 0; i < 128; i++) {
                Os.pause();
            }
            long allocated = bean.getCurrentThreadAllocatedBytes() - before;
            // Thread.sleep(0), which pause() used to call, allocates a 40-byte ThreadSleepEvent
            // per call since JDK 24. The loop must stay cold: C2 eliminates that allocation once
            // Thread.sleep compiles, so a hot fork cannot detect a regression.
            assertTrue("Os.pause() allocated " + allocated + " bytes over 128 calls", allocated < 1280);
        }
    }

    @Test
    public void testSleepDoesNotAllocate() {
        try (TestUtils.ThreadMetricsScope<ThreadMXBean> scope = TestUtils.threadAllocationScope()) {
            ThreadMXBean bean = scope.getBean();
            // Warm past the downcall handle bootstrap and the LambdaForm customization the
            // JDK triggers at invocation CUSTOMIZE_THRESHOLD + 1 (128 by default). The loop
            // must stay cold overall: C2 eliminates the legacy Thread.sleep allocation once
            // Thread.sleep compiles, so a hot fork cannot detect a regression.
            for (int i = 0; i < 256; i++) {
                Os.sleep(1);
            }
            long before = bean.getCurrentThreadAllocatedBytes();
            for (int i = 0; i < 128; i++) {
                Os.sleep(1);
            }
            long allocated = bean.getCurrentThreadAllocatedBytes() - before;
            assertTrue("Os.sleep(1) allocated " + allocated + " bytes over 128 calls", allocated < 1280);
        }
    }

    @Test
    public void testSleepDoesNotBlockSafepoints() throws Exception {
        // Warm the downcall adapter so method isolation exercises the steady-state binding.
        for (int i = 0; i < 256; i++) {
            Os.sleep(1);
        }
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicLong wakeNanos = new AtomicLong();
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                TestUtils.await(barrier);
                Os.sleep(5_000);
                wakeNanos.set(System.nanoTime());
            } catch (Throwable th) {
                error.set(th);
            }
        });
        t.setDaemon(true);
        t.start();
        TestUtils.await(barrier);
        // a Linker.Option.critical binding stalls this stack probe until the sleep ends,
        // so the Os.sleep frame is never observed
        boolean isSleeping = false;
        boolean hasThreadSleepFrame = false;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (!isSleeping && System.nanoTime() < deadline) {
            StackTraceElement[] stack = t.getStackTrace();
            for (StackTraceElement e : stack) {
                if (Os.class.getName().equals(e.getClassName()) && "sleep".equals(e.getMethodName())) {
                    isSleeping = true;
                    break;
                }
            }
            if (isSleeping) {
                for (StackTraceElement e : stack) {
                    if (Thread.class.getName().equals(e.getClassName()) && "sleep".equals(e.getMethodName())) {
                        hasThreadSleepFrame = true;
                        break;
                    }
                }
            }
            Os.pause();
        }
        assertTrue("peer never observed inside Os.sleep", isSleeping);
        Assert.assertFalse("Os.sleep parks through Thread.sleep", hasThreadSleepFrame);

        long gcCountBefore = totalGcCount();
        System.gc();
        long gcDoneNanos = System.nanoTime();
        boolean hasGcRun = totalGcCount() > gcCountBefore;

        t.join(TimeUnit.SECONDS.toMillis(10));
        Assert.assertFalse(t.isAlive());
        Assert.assertNull(error.get());
        Assume.assumeTrue("System.gc() is disabled", hasGcRun);
        // a Linker.Option.critical binding would keep the sleeper in _thread_in_Java
        // and stall the GC safepoint until the sleep ends
        assertTrue("System.gc() returned only after the sleeper woke", gcDoneNanos < wakeNanos.get());
    }

    @Test
    public void testSleepEnds() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicLong sleepNanos = new AtomicLong();
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                TestUtils.await(barrier);
                long start = System.nanoTime();
                try {
                    Os.sleep(1000);
                } finally {
                    sleepNanos.set(System.nanoTime() - start);
                }
            } catch (Throwable th) {
                error.set(th);
            }
        });

        t.setDaemon(true);
        t.start();

        TestUtils.await(barrier);
        t.interrupt();
        t.join(TimeUnit.SECONDS.toMillis(10));

        Assert.assertFalse(t.isAlive());
        Assert.assertNull(error.get());
        long sleepTimeMs = TimeUnit.NANOSECONDS.toMillis(sleepNanos.get());
        assertTrue("slept only " + sleepTimeMs + "ms", sleepTimeMs >= 1000);
    }

    @Test
    public void testSleepNonPositiveReturnsImmediately() throws Exception {
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                Os.sleep(0);
                Os.sleep(-1);
                Os.sleep(Long.MIN_VALUE);
            } catch (Throwable th) {
                error.set(th);
            }
        });
        t.setDaemon(true);
        long time = System.nanoTime();
        t.start();
        t.join(TimeUnit.SECONDS.toMillis(5));
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time);

        Assert.assertFalse(t.isAlive());
        Assert.assertNull(error.get());
        assertTrue("non-positive sleep took " + elapsedMs + "ms", elapsedMs < 500);
    }

    @Test
    public void testSleepSleepsAtLeastRequested() {
        long time = System.nanoTime();
        for (int i = 0; i < 50; i++) {
            Os.sleep(1);
        }
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time);
        assertTrue("50 x Os.sleep(1) took only " + elapsedMs + "ms", elapsedMs >= 50);
        assertTrue("50 x Os.sleep(1) took " + elapsedMs + "ms", elapsedMs < 10_000);
    }

    @Test
    public void testSystemMemoryByMXBean() {
        long fromMXBean = Os.getMemorySizeFromMXBean();
        assertTrue("Could not obtain memory size from OperatingSystemMXBean",
                fromMXBean > 0 && fromMXBean < (1L << 48));
    }

    private static long totalGcCount() {
        long total = 0;
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            long count = bean.getCollectionCount();
            if (count > 0) {
                total += count;
            }
        }
        return total;
    }
}
