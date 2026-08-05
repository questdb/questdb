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
import org.junit.Test;

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
            AtomicInteger resetResult = new AtomicInteger(-1);

            // Run on a spawned thread: pinning the JUnit runner thread would serialize
            // every subsequent test in this fork onto one CPU.
            Thread t = new Thread(() -> {
                cpu0Result.set(Os.setCurrentThreadAffinity(0));
                cpu1Result.set(Os.setCurrentThreadAffinity(1));
                resetResult.set(Os.setCurrentThreadAffinity(-1));
            });
            t.start();
            t.join(TimeUnit.SECONDS.toMillis(10));

            Assert.assertFalse(t.isAlive());
            Assert.assertEquals(0, cpu0Result.get());
            Assert.assertEquals(0, cpu1Result.get());
            Assert.assertEquals(0, resetResult.get());
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
    public void testParkDoesNotSpinWhenInterrupted() throws Exception {
        AtomicLong iterations = new AtomicLong();
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                Thread.currentThread().interrupt();
                long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
                long n = 0;
                while (System.nanoTime() < deadline) {
                    Os.park();
                    n++;
                }
                iterations.set(n);
            } catch (Throwable th) {
                error.set(th);
            }
        });
        t.start();
        t.join(TimeUnit.SECONDS.toMillis(10));

        Assert.assertFalse(t.isAlive());
        Assert.assertNull(error.get());
        assertTrue("park() did not run", iterations.get() >= 1);
        assertTrue("parked " + iterations.get() + " times in 500ms", iterations.get() < 100_000);
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
        ThreadMXBean bean = TestUtils.threadAllocationBean();
        for (int i = 0; i < 128; i++) {
            Os.pause();
        }
        long before = bean.getCurrentThreadAllocatedBytes();
        for (int i = 0; i < 128; i++) {
            Os.pause();
        }
        long allocated = bean.getCurrentThreadAllocatedBytes() - before;
        // Thread.sleep(0), which pause() used to call, allocates a 40-byte ThreadSleepEvent
        // per call on JDK 25. The loop must stay cold: C2 eliminates that allocation once
        // Thread.sleep compiles, so a hot fork cannot detect a regression.
        assertTrue("Os.pause() allocated " + allocated + " bytes over 128 calls", allocated < 1280);
    }

    @Test
    public void testSleepDoesNotAllocate() {
        ThreadMXBean bean = TestUtils.threadAllocationBean();
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

    @Test
    public void testSleepDoesNotBlockSafepoints() throws Exception {
        System.gc();
        long time = System.nanoTime();
        System.gc();
        long baselineMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time);

        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread t = new Thread(() -> {
            TestUtils.await(barrier);
            Os.sleep(2000);
        });
        t.start();
        TestUtils.await(barrier);
        // a Linker.Option.critical binding stalls this stack probe until the sleep ends,
        // so the Os.sleep frame is never observed
        boolean isSleeping = false;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!isSleeping && System.nanoTime() < deadline) {
            for (StackTraceElement e : t.getStackTrace()) {
                if (Os.class.getName().equals(e.getClassName()) && "sleep".equals(e.getMethodName())) {
                    isSleeping = true;
                    break;
                }
            }
            Os.pause();
        }
        assertTrue("peer never observed inside Os.sleep", isSleeping);

        time = System.nanoTime();
        System.gc();
        long gcMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time);
        t.join(TimeUnit.SECONDS.toMillis(10));
        Assert.assertFalse(t.isAlive());
        // a Linker.Option.critical binding would keep the sleeper in _thread_in_Java
        // and stall the GC safepoint for the remaining sleep
        assertTrue("System.gc() took " + gcMs + "ms (baseline " + baselineMs + "ms) with a peer in Os.sleep",
                gcMs < baselineMs + 900);
    }

    @Test
    public void testSleepEnds() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                TestUtils.await(barrier);
                Os.sleep(1000);
            } catch (Throwable th) {
                error.set(th);
            }
        });

        long time = System.nanoTime();
        t.start();

        TestUtils.await(barrier);
        t.interrupt();
        t.join(TimeUnit.SECONDS.toMillis(10));

        Assert.assertFalse(t.isAlive());
        Assert.assertNull(error.get());
        long sleepTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time);
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
}
