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

import io.questdb.mp.Job;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SynchronizedJobTest {

    @Test
    public void testLockWordHasCacheLinesOfItsOwn() {
        // the surefire argLine passes -XX:-RestrictContended, which lets HotSpot apply @Contended
        // outside the JDK; it pads the field with 128 bytes on each side
        final long lockedOffset = Unsafe.getFieldOffset(SynchronizedJob.class, "locked");
        Assert.assertTrue(
                "lock word is not padded, is -XX:-RestrictContended missing? [offset=" + lockedOffset + ']',
                lockedOffset >= 128
        );
        final long subclassOffset = Unsafe.getFieldOffset(CountingJob.class, "runs");
        Assert.assertTrue(
                "subclass field shares the lock word's cache lines [lockedOffset=" + lockedOffset
                        + ", subclassOffset=" + subclassOffset + ']',
                subclassOffset - lockedOffset >= 128
        );
    }

    @Test
    public void testRunSeriallyNeverOverlaps() throws Exception {
        final int threadCount = 8;
        final int runsPerThread = 200_000;
        final CountingJob job = new CountingJob();
        final CyclicBarrier barrier = new CyclicBarrier(threadCount);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicInteger attempts = new AtomicInteger();
        final Thread[] threads = new Thread[threadCount];
        for (int t = 0; t < threadCount; t++) {
            threads[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < runsPerThread; i++) {
                        job.run(Job.RUNNING_STATUS);
                        attempts.incrementAndGet();
                    }
                } catch (Throwable th) {
                    failure.compareAndSet(null, th);
                }
            });
            threads[t].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        Assert.assertEquals(threadCount * runsPerThread, attempts.get());
        Assert.assertEquals(0, job.overlaps.get());
        Assert.assertTrue("no run took the lock", job.runs > 0);
    }

    @Test
    public void testRunSkipsWhileLockIsHeld() {
        final CountingJob job = new CountingJob();
        job.holdLock();
        try {
            Assert.assertFalse(job.run(Job.RUNNING_STATUS));
            Assert.assertEquals(0, job.runs);
        } finally {
            job.dropLock();
        }
        Assert.assertTrue(job.run(Job.RUNNING_STATUS));
        Assert.assertEquals(1, job.runs);
    }

    private static class CountingJob extends SynchronizedJob {
        private final AtomicInteger inside = new AtomicInteger();
        private final AtomicInteger overlaps = new AtomicInteger();
        // written only under the run lock
        private int runs;

        void dropLock() {
            releaseRunLock();
        }

        void holdLock() {
            acquireRunLock();
        }

        @Override
        protected boolean runSerially() {
            if (inside.incrementAndGet() != 1) {
                overlaps.incrementAndGet();
            }
            runs++;
            inside.decrementAndGet();
            return true;
        }
    }
}
