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

package io.questdb.test.griffin.engine.table;

import io.questdb.griffin.engine.table.SelectivityStats;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SelectivityStatsTest {

    @Test
    public void testClearResetsWarmUp() {
        final SelectivityStats stats = new SelectivityStats();

        stats.update(100, 100);
        stats.update(100, 100);
        Assert.assertFalse(stats.shouldUseLateMaterialization());

        // The atom is reused across executions, so clear() must put the packed word back to
        // unsampled. Only the count is observable here: a stale average cannot be read through the
        // warm-up gate, and the next update() overwrites it on the first sample either way.
        stats.clear();
        Assert.assertTrue(stats.shouldUseLateMaterialization());
    }

    @Test
    public void testConcurrentUpdatesDoNotStall() throws Exception {
        final SelectivityStats stats = new SelectivityStats();
        final int threadCount = 8;
        final CyclicBarrier barrier = new CyclicBarrier(threadCount);
        final AtomicInteger errors = new AtomicInteger();
        final ObjList<Thread> threads = new ObjList<>();

        // Every reducing worker calls update() once per page frame against this one instance when
        // the filter is thread-safe. This is a liveness check on the CAS loop under contention, not
        // a race detector: every sample here is 1.0, so no interleaving can be observed through
        // shouldUseLateMaterialization(). Race-freedom rests on the single packed word, not on this.
        for (int i = 0; i < threadCount; i++) {
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                    for (int j = 0; j < 10_000; j++) {
                        stats.update(100, 100);
                    }
                } catch (Throwable th) {
                    errors.incrementAndGet();
                }
            });
            thread.start();
            threads.add(thread);
        }
        for (int i = 0; i < threadCount; i++) {
            final Thread thread = threads.getQuick(i);
            // Bounded: a CAS loop that failed to terminate is the thing this test is here for, and an
            // unbounded join would hand that to the CI timeout as a hang rather than a failure.
            thread.join(TimeUnit.MINUTES.toMillis(1));
            Assert.assertFalse("update() did not terminate", thread.isAlive());
        }

        Assert.assertEquals(0, errors.get());
        // Every sample was 1.0, so no interleaving of the EMA can land anywhere but 1.0.
        Assert.assertFalse(stats.shouldUseLateMaterialization());
    }

    @Test
    public void testEmaSurvivesPacking() {
        final SelectivityStats stats = new SelectivityStats();

        // Two zero-selectivity samples: the filter drops everything, so late materialization pays.
        stats.update(0, 100);
        stats.update(0, 100);
        Assert.assertTrue(stats.shouldUseLateMaterialization());

        // One full-selectivity sample moves the average to ALPHA (0.3), above the 20% threshold.
        // This pins the EMA arithmetic through the pack/unpack round trip.
        stats.update(100, 100);
        Assert.assertFalse(stats.shouldUseLateMaterialization());
    }

    @Test
    public void testUpdateIgnoresEmptyFrames() {
        final SelectivityStats stats = new SelectivityStats();

        // A frame with no rows carries no selectivity signal and must not count towards warm-up.
        stats.update(0, 0);
        stats.update(0, -1);
        stats.update(100, 100);
        Assert.assertTrue(stats.shouldUseLateMaterialization());
    }

    @Test
    public void testWarmUpRequiresMinSamples() {
        final SelectivityStats stats = new SelectivityStats();

        // Unsampled: the heuristic opts in until it has evidence either way.
        Assert.assertTrue(stats.shouldUseLateMaterialization());

        // One high-selectivity sample is not yet enough to opt out.
        stats.update(100, 100);
        Assert.assertTrue(stats.shouldUseLateMaterialization());

        stats.update(100, 100);
        Assert.assertFalse(stats.shouldUseLateMaterialization());
    }
}
