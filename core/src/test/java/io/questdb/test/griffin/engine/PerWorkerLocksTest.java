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

package io.questdb.test.griffin.engine;

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Positive control for the slot-leak oracle.
 * <p>
 * Every other user of {@link PerWorkerLocks#getAcquiredSlotCount()} asserts that it is zero, so a
 * {@code getAcquiredSlotCount()} that always answered zero would disable the single detector the
 * reducers' slot-leak coverage rests on, and the whole suite would stay green. These tests are the
 * only ones that require a non-zero answer, and they pin the {@code INTS_PER_SLOT} stride: a count
 * that read consecutive ints instead of one per cache-line-padded slot would report 1 for the two
 * slots held below.
 * <p>
 * Narrow unit test: PerWorkerLocks allocates no native memory, so it needs no assertMemoryLeak.
 */
public class PerWorkerLocksTest extends AbstractCairoTest {

    @Test
    public void testAcquireReleaseRoundTrip() {
        final PerWorkerLocks locks = new PerWorkerLocks(configuration, 4);
        Assert.assertEquals(0, locks.getAcquiredSlotCount());

        final int first = locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        Assert.assertEquals(1, locks.getAcquiredSlotCount());
        final int second = locks.acquireSlot(1, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        Assert.assertNotEquals(first, second);
        Assert.assertEquals(2, locks.getAcquiredSlotCount());

        locks.releaseSlot(first);
        Assert.assertEquals(1, locks.getAcquiredSlotCount());
        locks.releaseSlot(second);
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
    }

    @Test
    public void testAcquiresEverySlot() {
        final int workerCount = 4;
        final PerWorkerLocks locks = new PerWorkerLocks(configuration, workerCount);
        for (int i = 0; i < workerCount; i++) {
            locks.acquireSlot(i, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
            Assert.assertEquals(i + 1, locks.getAcquiredSlotCount());
        }
        // The pool is exhausted: a reducer that leaked all of these would spin here forever, which
        // is what makes a leaked slot fatal rather than merely wasteful.
        for (int i = 0; i < workerCount; i++) {
            locks.releaseSlot(i);
        }
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
    }

    @Test
    public void testReleaseIgnoresNoSlot() {
        // Reducers pass the id they got from maybeAcquire back to release unconditionally, and the
        // owner thread gets -1 (it uses its private state and takes no slot).
        final PerWorkerLocks locks = new PerWorkerLocks(configuration, 2);
        final int slot = locks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        locks.releaseSlot(-1);
        Assert.assertEquals(1, locks.getAcquiredSlotCount());
        locks.releaseSlot(slot);
        Assert.assertEquals(0, locks.getAcquiredSlotCount());
    }
}
