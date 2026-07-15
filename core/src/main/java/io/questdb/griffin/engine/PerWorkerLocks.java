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

package io.questdb.griffin.engine;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Used to synchronize access to list-like collections used by worker threads.
 * <p>
 * Each slot uses the 0/1 protocol: acquire changes 0 to 1 with one CAS and release stores 0.
 */
public class PerWorkerLocks {
    // Reserve extra int array elements to avoid false sharing. A cache line is assumed to take 64 bytes.
    private static final int INTS_PER_SLOT = 64 / Integer.BYTES;
    private final AtomicIntegerArray locks;
    // Used to randomize acquire attempts for work stealing threads. Accessed in a racy way, intentionally.
    private final Rnd rnd;
    private final AtomicLong slotAcquireCount = new AtomicLong();
    private volatile CountDownLatch testAcquireLatch;
    private final int workerCount;

    public PerWorkerLocks(@NotNull CairoConfiguration configuration, int workerCount) {
        this.rnd = new Rnd(
                configuration.getNanosecondClock().getTicks(),
                configuration.getMicrosecondClock().getTicks()
        );
        this.workerCount = workerCount;
        locks = new AtomicIntegerArray(INTS_PER_SLOT * workerCount);
    }

    public int acquireSlot(int workerId, SqlExecutionCircuitBreaker sqlCircuitBreaker) {
        // A shared pool has more workers than an atom has slots, so the incoming worker id can be
        // >= workerCount. Fold it into [0, workerCount) up front: the single conditional subtraction
        // in the loop only wraps a sum that stays under 2 * workerCount.
        workerId = workerId == -1
                ? rnd.nextInt(workerCount)
                : workerId >= workerCount ? workerId % workerCount : workerId;
        while (true) {
            for (int i = 0; i < workerCount; i++) {
                int id = i + workerId;
                if (id >= workerCount) {
                    id -= workerCount;
                }
                if (locks.compareAndSet(INTS_PER_SLOT * id, 0, 1)) {
                    assert tallyAcquire();
                    return id;
                }
            }
            sqlCircuitBreaker.statefulThrowExceptionIfTripped();
            Os.pause();
        }
    }

    public int acquireSlot(int carrierId, ExecutionCircuitBreaker circuitBreaker) {
        // A shared pool has more workers than an atom has slots, so the incoming carrier id can be
        // >= workerCount. Fold it into [0, workerCount) up front: the single conditional subtraction
        // in the loop only wraps a sum that stays under 2 * workerCount.
        carrierId = carrierId == -1
                ? rnd.nextInt(workerCount)
                : carrierId >= workerCount ? carrierId % workerCount : carrierId;
        while (!circuitBreaker.checkIfTripped()) {
            for (int i = 0; i < workerCount; i++) {
                int id = i + carrierId;
                if (id >= workerCount) {
                    id -= workerCount;
                }
                if (locks.compareAndSet(INTS_PER_SLOT * id, 0, 1)) {
                    assert tallyAcquire();
                    return id;
                }
            }
            Os.pause();
        }
        throw CairoException.nonCritical().put("query aborted").setInterruption(true);
    }

    /**
     * Returns the number of slots currently held. Every acquired slot must be released, so this is
     * zero whenever no worker is inside a locked section. A non-zero count once all workers are done
     * means a slot leaked: there is no reset, so a leaked slot is lost for the lifetime of the owning
     * atom, and the pool eventually starves.
     */
    @TestOnly
    public int getAcquiredSlotCount() {
        int count = 0;
        for (int i = 0; i < workerCount; i++) {
            if (locks.get(INTS_PER_SLOT * i) != 0) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns how many times a slot has been acquired since this instance was created. Unlike
     * {@link #getAcquiredSlotCount()} this tally never goes down, so it tells a run where every
     * worker released what it took from a run where no worker took a slot at all - both hold zero
     * at the end.
     */
    @TestOnly
    public long getSlotAcquireCount() {
        return slotAcquireCount.get();
    }

    public void releaseSlot(int slot) {
        if (slot > -1) {
            locks.set(INTS_PER_SLOT * slot, 0);
        }
    }

    @TestOnly
    public boolean awaitTestAcquire() {
        final CountDownLatch latch = testAcquireLatch;
        if (latch == null) {
            return true;
        }
        try {
            return latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @TestOnly
    public void setTestAcquireLatch(CountDownLatch latch) {
        testAcquireLatch = latch;
    }

    private boolean tallyAcquire() {
        slotAcquireCount.incrementAndGet();
        final CountDownLatch latch = testAcquireLatch;
        if (latch != null) {
            latch.countDown();
        }
        return true;
    }
}
