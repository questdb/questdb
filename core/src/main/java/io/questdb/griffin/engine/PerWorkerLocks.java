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
    private final int workerCount;

    public PerWorkerLocks(@NotNull CairoConfiguration configuration, int workerCount) {
        // Every parallel operator that builds locks is gated on sharedQueryWorkerCount > 0
        // (SqlExecutionContextImpl), so a zero-slot lock is unreachable. It would also be unusable:
        // acquireSlot() folds with workerId % workerCount and probes one slot per worker.
        assert workerCount > 0;
        this.rnd = new Rnd(
                configuration.getNanosecondClock().getTicks(),
                configuration.getMicrosecondClock().getTicks()
        );
        this.workerCount = workerCount;
        locks = new AtomicIntegerArray(INTS_PER_SLOT * workerCount);
    }

    private PerWorkerLocks(PerWorkerLocks locks) {
        this.locks = locks.locks;
        this.rnd = locks.rnd;
        this.workerCount = locks.workerCount;
    }

    /**
     * Acquires a slot for the given worker, spinning until one frees up. A successful acquire must be
     * paired with a {@link #releaseSlot(int)} in a finally: there is no reset here, and an atom
     * outlives the query that borrowed it, so a slot leaked on an error path stays lost for as long
     * as the owning factory sits in the SQL cache. Once every slot has leaked, each later execution
     * spins here forever for a slot nobody will release. That is why a reducer must keep every
     * statement that can throw - decoding a frame, charging the per-query memory tracker - inside the
     * try that releases the slot.
     *
     * @throws io.questdb.cairo.CairoException when the circuit breaker has tripped
     */
    public int acquireSlot(int workerId, SqlExecutionCircuitBreaker sqlCircuitBreaker) {
        // A shared pool has more workers than an atom has slots, so the incoming worker id can be
        // >= workerCount. Folding it up front hoists the wrap out of the probe: the loop then only
        // needs a conditional subtraction, because i + workerId stays under 2 * workerCount. That
        // bound is also what keeps the sum from overflowing to a negative slot index.
        // Wrapping inside the loop instead is correct for every id a caller can actually pass.
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
                    return id;
                }
            }
            sqlCircuitBreaker.statefulThrowExceptionIfTripped();
            Os.pause();
        }
    }

    public int acquireSlot(int carrierId, ExecutionCircuitBreaker circuitBreaker) {
        // See acquireSlot(int, SqlExecutionCircuitBreaker): the fold hoists the wrap out of the
        // probe and keeps i + carrierId from overflowing to a negative slot index.
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
                    return id;
                }
            }
            Os.pause();
        }
        throw CairoException.nonCritical().put("query aborted").setInterruption(true);
    }

    @TestOnly
    public boolean awaitTestAcquire() {
        return true;
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
     * <p>
     * Only {@link #withTestAcquireLatch(CountDownLatch)}'s instance counts; acquireSlot() keeps no
     * tally, so the tally costs production reducers nothing and this base always reports zero.
     */
    @TestOnly
    public long getSlotAcquireCount() {
        return 0;
    }

    @TestOnly
    public boolean hasTestAcquireLatch() {
        return false;
    }

    public void releaseSlot(int slot) {
        if (slot > -1) {
            locks.set(INTS_PER_SLOT * slot, 0);
        }
    }

    @TestOnly
    public PerWorkerLocks withTestAcquireLatch(CountDownLatch latch) {
        return latch != null ? new TestPerWorkerLocks(this, latch) : this;
    }

    private static class TestPerWorkerLocks extends PerWorkerLocks {
        private final PerWorkerLocks normalLocks;
        private final AtomicLong slotAcquireCount = new AtomicLong();
        private final CountDownLatch testAcquireLatch;

        private TestPerWorkerLocks(PerWorkerLocks locks, CountDownLatch testAcquireLatch) {
            super(locks);
            this.normalLocks = locks instanceof TestPerWorkerLocks testLocks ? testLocks.normalLocks : locks;
            this.testAcquireLatch = testAcquireLatch;
        }

        @Override
        public int acquireSlot(int workerId, SqlExecutionCircuitBreaker sqlCircuitBreaker) {
            final int slot = super.acquireSlot(workerId, sqlCircuitBreaker);
            slotAcquireCount.incrementAndGet();
            testAcquireLatch.countDown();
            return slot;
        }

        @Override
        public int acquireSlot(int carrierId, ExecutionCircuitBreaker circuitBreaker) {
            final int slot = super.acquireSlot(carrierId, circuitBreaker);
            slotAcquireCount.incrementAndGet();
            testAcquireLatch.countDown();
            return slot;
        }

        @Override
        public boolean awaitTestAcquire() {
            try {
                return testAcquireLatch.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        @Override
        public long getSlotAcquireCount() {
            return slotAcquireCount.get();
        }

        @Override
        public boolean hasTestAcquireLatch() {
            return true;
        }

        @Override
        public PerWorkerLocks withTestAcquireLatch(CountDownLatch latch) {
            return latch != null ? new TestPerWorkerLocks(normalLocks, latch) : normalLocks;
        }
    }
}
