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

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Used to synchronize access to list-like collections used by worker threads.
 * <p>
 * A slot's int is a counter, not a flag: acquire increments it to an odd value, release increments
 * it again to an even one. So the parity says whether the slot is held, and the counter says how
 * many times it has been acquired. The counter never goes down and, short of a full 2^32 wrap, never
 * returns to a value an acquirer has already read, so it stays ABA-free. The count comes for free:
 * the acquire is the same single CAS it always was, and the release the same single store.
 */
public class PerWorkerLocks {
    // Reserve extra int array elements to avoid false sharing. A cache line is assumed to take 64 bytes.
    private static final int INTS_PER_SLOT = 64 / Integer.BYTES;
    private final AtomicIntegerArray locks;
    // Used to randomize acquire attempts for work stealing threads. Accessed in a racy way, intentionally.
    private final Rnd rnd;
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
        workerId = workerId == -1 ? rnd.nextInt(workerCount) : workerId;
        while (true) {
            for (int i = 0; i < workerCount; i++) {
                int id = (i + workerId) % workerCount;
                int idx = INTS_PER_SLOT * id;
                int state = locks.get(idx);
                if (isFree(state) && locks.compareAndSet(idx, state, state + 1)) {
                    return id;
                }
            }
            sqlCircuitBreaker.statefulThrowExceptionIfTripped();
            Os.pause();
        }
    }

    public int acquireSlot(int carrierId, ExecutionCircuitBreaker circuitBreaker) {
        carrierId = carrierId == -1 ? rnd.nextInt(workerCount) : carrierId;
        while (!circuitBreaker.checkIfTripped()) {
            for (int i = 0; i < workerCount; i++) {
                int id = (i + carrierId) % workerCount;
                int idx = INTS_PER_SLOT * id;
                int state = locks.get(idx);
                if (isFree(state) && locks.compareAndSet(idx, state, state + 1)) {
                    return id;
                }
            }
            Os.pause();
        }
        throw CairoException.nonCritical().put("query aborted").setInterruption(true);
    }

    /**
     * Returns how many times a slot has been acquired since this instance was created. Unlike
     * {@link #getAcquiredSlotCount()} this tally never goes down, so it tells a run where every
     * worker released what it took from a run where no worker took a slot at all - both hold zero
     * at the end.
     */
    @TestOnly
    public long getAcquireCount() {
        long count = 0;
        for (int i = 0; i < workerCount; i++) {
            count += (Integer.toUnsignedLong(locks.get(INTS_PER_SLOT * i)) + 1) >>> 1;
        }
        return count;
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
            if (!isFree(locks.get(INTS_PER_SLOT * i))) {
                count++;
            }
        }
        return count;
    }

    public void releaseSlot(int slot) {
        if (slot > -1) {
            final int idx = INTS_PER_SLOT * slot;
            final int state = locks.get(idx);
            assert !isFree(state) : "releasing a slot that is not held: " + slot;
            // The holder is the only thread that can write a held slot - an acquirer's CAS demands
            // the even state it read - so this needs no atomicity, and the volatile store publishes
            // the slot's state to the next acquirer just as the plain unlock store used to.
            locks.set(idx, state + 1);
        }
    }

    private static boolean isFree(int state) {
        return (state & 1) == 0;
    }
}
