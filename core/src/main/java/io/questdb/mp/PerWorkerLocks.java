/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.mp;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Used to synchronize access to list-like collections used by worker threads.
 */
public class PerWorkerLocks {

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
        locks = new AtomicIntegerArray(workerCount);
    }

    @SuppressWarnings("unused")
    public int acquireSlot() {
        return acquireSlot(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
    }

    public int acquireSlot(SqlExecutionCircuitBreaker sqlCircuitBreaker) {
        final Thread thread = Thread.currentThread();
        final int workerId;
        if (thread instanceof Worker) {
            // it's a worker thread, potentially from the shared pool
            workerId = ((Worker) thread).getWorkerId() % workerCount;
        } else {
            // it's an embedder's thread, so use a random slot
            workerId = -1;
        }
        return acquireSlot(workerId, sqlCircuitBreaker);
    }

    public int acquireSlot(int workerId, SqlExecutionCircuitBreaker sqlCircuitBreaker) {
        workerId = workerId == -1 ? rnd.nextInt(workerCount) : workerId;
        while (true) {
            for (int i = 0; i < workerCount; i++) {
                int id = (i + workerId) % workerCount;
                if (locks.compareAndSet(id, 0, 1)) {
                    return id;
                }
            }
            sqlCircuitBreaker.statefulThrowExceptionIfTripped();
            Os.pause();
        }
    }

    public int acquireSlot(int workerId, ExecutionCircuitBreaker circuitBreaker) {
        workerId = workerId == -1 ? rnd.nextInt(workerCount) : workerId;
        while (!circuitBreaker.checkIfTripped()) {
            for (int i = 0; i < workerCount; i++) {
                int id = (i + workerId) % workerCount;
                if (locks.compareAndSet(id, 0, 1)) {
                    return id;
                }
            }
            Os.pause();
        }
        return -1;
    }

    public void releaseSlot(int slot) {
        if (slot == -1) {
            return;
        }
        locks.set(slot, 0);
    }
}
