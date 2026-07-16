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

package io.questdb.cairo.sql;

import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.CountDownLatch;

public interface StatefulAtom extends QuietCloseable, Mutable {

    @TestOnly
    default boolean awaitTestSlotAcquire() {
        return true;
    }

    @Override
    default void clear() {
    }

    @Override
    default void close() {
    }

    /**
     * Returns the number of per-worker slots this atom currently holds, or -1 when the atom guards
     * no per-worker state at all. Every acquired slot must be released, so the count is zero once
     * the frame sequence has been awaited; a non-zero count there means a slot leaked, and since
     * {@link io.questdb.griffin.engine.PerWorkerLocks} has no reset and the atom belongs to the
     * factory, the pool eventually starves.
     * <p>
     * The -1 sentinel keeps a test honest: an atom may hold no locks at all - an
     * {@link io.questdb.griffin.engine.table.AsyncFilterAtom} over a thread-safe filter clones no
     * per-worker filters - and asserting zero against it would pass for the wrong reason.
     *
     * @return the number of slots held, or -1 when this atom holds no per-worker locks
     */
    @TestOnly
    default int getAcquiredSlotCount() {
        return -1;
    }

    /**
     * Returns how many times this atom's reducers have acquired a per-worker slot, or -1 when the
     * atom guards no per-worker state at all. Unlike {@link #getAcquiredSlotCount()} the tally never
     * goes down, so it tells a run that released every slot it took from a run where no worker took
     * one - the owner thread reduces with its own state and acquires nothing, so both end at zero
     * held slots.
     * <p>
     * The tally is only kept while a latch is installed, and
     * {@link #setTestSlotAcquireLatch(CountDownLatch)} restarts it from zero, so it counts this
     * latch's acquisitions rather than the atom's lifetime total. Production reducers pay nothing
     * for it.
     *
     * @return the number of slot acquisitions, or -1 when this atom holds no per-worker locks
     */
    @TestOnly
    default long getSlotAcquireCount() {
        return -1;
    }

    /**
     * Initializes state required for filtering, such as child atoms, symbol table sources,
     * bind variable values, circuit breakers, etc.
     *
     * @param symbolTableSource symbol table source
     * @param executionContext  execution context
     * @throws SqlException when bind variable validation or any other kind of validation fails
     */
    default void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
    }

    @TestOnly
    default boolean isTestSlotAcquireWaitEnabled() {
        return false;
    }

    @TestOnly
    default void setTestSlotAcquireLatch(CountDownLatch latch) {
    }
}
