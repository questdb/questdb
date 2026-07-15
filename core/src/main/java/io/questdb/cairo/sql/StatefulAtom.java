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
     * <p>
     * Not every {@link io.questdb.griffin.engine.PerWorkerLocks} is reachable this way.
     * {@code griffin.engine.groupby.vect.GroupByRecordCursorFactory} holds one with no atom behind it
     * at all - the vectorized GROUP BY does not run on a {@code PageFrameSequence} - so its reducer is
     * invisible to a test that walks the factory tree for an atom, and its slot handling has to be
     * covered some other way.
     *
     * @return the number of slots held, or -1 when this atom holds no per-worker locks
     */
    @TestOnly
    default int getAcquiredSlotCount() {
        return -1;
    }

    /**
     * Returns how many times this atom's reducers have acquired a per-worker slot, or -1 when the
     * atom guards no per-worker state at all. The tally only grows, so unlike
     * {@link #getAcquiredSlotCount()} it tells a run that released every slot it took from a run
     * where no worker took one - the owner thread reduces with its own state and acquires nothing,
     * so both end at zero held slots.
     *
     * @return the number of slot acquisitions, or -1 when this atom holds no per-worker locks
     */
    @TestOnly
    default long getSlotAcquireCount() {
        return -1;
    }

    @TestOnly
    default void setTestSlotAcquireLatch(CountDownLatch latch) {
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
}
