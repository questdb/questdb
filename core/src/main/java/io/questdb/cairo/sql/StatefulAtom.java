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

public interface StatefulAtom extends QuietCloseable, Mutable {

    @Override
    default void clear() {
    }

    @Override
    default void close() {
    }

    /**
     * Returns the number of per-worker slots this atom currently holds, or -1 when the atom
     * guards no per-worker state at all.
     * <p>
     * Every slot a reducer acquires must be released, so the count is zero once the frame
     * sequence has been awaited and no worker sits inside a locked section. A non-zero count
     * at that point means a slot leaked: {@link io.questdb.griffin.engine.PerWorkerLocks} has
     * no reset and the atom belongs to the factory, so the slot is lost for as long as the
     * factory stays in the SQL cache, and the pool eventually starves.
     * <p>
     * The -1 sentinel keeps a test honest. An atom may legitimately hold no locks - an
     * {@link io.questdb.griffin.engine.table.AsyncFilterAtom} over a thread-safe filter clones
     * no per-worker filters and so never acquires a slot. A query that lands on such an atom
     * cannot exercise the leak, and asserting zero against it would pass for the wrong reason.
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
     * {@link #getAcquiredSlotCount()} it tells a query that released every slot it took from one
     * that never took a slot - both hold zero at the end. The owner thread reduces with its own
     * state and acquires nothing, so a run where the owner drained every frame leaves the atom at
     * zero having exercised nothing. A leak test asserts this is non-zero to rule that out.
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
}
