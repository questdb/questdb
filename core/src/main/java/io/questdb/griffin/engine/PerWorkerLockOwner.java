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

import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Implemented by the atoms that guard per-worker state with {@link PerWorkerLocks}.
 * <p>
 * Nothing in production calls this: it exists so that a slot-leak test can reach the locks through
 * one typed accessor instead of naming a private field in each atom. A reducer that loses its slot
 * loses it permanently - the locks have no reset and the atom outlives the query that borrowed it -
 * so the pool starves once every slot has leaked, which is the bug the tests behind this watch for.
 */
public interface PerWorkerLockOwner {

    /**
     * Returns the locks guarding this atom's per-worker state, or null when it guards none. An
     * {@link io.questdb.griffin.engine.table.AsyncFilterAtom} over a thread-safe filter clones no
     * per-worker filters and builds no locks, and so can neither take a slot nor leak one.
     */
    @TestOnly
    @Nullable
    PerWorkerLocks getPerWorkerLocks();
}
