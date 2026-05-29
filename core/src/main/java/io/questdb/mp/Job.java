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

package io.questdb.mp;

import org.jetbrains.annotations.NotNull;

@FunctionalInterface
public interface Job {
    RunStatus RUNNING_STATUS = () -> false;
    RunStatus TERMINATING_STATUS = () -> true;

    /**
     * Produces a Job instance safe to use concurrently with the receiver on a
     * different OS thread. The framework calls this on each Job in a worker's
     * current generation when a continuation suspends mid-iteration, so the
     * fresh generation that A's next continuation runs shares no mutable state
     * with the parked generation that may later resume on a peer carrier.
     * <p>
     * Default returns {@code this}. Correct only for stateless Jobs whose
     * mutable state is limited to engine-level shared collaborators
     * (e.g., {@code CairoEngine}, {@code MessageBus}, {@code CairoConfiguration})
     * that are themselves concurrency-safe.
     * <p>
     * Contract for overrides:
     * <ul>
     *   <li>MUST allocate a brand new instance (or return one previously
     *       released via {@link #recycleInstance()}). MUST NOT alias any
     *       mutable field of the receiver.</li>
     *   <li>MAY reuse references to engine-level shared collaborators.</li>
     *   <li>A pooled instance must be indistinguishable from a freshly
     *       constructed one; {@code recycleInstance()} is responsible for
     *       the reset before the instance returns to the pool.</li>
     * </ul>
     */
    default Job cloneInstance() {
        return this;
    }

    /**
     * Frees per-continuation resources this job owns. The framework calls it at
     * shutdown on jobs attached to a continuation still parked when the pool
     * halts -- in the resume queue or in a TimerShards waiter -- which never
     * complete, so {@link #recycleInstance()} never runs and any native handle
     * (e.g. a selector) would leak.
     * <p>
     * Override only on jobs whose {@link #cloneInstance()} mints a fresh instance
     * per generation. A job that returns {@code this} is a caller-owned shared
     * singleton (e.g. an IODispatcher freed by its server) and MUST keep the
     * no-op default. Must be idempotent and must not throw.
     */
    default void closeInstance() {
    }

    default void drain(int workerId) {
        while (true) {
            if (!run(workerId)) {
                return;
            }
        }
    }

    /**
     * Per-iteration scratch reset hook. The framework calls this when a
     * continuation snapshot containing this instance completes, before the
     * snapshot is returned to the worker's snapshot pool for reuse.
     * <p>
     * Default is a no-op. Correct for stateless Jobs and for Jobs whose
     * per-iteration state is fully reinitialized on entry to {@link #run}.
     * <p>
     * Contract for overrides:
     * <ul>
     *   <li>MUST clear every per-iteration mutable field so the next reuse
     *       starts clean.</li>
     *   <li>MUST NOT close or release engine-level shared collaborators.</li>
     *   <li>MUST be safe to call from any worker's outer driver.</li>
     *   <li>MUST NOT throw; the framework drops misbehaving snapshots silently.</li>
     * </ul>
     */
    default void recycleInstance() {
    }

    /**
     * Runs and returns true if it should be rescheduled ASAP.
     * <p>
     * {@code workerId} is the calling carrier's globally-unique id (see
     * {@link io.questdb.mp.CarrierIdentity}). Unlike the legacy pool-local
     * worker index, ids do NOT overlap across pools: shared:0 and io:0 carriers
     * receive distinct ids. The {@link Worker} loop body fills this argument
     * with {@code CarrierIdentity.current()} at every call site.
     *
     * @param workerId  caller's carrier id; unique across the JVM for a given
     *                  carrier's bound lifetime
     * @param runStatus signals lifecycle: terminating when the worker pool
     *                  is halting
     * @return true if job should be rescheduled ASAP
     */
    boolean run(int workerId, @NotNull RunStatus runStatus);

    /**
     * Runs and returns true if it should be rescheduled ASAP.
     *
     * @param workerId caller's carrier id; unique across the JVM
     * @return true if job should be rescheduled ASAP
     */
    default boolean run(int workerId) {
        return run(workerId, RUNNING_STATUS);
    }

    interface RunStatus {
        boolean isTerminating();
    }
}
