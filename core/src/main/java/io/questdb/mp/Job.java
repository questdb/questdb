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
    // Detached contexts for callers that drive a job outside a Worker loop
    // (drains, tests, shutdown paths). carrierId() is -1, the "no affinity"
    // sentinel that PerWorkerLocks.acquireSlot() maps to a random slot.
    WorkerContext RUNNING_STATUS = new ImmutableWorkerContext(-1, false);
    WorkerContext TERMINATING_STATUS = new ImmutableWorkerContext(-1, true);

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
        final WorkerContext workerContext = workerId == -1 ? RUNNING_STATUS : new ImmutableWorkerContext(workerId, false);
        while (run(workerContext)) {
            // keep draining until the job reports it has no more work
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
     * The job pulls its pool-local worker id from {@link WorkerContext#carrierId()}
     * only when it needs it (lazy poll), instead of the framework pushing the id
     * on every loop iteration. {@code carrierId()} is the <em>pool-local</em> worker
     * index in {@code [0, poolWorkerCount)}: NOT unique across the JVM, since every
     * pool numbers its workers from 0, so shared:0 and io:0 share an id. Consumers
     * such as {@link io.questdb.griffin.engine.PerWorkerLocks} rely on this -- they
     * size per-pool arrays by the pool's worker count and index them by this id,
     * so a globally-unique value would be out of range.
     * <p>
     * The {@link Worker} backs {@code carrierId()} with {@code Worker.WORKER_ID} (the
     * pool-local {@link io.questdb.std.CarrierLocal} holding the carrier's worker
     * index) on continuation-aware pools, so each call returns the id of the carrier
     * actually running now -- correct even after a continuation remounts on a peer
     * carrier. On legacy pools, which never migrate, it returns the stable worker
     * field directly and pays no carrier-local read. It is NOT
     * {@link CarrierIdentity#current()}; that globally-unique carrier id is a
     * separate concept used only to index {@code CarrierLocal} rows.
     *
     * @param workerContext provides the caller's pool-local worker index (lazily,
     *                      via {@link WorkerContext#carrierId()}) and the lifecycle
     *                      signal ({@link WorkerContext#isTerminating()}, terminating
     *                      when the worker pool is halting)
     * @return true if job should be rescheduled ASAP
     */
    boolean run(@NotNull WorkerContext workerContext);

    /**
     * Runs and returns true if it should be rescheduled ASAP. Convenience for
     * callers outside a worker loop; wraps {@code workerId} in a detached,
     * non-terminating {@link WorkerContext}.
     *
     * @return true if job should be rescheduled ASAP
     */
    default boolean run() {
        return run(RUNNING_STATUS);
    }

    /**
     * Immutable {@link WorkerContext} for callers that drive a job outside a
     * {@link Worker} loop (drains, tests, shutdown paths), where the worker id
     * is fixed for the call and no carrier migration can occur.
     */
    final class ImmutableWorkerContext implements WorkerContext {
        private final int carrierId;
        private final boolean terminating;

        public ImmutableWorkerContext(int carrierId, boolean terminating) {
            this.carrierId = carrierId;
            this.terminating = terminating;
        }

        @Override
        public int carrierId() {
            return carrierId;
        }

        @Override
        public boolean isTerminating() {
            return terminating;
        }
    }

    /**
     * The per-tick context a {@link Worker} hands to {@link #run(WorkerContext)}.
     * Carries the lifecycle signal ({@link #isTerminating()}) alongside a lazy
     * pull of the carrier's pool-local worker id ({@link #carrierId()}).
     */
    interface WorkerContext {
        /**
         * The pool-local worker index of the carrier executing the current tick.
         * Resolved on each call so it tracks continuation remounts; see
         * {@link #run(WorkerContext)}.
         */
        int carrierId();

        /**
         * True when the worker pool is halting, signalling the job to wind down.
         */
        boolean isTerminating();
    }
}
