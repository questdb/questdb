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
     * different OS thread. The worker pool calls this once per additional
     * worker assigned the job.
     * <p>
     * Default returns {@code this}. Correct only for stateless Jobs whose
     * mutable state is limited to engine-level shared collaborators
     * (e.g., {@code CairoEngine}, {@code MessageBus}, {@code CairoConfiguration})
     * that are themselves concurrency-safe.
     * <p>
     * An override must return a new instance that does not alias mutable
     * per-worker state. It may reuse concurrency-safe shared collaborators.
     */
    default Job cloneInstance() {
        return this;
    }

    /**
     * Frees resources owned by a per-worker clone. The pool calls this at
     * shutdown for instances created through {@link #cloneInstance()}.
     * <p>
     * Override only on jobs whose {@link #cloneInstance()} mints a fresh instance
     * per worker. A job that returns {@code this} is a caller-owned shared
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
     * A Worker always returns its stable pool-local worker index. It is not
     * {@link CarrierIdentity#current()}; that globally unique carrier id indexes
     * {@code CarrierLocal} rows.
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
         * See {@link #run(WorkerContext)}.
         */
        int carrierId();

        /**
         * True when the worker pool is halting, signalling the job to wind down.
         */
        boolean isTerminating();
    }
}
