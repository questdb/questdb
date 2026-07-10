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

import io.questdb.Metrics;

public interface WorkerPoolConfiguration {

    default Metrics getMetrics() {
        return Metrics.ENABLED;
    }

    default long getNapThreshold() {
        return 7000;
    }

    default String getPoolName() {
        return "worker";
    }

    default long getSleepThreshold() {
        return 10000;
    }

    default long getSleepTimeout() {
        return 10;
    }

    default int[] getWorkerAffinity() {
        return null;
    }

    int getWorkerCount();

    default long getYieldThreshold() {
        return 10;
    }

    default boolean haltOnError() {
        return false;
    }

    default boolean isDaemonPool() {
        return false;
    }

    default boolean isEnabled() {
        return true;
    }

    /**
     * If true, the pool runs in legacy mode: workers do NOT wrap their loop
     * body in a {@link io.questdb.mp.continuation.WorkerContinuation} and
     * {@code Job.cloneInstance()} is never invoked by the framework. Per-worker
     * assignment via {@code WorkerPool.assign(int worker, Job job)} is only
     * allowed on legacy pools, since the workerId carries identity meaning
     * (used by the assigned Job's instance state).
     * <p>
     * Non-legacy (default) pools install continuations and require
     * {@code Job.cloneInstance()} to provide per-cont-snapshot isolation;
     * callers register Jobs via {@code WorkerPool.assign(Job job)} which
     * clones once per worker.
     */
    /**
     * If true, the pool runs in fiber-host mode, the end-state worker loop of the
     * query-fiber tier: workers run a PLAIN loop -- never wrapped in a
     * {@link io.questdb.mp.continuation.WorkerContinuation}, no handoff mechanism,
     * no job generation minting -- and mount parked {@code QueryFiber}s from the
     * pool's continuation queue directly, which is legal because a plain frame
     * carries no continuation. A fiber mount costs no allocation at all.
     * <p>
     * Jobs on a fiber-host pool must not suspend on the worker loop; all query
     * suspension happens inside the hosted fibers. A wait function reached inline
     * anyway finds no mounted continuation and takes its legacy polling fallback,
     * so a misconfigured pool degrades to pre-continuation behavior instead of
     * failing.
     * <p>
     * Mutually exclusive with {@link #isLegacy()}: a legacy pool has no
     * continuation queue and cannot host fibers.
     */
    default boolean isFiberHost() {
        return false;
    }

    default boolean isLegacy() {
        return false;
    }

    default int workerPoolPriority() {
        return Thread.NORM_PRIORITY;
    }
}
