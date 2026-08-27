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

    /**
     * Number of idle loop passes credited back to a worker's back-off ticker each
     * time one of its jobs reports having done work. The ticker decays by this
     * amount rather than resetting to zero, so back-off depends on how OFTEN a
     * worker finds work instead of on whether it ever does: it keeps climbing
     * while work arrives less than once per {@code getBackoffCredit()} passes and
     * stays pinned at zero for a worker that is genuinely busy.
     * <p>
     * The default keeps a worker busy on more than roughly 1.5% of its passes at
     * full spin. Raising it makes back-off harder to reach; a value greater than
     * or equal to {@link #getSleepThreshold()} reproduces the historical
     * reset-to-zero behaviour.
     */
    default long getBackoffCredit() {
        return 64;
    }

    default Metrics getMetrics() {
        return Metrics.ENABLED;
    }

    default long getNapThreshold() {
        return 7000;
    }

    /**
     * Milliseconds a worker sleeps on the nap rung, i.e. once its back-off ticker
     * has passed {@link #getNapThreshold()} but not {@link #getSleepThreshold()}.
     * Distinct from {@link #getSleepTimeout()}, which applies to the deeper sleep
     * rung only.
     */
    default long getNapTimeout() {
        return 1;
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
    default boolean isLegacy() {
        return false;
    }

    default int workerPoolPriority() {
        return Thread.NORM_PRIORITY;
    }
}
