/*******************************************************************************
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

    default int workerPoolPriority() {
        return Thread.NORM_PRIORITY;
    }
}
