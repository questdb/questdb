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

package io.questdb.metrics;

public interface MetricsConfiguration {
    String DEFAULT_PERSIST_EXCLUDE = "worker_pool_fiber_" +
            "(max_live|mounted|retained|finalizing|outstanding|created|retired|mount|wake|launch|" +
            "scheduler_publication|scheduler_selection|orphan_recovery|mount_budget_exhaustion)(__.*)?";

    default void appendPersistedMetricDefinitions(MetricSnapshotVisitor visitor) {
    }

    default CharSequence getPersistExclude() {
        return DEFAULT_PERSIST_EXCLUDE;
    }

    default long getPersistIntervalMicros() {
        return 1_000_000;
    }

    default CharSequence getPersistTtl() {
        return "1 WEEK";
    }

    default long getPersistVirtualIntervalMicros() {
        return 60_000_000;
    }

    boolean isEnabled();

    default boolean isPersistEnabled() {
        return false;
    }

    default boolean isPersistParquetEnabled() {
        return true;
    }
}
