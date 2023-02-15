/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.cutlass.http.processors;

import io.questdb.metrics.Counter;
import io.questdb.metrics.LongGauge;
import io.questdb.metrics.MetricsRegistry;
import org.jetbrains.annotations.TestOnly;

public class JsonQueryMetrics {

    private final LongGauge cachedQueriesGauge;
    private final Counter completedQueriesCounter;
    private final Counter startedQueriesCounter;

    public JsonQueryMetrics(MetricsRegistry metricsRegistry) {
        this.startedQueriesCounter = metricsRegistry.newCounter("json_queries");
        this.completedQueriesCounter = metricsRegistry.newCounter("json_queries_completed");
        this.cachedQueriesGauge = metricsRegistry.newLongGauge("json_queries_cached");
    }

    public LongGauge cachedQueriesGauge() {
        return cachedQueriesGauge;
    }

    @TestOnly
    public long completedQueriesCount() {
        return completedQueriesCounter.getValue();
    }

    public void markComplete() {
        completedQueriesCounter.inc();
    }

    public void markStart() {
        startedQueriesCounter.inc();
    }

    @TestOnly
    public long startedQueriesCount() {
        return startedQueriesCounter.getValue();
    }
}
