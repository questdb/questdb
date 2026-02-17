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

package io.questdb.cutlass.http.processors;

import io.questdb.metrics.AtomicLongGauge;
import io.questdb.metrics.Counter;
import io.questdb.metrics.LongGauge;
import io.questdb.metrics.MetricsRegistry;
import io.questdb.std.Mutable;
import org.jetbrains.annotations.TestOnly;

public class JsonQueryMetrics implements Mutable {
    private final Counter cacheHitCounter;
    private final Counter cacheMissCounter;
    private final LongGauge cachedQueriesGauge;
    private final Counter completedQueriesCounter;
    private final AtomicLongGauge connectionCountGauge;
    private final Counter startedQueriesCounter;

    public JsonQueryMetrics(MetricsRegistry metricsRegistry) {
        this.connectionCountGauge = metricsRegistry.newAtomicLongGauge("json_queries_connections");
        this.startedQueriesCounter = metricsRegistry.newCounter("json_queries");
        this.completedQueriesCounter = metricsRegistry.newCounter("json_queries_completed");
        this.cachedQueriesGauge = metricsRegistry.newLongGauge("json_queries_cached");
        this.cacheHitCounter = metricsRegistry.newCounter("json_queries_cache_hits");
        this.cacheMissCounter = metricsRegistry.newCounter("json_queries_cache_misses");
    }

    public Counter cacheHitCounter() {
        return cacheHitCounter;
    }

    public Counter cacheMissCounter() {
        return cacheMissCounter;
    }

    public LongGauge cachedQueriesGauge() {
        return cachedQueriesGauge;
    }

    @Override
    public void clear() {
        connectionCountGauge.setValue(0);
        cacheHitCounter.reset();
        cacheMissCounter.reset();
        cachedQueriesGauge.setValue(0);
        completedQueriesCounter.reset();
        startedQueriesCounter.reset();
    }

    @TestOnly
    public long completedQueriesCount() {
        return completedQueriesCounter.getValue();
    }

    public AtomicLongGauge connectionCountGauge() {
        return connectionCountGauge;
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
