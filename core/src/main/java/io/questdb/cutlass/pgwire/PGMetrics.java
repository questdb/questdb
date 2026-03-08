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

package io.questdb.cutlass.pgwire;

import io.questdb.metrics.Counter;
import io.questdb.metrics.LongGauge;
import io.questdb.metrics.MetricsRegistry;
import io.questdb.std.Mutable;
import org.jetbrains.annotations.TestOnly;

public class PGMetrics implements Mutable {
    private final LongGauge cachedSelectsGauge;
    private final LongGauge cachedUpdatesGauge;
    private final Counter completedQueriesCounter;
    private final LongGauge connectionCountGauge;
    private final Counter errorCounter;
    private final Counter listenerStateChangeCounter;
    private final Counter selectCacheHitCounter;
    private final Counter selectCacheMissCounter;
    private final Counter startedQueriesCounter;

    public PGMetrics(MetricsRegistry metricsRegistry) {
        this.connectionCountGauge = metricsRegistry.newLongGauge("pg_wire_connections");
        this.cachedSelectsGauge = metricsRegistry.newLongGauge("pg_wire_select_queries_cached");
        this.completedQueriesCounter = metricsRegistry.newCounter("pg_wire_queries_completed");
        this.startedQueriesCounter = metricsRegistry.newCounter("pg_wire_queries");
        this.cachedUpdatesGauge = metricsRegistry.newLongGauge("pg_wire_update_queries_cached");
        this.selectCacheHitCounter = metricsRegistry.newCounter("pg_wire_select_cache_hits");
        this.selectCacheMissCounter = metricsRegistry.newCounter("pg_wire_select_cache_misses");
        this.errorCounter = metricsRegistry.newCounter("pg_wire_errors");
        this.listenerStateChangeCounter = metricsRegistry.newCounter("pg_wire_listener_state_change_count");
    }

    public LongGauge cachedSelectsGauge() {
        return cachedSelectsGauge;
    }

    @Override
    public void clear() {
        cachedSelectsGauge.setValue(0);
        cachedUpdatesGauge.setValue(0);
        completedQueriesCounter.reset();
        connectionCountGauge.setValue(0);
        errorCounter.reset();
        selectCacheHitCounter.reset();
        selectCacheMissCounter.reset();
        startedQueriesCounter.reset();
        listenerStateChangeCounter.reset();
    }

    @TestOnly
    public long completedQueriesCount() {
        return completedQueriesCounter.getValue();
    }

    public LongGauge connectionCountGauge() {
        return connectionCountGauge;
    }

    public Counter getErrorCounter() {
        return errorCounter;
    }

    public Counter listenerStateChangeCounter() {
        return listenerStateChangeCounter;
    }

    public void markComplete() {
        completedQueriesCounter.inc();
    }

    public void markStart() {
        startedQueriesCounter.inc();
    }

    @TestOnly
    public void resetQueryCounters() {
        startedQueriesCounter.reset();
        completedQueriesCounter.reset();
    }

    public Counter selectCacheHitCounter() {
        return selectCacheHitCounter;
    }

    public Counter selectCacheMissCounter() {
        return selectCacheMissCounter;
    }

    @TestOnly
    public long startedQueriesCount() {
        return startedQueriesCounter.getValue();
    }
}
