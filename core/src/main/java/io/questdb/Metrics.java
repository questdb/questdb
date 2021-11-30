/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb;

import io.questdb.cutlass.http.processors.JsonQueryMetrics;
import io.questdb.metrics.MetricsRegistry;
import io.questdb.metrics.MetricsRegistryImpl;
import io.questdb.metrics.NullMetricsRegistry;
import io.questdb.metrics.Scrapable;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;

public class Metrics implements Scrapable {
    private final boolean enabled;
    private final JsonQueryMetrics jsonQuery;
    private final MetricsRegistry metricsRegistry;

    Metrics(boolean enabled, MetricsRegistry metricsRegistry) {
        this.enabled = enabled;
        this.jsonQuery = new JsonQueryMetrics(metricsRegistry);
        createMemoryGauges(metricsRegistry);
        this.metricsRegistry = metricsRegistry;
    }

    private void createMemoryGauges(MetricsRegistry metricsRegistry) {
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            metricsRegistry.newGauge(i);
        }

        metricsRegistry.newVirtualGauge("memory_free_count", Unsafe::getFreeCount);
        metricsRegistry.newVirtualGauge("memory_mem_used", Unsafe::getMemUsed);
        metricsRegistry.newVirtualGauge("memory_malloc_count", Unsafe::getMallocCount);
    }

    public static Metrics enabled() {
        return new Metrics(true, new MetricsRegistryImpl());
    }

    public static Metrics disabled() {
        return new Metrics(false, new NullMetricsRegistry());
    }

    public boolean isEnabled() {
        return enabled;
    }

    public JsonQueryMetrics jsonQuery() {
        return jsonQuery;
    }

    @Override
    public void scrapeIntoPrometheus(CharSink sink) {
        metricsRegistry.scrapeIntoPrometheus(sink);
    }
}
