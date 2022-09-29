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

import io.questdb.cairo.TableWriterMetrics;
import io.questdb.metrics.HealthMetricsImpl;
import io.questdb.cutlass.http.processors.JsonQueryMetrics;
import io.questdb.cutlass.pgwire.PGWireMetrics;
import io.questdb.metrics.*;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;

public class Metrics implements Scrapable {
    private final boolean enabled;
    private final GCMetrics gcMetrics;
    private final JsonQueryMetrics jsonQuery;
    private final PGWireMetrics pgWire;
    private final HealthMetricsImpl healthCheck;
    private final TableWriterMetrics tableWriter;
    private final MetricsRegistry metricsRegistry;
    private final Runtime runtime = Runtime.getRuntime();
    private final VirtualGauge.StatProvider jvmFreeMemRef = runtime::freeMemory;
    private final VirtualGauge.StatProvider jvmTotalMemRef = runtime::totalMemory;
    private final VirtualGauge.StatProvider jvmMaxMemRef = runtime::maxMemory;

    Metrics(boolean enabled, MetricsRegistry metricsRegistry) {
        this.enabled = enabled;
        this.gcMetrics = new GCMetrics();
        this.jsonQuery = new JsonQueryMetrics(metricsRegistry);
        this.pgWire = new PGWireMetrics(metricsRegistry);
        this.healthCheck = new HealthMetricsImpl(metricsRegistry);
        this.tableWriter = new TableWriterMetrics(metricsRegistry);
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
        metricsRegistry.newVirtualGauge("memory_realloc_count", Unsafe::getReallocCount);
        metricsRegistry.newVirtualGauge("memory_jvm_free", jvmFreeMemRef);
        metricsRegistry.newVirtualGauge("memory_jvm_total", jvmTotalMemRef);
        metricsRegistry.newVirtualGauge("memory_jvm_max", jvmMaxMemRef);
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

    public PGWireMetrics pgWire() {
        return pgWire;
    }

    public HealthMetricsImpl health() {
        return healthCheck;
    }

    public TableWriterMetrics tableWriter() {
        return tableWriter;
    }

    @Override
    public void scrapeIntoPrometheus(CharSink sink) {
        metricsRegistry.scrapeIntoPrometheus(sink);
        if (enabled) {
            gcMetrics.scrapeIntoPrometheus(sink);
        }
    }
}
