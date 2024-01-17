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

package io.questdb;

import io.questdb.cairo.TableWriterMetrics;
import io.questdb.cairo.wal.WalMetrics;
import io.questdb.cutlass.http.processors.JsonQueryMetrics;
import io.questdb.cutlass.line.LineMetrics;
import io.questdb.cutlass.pgwire.PGWireMetrics;
import io.questdb.metrics.*;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.BorrowableUtf8Sink;
import org.jetbrains.annotations.NotNull;

public class Metrics implements Scrapable {
    private final boolean enabled;
    private final GCMetrics gcMetrics;
    private final HealthMetricsImpl healthCheck;
    private final JsonQueryMetrics jsonQuery;
    private final LineMetrics line;
    private final MetricsRegistry metricsRegistry;
    private final PGWireMetrics pgWire;
    private final Runtime runtime = Runtime.getRuntime();
    private final VirtualLongGauge.StatProvider jvmFreeMemRef = runtime::freeMemory;
    private final VirtualLongGauge.StatProvider jvmMaxMemRef = runtime::maxMemory;
    private final VirtualLongGauge.StatProvider jvmTotalMemRef = runtime::totalMemory;
    private final TableWriterMetrics tableWriter;
    private final WalMetrics walMetrics;
    private final WorkerMetrics workerMetrics;

    public Metrics(boolean enabled, MetricsRegistry metricsRegistry) {
        this.enabled = enabled;
        this.gcMetrics = new GCMetrics();
        this.jsonQuery = new JsonQueryMetrics(metricsRegistry);
        this.pgWire = new PGWireMetrics(metricsRegistry);
        this.line = new LineMetrics(metricsRegistry);
        this.healthCheck = new HealthMetricsImpl(metricsRegistry);
        this.tableWriter = new TableWriterMetrics(metricsRegistry);
        this.walMetrics = new WalMetrics(metricsRegistry);
        createMemoryGauges(metricsRegistry);
        this.metricsRegistry = metricsRegistry;
        this.workerMetrics = new WorkerMetrics(metricsRegistry);
    }

    public static Metrics disabled() {
        return new Metrics(false, new NullMetricsRegistry());
    }

    public static Metrics enabled() {
        return new Metrics(true, new MetricsRegistryImpl());
    }

    public MetricsRegistry getRegistry() {
        return metricsRegistry;
    }

    public HealthMetricsImpl health() {
        return healthCheck;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public JsonQueryMetrics jsonQuery() {
        return jsonQuery;
    }

    public LineMetrics line() {
        return line;
    }

    public PGWireMetrics pgWire() {
        return pgWire;
    }

    @Override
    public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
        metricsRegistry.scrapeIntoPrometheus(sink);
        if (enabled) {
            gcMetrics.scrapeIntoPrometheus(sink);
        }
    }

    public TableWriterMetrics tableWriter() {
        return tableWriter;
    }

    public WalMetrics walMetrics() {
        return walMetrics;
    }

    public WorkerMetrics workerMetrics() {
        return workerMetrics;
    }

    private void createMemoryGauges(MetricsRegistry metricsRegistry) {
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            metricsRegistry.newLongGauge(i);
        }

        metricsRegistry.newVirtualGauge("memory_free_count", Unsafe::getFreeCount);
        metricsRegistry.newVirtualGauge("memory_mem_used", Unsafe::getMemUsed);
        metricsRegistry.newVirtualGauge("memory_malloc_count", Unsafe::getMallocCount);
        metricsRegistry.newVirtualGauge("memory_realloc_count", Unsafe::getReallocCount);
        metricsRegistry.newVirtualGauge("memory_rss", Os::getRss);
        metricsRegistry.newVirtualGauge("memory_jvm_free", jvmFreeMemRef);
        metricsRegistry.newVirtualGauge("memory_jvm_total", jvmTotalMemRef);
        metricsRegistry.newVirtualGauge("memory_jvm_max", jvmMaxMemRef);
    }

    void addScrapable(Scrapable scrapable) {
        metricsRegistry.addScrapable(scrapable);
    }
}
