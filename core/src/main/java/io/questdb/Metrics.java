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

package io.questdb;

import io.questdb.cairo.TableWriterMetrics;
import io.questdb.cairo.wal.WalMetrics;
import io.questdb.cutlass.http.processors.HttpMetrics;
import io.questdb.cutlass.http.processors.JsonQueryMetrics;
import io.questdb.cutlass.line.LineMetrics;
import io.questdb.cutlass.pgwire.PGMetrics;
import io.questdb.metrics.GCMetrics;
import io.questdb.metrics.HealthMetricsImpl;
import io.questdb.metrics.MetricsRegistry;
import io.questdb.metrics.MetricsRegistryImpl;
import io.questdb.metrics.NullMetricsRegistry;
import io.questdb.metrics.Target;
import io.questdb.metrics.VirtualLongGauge;
import io.questdb.metrics.WorkerMetrics;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.BorrowableUtf8Sink;
import org.jetbrains.annotations.NotNull;

public class Metrics implements Target, Mutable {
    public static final Metrics DISABLED = new Metrics(false, new NullMetricsRegistry());
    public static final Metrics ENABLED = new Metrics(true, new MetricsRegistryImpl());
    private final GCMetrics gcMetrics;
    private final HealthMetricsImpl healthCheck;
    private final HttpMetrics httpMetrics;
    private final JsonQueryMetrics jsonQueryMetrics;
    private final LineMetrics lineMetrics;
    private final MetricsRegistry metricsRegistry;
    private final PGMetrics pgMetrics;
    private final Runtime runtime = Runtime.getRuntime();
    private final VirtualLongGauge.StatProvider jvmFreeMemRef = runtime::freeMemory;
    private final VirtualLongGauge.StatProvider jvmMaxMemRef = runtime::maxMemory;
    private final VirtualLongGauge.StatProvider jvmTotalMemRef = runtime::totalMemory;
    private final TableWriterMetrics tableWriterMetrics;
    private final WalMetrics walMetrics;
    private final WorkerMetrics workerMetrics;
    private boolean enabled;

    public Metrics(boolean enabled, MetricsRegistry metricsRegistry) {
        this.enabled = enabled;
        this.gcMetrics = new GCMetrics();
        this.jsonQueryMetrics = new JsonQueryMetrics(metricsRegistry);
        this.httpMetrics = new HttpMetrics(metricsRegistry);
        this.pgMetrics = new PGMetrics(metricsRegistry);
        this.lineMetrics = new LineMetrics(metricsRegistry);
        this.healthCheck = new HealthMetricsImpl(metricsRegistry);
        this.tableWriterMetrics = new TableWriterMetrics(metricsRegistry);
        this.walMetrics = new WalMetrics(metricsRegistry);
        createMemoryGauges(metricsRegistry);
        this.metricsRegistry = metricsRegistry;
        this.workerMetrics = new WorkerMetrics(metricsRegistry);
    }

    @Override
    public void clear() {
        gcMetrics.clear();
        jsonQueryMetrics.clear();
        pgMetrics.clear();
        lineMetrics.clear();
        healthCheck.clear();
        tableWriterMetrics.clear();
        walMetrics.clear();
        workerMetrics.clear();
        httpMetrics.clear();
        enabled = true;
    }

    public void disable() {
        enabled = false;
    }

    public MetricsRegistry getRegistry() {
        return metricsRegistry;
    }

    public HealthMetricsImpl healthMetrics() {
        return healthCheck;
    }

    public HttpMetrics httpMetrics() {
        return httpMetrics;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public JsonQueryMetrics jsonQueryMetrics() {
        return jsonQueryMetrics;
    }

    public LineMetrics lineMetrics() {
        return lineMetrics;
    }

    public PGMetrics pgWireMetrics() {
        return pgMetrics;
    }

    @Override
    public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
        metricsRegistry.scrapeIntoPrometheus(sink);
        if (enabled) {
            gcMetrics.scrapeIntoPrometheus(sink);
        }
    }

    public TableWriterMetrics tableWriterMetrics() {
        return tableWriterMetrics;
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

    void addScrapable(Target target) {
        metricsRegistry.addTarget(target);
    }
}
