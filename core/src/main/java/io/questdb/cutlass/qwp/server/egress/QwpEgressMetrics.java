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

package io.questdb.cutlass.qwp.server.egress;

import io.questdb.metrics.AtomicLongGauge;
import io.questdb.metrics.Counter;
import io.questdb.metrics.MetricsRegistry;
import io.questdb.std.Mutable;
import org.jetbrains.annotations.TestOnly;

/**
 * Prometheus-scrapable counters and gauges for the QWP egress endpoint
 * (SQL query results streamed over WebSocket at {@code /read/v1}).
 * <p>
 * All names are prefixed {@code questdb_qwp_egress_*} so they group cleanly
 * against the existing {@code questdb_json_*}, {@code questdb_ilp_*}, and
 * {@code questdb_pgwire_*} families in Prometheus / Grafana dashboards.
 * <p>
 * The public Java surface is minimal: the server-side code updates counters
 * through the {@code markX} helpers, the connection gauge via
 * {@link #connectionCountGauge()}, and tests read values through
 * {@link #bytesCompressedSavedCounter()} /
 * {@link #bytesSentCounter()} / {@link #queriesErroredCounter()} /
 * {@link #rowsStreamedCounter()} plus a few {@code @TestOnly} convenience
 * accessors. All registered counters are scraped via the shared
 * {@link MetricsRegistry} regardless of whether the Java-side getter is
 * exposed.
 */
public class QwpEgressMetrics implements Mutable {
    private final Counter batchesSentCounter;
    private final Counter bytesCompressedSavedCounter;
    private final Counter bytesSentCounter;
    private final Counter cacheResetDictCounter;
    private final Counter cacheResetSchemasCounter;
    private final AtomicLongGauge connectionCountGauge;
    private final Counter queriesCancelledCounter;
    private final Counter queriesErroredCounter;
    private final Counter queriesStartedCounter;
    private final Counter rowsStreamedCounter;

    public QwpEgressMetrics(MetricsRegistry metricsRegistry) {
        this.connectionCountGauge = metricsRegistry.newAtomicLongGauge("qwp_egress_connections");
        this.queriesStartedCounter = metricsRegistry.newCounter("qwp_egress_queries_started");
        this.queriesErroredCounter = metricsRegistry.newCounter("qwp_egress_queries_errored");
        this.queriesCancelledCounter = metricsRegistry.newCounter("qwp_egress_queries_cancelled");
        this.batchesSentCounter = metricsRegistry.newCounter("qwp_egress_batches_sent");
        this.bytesSentCounter = metricsRegistry.newCounter("qwp_egress_bytes_sent");
        this.bytesCompressedSavedCounter = metricsRegistry.newCounter("qwp_egress_bytes_zstd_saved");
        this.rowsStreamedCounter = metricsRegistry.newCounter("qwp_egress_rows_streamed");
        this.cacheResetDictCounter = metricsRegistry.newCounter("qwp_egress_cache_reset_dict");
        this.cacheResetSchemasCounter = metricsRegistry.newCounter("qwp_egress_cache_reset_schemas");
    }

    @TestOnly
    public long batchesSentCount() {
        return batchesSentCounter.getValue();
    }

    public Counter bytesCompressedSavedCounter() {
        return bytesCompressedSavedCounter;
    }

    public Counter bytesSentCounter() {
        return bytesSentCounter;
    }

    @TestOnly
    public long cacheResetDictCount() {
        return cacheResetDictCounter.getValue();
    }

    @TestOnly
    public long cacheResetSchemasCount() {
        return cacheResetSchemasCounter.getValue();
    }

    @Override
    public void clear() {
        connectionCountGauge.setValue(0);
        queriesStartedCounter.reset();
        queriesErroredCounter.reset();
        queriesCancelledCounter.reset();
        batchesSentCounter.reset();
        bytesSentCounter.reset();
        bytesCompressedSavedCounter.reset();
        rowsStreamedCounter.reset();
        cacheResetDictCounter.reset();
        cacheResetSchemasCounter.reset();
    }

    public AtomicLongGauge connectionCountGauge() {
        return connectionCountGauge;
    }

    public void markBatchSent(int bytes, int rows) {
        batchesSentCounter.inc();
        bytesSentCounter.add(bytes);
        rowsStreamedCounter.add(rows);
    }

    public void markBytesCompressedSaved(int bytes) {
        if (bytes > 0) {
            bytesCompressedSavedCounter.add(bytes);
        }
    }

    public void markCacheResetDict() {
        cacheResetDictCounter.inc();
    }

    public void markCacheResetSchemas() {
        cacheResetSchemasCounter.inc();
    }

    public void markQueryCancelled() {
        queriesCancelledCounter.inc();
    }

    public void markQueryErrored() {
        queriesErroredCounter.inc();
    }

    public void markQueryStarted() {
        queriesStartedCounter.inc();
    }

    public Counter queriesErroredCounter() {
        return queriesErroredCounter;
    }

    @TestOnly
    public long queriesStartedCount() {
        return queriesStartedCounter.getValue();
    }

    public Counter rowsStreamedCounter() {
        return rowsStreamedCounter;
    }
}
