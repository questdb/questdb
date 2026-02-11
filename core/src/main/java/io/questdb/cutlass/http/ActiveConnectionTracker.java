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

package io.questdb.cutlass.http;

import io.questdb.Metrics;
import io.questdb.metrics.AtomicLongGauge;
import io.questdb.metrics.AtomicLongGaugeImpl;
import io.questdb.metrics.NullLongGauge;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ActiveConnectionTracker {
    public static final String PROCESSOR_EXPORT_HTTP = "http_export_connections";
    public static final String PROCESSOR_ILP_HTTP = "http_ilp_connections";
    public static final String PROCESSOR_JSON = "http_json_connections";
    public static final String PROCESSOR_OTHER = "http_other_connections";
    public static final int UNLIMITED = -1;
    public static final ActiveConnectionTracker NO_TRACKING = new ActiveConnectionTracker() {
        @Override
        public long dec(@NotNull String name) {
            return 0;
        }

        public long get(@NotNull String processorName) {
            return 0;
        }

        @Override
        public int getLimit(@NotNull String processorName) {
            return UNLIMITED;
        }

        @Override
        public long inc(@NotNull String processorName) {
            return 0;
        }
    };
    private final HttpContextConfiguration contextConfiguration;
    private final AtomicLongGaugeImpl exportActiveConnections = new AtomicLongGaugeImpl("http_export_connections");
    private final AtomicLongGaugeImpl ilpActiveConnections = new AtomicLongGaugeImpl("http_ilp_connections");
    private final AtomicLongGaugeImpl jsonActiveConnections = new AtomicLongGaugeImpl("http_json_connections");
    private final Metrics metrics;

    private ActiveConnectionTracker() {
        contextConfiguration = null;
        metrics = null;
    }

    public ActiveConnectionTracker(HttpContextConfiguration contextConfiguration) {
        this.contextConfiguration = contextConfiguration;
        this.metrics = contextConfiguration.getMetrics();
    }

    public long dec(@NotNull String processorName) {
        getMetricsGauge(processorName).dec();
        return getLocalGauge(processorName).decrementAndGet();
    }

    public long get(@NotNull String processorName) {
        return getLocalGauge(processorName).getValue();
    }

    public int getLimit(@Nullable String processorName) {
        if (processorName == null) {
            // No limit.
            return UNLIMITED;
        }
        return switch (processorName) {
            case PROCESSOR_JSON -> contextConfiguration.getJsonQueryConnectionLimit();
            case PROCESSOR_ILP_HTTP -> contextConfiguration.getIlpConnectionLimit();
            case PROCESSOR_EXPORT_HTTP -> contextConfiguration.getExportConnectionLimit();
            case PROCESSOR_OTHER -> UNLIMITED;
            default -> throw new UnsupportedOperationException();
        };
    }

    public long inc(@NotNull String processorName) {
        getMetricsGauge(processorName).inc();
        return getLocalGauge(processorName).incrementAndGet();
    }

    private AtomicLongGauge getLocalGauge(@NotNull String processorName) {
        return switch (processorName) {
            case PROCESSOR_ILP_HTTP -> ilpActiveConnections;
            case PROCESSOR_EXPORT_HTTP -> exportActiveConnections;
            case PROCESSOR_JSON -> jsonActiveConnections;
            case PROCESSOR_OTHER -> NullLongGauge.INSTANCE;
            // noop, not currently in use
            default -> throw new UnsupportedOperationException();
        };
    }

    private AtomicLongGauge getMetricsGauge(@NotNull String processorName) {
        return switch (processorName) {
            case PROCESSOR_ILP_HTTP -> metrics.lineMetrics().httpConnectionCountGauge();
            case PROCESSOR_JSON -> metrics.jsonQueryMetrics().connectionCountGauge();
            case PROCESSOR_EXPORT_HTTP ->
                    NullLongGauge.INSTANCE;  // intentional noop, we do not have a prom metrics entry
            case PROCESSOR_OTHER -> NullLongGauge.INSTANCE;
            default -> throw new UnsupportedOperationException();
        };
    }
}
