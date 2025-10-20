/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicLong;

public class ActiveConnectionTracker {
    public static final String PROCESSOR_EXPORT = "export-http";
    public static final String PROCESSOR_ILP = "ilp-http";
    public static final String PROCESSOR_JSON = "json-http";
    public static final String PROCESSOR_OTHER = "other-http";
    public static final int UNLIMITED = -1;
    public static final ActiveConnectionTracker NO_TRACKING = new ActiveConnectionTracker() {
        @Override
        public long decrementActiveConnection(@NotNull String name) {
            return 0;
        }

        public long getActiveConnections(@NotNull String processorName) {
            return 0;
        }

        @Override
        public int getLimit(@NotNull String processorName) {
            return UNLIMITED;
        }

        @Override
        public long incrementActiveConnection(@NotNull String processorName) {
            return 0;
        }
    };
    private final HttpContextConfiguration contextConfiguration;
    private final AtomicLong exportActiveConnections = new AtomicLong();
    private final AtomicLong ilpActiveConnections = new AtomicLong();
    private final AtomicLong jsonActiveConnections = new AtomicLong();
    private final Metrics metrics;

    private ActiveConnectionTracker() {
        contextConfiguration = null;
        metrics = null;
    }

    public ActiveConnectionTracker(HttpContextConfiguration contextConfiguration) {
        this.contextConfiguration = contextConfiguration;
        this.metrics = contextConfiguration.getMetrics();
    }

    public long decrementActiveConnection(@NotNull String processorName) {
        getGauge(processorName).dec();
        return getCounter(processorName).decrementAndGet();
    }

    public long getActiveConnections(@NotNull String processorName) {
        return getCounter(processorName).get();
    }

    public int getLimit(String processorName) {
        if (processorName == null) {
            // No limit.
            return UNLIMITED;
        }
        switch (processorName) {
            case PROCESSOR_JSON:
                return contextConfiguration.getJsonQueryConnectionLimit();
            case PROCESSOR_ILP:
                return contextConfiguration.getIlpConnectionLimit();
            case PROCESSOR_EXPORT:
                return contextConfiguration.getExportConnectionLimit();
            default:
                // No limit.
                return UNLIMITED;
        }
    }

    public long incrementActiveConnection(@NotNull String processorName) {
        getGauge(processorName).inc();
        return getCounter(processorName).incrementAndGet();
    }

    private AtomicLong getCounter(@NotNull String processorName) {
        switch (processorName) {
            case PROCESSOR_ILP:
                return ilpActiveConnections;
            case PROCESSOR_EXPORT:
                return exportActiveConnections;
            default:
                return jsonActiveConnections;
        }
    }

    private AtomicLongGauge getGauge(@NotNull String processorName) {
        switch (processorName) {
            case PROCESSOR_ILP:
                return metrics.lineMetrics().httpConnectionCountGauge();
            default:
                return metrics.jsonQueryMetrics().connectionCountGauge();
        }
    }
}
