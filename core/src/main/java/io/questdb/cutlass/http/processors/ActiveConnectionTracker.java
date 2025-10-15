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

package io.questdb.cutlass.http.processors;

import io.questdb.Metrics;
import io.questdb.cutlass.http.HttpContextConfiguration;
import io.questdb.metrics.AtomicLongGauge;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicInteger;

public class ActiveConnectionTracker {
    public static final ActiveConnectionTracker NO_TRACKING = new ActiveConnectionTracker() {
        @Override
        public void decrementActiveConnection(@NotNull String name) {
        }

        public int getActiveConnections(@NotNull String processorName) {
            return 0;
        }

        @Override
        public int getLimit(@NotNull String processorName) {
            return Integer.MAX_VALUE;
        }

        @Override
        public long incrementActiveConnection(@NotNull String processorName) {
            return 0;
        }
    };
    public static final String PROCESSOR_EXPORT = "export-http";
    public static final String PROCESSOR_ILP = "ilp-http";
    public static final String PROCESSOR_JSON = "json-http";
    public static final String PROCESSOR_OTHER = "other-http";
    private final HttpContextConfiguration contextConfiguration;
    private final AtomicInteger exportActiveConnections = new AtomicInteger();
    private final AtomicInteger ilpActiveConnections = new AtomicInteger();
    private final AtomicInteger jsonActiveConnections = new AtomicInteger();
    private final Metrics metrics;

    private ActiveConnectionTracker() {
        contextConfiguration = null;
        metrics = null;
    }

    public ActiveConnectionTracker(HttpContextConfiguration contextConfiguration) {
        this.contextConfiguration = contextConfiguration;
        this.metrics = contextConfiguration.getMetrics();
    }

    public void decrementActiveConnection(@NotNull String processorName) {
        getCounter(processorName).decrementAndGet();
        getGauge(processorName).dec();
    }

    public int getActiveConnections(@NotNull String processorName) {
        return getCounter(processorName).get();
    }

    public int getLimit(String processorName) {
        if (processorName == null) {
            // No limit.
            return -1;
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
                return -1;
        }
    }

    public long incrementActiveConnection(@NotNull String processorName) {
        getGauge(processorName).incrementAndGet();
        return getCounter(processorName).incrementAndGet();
    }

    private AtomicInteger getCounter(@NotNull String processorName) {
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
            case PROCESSOR_JSON:
                return metrics.jsonQueryMetrics().connectionCountGauge();
            case PROCESSOR_ILP:
                return metrics.lineMetrics().httpConnectionCountGauge();
            default:
                return metrics.jsonQueryMetrics().connectionCountGauge();
        }
    }
}
