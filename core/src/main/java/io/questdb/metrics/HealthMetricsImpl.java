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

package io.questdb.metrics;


import io.questdb.std.Mutable;

public class HealthMetricsImpl implements HealthMetrics, Mutable {

    private final Counter queryErrorCounter;
    private final Counter readerLeakCounter;
    private final Counter unhandledErrorCounter;

    public HealthMetricsImpl(MetricsRegistry metricsRegistry) {
        this.unhandledErrorCounter = metricsRegistry.newCounter("unhandled_errors");
        this.readerLeakCounter = metricsRegistry.newCounter("reader_leak_counter");
        this.queryErrorCounter = metricsRegistry.newCounter("query_error_counter");
    }

    @Override
    public void clear() {
        unhandledErrorCounter.reset();
        readerLeakCounter.reset();
        queryErrorCounter.reset();
    }

    @Override
    public void incrementQueryErrorCounter() {
        queryErrorCounter.inc();
    }

    @Override
    public void incrementReaderLeakCounter(int count) {
        readerLeakCounter.add(count);
    }

    @Override
    public void incrementUnhandledErrors() {
        unhandledErrorCounter.inc();
    }

    @Override
    public long queryErrorCounter() {
        return queryErrorCounter.getValue();
    }

    @Override
    public long readerLeakCounter() {
        return readerLeakCounter.getValue();
    }

    @Override
    public long unhandledErrorsCount() {
        return unhandledErrorCounter.getValue();
    }
}
