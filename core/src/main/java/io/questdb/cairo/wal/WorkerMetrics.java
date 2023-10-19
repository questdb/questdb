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

package io.questdb.cairo.wal;

import io.questdb.metrics.LongGauge;
import io.questdb.metrics.MetricsRegistry;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;

public class WorkerMetrics {
    private final static MicrosecondClock CLOCK_MICRO = MicrosecondClockImpl.INSTANCE;
    private final ConcurrentHashMap<Long> beginMicro;
    private final MetricsRegistry metricsRegistry;
    private final ConcurrentHashMap<LongGauge> workerMaxElapsed;
    private final ConcurrentHashMap<LongGauge> workerMinElapsed;

    public WorkerMetrics(MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
        beginMicro = new ConcurrentHashMap<>();
        workerMinElapsed = new ConcurrentHashMap<>();
        workerMaxElapsed = new ConcurrentHashMap<>();

    }

    public void beginJob(String workerName) {
        beginMicro.put(workerName, CLOCK_MICRO.getTicks());
    }

    public void endJob(String workerName) {
        long start = beginMicro.get(workerName);
        long elapsed = CLOCK_MICRO.getTicks() - start;
        LongGauge min = workerMinElapsed.get(workerName);
        min.setValue(Math.min(min.getValue(), elapsed));
        LongGauge max = workerMaxElapsed.get(workerName);
        max.setValue(Math.max(max.getValue(), elapsed));
    }

    public long getMaxElapsed(String workerName) {
        return getValue(workerMaxElapsed, workerName);
    }

    public long getMinElapsed(String workerName) {
        return getValue(workerMinElapsed, workerName);
    }

    public WorkerMetrics init(String workerName) {
        assert beginMicro.get(workerName) == null;
        LongGauge min = metricsRegistry.newLongGauge("worker_" + workerName + "_min_elapsed_micros");
        LongGauge max = metricsRegistry.newLongGauge("worker_" + workerName + "_max_elapsed_micros");
        min.setValue(Long.MAX_VALUE);
        max.setValue(Long.MIN_VALUE);
        beginMicro.put(workerName, CLOCK_MICRO.getTicks());
        workerMinElapsed.put(workerName, min);
        workerMaxElapsed.put(workerName, max);
        return this;
    }

    private static long getValue(ConcurrentHashMap<LongGauge> source, String workerName) {
        LongGauge gauge = source.get(workerName);
        return gauge != null ? gauge.getValue() : null;
    }
}
