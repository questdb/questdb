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

package io.questdb.metrics;

import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import org.jetbrains.annotations.TestOnly;

public class WorkerMetricsImpl implements WorkerMetrics {
    private final static MicrosecondClock CLOCK_MICRO = MicrosecondClockImpl.INSTANCE;
    private final MetricsRegistry metricsRegistry;
    private final ConcurrentHashMap<WorkerRecord> workerRecords;

    public WorkerMetricsImpl(MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
        workerRecords = new ConcurrentHashMap<>();
    }

    @Override
    public void beginJob(String workerName) {
        WorkerRecord record = workerRecords.get(workerName);
        if (record != null) {
            record.beginJob();
        }
    }

    @Override
    public void endJob(String workerName) {
        WorkerRecord record = workerRecords.get(workerName);
        if (record != null) {
            record.endJob();
        }
    }

    @TestOnly
    public long getMaxElapsed(String workerName) {
        return workerRecords.get(workerName).maxElapsed.getValue();
    }

    @TestOnly
    public long getMinElapsed(String workerName) {
        return workerRecords.get(workerName).minElapsed.getValue();
    }

    @Override
    public WorkerMetricsImpl initWorker(String workerName) {
        WorkerRecord record = workerRecords.get(workerName);
        if (record != null) {
            record.init();
        }
        workerRecords.put(workerName, new WorkerRecord(
                metricsRegistry.newLongGauge("worker_" + workerName + "_min_elapsed_micros"),
                metricsRegistry.newLongGauge("worker_" + workerName + "_max_elapsed_micros")
        ));
        return this;
    }

    private static long getValue(ConcurrentHashMap<LongGauge> source, String workerName) {
        LongGauge gauge = source.get(workerName);
        return gauge != null ? gauge.getValue() : null;
    }

    private static class WorkerRecord {
        private final LongGauge maxElapsed;
        private final LongGauge minElapsed;
        private long tsMicro;

        private WorkerRecord(LongGauge minElapsed, LongGauge maxElapsed) {
            this.minElapsed = minElapsed;
            this.maxElapsed = maxElapsed;
            init();
        }

        private void beginJob() {
            tsMicro = CLOCK_MICRO.getTicks();
        }

        private void endJob() {
            long elapsed = CLOCK_MICRO.getTicks() - tsMicro;
            long min = minElapsed.getValue();
            if (elapsed < min) {
                minElapsed.setValue(elapsed);
            }
            long max = maxElapsed.getValue();
            if (elapsed > max) {
                maxElapsed.setValue(elapsed);
            }
        }

        private void init() {
            minElapsed.setValue(Long.MAX_VALUE);
            maxElapsed.setValue(Long.MIN_VALUE);
            tsMicro = 0L;
        }
    }
}
