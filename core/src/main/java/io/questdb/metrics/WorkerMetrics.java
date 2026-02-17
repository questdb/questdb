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

public class WorkerMetrics implements Mutable {

    private final LongGauge max;
    private final LongGauge min;

    public WorkerMetrics(MetricsRegistry metricsRegistry) {
        min = metricsRegistry.newLongGauge("workers_job_start_micros_min");
        max = metricsRegistry.newLongGauge("workers_job_start_micros_max");
        min.setValue(Long.MAX_VALUE);
        max.setValue(Long.MIN_VALUE);
    }

    @Override
    public void clear() {
        min.setValue(Long.MAX_VALUE);
        max.setValue(Long.MIN_VALUE);
    }

    public long getMaxElapsedMicros() {
        return max.getValue();
    }

    public long getMinElapsedMicros() {
        return min.getValue();
    }

    public void update(long candidateMin, long candidateMax) {
        if (candidateMin < min.getValue()) {
            min.setValue(candidateMin);
        }
        if (candidateMax > max.getValue()) {
            max.setValue(candidateMax);
        }
    }
}
