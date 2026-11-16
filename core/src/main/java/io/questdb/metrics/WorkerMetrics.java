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

package io.questdb.metrics;

import io.questdb.std.Mutable;
import io.questdb.std.Numbers;

public class WorkerMetrics implements Mutable {

    private final LongGauge max;
    private final LongGauge min;

    public WorkerMetrics(MetricsRegistry metricsRegistry) {
        min = metricsRegistry.newLongGauge("workers_job_start_micros_min");
        max = metricsRegistry.newLongGauge("workers_job_start_micros_max");
        min.setValue(Numbers.LONG_NULL);
        max.setValue(Numbers.LONG_NULL);
    }

    @Override
    public synchronized void clear() {
        min.setValue(Numbers.LONG_NULL);
        max.setValue(Numbers.LONG_NULL);
    }

    public synchronized long getMaxElapsedMicros() {
        return max.getValue();
    }

    public synchronized long getMinElapsedMicros() {
        return min.getValue();
    }

    public synchronized void update(long candidateMin, long candidateMax) {
        final long currentMin = min.getValue();
        if (currentMin == Numbers.LONG_NULL || candidateMin < currentMin) {
            min.setValue(candidateMin);
        }
        // LONG_NULL equals Long.MIN_VALUE, so any sample replaces a max with no value
        if (candidateMax > max.getValue()) {
            max.setValue(candidateMax);
        }
    }
}
