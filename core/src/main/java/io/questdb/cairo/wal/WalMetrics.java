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

import io.questdb.metrics.Counter;
import io.questdb.metrics.LongGauge;
import io.questdb.metrics.MetricsRegistry;

import java.util.concurrent.atomic.AtomicLong;

public class WalMetrics {
    private final Counter applyPhysicallyWrittenRowsCounter;
    private final LongGauge applyRowsWriteRateGauge;
    private final Counter applyRowsWrittenCounter;
    private final Counter rowsWrittenCounter;
    private final AtomicLong totalRowsWritten = new AtomicLong();
    private final AtomicLong totalRowsWrittenTotalTime = new AtomicLong();

    public WalMetrics(MetricsRegistry metricsRegistry) {
        this.applyPhysicallyWrittenRowsCounter = metricsRegistry.newCounter("wal_apply_physically_written_rows");
        this.applyRowsWrittenCounter = metricsRegistry.newCounter("wal_apply_written_rows");
        this.applyRowsWriteRateGauge = metricsRegistry.newLongGauge("wal_apply_rows_per_second");
        this.rowsWrittenCounter = metricsRegistry.newCounter("wal_written_rows");
    }

    public void addApplyRowsWritten(long rows, long physicallyWrittenRows, long timeMicros) {
        applyRowsWrittenCounter.add(rows);
        applyPhysicallyWrittenRowsCounter.add(physicallyWrittenRows);

        long totalRows = totalRowsWritten.addAndGet(rows);
        long rowsAppendRate = totalRows * 1_000_000L / Math.max(1, totalRowsWrittenTotalTime.addAndGet(timeMicros));
        applyRowsWriteRateGauge.setValue(rowsAppendRate);
    }

    public void addRowsWritten(long rows) {
        rowsWrittenCounter.add(rows);
    }
}
