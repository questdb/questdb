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

package io.questdb.cairo.wal;

import io.questdb.metrics.Counter;
import io.questdb.metrics.LongGauge;
import io.questdb.metrics.MetricsRegistry;
import io.questdb.std.Mutable;

import java.util.concurrent.atomic.AtomicLong;

public class WalMetrics implements Mutable {
    private final Counter applyPhysicallyWrittenRowsCounter;
    private final LongGauge applyRowsWriteRateGauge;
    private final Counter applyRowsWrittenCounter;
    private final Counter rowsWrittenCounter;
    private final LongGauge seqTxnGauge;
    private final AtomicLong totalRowsWritten = new AtomicLong();
    private final AtomicLong totalRowsWrittenTotalTime = new AtomicLong();
    private final LongGauge writerTxnGauge;

    public WalMetrics(MetricsRegistry metricsRegistry) {
        this.applyPhysicallyWrittenRowsCounter = metricsRegistry.newCounter("wal_apply_physically_written_rows");
        this.applyRowsWriteRateGauge = metricsRegistry.newLongGauge("wal_apply_rows_per_second");
        this.applyRowsWrittenCounter = metricsRegistry.newCounter("wal_apply_written_rows");
        this.rowsWrittenCounter = metricsRegistry.newCounter("wal_written_rows");
        this.seqTxnGauge = metricsRegistry.newAtomicLongGauge("wal_apply_seq_txn");
        this.writerTxnGauge = metricsRegistry.newAtomicLongGauge("wal_apply_writer_txn");
    }

    public void addApplyRowsWritten(long rows, long physicallyWrittenRows, long timeMicros) {
        applyRowsWrittenCounter.add(rows);
        applyPhysicallyWrittenRowsCounter.add(physicallyWrittenRows);

        long totalRows = totalRowsWritten.addAndGet(rows);
        long rowsAppendRate = totalRows * 1_000_000L / Math.max(1, totalRowsWrittenTotalTime.addAndGet(timeMicros));
        applyRowsWriteRateGauge.setValue(rowsAppendRate);
    }

    public void addRowsWritten(long txnRowCount) {
        rowsWrittenCounter.add(txnRowCount);
    }

    public void addSeqTxn(long txnDelta) {
        seqTxnGauge.add(txnDelta);
    }

    public void addWriterTxn(long txnDelta) {
        writerTxnGauge.add(txnDelta);
    }

    @Override
    public void clear() {
        applyPhysicallyWrittenRowsCounter.reset();
        applyRowsWriteRateGauge.setValue(0);
        applyRowsWrittenCounter.reset();
        rowsWrittenCounter.reset();
        seqTxnGauge.setValue(0);
        totalRowsWritten.set(0);
        totalRowsWrittenTotalTime.set(0);
        writerTxnGauge.setValue(0);
    }
}
