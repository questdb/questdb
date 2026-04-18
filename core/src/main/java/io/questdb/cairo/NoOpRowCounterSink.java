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

package io.questdb.cairo;

import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;

/**
 * {@link SortedRowSink} that discards column values, counts delivered rows,
 * and records the first output row that violates timestamp monotonicity.
 * Intended for Phase B correctness tests and memory-bounds probes.
 * <p>
 * After {@link #onFinish}, call {@link #getRowCount} and
 * {@link #getFirstViolationRow}. A violation row of {@code -1} means the
 * stream was monotonically non-decreasing in ts.
 * <p>
 * This sink is stateful and single-use-per-{@link #onStart}. Each
 * {@link #onStart} resets all counters; calling {@link #acceptRow} without
 * a preceding {@link #onStart} yields undefined results.
 */
public class NoOpRowCounterSink implements SortedRowSink {

    private long expectedRows;
    private long firstViolationRow = -1;
    private boolean hasPrev;
    private long prevTs;
    private long rowCount;

    @Override
    public void acceptRow(RowGroupBuffers src, int row, long ts) {
        if (hasPrev && ts < prevTs && firstViolationRow < 0) {
            firstViolationRow = rowCount;
        }
        prevTs = ts;
        hasPrev = true;
        rowCount++;
    }

    @Override
    public void close() {
        // Stateful by design; nothing to release.
    }

    /**
     * @return the row index (0-based in the merged stream) of the first
     *         timestamp violation, or -1 if the stream was monotonic.
     */
    public long getFirstViolationRow() {
        return firstViolationRow;
    }

    public long getRowCount() {
        return rowCount;
    }

    @Override
    public void onFinish() {
        // no-op; counters are already up to date.
    }

    @Override
    public void onStart(PartitionDecoder.Metadata meta, int tsColumnIndex, long totalRows) {
        expectedRows = totalRows;
        firstViolationRow = -1;
        hasPrev = false;
        prevTs = 0;
        rowCount = 0;
    }

    /**
     * Compare {@link #getRowCount} against the {@code totalRows} the merger
     * declared in {@link #onStart}. Only meaningful once {@link #onFinish}
     * has been called; mid-stream values are indistinguishable from a
     * completed stream of matching length.
     *
     * @return {@code true} iff the two counters match.
     */
    public boolean rowCountMatchesExpected() {
        return rowCount == expectedRows;
    }
}
