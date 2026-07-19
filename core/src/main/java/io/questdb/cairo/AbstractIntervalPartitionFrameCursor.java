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

package io.questdb.cairo;

import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.std.Vect.BIN_SEARCH_SCAN_UP;

public abstract class AbstractIntervalPartitionFrameCursor implements PartitionFrameCursor {
    protected final IntervalPartitionFrame frame = new IntervalPartitionFrame();
    protected final RuntimeIntrinsicIntervalModel intervalModel;
    protected final ParquetPartitionDecoder parquetDecoder = new ParquetPartitionDecoder();
    protected final int timestampIndex;
    private final NativeTimestampFinder nativeTimestampFinder = new NativeTimestampFinder();
    private final ParquetTimestampFinder parquetTimestampFinder;
    protected LongList intervals;
    protected int intervalsHi;
    protected int intervalsLo;
    protected int partitionHi;
    // This is where begin binary search on partition. When there are more
    // than one searches to be performed we can use this variable to avoid
    // searching partition from top every time
    protected long partitionLimit;
    protected int partitionLo;
    protected TableReader reader;
    protected long sizeSoFar = 0;
    private int initialIntervalsHi;
    private int initialIntervalsLo;
    private int initialPartitionHi;
    private int initialPartitionLo;

    public AbstractIntervalPartitionFrameCursor(RuntimeIntrinsicIntervalModel intervalModel, int timestampIndex) {
        assert timestampIndex > -1;
        this.intervalModel = intervalModel;
        this.timestampIndex = timestampIndex;
        this.parquetTimestampFinder = new ParquetTimestampFinder(parquetDecoder);
    }

    @Override
    public void close() {
        reader = Misc.free(reader);
        Misc.free(parquetTimestampFinder);
        Misc.free(parquetDecoder);
        nativeTimestampFinder.clear();
    }

    @Override
    public SymbolMapReader getSymbolTable(int columnIndex) {
        return reader.getSymbolMapReader(columnIndex);
    }

    @Override
    public TableReader getTableReader() {
        return reader;
    }

    /**
     * Composite-partitioning (Task 6c review, Part A) LOUD scope boundary for the one interval-scan shape
     * these cursors cannot yet serve correctly: 2+ time intervals that each fall within (or reach into)
     * the SAME multi-cell day. Both concrete cursors walk partitions (cells) and intervals strictly
     * MONOTONICALLY, so once an earlier interval consumes a multi-cell day's cells a later interval can
     * never revisit that day's earlier cells -- their matching rows would be SILENTLY dropped. Commit
     * {@code d31aa88716} fixed the SINGLE-interval sibling visit; multiple sub-day intervals over one
     * multi-cell day remained broken. A real fix would have to iterate cells and intervals as a 2D grid
     * (per-cell interval reset) while still emitting frames in the day-contiguous, per-cell-contiguous
     * order the downstream {@code CompositeMergePartitionRecordCursor} requires -- too invasive to do
     * safely within this review without risking a subtly-wrong scan, so this shape is LOUD-GATED instead.
     * Each cursor throws this at the EXACT point it detects the drop is imminent: a fragmented cell of a
     * multi-cell day about to be abandoned for a sibling while a LATER interval still reaches into that
     * cell's own timestamp span (proven to fire on every actual drop, and never on a correct multi-DAY
     * date-list, single-interval, or non-interleaving scan). Never reachable for a plain table (its
     * neighbour partition is always a distinct day, so the same-timestamp sibling precondition is never
     * met) -- plain interval scans stay byte-identical. Workaround: issue one interval per query, or widen
     * the predicate to whole days.
     */
    protected CairoException multipleSubDayIntervalsOverMultiCellDayUnsupported() {
        return CairoException.critical(0)
                .put("composite partitioning does not yet support multiple sub-day time intervals over a single multi-cell day; ")
                .put("issue one interval per query, or widen the range to whole days [table=")
                .put(reader.getTableToken().getTableName())
                .put(']');
    }

    public int getTimestampIndex() {
        return timestampIndex;
    }

    @Override
    public boolean hasIntervalFilter() {
        return true;
    }

    @Override
    public StaticSymbolTable newSymbolTable(int columnIndex) {
        return reader.newSymbolTable(columnIndex);
    }

    public AbstractIntervalPartitionFrameCursor of(TableReader reader, SqlExecutionContext sqlExecutionContext) throws SqlException {
        this.intervals = intervalModel.calculateIntervals(sqlExecutionContext);
        calculateRanges(reader, intervals);
        this.reader = reader;
        return this;
    }

    @TestOnly
    @Override
    public boolean reload() {
        if (reader != null && reader.reload()) {
            calculateRanges(reader, intervals);
            return true;
        }
        return false;
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public boolean supportsSizeCalculation() {
        return true;
    }

    @Override
    public void toTop() {
        parquetTimestampFinder.clear();
        nativeTimestampFinder.clear();
        intervalsLo = initialIntervalsLo;
        intervalsHi = initialIntervalsHi;
        partitionLo = initialPartitionLo;
        partitionHi = initialPartitionHi;
        sizeSoFar = 0;
    }

    private void calculateRanges(TableReader reader, LongList intervals) {
        if (intervals.size() > 0) {
            if (PartitionBy.isPartitioned(reader.getPartitionedBy())) {
                cullIntervals(reader, intervals);
                if (initialIntervalsLo < initialIntervalsHi) {
                    cullPartitions(reader, intervals);
                }
            } else {
                initialIntervalsLo = 0;
                initialIntervalsHi = intervals.size() / 2;
                initialPartitionLo = 0;
                initialPartitionHi = reader.getPartitionCount();
            }
        } else {
            initialIntervalsLo = 0;
            initialIntervalsHi = 0;
            initialPartitionLo = 0;
            initialPartitionHi = 0;
        }
        toTop();
    }

    private void cullIntervals(TableReader reader, LongList intervals) {
        int intervalsLo = intervals.binarySearch(reader.getMinTimestamp(), BIN_SEARCH_SCAN_UP);

        // not a direct hit
        if (intervalsLo < 0) {
            intervalsLo = -intervalsLo - 1;
        }

        // normalise interval index
        this.initialIntervalsLo = intervalsLo / 2;

        if (reader.getMaxTimestamp() == intervals.getQuick(intervals.size() - 1)) {
            this.initialIntervalsHi = intervals.size() / 2;
        } else if (reader.getMaxTimestamp() == intervals.getQuick(0)) {
            this.initialIntervalsHi = 1;
        } else {
            int intervalsHi = intervals.binarySearch(reader.getMaxTimestamp(), BIN_SEARCH_SCAN_UP);
            if (intervalsHi < 0) { // negative value means inexact match
                intervalsHi = -intervalsHi - 1;

                // when interval index is "even" we scored just between two interval
                // in which case we chose previous interval
                if (intervalsHi % 2 == 0) {
                    this.initialIntervalsHi = intervalsHi / 2;
                } else {
                    this.initialIntervalsHi = intervalsHi / 2 + 1;
                }
            } else { // positive value means exact match
                this.initialIntervalsHi = intervalsHi / 2 + 1;
            }
        }
    }

    private void cullPartitions(TableReader reader, LongList intervals) {
        final long lo = intervals.getQuick(initialIntervalsLo * 2);
        long intervalLo;
        if (lo == Long.MIN_VALUE) {
            intervalLo = reader.floorToPartitionTimestamp(reader.getMinTimestamp());
        } else {
            intervalLo = reader.floorToPartitionTimestamp(lo);
        }
        this.initialPartitionLo = reader.getMinTimestamp() < intervalLo ? reader.getPartitionIndexByTimestamp(intervalLo) : 0;
        long intervalHi = reader.floorToPartitionTimestamp(intervals.getQuick((initialIntervalsHi - 1) * 2 + 1));
        // High boundary must resolve to the LAST (highest cellKey) partition sharing intervalHi's
        // timestamp, not the first -- a composite table's multi-cell day would otherwise have its
        // cellKey >= 1 siblings excluded by the "+1" below. getPartitionIndexByTimestampScanDown is
        // byte-identical to getPartitionIndexByTimestamp for a plain table (one cell/day) and for any
        // not-found (between-days) boundary -- see TableReader#getPartitionIndexByTimestampScanDown's
        // own javadoc.
        this.initialPartitionHi = Math.min(reader.getPartitionCount(), reader.getPartitionIndexByTimestampScanDown(intervalHi) + 1);
    }

    protected TimestampFinder initTimestampFinder(int partitionIndex, long rowCount) {
        if (reader.getPartitionFormatFromMetadata(partitionIndex) == PartitionFormat.PARQUET) {
            return parquetTimestampFinder.of(reader, partitionIndex, timestampIndex);
        }
        return nativeTimestampFinder.of(reader, partitionIndex, timestampIndex, rowCount);
    }

    protected static class IntervalPartitionFrame implements PartitionFrame {
        protected byte format;
        protected ParquetPartitionDecoder parquetMetaDecoder;
        protected int partitionIndex;
        protected long rowHi;
        protected long rowLo;

        @Override
        public ParquetPartitionDecoder getParquetMetaDecoder() {
            return parquetMetaDecoder;
        }

        @Override
        public byte getPartitionFormat() {
            return format;
        }

        @Override
        public int getPartitionIndex() {
            return partitionIndex;
        }

        @Override
        public long getRowHi() {
            return rowHi;
        }

        @Override
        public long getRowLo() {
            return rowLo;
        }
    }
}
