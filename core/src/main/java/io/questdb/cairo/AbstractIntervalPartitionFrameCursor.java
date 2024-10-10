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

package io.questdb.cairo;

import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupStatBuffers;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.std.DirectIntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Vect;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.std.Vect.BIN_SEARCH_SCAN_DOWN;
import static io.questdb.std.Vect.BIN_SEARCH_SCAN_UP;

public abstract class AbstractIntervalPartitionFrameCursor implements PartitionFrameCursor {
    protected final RuntimeIntrinsicIntervalModel intervalModel;
    protected final IntervalPartitionFrame partitionFrame = new IntervalPartitionFrame();
    protected final int timestampIndex;
    private final NativeTimestampFinder nativeTimestampFinder = new NativeTimestampFinder();
    private final ParquetTimestampFinder parquetTimestampFinder = new ParquetTimestampFinder();
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
    protected long size = -1;
    protected long sizeSoFar = 0;
    private int initialIntervalsHi;
    private int initialIntervalsLo;
    private int initialPartitionHi;
    private int initialPartitionLo;

    public AbstractIntervalPartitionFrameCursor(RuntimeIntrinsicIntervalModel intervalModel, int timestampIndex) {
        assert timestampIndex > -1;
        this.intervalModel = intervalModel;
        this.timestampIndex = timestampIndex;
    }

    public static long binarySearch(MemoryR column, long value, long low, long high, int scanDir) {
        return Vect.binarySearch64Bit(column.getPageAddress(0), value, low, high, scanDir);
    }

    @Override
    public void close() {
        reader = Misc.free(reader);
        Misc.free(parquetTimestampFinder);
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

    public int getTimestampIndex() {
        return timestampIndex;
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
        return size > -1 ? size : computeSize();
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
        size = -1;
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

    /**
     * Best effort size calculation.
     */
    private long computeSize() {
        int intervalsLo = this.intervalsLo;
        int intervalsHi = this.intervalsHi;
        int partitionLo = this.partitionLo;
        int partitionHi = this.partitionHi;
        long partitionLimit = this.partitionLimit;
        long size = this.sizeSoFar;

        while (intervalsLo < intervalsHi && partitionLo < partitionHi) {
            // We don't need to worry about column tops and null column because we
            // are working with timestamp. Timestamp column cannot be added to existing table.
            long rowCount;
            try {
                rowCount = reader.openPartition(partitionLo);
            } catch (DataUnavailableException e) {
                // The data is in cold storage, close the event and give up on size calculation.
                Misc.free(e.getEvent());
                return -1;
            }

            if (rowCount > 0) {
                final TimestampFinder timestampFinder = initTimestampFinder(partitionLo, rowCount);

                final long intervalLo = intervals.getQuick(intervalsLo * 2);
                final long intervalHi = intervals.getQuick(intervalsLo * 2 + 1);

                final long partitionTimestampLo = timestampFinder.minTimestamp();
                // interval is wholly above partition, skip interval
                if (partitionTimestampLo > intervalHi) {
                    intervalsLo++;
                    continue;
                }

                final long partitionTimestampHi = timestampFinder.maxTimestamp();
                // interval is wholly below partition, skip partition
                if (partitionTimestampHi < intervalLo) {
                    partitionLimit = 0;
                    partitionLo++;
                    continue;
                }

                // calculate intersection
                long lo;
                if (partitionTimestampLo >= intervalLo) {
                    lo = 0;
                } else {
                    lo = timestampFinder.findTimestamp(intervalLo, partitionLimit == -1 ? 0 : partitionLimit, rowCount, BIN_SEARCH_SCAN_UP);
                }

                // Interval is inclusive of edges, and we have to bump to high bound because it is non-inclusive.
                long hi = timestampFinder.findTimestamp(intervalHi, lo, rowCount - 1, BIN_SEARCH_SCAN_DOWN) + 1;
                if (lo < hi) {
                    size += (hi - lo);

                    // we do have whole partition of fragment?
                    if (hi == rowCount) {
                        // whole partition, will need to skip to next one
                        partitionLimit = 0;
                        partitionLo++;
                    } else {
                        // only fragment, need to skip to next interval
                        partitionLimit = hi;
                        intervalsLo++;
                    }
                    continue;
                }
                // interval yielded empty partition frame
                partitionLimit = hi;
                intervalsLo++;
            } else {
                // partition was empty, just skip to next
                partitionLo++;
            }
        }

        return this.size = size;
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
        this.initialPartitionHi = Math.min(reader.getPartitionCount(), reader.getPartitionIndexByTimestamp(intervalHi) + 1);
    }

    protected TimestampFinder initTimestampFinder(int partitionIndex, long rowCount) {
        if (reader.getPartitionFormat(partitionIndex) == PartitionFormat.PARQUET) {
            return parquetTimestampFinder.of(reader, partitionIndex, timestampIndex, rowCount);
        }
        return nativeTimestampFinder.of(reader, partitionIndex, timestampIndex, rowCount);
    }

    protected interface TimestampFinder {

        /**
         * Performs search on timestamp column.
         *
         * @param value   timestamp value to find, the found value is equal or less than this value
         * @param rowLo   row low index for the search boundary, inclusive
         * @param rowHi   row high index for the search boundary, inclusive
         * @param scanDir scan direction {@link Vect#BIN_SEARCH_SCAN_DOWN} (1) or {@link Vect#BIN_SEARCH_SCAN_UP} (-1)
         * @return TODO(puzpuzpuz): document me
         */
        long findTimestamp(long value, long rowLo, long rowHi, int scanDir);

        long maxTimestamp();

        long minTimestamp();

        long timestampAt(long rowIndex);
    }

    protected static class IntervalPartitionFrame implements PartitionFrame {
        protected byte format;
        protected long parquetFd;
        protected int partitionIndex;
        protected int rowGroupIndex;
        // we don't need rowGroupLo as it can be calculated as rowGroupLo+(rowHi-rowLo)
        protected int rowGroupLo;
        protected long rowHi;
        protected long rowLo;

        @Override
        public long getParquetFd() {
            return parquetFd;
        }

        @Override
        public int getParquetRowGroup() {
            return rowGroupIndex;
        }

        @Override
        public int getParquetRowGroupLo() {
            return rowGroupLo;
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

    private static class NativeTimestampFinder implements TimestampFinder, Mutable {
        private MemoryR column;
        private long rowCount;

        @Override
        public void clear() {
            column = null;
            rowCount = 0;
        }

        @Override
        public long findTimestamp(long value, long rowLo, long rowHi, int scanDir) {
            long idx = binarySearch(column, value, rowLo, rowHi, scanDir);
            if (idx < 0) {
                if (scanDir == BIN_SEARCH_SCAN_UP) {
                    idx = -idx - 1;
                } else if (scanDir == BIN_SEARCH_SCAN_DOWN) {
                    idx = -idx - 2;
                }
            }
            return idx;
        }

        @Override
        public long maxTimestamp() {
            return column.getLong((rowCount - 1) * 8);
        }

        @Override
        public long minTimestamp() {
            return column.getLong(0);
        }

        public NativeTimestampFinder of(TableReader reader, int partitionIndex, int timestampIndex, long rowCount) {
            this.column = reader.getColumn(TableReader.getPrimaryColumnIndex(reader.getColumnBase(partitionIndex), timestampIndex));
            this.rowCount = rowCount;
            return this;
        }

        @Override
        public long timestampAt(long rowIndex) {
            return column.getLong(rowIndex * 8);
        }
    }

    private static class ParquetTimestampFinder implements TimestampFinder, Mutable, QuietCloseable {
        private final PartitionDecoder partitionDecoder = new PartitionDecoder();
        private final RowGroupStatBuffers statBuffers = new RowGroupStatBuffers();
        private final DirectIntList timestampIdAndType = new DirectIntList(2, MemoryTag.NATIVE_DEFAULT);
        private long maxTimestamp;
        private long minTimestamp;
        private int timestampIndex = -1; // timestamp column index in Parquet file

        @Override
        public void clear() {
            timestampIndex = -1;
            minTimestamp = 0;
            maxTimestamp = 0;
        }

        @Override
        public void close() {
            Misc.free(partitionDecoder);
            Misc.free(statBuffers);
            Misc.free(timestampIdAndType);
            clear();
        }

        @Override
        public long findTimestamp(long value, long rowLo, long rowHi, int scanDir) {
            // TODO(puzpuzpuz): implement me
            return 0;
        }

        @Override
        public long maxTimestamp() {
            return maxTimestamp;
        }

        @Override
        public long minTimestamp() {
            return minTimestamp;
        }

        public ParquetTimestampFinder of(TableReader reader, int partitionIndex, int timestampIndex, long rowCount) {
            // TODO(puzpuzpuz): also use parquet metadata offset
            partitionDecoder.of(reader.getParquetFd(partitionIndex));
            this.timestampIndex = findTimestampIndex(partitionDecoder, timestampIndex);
            if (this.timestampIndex == -1) {
                throw CairoException.critical(0).put("missing timestamp column in parquet partition [table=").put(reader.getTableToken())
                        .put(", partitionIndex=").put(partitionIndex)
                        .put(", timestampIndex=").put(timestampIndex)
                        .put(']');
            }
            readMinMaxTimestamps();
            return this;
        }

        @Override
        public long timestampAt(long rowIndex) {
            return partitionDecoder.timestampAt(timestampIndex, rowIndex);
        }

        private static int findTimestampIndex(PartitionDecoder partitionDecoder, int timestampIndex) {
            final PartitionDecoder.Metadata metadata = partitionDecoder.metadata();
            for (int i = 0, n = metadata.columnCount(); i < n; i++) {
                if (metadata.columnId(i) == timestampIndex) {
                    return i;
                }
            }
            return -1;
        }

        private void readMinMaxTimestamps() {
            timestampIdAndType.reopen();
            timestampIdAndType.clear();
            timestampIdAndType.add(timestampIndex);
            timestampIdAndType.add(ColumnType.TIMESTAMP);

            final int rowGroupCount = partitionDecoder.metadata().rowGroupCount();
            partitionDecoder.readRowGroupStats(statBuffers, timestampIdAndType, 0);
            minTimestamp = statBuffers.getMinValueLong(0);
            partitionDecoder.readRowGroupStats(statBuffers, timestampIdAndType, rowGroupCount - 1);
            maxTimestamp = statBuffers.getMaxValueLong(0);
        }
    }
}
