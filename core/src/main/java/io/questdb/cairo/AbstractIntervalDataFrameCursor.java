/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.DataFrameCursor;
import io.questdb.cairo.vm.ReadOnlyVirtualMemory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.std.LongList;

public abstract class AbstractIntervalDataFrameCursor implements DataFrameCursor {
    static final int SCAN_UP = -1;
    static final int SCAN_DOWN = 1;
    protected final RuntimeIntrinsicIntervalModel intervalsModel;
    protected LongList intervals;
    protected final IntervalDataFrame dataFrame = new IntervalDataFrame();
    protected final int timestampIndex;
    protected TableReader reader;
    protected int intervalsLo;
    protected int intervalsHi;
    protected int partitionLo;
    protected int partitionHi;
    // This is where begin binary search on partition. When there are more
    // than one searches to be performed we can use this variable to avoid
    // searching partition from top every time
    protected long partitionLimit;
    protected long sizeSoFar = 0;
    protected long size = -1;
    private int initialIntervalsLo;
    private int initialIntervalsHi;
    private int initialPartitionLo;
    private int initialPartitionHi;

    public AbstractIntervalDataFrameCursor(RuntimeIntrinsicIntervalModel intervals, int timestampIndex) {
        assert timestampIndex > -1;
        this.intervalsModel = intervals;
        this.timestampIndex = timestampIndex;
    }

    @Override
    public TableReader getTableReader() {
        return reader;
    }

    @Override
    public boolean reload() {
        if (reader != null && reader.reload()) {
            calculateRanges(intervals);
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    @Override
    public void toTop() {
        intervalsLo = initialIntervalsLo;
        intervalsHi = initialIntervalsHi;
        partitionLo = initialPartitionLo;
        partitionHi = initialPartitionHi;
        sizeSoFar = 0;
    }

    @Override
    public long size() {
        return size > -1 ? size : computeSize();
    }

    @Override
    public SymbolMapReader getSymbolTable(int columnIndex) {
        return reader.getSymbolMapReader(columnIndex);
    }

    public void of(TableReader reader, SqlExecutionContext sqlContext) throws SqlException {
        this.reader = reader;
        this.intervals = this.intervalsModel.calculateIntervals(sqlContext);
        calculateRanges(intervals);
    }

    protected static long search(ReadOnlyVirtualMemory column, long value, long low, long high, int increment) {
        while (low < high) {
            long mid = (low + high - 1) >>> 1;
            long midVal = column.getLong(mid * 8);

            if (midVal < value)
                low = mid + 1;
            else if (midVal > value)
                high = mid;
            else {
                // In case of multiple equal values, find the first
                mid += increment;
                while (mid > 0 && mid < high && midVal == column.getLong(mid * 8)) {
                    mid += increment;
                }
                return mid - increment;
            }
        }
        return -(low + 1);
    }

    private void calculateRanges(LongList intervals) {
        size = -1;
        if (intervals.size() > 0) {
            if (reader.getPartitionedBy() == PartitionBy.NONE) {
                initialIntervalsLo = 0;
                initialIntervalsHi = intervals.size() / 2;
                initialPartitionLo = 0;
                initialPartitionHi = reader.getPartitionCount();
            } else {
                cullIntervals(intervals);
                if (initialIntervalsLo < initialIntervalsHi) {
                    cullPartitions(intervals);
                }
            }
            toTop();
        }
    }

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
            long rowCount = reader.openPartition(partitionLo);
            if (rowCount > 0) {

                final ReadOnlyVirtualMemory column = reader.getColumn(TableReader.getPrimaryColumnIndex(reader.getColumnBase(partitionLo), timestampIndex));
                final long intervalLo = intervals.getQuick(intervalsLo * 2);
                final long intervalHi = intervals.getQuick(intervalsLo * 2 + 1);


                final long partitionTimestampLo = column.getLong(0);
                // interval is wholly above partition, skip interval
                if (partitionTimestampLo > intervalHi) {
                    intervalsLo++;
                    continue;
                }

                final long partitionTimestampHi = column.getLong((rowCount - 1) * 8);
                // interval is wholly below partition, skip partition
                if (partitionTimestampHi < intervalLo) {
                    partitionLimit = 0;
                    partitionLo++;
                    continue;
                }

                // calculate intersection

                long lo;
                if (partitionTimestampLo == intervalLo) {
                    lo = 0;
                } else {
                    lo = search(column, intervalLo, partitionLimit, rowCount, AbstractIntervalDataFrameCursor.SCAN_UP);
                    if (lo < 0) {
                        lo = -lo - 1;
                    }
                }

                long hi = search(column, intervalHi, lo, rowCount, AbstractIntervalDataFrameCursor.SCAN_DOWN);

                if (hi < 0) {
                    hi = -hi - 1;
                } else {
                    // We have direct hit. Interval is inclusive of edges and we have to
                    // bump to high bound because it is non-inclusive
                    hi++;
                }

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
                // interval yielded empty data frame
                partitionLimit = hi;
                intervalsLo++;
            } else {
                // partition was empty, just skip to next
                partitionLo++;
            }
        }

        return this.size = size;
    }

    private void cullIntervals(LongList intervals) {
        int intervalsLo = intervals.binarySearch(reader.getMinTimestamp());

        // not a direct hit
        if (intervalsLo < 0) {
            intervalsLo = -intervalsLo - 1;
        }

        // normalise interval index
        this.initialIntervalsLo = intervalsLo / 2;

        int intervalsHi = intervals.binarySearch(reader.getMaxTimestamp());
        if (reader.getMaxTimestamp() == intervals.getQuick(intervals.size() - 1)) {
            this.initialIntervalsHi = intervals.size() - 1;
        }
        if (reader.getMaxTimestamp() == intervals.getQuick(0)) {
            this.initialIntervalsHi = 1;
        }
        if (intervalsHi < 0) {
            intervalsHi = -intervalsHi - 1;
            // when interval index is "even" we scored just between two interval
            // in which case we chose previous interval
            if (intervalsHi % 2 == 0) {
                this.initialIntervalsHi = intervalsHi / 2;
            } else {
                this.initialIntervalsHi = intervalsHi / 2 + 1;
            }
        }
    }

    private void cullPartitions(LongList intervals) {
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

    protected class IntervalDataFrame implements DataFrame {

        protected long rowLo = 0;
        protected long rowHi;
        protected int partitionIndex;

        @Override
        public BitmapIndexReader getBitmapIndexReader(int columnIndex, int direction) {
            return reader.getBitmapIndexReader(partitionIndex, columnIndex, direction);
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
