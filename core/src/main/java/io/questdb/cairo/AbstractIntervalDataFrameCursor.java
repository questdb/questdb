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
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.std.LongList;
import io.questdb.std.Transient;

public abstract class AbstractIntervalDataFrameCursor implements DataFrameCursor {
    protected final LongList intervals;
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
    private int initialIntervalsLo;
    private int initialIntervalsHi;
    private int initialPartitionLo;
    private int initialPartitionHi;

    static final int SCAN_UP = -1;
    static final int SCAN_DOWN = 1;

    public AbstractIntervalDataFrameCursor(@Transient LongList intervals, int timestampIndex) {
        assert timestampIndex > -1;
        this.intervals = new LongList(intervals);
        this.timestampIndex = timestampIndex;
    }

    protected static long search(ReadOnlyColumn column, long value, long low, long high, int increment) {
        while (low < high) {
            long mid = (low + high - 1) >>> 1;
            long midVal = column.getLong(mid * 8);

            if (midVal < value)
                low = mid + 1;
            else if (midVal > value)
                high = mid;
            else {
                // In case of multiple equal values, find the first
                long rowIndex = mid + increment;
                while (rowIndex > 0 && rowIndex < high && midVal == column.getLong(rowIndex * 8)) {
                    rowIndex += increment;
                }
                return rowIndex - increment;
            }
        }
        return -(low + 1);
    }

    @Override
    public void close() {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    @Override
    public StaticSymbolTable getSymbolTable(int columnIndex) {
        return reader.getSymbolMapReader(columnIndex);
    }

    @Override
    public TableReader getTableReader() {
        return reader;
    }

    @Override
    public boolean reload() {
        if (reader != null && reader.reload()) {
            calculateRanges();
            return true;
        }
        return false;
    }

    @Override
    public void toTop() {
        intervalsLo = initialIntervalsLo;
        intervalsHi = initialIntervalsHi;
        partitionLo = initialPartitionLo;
        partitionHi = initialPartitionHi;
    }

    public void of(TableReader reader) {
        this.reader = reader;
        calculateRanges();
    }

    private void calculateRanges() {
        if (intervals.size() > 0) {
            if (reader.getPartitionedBy() == PartitionBy.NONE) {
                initialIntervalsLo = 0;
                initialIntervalsHi = intervals.size() / 2;
                initialPartitionLo = 0;
                initialPartitionHi = reader.getPartitionCount();
            } else {
                cullIntervals();
                if (initialIntervalsLo < initialIntervalsHi) {
                    cullPartitions();
                }
            }
            toTop();
        }
    }

    private void cullIntervals() {
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

    private void cullPartitions() {
        final long lo = intervals.getQuick(initialIntervalsLo * 2);
        long intervalLo;
        if (lo == Long.MIN_VALUE) {
            intervalLo = reader.floorToPartitionTimestamp(reader.getMinTimestamp());
        } else {
            intervalLo = reader.floorToPartitionTimestamp(lo);
        }
        this.initialPartitionLo = reader.getMinTimestamp() < intervalLo ? reader.getPartitionCountBetweenTimestamps(reader.getMinTimestamp(), intervalLo) : 0;
        long intervalHi = reader.floorToPartitionTimestamp(intervals.getQuick((initialIntervalsHi - 1) * 2 + 1));
        this.initialPartitionHi = Math.min(reader.getPartitionCount(), reader.getPartitionCountBetweenTimestamps(reader.getMinTimestamp(), intervalHi) + 1);
    }

    protected class IntervalDataFrame implements DataFrame {

        protected long rowLo = 0;
        protected long rowHi;
        protected int partitionIndex;

        @Override
        public BitmapIndexReader getBitmapIndexReader(int columnIndex, int direction) {
            return reader.getBitmapIndexReader(reader.getColumnBase(partitionIndex), columnIndex, direction);
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

        @Override
        public long getPageAddress(int columnIndex) {
            return reader.getPageAddressAt(partitionIndex, rowLo, columnIndex);
        }

        @Override
        public long getPageValueCount(int columnIndex) {
            return reader.getPageValueCount(partitionIndex, rowLo, rowHi, columnIndex);
        }
    }
}
