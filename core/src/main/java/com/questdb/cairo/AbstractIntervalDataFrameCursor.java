/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.cairo.sql.DataFrame;
import com.questdb.cairo.sql.DataFrameCursor;
import com.questdb.common.PartitionBy;
import com.questdb.common.SymbolTable;
import com.questdb.std.LongList;

public abstract class AbstractIntervalDataFrameCursor implements DataFrameCursor {
    protected final LongList intervals;
    protected final IntervalDataFrame dataFrame = new IntervalDataFrame();
    protected int timestampIndex;
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

    public AbstractIntervalDataFrameCursor(LongList intervals) {
        this.intervals = intervals;
    }

    @Override
    public void close() {
        if (reader != null) {
            reader.close();
            reader = null;
        }
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

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return reader.getSymbolMapReader(columnIndex);
    }

    @Override
    public DataFrame next() {
        return dataFrame;
    }

    public void of(TableReader reader) {
        this.timestampIndex = reader.getMetadata().getTimestampIndex();
        if (this.timestampIndex == -1) {
            throw CairoException.instance(0).put("table '").put(reader.getTableName()).put("' has no timestamp");
        }
        this.reader = reader;
        calculateRanges();
    }

    protected static long search(ReadOnlyColumn column, long value, long low, long high) {
        while (low < high) {
            long mid = (low + high - 1) >>> 1;
            long midVal = column.getLong(mid * 8);

            if (midVal < value)
                low = mid + 1;
            else if (midVal > value)
                high = mid;
            else
                return mid;
        }
        return -(low + 1);
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
        int intervalsLo = intervals.binarySearch(reader.getPartitionMin());

        // not a direct hit
        if (intervalsLo < 0) {
            intervalsLo = -intervalsLo - 1;
        }

        // normalise interval index
        this.initialIntervalsLo = intervalsLo / 2;

        int intervalsHi = intervals.binarySearch(reader.getMaxTimestamp());
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
        long intervalLo = reader.floorToPartitionTimestamp(intervals.getQuick(initialIntervalsLo * 2));
        this.initialPartitionLo = reader.getPartitionCountBetweenTimestamps(reader.getPartitionMin(), intervalLo);
        long intervalHi = reader.floorToPartitionTimestamp(intervals.getQuick((initialIntervalsHi - 1) * 2 + 1));
        this.initialPartitionHi = Math.min(reader.getPartitionCount(), reader.getPartitionCountBetweenTimestamps(reader.getPartitionMin(), intervalHi) + 1);
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
    }
}
