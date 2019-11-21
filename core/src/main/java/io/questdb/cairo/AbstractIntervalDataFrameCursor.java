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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.LongList;

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

    @Override
    public void close() {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
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

    @Override
    public DataFrame next() {
        return dataFrame;
    }

    public void of(TableReader reader, int timestampIndex) {
        this.timestampIndex = timestampIndex;
        if (this.timestampIndex == -1) {
            throw CairoException.instance(0).put("table '").put(reader.getTableName()).put("' has no timestamp");
        }
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
        this.initialPartitionLo = reader.getPartitionCountBetweenTimestamps(reader.getMinTimestamp(), intervalLo);
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
    }
}
