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

public class IntervalFrameCursor implements DataFrameCursor {
    private final LongList intervals;
    private final int timestampIndex;
    private final IntervalDataFrame dataFrame = new IntervalDataFrame();
    private TableReader reader;
    private int initialIntervalsLo = -1;
    private int intervalsLo = -1;
    private int intervalsHi = -1;
    private int initialPartitionLo;
    private int partitionLo;
    private int partitionHi;
    // This is where begin binary search on partition. When there are more
    // than one searches to be performed we can use this variable to avoid
    // searching partition from top every time
    private long partitionSearchTop;

    /**
     * Cursor for data frames that chronologically intersect collection of intervals.
     * Data frame low and high row will be within intervals inclusive of edges. Intervals
     * themselves are pairs of microsecond time.
     *
     * @param intervals pairs of microsecond interval values, as in "low" and "high" inclusive of
     *                  edges.
     */
    public IntervalFrameCursor(LongList intervals, int timestampIndex) {
        this.intervals = intervals;
        this.timestampIndex = timestampIndex;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return reader.getSymbolMapReader(columnIndex);
    }

    @Override
    public boolean hasNext() {
        // order of logical operations is important
        // we are not calculating partition rages when intervals are empty
        while (intervalsLo < intervalsHi && partitionLo < partitionHi) {
            // We don't need to worry about column tops and null column because we
            // are working with timestamp. Timestamp column cannot be added to existing table.
            long rowCount = reader.openPartition(partitionLo);
            if (rowCount > 0) {

                ReadOnlyColumn column = reader.getColumn(TableReader.getPrimaryColumnIndex(reader.getColumnBase(partitionLo), timestampIndex));
                final long intervalLo = intervals.getQuick(intervalsLo * 2);
                final long intervalHi = intervals.getQuick(intervalsLo * 2 + 1);


                // interval is wholly above partition, skip interval
                if (column.getLong(0) > intervalHi) {
                    intervalsLo++;
                    continue;
                }

                // interval is wholly below partition, skip partition
                if (column.getLong((rowCount - 1) * 8) < intervalLo) {
                    partitionSearchTop = 0;
                    partitionLo++;
                    continue;
                }

                // calculate intersection

                long lo = search(column, intervalLo, partitionSearchTop, rowCount);
                if (lo < 0) {
                    lo = -lo - 1;
                }

                long hi = search(column, intervalHi, lo, rowCount);

                if (hi < 0) {
                    hi = -hi - 1;
                } else {
                    // We have direct hit. Interval is inclusive of edges and we have to
                    // bump to high bound because it is non-inclusive
                    hi++;
                }

                if (lo < hi) {
                    dataFrame.partitionIndex = partitionLo;
                    dataFrame.rowLo = lo;
                    dataFrame.rowHi = hi;

                    // we do have whole partition of fragment?
                    if (hi == rowCount) {
                        // whole partition, will need to skip to next one
                        partitionSearchTop = 0;
                        partitionLo++;
                    } else {
                        // only fragment, need to skip to next interval
                        partitionSearchTop = hi;
                        intervalsLo++;
                    }

                    return true;
                }
                // interval yielded empty data frame
                partitionSearchTop = hi;
                intervalsLo++;
            } else {
                // partition was empty, just skip to next
                partitionLo++;
            }
        }
        return false;
    }

    @Override
    public DataFrame next() {
        return dataFrame;
    }

    public void of(TableReader reader) {
        this.reader = reader;
        calculateRanges();
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
    public void close() {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    @Override
    public TableReader getReader() {
        return reader;
    }

    @Override
    public void toTop() {
        intervalsLo = initialIntervalsLo;
        partitionLo = initialPartitionLo;
        partitionSearchTop = 0;
    }

    private static long search(ReadOnlyColumn column, long value, long low, long high) {
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
                intervalsHi = intervals.size();
                initialPartitionLo = 0;
                partitionHi = reader.getPartitionCount();
            } else {
                cullIntervals();
                if (initialIntervalsLo < intervalsHi) {
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
                this.intervalsHi = intervalsHi / 2;
            } else {
                this.intervalsHi = intervalsHi / 2 + 1;
            }
        }
    }

    private void cullPartitions() {
        long intervalLo = reader.floorToPartitionTimestamp(intervals.getQuick(initialIntervalsLo * 2));
        this.initialPartitionLo = reader.getPartitionCountBetweenTimestamps(reader.getPartitionMin(), intervalLo);
        long intervalHi = reader.floorToPartitionTimestamp(intervals.getQuick((intervalsHi - 1) * 2 + 1));
        partitionHi = Math.min(reader.getPartitionCount(), reader.getPartitionCountBetweenTimestamps(reader.getPartitionMin(), intervalHi) + 1);
    }

    private class IntervalDataFrame implements DataFrame {

        private long rowLo = 0;
        private long rowHi;
        private int partitionIndex;

        @Override
        public BitmapIndexReader getBitmapIndexReader(int columnIndex) {
            return reader.getBitmapIndexReader(reader.getColumnBase(partitionIndex), columnIndex);
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
