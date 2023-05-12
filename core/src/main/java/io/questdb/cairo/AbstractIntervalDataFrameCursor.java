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

package io.questdb.cairo;

import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.DataFrameCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Vect;
import org.jetbrains.annotations.TestOnly;

public abstract class AbstractIntervalDataFrameCursor implements DataFrameCursor {
    public static final int SCAN_DOWN = 1;
    public static final int SCAN_UP = -1;

    protected final IntervalDataFrame dataFrame = new IntervalDataFrame();
    protected final RuntimeIntrinsicIntervalModel intervalsModel;
    protected final int timestampIndex;
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

    public AbstractIntervalDataFrameCursor(RuntimeIntrinsicIntervalModel intervals, int timestampIndex) {
        assert timestampIndex > -1;
        this.intervalsModel = intervals;
        this.timestampIndex = timestampIndex;
    }

    public static long binarySearch(MemoryR column, long value, long low, long high, int scanDir) {
        return Vect.binarySearch64Bit(column.getPageAddress(0), value, low, high, scanDir);
    }

    @Override
    public void close() {
        reader = Misc.free(reader);
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

    public AbstractIntervalDataFrameCursor of(TableReader reader, SqlExecutionContext sqlContext) throws SqlException {
        this.reader = reader;
        this.intervals = this.intervalsModel.calculateIntervals(sqlContext);
        calculateRanges(intervals);
        return this;
    }

    @TestOnly
    @Override
    public boolean reload() {
        if (reader != null && reader.reload()) {
            calculateRanges(intervals);
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
        intervalsLo = initialIntervalsLo;
        intervalsHi = initialIntervalsHi;
        partitionLo = initialPartitionLo;
        partitionHi = initialPartitionHi;
        sizeSoFar = 0;
    }

    private void calculateRanges(LongList intervals) {
        size = -1;
        if (intervals.size() > 0) {
            if (PartitionBy.isPartitioned(reader.getPartitionedBy())) {
                cullIntervals(intervals);
                if (initialIntervalsLo < initialIntervalsHi) {
                    cullPartitions(intervals);
                }
            } else {
                initialIntervalsLo = 0;
                initialIntervalsHi = intervals.size() / 2;
                initialPartitionLo = 0;
                initialPartitionHi = reader.getPartitionCount();
            }
            toTop();
        }
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

                final MemoryR column = reader.getColumn(TableReader.getPrimaryColumnIndex(reader.getColumnBase(partitionLo), timestampIndex));
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
                if (partitionTimestampLo >= intervalLo) {
                    lo = 0;
                } else {
                    lo = binarySearch(column, intervalLo, partitionLimit == -1 ? 0 : partitionLimit, rowCount, SCAN_UP);
                    if (lo < 0) {
                        lo = -lo - 1;
                    }
                }

                long hi = binarySearch(column, intervalHi, lo, rowCount - 1, SCAN_DOWN);

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
        int intervalsLo = intervals.binarySearch(reader.getMinTimestamp(), BinarySearch.SCAN_UP);

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
            int intervalsHi = intervals.binarySearch(reader.getMaxTimestamp(), BinarySearch.SCAN_UP);
            if (intervalsHi < 0) {//negative value means inexact match 
                intervalsHi = -intervalsHi - 1;

                // when interval index is "even" we scored just between two interval
                // in which case we chose previous interval
                if (intervalsHi % 2 == 0) {
                    this.initialIntervalsHi = intervalsHi / 2;
                } else {
                    this.initialIntervalsHi = intervalsHi / 2 + 1;
                }
            } else {//positive value means exact match
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

        protected int partitionIndex;
        protected long rowHi;
        protected long rowLo = 0;

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
