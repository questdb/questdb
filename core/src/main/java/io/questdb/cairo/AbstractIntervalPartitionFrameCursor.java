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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.std.Vect.BIN_SEARCH_SCAN_UP;

public abstract class AbstractIntervalPartitionFrameCursor implements PartitionFrameCursor {
    protected final IntervalPartitionFrame frame = new IntervalPartitionFrame();
    protected final RuntimeIntrinsicIntervalModel intervalModel;
    protected final PartitionDecoder parquetDecoder = new PartitionDecoder();
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
    public void calculateSize(RecordCursor.Counter counter) {
        int intervalsLo1 = this.intervalsLo;
        int intervalsHi1 = this.intervalsHi;
        int partitionLo1 = this.partitionLo;
        int partitionHi1 = this.partitionHi;
        long partitionLimit1 = this.partitionLimit;
        long size = this.sizeSoFar;

        while (intervalsLo1 < intervalsHi1 && partitionLo1 < partitionHi1) {
            // We don't need to worry about column tops and null column because we
            // are working with timestamp. Timestamp column cannot be added to existing table.
            long rowCount;
            try {
                rowCount = reader.getPartitionRowCountFromMetadata(partitionLo1);
            } catch (DataUnavailableException e) {
                // The data is in cold storage, close the event and give up on size calculation.
                Misc.free(e.getEvent());
                return;
            }

            if (rowCount > 0) {
                final TimestampFinder timestampFinder = initTimestampFinder(partitionLo1, rowCount);

                final long intervalLo = intervals.getQuick(intervalsLo1 * 2);
                final long intervalHi = intervals.getQuick(intervalsLo1 * 2 + 1);

                final long partitionTimestampLoApprox = timestampFinder.minTimestampApproxFromMetadata();
                // interval is wholly above partition, skip interval
                if (partitionTimestampLoApprox > intervalHi) {
                    intervalsLo1++;
                    continue;
                }

                final long partitionTimestampHiApprox = timestampFinder.maxTimestampApproxFromMetadata();
                // interval is wholly below partition, skip partition
                if (partitionTimestampHiApprox < intervalLo) {
                    partitionLimit1 = 0;
                    partitionLo1++;
                    continue;
                }

                reader.openPartition(partitionLo1);
                timestampFinder.prepare();

                final long partitionTimestampLoExact = timestampFinder.minTimestampExact();
                // interval is wholly above partition, skip interval
                if (partitionTimestampLoExact > intervalHi) {
                    intervalsLo1++;
                    continue;
                }

                final long partitionTimestampHiExact = timestampFinder.maxTimestampExact();
                // interval is wholly below partition, skip partition
                if (partitionTimestampHiExact < intervalLo) {
                    partitionLimit1 = 0;
                    partitionLo1++;
                    continue;
                }

                // calculate intersection
                long lo;
                if (partitionTimestampLoExact >= intervalLo) {
                    lo = 0;
                } else {
                    // intervalLo is inclusive of value. We will look for bottom index of intervalLo - 1
                    // and then do index + 1 to skip to top of where we need to be.
                    lo = timestampFinder.findTimestamp(intervalLo - 1, partitionLimit1 == -1 ? 0 : partitionLimit1, rowCount - 1) + 1;
                }

                // Interval is inclusive of edges, and we have to bump to high bound because it is non-inclusive.
                long hi = timestampFinder.findTimestamp(intervalHi, lo, rowCount - 1) + 1;
                if (lo < hi) {
                    size += (hi - lo);

                    // we do have whole partition of fragment?
                    if (hi == rowCount) {
                        // whole partition, will need to skip to next one
                        partitionLimit1 = 0;
                        partitionLo1++;
                    } else {
                        // only fragment, need to skip to next interval
                        partitionLimit1 = hi;
                        intervalsLo1++;
                    }
                    continue;
                }
                // interval yielded empty partition frame
                partitionLimit1 = hi;
                intervalsLo1++;
            } else {
                // partition was empty, just skip to next
                partitionLo1++;
            }
        }

        counter.add(size - this.sizeSoFar);
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
        this.initialPartitionHi = Math.min(reader.getPartitionCount(), reader.getPartitionIndexByTimestamp(intervalHi) + 1);
    }

    protected TimestampFinder initTimestampFinder(int partitionIndex, long rowCount) {
        if (reader.getPartitionFormatFromMetadata(partitionIndex) == PartitionFormat.PARQUET) {
            return parquetTimestampFinder.of(reader, partitionIndex, timestampIndex);
        }
        return nativeTimestampFinder.of(reader, partitionIndex, timestampIndex, rowCount);
    }

    protected static class IntervalPartitionFrame implements PartitionFrame {
        protected byte format;
        protected PartitionDecoder parquetDecoder;
        protected int partitionIndex;
        protected long rowHi;
        protected long rowLo;

        @Override
        public PartitionDecoder getParquetDecoder() {
            return parquetDecoder;
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
