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
import io.questdb.cairo.sql.PartitionFrameState;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.std.Vect.BIN_SEARCH_SCAN_UP;

public abstract class AbstractIntervalPartitionFrameCursor implements PartitionFrameCursor {
    protected final IntervalPartitionFrame frame = new IntervalPartitionFrame();
    protected final RuntimeIntrinsicIntervalModel intervalModel;
    protected final ParquetPartitionDecoder parquetDecoder;
    protected final int timestampIndex;
    private final NativeTimestampFinder nativeTimestampFinder = new NativeTimestampFinder();
    private final ParquetTimestampFinder parquetTimestampFinder;
    private final @Nullable AbstractTimestampFinder timestampFinder;
    protected LongList intervals;
    protected int intervalsHi;
    protected int intervalsLo;
    protected int partitionHi;
    protected int partitionLo;
    protected TableReader reader;
    protected long sizeSoFar = 0;
    private SqlExecutionCircuitBreaker circuitBreaker = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
    private long currentLogicalRowCount;
    private long currentPartitionFrameState;
    private int initialIntervalsHi;
    private int initialIntervalsLo;
    private int initialPartitionHi;
    private int initialPartitionLo;
    private @Nullable MemoryTracker memoryTracker;

    public AbstractIntervalPartitionFrameCursor(CairoConfiguration configuration, RuntimeIntrinsicIntervalModel intervalModel, int timestampIndex) {
        assert timestampIndex > -1;
        this.intervalModel = intervalModel;
        this.timestampIndex = timestampIndex;
        this.parquetDecoder = configuration.newParquetPartitionDecoder();
        this.parquetTimestampFinder = new ParquetTimestampFinder(parquetDecoder);
        this.timestampFinder = configuration.newTimestampFinder();
    }

    @Override
    public void close() {
        Misc.free(timestampFinder);
        Misc.free(parquetTimestampFinder);
        Misc.free(parquetDecoder);
        nativeTimestampFinder.clear();
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
    public boolean hasIntervalFilter() {
        return true;
    }

    @Override
    public StaticSymbolTable newSymbolTable(int columnIndex) {
        return reader.newSymbolTable(columnIndex);
    }

    public AbstractIntervalPartitionFrameCursor of(TableReader reader, SqlExecutionContext sqlExecutionContext) throws SqlException {
        memoryTracker = sqlExecutionContext != null ? sqlExecutionContext.getMemoryTracker() : null;
        circuitBreaker = sqlExecutionContext != null
                ? sqlExecutionContext.getCircuitBreaker()
                : SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
        parquetTimestampFinder.setMemoryTracker(memoryTracker);
        if (timestampFinder != null) {
            timestampFinder.clear();
        }
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
        if (timestampFinder != null) {
            timestampFinder.clear();
        }
        parquetTimestampFinder.clear();
        nativeTimestampFinder.clear();
        frame.partitionFrameState = 0;
        intervalsLo = initialIntervalsLo;
        intervalsHi = initialIntervalsHi;
        partitionLo = initialPartitionLo;
        partitionHi = initialPartitionHi;
        sizeSoFar = 0;
    }

    private void calculateRanges(TableReader reader, LongList intervals) {
        if (intervals.size() > 0) {
            if (reader.hasAnyDelta()) {
                initialIntervalsLo = 0;
                initialIntervalsHi = intervals.size() / 2;
                initialPartitionLo = 0;
                initialPartitionHi = reader.getPartitionCount();
            } else if (PartitionBy.isPartitioned(reader.getPartitionedBy())) {
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

    private static int findRelevantIntervalsHi(LongList intervals, long calendarHi) {
        final int intervalCount = Math.toIntExact(intervals.size() / 2);
        if (calendarHi == Long.MAX_VALUE) {
            return intervalCount;
        }
        int lo = 0;
        int hi = intervalCount;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (intervals.getQuick(2 * mid) < calendarHi) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    private static int findRelevantIntervalsLo(LongList intervals, long calendarLo) {
        int lo = 0;
        int hi = Math.toIntExact(intervals.size() / 2);
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (intervals.getQuick(2 * mid + 1) < calendarLo) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    protected long getCurrentLogicalRowCount() {
        return currentLogicalRowCount;
    }

    protected long getCurrentPartitionFrameState() {
        return currentPartitionFrameState;
    }

    protected long getPartitionCalendarHi(int partitionIndex) {
        if (!PartitionBy.isPartitioned(reader.getPartitionedBy())) {
            return Long.MAX_VALUE;
        }
        final long partitionTimestamp = reader.getPartitionTimestampByIndex(partitionIndex);
        return reader.getTxFile().getNextLogicalPartitionTimestamp(partitionTimestamp);
    }

    protected long getPartitionCalendarLo(int partitionIndex) {
        if (!PartitionBy.isPartitioned(reader.getPartitionedBy())) {
            return Long.MIN_VALUE;
        }
        return reader.floorToPartitionTimestamp(reader.getPartitionTimestampByIndex(partitionIndex));
    }

    protected boolean hasAnyDelta() {
        return reader.hasAnyDelta();
    }

    protected TimestampFinder initTimestampFinder(int partitionIndex, long baseRowCount) {
        frame.partitionFrameState = 0;
        currentLogicalRowCount = baseRowCount;
        currentPartitionFrameState = 0;
        final TimestampFinder baseFinder;
        if (reader.getPartitionFormatFromMetadata(partitionIndex) == PartitionFormat.PARQUET) {
            baseFinder = parquetTimestampFinder.of(reader, partitionIndex, timestampIndex, baseRowCount);
        } else {
            baseFinder = nativeTimestampFinder.of(reader, partitionIndex, timestampIndex, baseRowCount);
        }

        if (reader.getTxFile().getPartitionHasDelta(partitionIndex)) {
            reader.openPartition(partitionIndex);
            final long state = reader.getOrOpenPartitionFrameState(partitionIndex);
            if (state == 0) {
                throw CairoException.critical(0)
                        .put("cold delta partition state is unavailable [partitionIndex=")
                        .put(partitionIndex).put(']');
            }
            currentLogicalRowCount = PartitionFrameState.getLogicalPartitionRowCount(state);
            if (!PartitionFrameState.hasCustomFrames(state)) {
                if (currentLogicalRowCount != baseRowCount) {
                    throw CairoException.critical(0)
                            .put("cold delta residual-free row count mismatch [partitionIndex=")
                            .put(partitionIndex)
                            .put(", baseRows=").put(baseRowCount)
                            .put(", logicalRows=").put(currentLogicalRowCount)
                            .put(']');
                }
                return baseFinder;
            }
            if (timestampFinder == null) {
                throw CairoException.critical(0)
                        .put("cold delta timestamp finder is unavailable [partitionIndex=")
                        .put(partitionIndex).put(']');
            }
            currentPartitionFrameState = state;
            final int relevantIntervalsLo = findRelevantIntervalsLo(intervals, getPartitionCalendarLo(partitionIndex));
            final int relevantIntervalsHi = findRelevantIntervalsHi(intervals, getPartitionCalendarHi(partitionIndex));
            if (relevantIntervalsLo >= relevantIntervalsHi) {
                throw CairoException.critical(0)
                        .put("cold delta partition has no calendar-relevant intervals [partitionIndex=")
                        .put(partitionIndex).put(']');
            }
            return timestampFinder.of(
                    baseFinder,
                    state,
                    intervals,
                    relevantIntervalsLo,
                    relevantIntervalsHi,
                    memoryTracker,
                    circuitBreaker
            );
        }
        return baseFinder;
    }

    protected void populateFrame(int partitionIndex, long rowLo, long rowHi) {
        frame.partitionFrameState = currentPartitionFrameState;
        frame.partitionIndex = partitionIndex;
        frame.rowHi = rowHi;
        frame.rowLo = rowLo;
        final byte format = reader.getPartitionFormat(partitionIndex);
        if (format == PartitionFormat.PARQUET) {
            frame.format = PartitionFormat.PARQUET;
            frame.parquetMetaDecoder = reader.getAndInitParquetPartitionDecoder(partitionIndex);
        } else {
            assert format == PartitionFormat.NATIVE;
            frame.format = PartitionFormat.NATIVE;
            frame.parquetMetaDecoder = null;
        }
    }

    protected void validateIntervalBounds(int partitionIndex, long lo, long hi) {
        if (lo < 0 || lo > hi || hi > currentLogicalRowCount) {
            throw CairoException.critical(0)
                    .put("invalid timestamp interval bounds [partitionIndex=").put(partitionIndex)
                    .put(", lo=").put(lo)
                    .put(", hi=").put(hi)
                    .put(", rows=").put(currentLogicalRowCount)
                    .put(']');
        }
    }

    protected static class IntervalPartitionFrame implements PartitionFrame {
        protected byte format;
        protected ParquetPartitionDecoder parquetMetaDecoder;
        protected long partitionFrameState;
        protected int partitionIndex;
        protected long rowHi;
        protected long rowLo;

        @Override
        public ParquetPartitionDecoder getParquetMetaDecoder() {
            return parquetMetaDecoder;
        }

        @Override
        public long getPartitionFrameState() {
            return partitionFrameState;
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
