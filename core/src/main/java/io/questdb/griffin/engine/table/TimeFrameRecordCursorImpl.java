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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;

/**
 * The only supported partition order is forward, i.e. navigation
 * should start with a {@link #next()} call.
 */
public class TimeFrameRecordCursorImpl implements TimeFrameRecordCursor {
    private final PageFrameAddressCache frameAddressCache;
    private final PageFrameMemoryPool frameMemoryPool;
    private final IntList framePartitionIndexes = new IntList();
    private final LongList frameRowCounts = new LongList();
    private final PageFrameMemoryRecord recordA = new PageFrameMemoryRecord();
    private final PageFrameMemoryRecord recordB = new PageFrameMemoryRecord();
    private final TableReaderTimeFrame timeFrame = new TableReaderTimeFrame();
    private final int timestampIndex;
    private int frameCount = 0;
    private PageFrameCursor frameCursor;
    private boolean isFrameCacheBuilt;
    private PartitionBy.PartitionCeilMethod partitionCeilMethod;
    private int partitionHi;
    private TableReader reader;

    public TimeFrameRecordCursorImpl(
            @NotNull CairoConfiguration configuration,
            @NotNull @Transient RecordMetadata metadata
    ) {
        frameAddressCache = new PageFrameAddressCache(configuration);
        frameAddressCache.of(metadata);
        timestampIndex = metadata.getTimestampIndex();
        frameMemoryPool = new PageFrameMemoryPool();
    }

    @Override
    public void close() {
        Misc.free(frameMemoryPool);
        frameCursor = Misc.free(frameCursor);
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public StaticSymbolTable getSymbolTable(int columnIndex) {
        return frameCursor.getSymbolTable(columnIndex);
    }

    @Override
    public TimeFrame getTimeFrame() {
        return timeFrame;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return frameCursor.newSymbolTable(columnIndex);
    }

    @Override
    public boolean next() {
        buildFrameCache();

        int frameIndex = timeFrame.frameIndex;
        if (++frameIndex < frameCount) {
            int partitionIndex = framePartitionIndexes.getQuick(frameIndex);
            long timestampLo = reader.getPartitionTimestampByIndex(partitionIndex);
            long maxTimestampHi = partitionIndex < partitionHi - 2 ? reader.getPartitionTimestampByIndex(partitionIndex + 1) : Long.MAX_VALUE;
            timeFrame.of(frameIndex, timestampLo, estimatePartitionHi(timestampLo, maxTimestampHi));
            return true;
        }
        // Update frame index in case of subsequent prev() call.
        timeFrame.of(frameCount, Long.MIN_VALUE, Long.MIN_VALUE);
        return false;
    }

    public TimeFrameRecordCursor of(PageFrameCursor frameCursor) {
        this.frameCursor = frameCursor;
        frameAddressCache.clear();
        frameMemoryPool.of(frameAddressCache);
        reader = frameCursor.getTableReader();
        recordA.of(frameCursor);
        recordB.of(frameCursor);
        partitionHi = reader.getPartitionCount();
        partitionCeilMethod = PartitionBy.getPartitionCeilMethod(reader.getPartitionedBy());
        isFrameCacheBuilt = false;
        toTop();
        return this;
    }

    @Override
    public long open() throws DataUnavailableException {
        final int frameIndex = timeFrame.frameIndex;
        if (frameIndex < 0 || frameIndex >= frameCount) {
            throw CairoException.nonCritical().put("open call on uninitialized time frame");
        }
        final long rowCount = frameRowCounts.getQuick(frameIndex);
        if (rowCount > 0) {
            timeFrame.rowLo = 0;
            timeFrame.rowHi = rowCount;
            final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(frameIndex);
            final long timestampAddress = frameMemory.getPageAddress(timestampIndex);
            timeFrame.timestampLo = Unsafe.getUnsafe().getLong(timestampAddress);
            timeFrame.timestampHi = Unsafe.getUnsafe().getLong(timestampAddress + (rowCount - 1) * 8) + 1;
            return rowCount;
        }
        timeFrame.rowLo = 0;
        timeFrame.rowHi = 0;
        timeFrame.timestampLo = timeFrame.estimateTimestampLo;
        timeFrame.timestampHi = timeFrame.estimateTimestampLo;
        return 0;
    }

    @Override
    public boolean prev() {
        buildFrameCache();

        int frameIndex = timeFrame.frameIndex;
        if (--frameIndex >= 0) {
            int partitionIndex = framePartitionIndexes.getQuick(frameIndex);
            long timestampLo = reader.getPartitionTimestampByIndex(partitionIndex);
            long maxTimestampHi = partitionIndex < partitionHi - 2 ? reader.getPartitionTimestampByIndex(partitionIndex + 1) : Long.MAX_VALUE;
            timeFrame.of(frameIndex, timestampLo, estimatePartitionHi(timestampLo, maxTimestampHi));
            return true;
        }
        // Update frame index in case of subsequent next() call.
        timeFrame.of(-1, Long.MIN_VALUE, Long.MIN_VALUE);
        return false;
    }

    @Override
    public void recordAt(Record record, long rowId) {
        final PageFrameMemoryRecord frameMemoryRecord = (PageFrameMemoryRecord) record;
        frameMemoryPool.navigateTo(Rows.toPartitionIndex(rowId), frameMemoryRecord);
        frameMemoryRecord.setRowIndex(Rows.toLocalRowID(rowId));
    }

    @Override
    public void toTop() {
        timeFrame.clear();
        if (!isFrameCacheBuilt) {
            frameCount = 0;
            frameCursor.toTop();
            framePartitionIndexes.clear();
            frameRowCounts.clear();
        }
    }

    private void buildFrameCache() {
        // TODO: bulding page frame cache assumes opening all partitions;
        //       would be great if we could open partitions lazily
        if (!isFrameCacheBuilt) {
            PageFrame frame;
            while ((frame = frameCursor.next()) != null) {
                framePartitionIndexes.add(frame.getPartitionIndex());
                frameRowCounts.add(frame.getPartitionHi() - frame.getPartitionLo());
                frameAddressCache.add(frameCount++, frame);
            }
            isFrameCacheBuilt = true;
        }
    }

    // maxTimestampHi is used to handle split partitions correctly as ceil method
    // will return the same value for all split partitions
    private long estimatePartitionHi(long partitionTimestamp, long maxTimestampHi) {
        // partitionCeilMethod is null in case of partition by NONE
        long partitionHi = partitionCeilMethod != null ? partitionCeilMethod.ceil(partitionTimestamp) : Long.MAX_VALUE;
        return Math.min(partitionHi, maxTimestampHi);
    }

    private static class TableReaderTimeFrame implements TimeFrame, Mutable {
        private long estimateTimestampHi;
        private long estimateTimestampLo;
        private int frameIndex;
        private long rowHi;
        private long rowLo;
        private long timestampHi;
        private long timestampLo;

        @Override
        public void clear() {
            frameIndex = -1;
            estimateTimestampLo = Long.MIN_VALUE;
            estimateTimestampHi = Long.MIN_VALUE;
            timestampLo = Long.MIN_VALUE;
            timestampHi = Long.MIN_VALUE;
            rowLo = -1;
            rowHi = -1;
        }

        @Override
        public int getIndex() {
            return frameIndex;
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
        public long getTimestampEstimateHi() {
            return estimateTimestampHi;
        }

        @Override
        public long getTimestampEstimateLo() {
            return estimateTimestampLo;
        }

        @Override
        public long getTimestampHi() {
            return timestampHi;
        }

        @Override
        public long getTimestampLo() {
            return timestampLo;
        }

        @Override
        public boolean isOpen() {
            return rowLo != -1 && rowHi != -1;
        }

        public void of(int frameIndex, long estimateTimestampLo, long estimateTimestampHi) {
            this.frameIndex = frameIndex;
            this.estimateTimestampLo = estimateTimestampLo;
            this.estimateTimestampHi = estimateTimestampHi;
            timestampLo = Long.MIN_VALUE;
            timestampHi = Long.MIN_VALUE;
            rowLo = -1;
            rowHi = -1;
        }
    }
}
