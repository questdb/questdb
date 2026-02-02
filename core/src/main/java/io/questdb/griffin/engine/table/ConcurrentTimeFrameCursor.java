/*******************************************************************************
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;

import static io.questdb.griffin.engine.table.TimeFrameCursorImpl.estimatePartitionHi;

/**
 * Instances of this time frame cursor can be used by multiple threads as interactions
 * with the table reader are synchronized. Yet, a single instance can't be called by
 * multiple threads concurrently.
 * <p>
 * The only supported partition order is forward, i.e. navigation
 * should start with a {@link #next()} call.
 */
public final class ConcurrentTimeFrameCursor implements TimeFrameCursor {
    private final PageFrameMemoryPool frameMemoryPool;
    private final RecordMetadata metadata;
    private final PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
    private final TimeFrame timeFrame = new TimeFrame();
    private int frameCount = 0;
    // cursor's lifecycle is managed externally
    private PageFrameCursor frameCursor;
    private IntList framePartitionIndexes;
    private LongList frameRowCounts;
    private TimestampDriver.TimestampCeilMethod partitionCeilMethod;
    private int partitionHi;
    private LongList partitionTimestamps;

    public ConcurrentTimeFrameCursor(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata
    ) {
        this.metadata = metadata;
        frameMemoryPool = new PageFrameMemoryPool(configuration.getSqlParquetFrameCacheCapacity());
    }

    @Override
    public void close() {
        Misc.free(frameMemoryPool);
    }

    @Override
    public PageFrameMemoryRecord getRecord() {
        return record;
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
    public int getTimestampIndex() {
        return metadata.getTimestampIndex();
    }

    @Override
    public void jumpTo(int frameIndex) {
        if (frameIndex >= frameCount || frameIndex < 0) {
            throw CairoException.nonCritical().put("frame index out of bounds. [frameIndex=]").put(frameIndex).put(", frameCount=").put(frameCount).put(']');
        }

        int partitionIndex = framePartitionIndexes.getQuick(frameIndex);
        long timestampLo = partitionTimestamps.getQuick(partitionIndex);
        long maxTimestampHi = partitionIndex < partitionHi - 2 ? partitionTimestamps.getQuick(partitionIndex + 1) : Long.MAX_VALUE;
        timeFrame.ofEstimate(frameIndex, timestampLo, estimatePartitionHi(partitionCeilMethod, timestampLo, maxTimestampHi));
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return frameCursor.newSymbolTable(columnIndex);
    }

    @Override
    public boolean next() {
        int frameIndex = timeFrame.getFrameIndex();
        if (++frameIndex < frameCount) {
            int partitionIndex = framePartitionIndexes.getQuick(frameIndex);
            long timestampLo = partitionTimestamps.getQuick(partitionIndex);
            long maxTimestampHi = partitionIndex < partitionHi - 2 ? partitionTimestamps.getQuick(partitionIndex + 1) : Long.MAX_VALUE;
            timeFrame.ofEstimate(frameIndex, timestampLo, estimatePartitionHi(partitionCeilMethod, timestampLo, maxTimestampHi));
            return true;
        }
        // Update frame index in case of subsequent prev() call.
        timeFrame.ofEstimate(frameCount, Long.MIN_VALUE, Long.MIN_VALUE);
        return false;
    }

    public TimeFrameCursor of(
            TablePageFrameCursor frameCursor,
            PageFrameAddressCache frameAddressCache,
            IntList framePartitionIndexes,
            LongList frameRowCounts,
            LongList partitionTimestamps,
            int frameCount
    ) {
        this.frameCursor = frameCursor;
        this.framePartitionIndexes = framePartitionIndexes;
        this.frameRowCounts = frameRowCounts;
        this.partitionTimestamps = partitionTimestamps;
        this.frameCount = frameCount;
        frameMemoryPool.of(frameAddressCache);
        final TableReader reader = frameCursor.getTableReader();
        record.of(frameCursor);
        partitionHi = reader.getPartitionCount();
        partitionCeilMethod = PartitionBy.getPartitionCeilMethod(
                reader.getMetadata().getTimestampType(),
                reader.getPartitionedBy()
        );
        toTop();
        return this;
    }

    @Override
    public long open() {
        final int frameIndex = timeFrame.getFrameIndex();
        if (frameIndex < 0 || frameIndex >= frameCount) {
            throw CairoException.nonCritical().put("open call on uninitialized time frame");
        }
        final long rowCount = frameRowCounts.getQuick(frameIndex);
        if (rowCount > 0) {
            frameMemoryPool.navigateTo(frameIndex, record);
            record.setRowIndex(0);
            final long timestampLo = record.getTimestamp(metadata.getTimestampIndex());
            record.setRowIndex(rowCount - 1);
            final long timestampHi = record.getTimestamp(metadata.getTimestampIndex());
            timeFrame.ofOpen(
                    timestampLo,
                    timestampHi + 1,
                    0,
                    rowCount
            );
            return rowCount;
        }
        timeFrame.ofOpen(
                timeFrame.getTimestampEstimateLo(),
                timeFrame.getTimestampEstimateHi(),
                0,
                0
        );
        return 0;
    }

    @Override
    public boolean prev() {
        int frameIndex = timeFrame.getFrameIndex();
        if (--frameIndex >= 0) {
            int partitionIndex = framePartitionIndexes.getQuick(frameIndex);
            long timestampLo = partitionTimestamps.getQuick(partitionIndex);
            long maxTimestampHi = partitionIndex < partitionHi - 2 ? partitionTimestamps.getQuick(partitionIndex + 1) : Long.MAX_VALUE;
            timeFrame.ofEstimate(frameIndex, timestampLo, estimatePartitionHi(partitionCeilMethod, timestampLo, maxTimestampHi));
            return true;
        }
        // Update frame index in case of subsequent next() call.
        timeFrame.ofEstimate(-1, Long.MIN_VALUE, Long.MIN_VALUE);
        return false;
    }

    @Override
    public void recordAt(Record record, long rowId) {
        final PageFrameMemoryRecord frameMemoryRecord = (PageFrameMemoryRecord) record;
        frameMemoryPool.navigateTo(Rows.toPartitionIndex(rowId), frameMemoryRecord);
        frameMemoryRecord.setRowIndex(Rows.toLocalRowID(rowId));
    }

    @Override
    public void recordAtRowIndex(Record record, long rowIndex) {
        final PageFrameMemoryRecord frameMemoryRecord = (PageFrameMemoryRecord) record;
        frameMemoryRecord.setRowIndex(rowIndex);
    }

    @Override
    public void toTop() {
        timeFrame.clear();
    }
}
