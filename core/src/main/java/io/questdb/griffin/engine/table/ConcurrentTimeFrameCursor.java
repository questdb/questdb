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
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

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
    // Cache for frame timestamps: [tsLo0, tsHi0, tsLo1, tsHi1, ...] - avoids re-reading on repeated open()
    private final DirectLongList frameTimestampCache;
    private final RecordMetadata metadata;
    private final PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
    private final TimeFrame timeFrame = new TimeFrame();
    private int frameCount = 0;
    // Cursor's lifecycle is managed externally
    private PageFrameCursor frameCursor;
    // Off-heap because it's per-frame and can be large unlike per-partition lists
    private DirectIntList framePartitionIndexes;
    private LongList frameRowCounts;
    private LongList partitionCeilings;
    private LongList partitionTimestamps;

    public ConcurrentTimeFrameCursor(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata
    ) {
        try {
            this.metadata = metadata;
            this.frameMemoryPool = new PageFrameMemoryPool(configuration.getSqlParquetFrameCacheCapacity());
            this.frameTimestampCache = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT, true);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    public static void populatePartitionTimestamps(
            TablePageFrameCursor frameCursor,
            LongList partitionTimestamps,
            LongList partitionCeilings
    ) {
        partitionTimestamps.clear();
        partitionCeilings.clear();
        final TableReader reader = frameCursor.getTableReader();
        final int partitionCount = reader.getPartitionCount();
        final TimestampDriver.TimestampCeilMethod ceilMethod = PartitionBy.getPartitionCeilMethod(
                reader.getMetadata().getTimestampType(),
                reader.getPartitionedBy()
        );
        for (int i = 0; i < partitionCount; i++) {
            final long tsLo = reader.getPartitionTimestampByIndex(i);
            partitionTimestamps.add(tsLo);
            final long maxTsHi = i < partitionCount - 2 ? reader.getPartitionTimestampByIndex(i + 1) : Long.MAX_VALUE;
            partitionCeilings.add(TimeFrameCursorImpl.estimatePartitionHi(ceilMethod, tsLo, maxTsHi));
        }
    }

    @Override
    public void close() {
        Misc.free(frameMemoryPool);
        Misc.free(frameTimestampCache);
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
            throw CairoException.nonCritical().put("frame index out of bounds. [frameIndex=]").put(frameIndex)
                    .put(", frameCount=").put(frameCount).put(']');
        }

        int partitionIndex = framePartitionIndexes.get(frameIndex);
        long timestampLo = partitionTimestamps.getQuick(partitionIndex);
        timeFrame.ofEstimate(frameIndex, timestampLo, partitionCeilings.getQuick(partitionIndex));
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return frameCursor.newSymbolTable(columnIndex);
    }

    @Override
    public boolean next() {
        int frameIndex = timeFrame.getFrameIndex();
        if (++frameIndex < frameCount) {
            int partitionIndex = framePartitionIndexes.get(frameIndex);
            long timestampLo = partitionTimestamps.getQuick(partitionIndex);
            timeFrame.ofEstimate(frameIndex, timestampLo, partitionCeilings.getQuick(partitionIndex));
            return true;
        }
        // Update frame index in case of subsequent prev() call.
        timeFrame.ofEstimate(frameCount, Long.MIN_VALUE, Long.MIN_VALUE);
        return false;
    }

    public TimeFrameCursor of(
            TablePageFrameCursor frameCursor,
            PageFrameAddressCache frameAddressCache,
            DirectIntList framePartitionIndexes,
            LongList frameRowCounts,
            LongList partitionTimestamps,
            LongList partitionCeilings,
            int frameCount
    ) {
        this.frameCursor = frameCursor;
        this.framePartitionIndexes = framePartitionIndexes;
        this.frameRowCounts = frameRowCounts;
        this.partitionTimestamps = partitionTimestamps;
        this.partitionCeilings = partitionCeilings;
        this.frameCount = frameCount;
        frameMemoryPool.of(frameAddressCache);
        record.of(frameCursor);
        // Initialize timestamp cache (2 entries per frame: tsLo, tsHi)
        // Note: setCapacity is safe to call on a closed list - it will allocate memory
        final int cacheSize = 2 * frameCount;
        if (cacheSize > 0) {
            frameTimestampCache.setCapacity(cacheSize);
            frameTimestampCache.clear();
            for (int i = 0; i < cacheSize; i++) {
                frameTimestampCache.set(i, Numbers.LONG_NULL);
            }
        }
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
            final int cacheOffset = frameIndex * 2;
            long timestampLo = frameTimestampCache.get(cacheOffset);
            long timestampHi;
            if (timestampLo != Numbers.LONG_NULL) {
                // Cache hit - use cached timestamps
                timestampHi = frameTimestampCache.get(cacheOffset + 1);
            } else {
                // Cache miss - read timestamps directly from frame memory
                final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(frameIndex);
                final long timestampAddress = frameMemory.getPageAddress(metadata.getTimestampIndex());
                timestampLo = Unsafe.getUnsafe().getLong(timestampAddress);
                timestampHi = Unsafe.getUnsafe().getLong(timestampAddress + (rowCount - 1) * 8);
                frameTimestampCache.set(cacheOffset, timestampLo);
                frameTimestampCache.set(cacheOffset + 1, timestampHi);
            }
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
            int partitionIndex = framePartitionIndexes.get(frameIndex);
            long timestampLo = partitionTimestamps.getQuick(partitionIndex);
            timeFrame.ofEstimate(frameIndex, timestampLo, partitionCeilings.getQuick(partitionIndex));
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
    public void recordAt(Record record, int frameIndex, long rowIndex) {
        final PageFrameMemoryRecord frameMemoryRecord = (PageFrameMemoryRecord) record;
        frameMemoryPool.navigateTo(frameIndex, frameMemoryRecord);
        frameMemoryRecord.setRowIndex(rowIndex);
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
