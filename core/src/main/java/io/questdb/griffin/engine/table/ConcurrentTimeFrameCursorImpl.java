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
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.cairo.sql.TimeFrameMemoryRecord;
import io.questdb.std.DirectLongList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

/**
 * Concurrent time frame cursor with lazy partition opening via shared
 * {@link ConcurrentTimeFrameState}. Each time frame maps to a partition.
 * Page frames within a partition are an internal detail.
 * <p>
 * Multiple instances of this cursor share the same {@link ConcurrentTimeFrameState}
 * which handles synchronized lazy partition opening. Yet, a single instance
 * must not be called by multiple threads concurrently.
 * <p>
 * The only supported partition order is forward, i.e. navigation
 * should start with a {@link #next()} call.
 */
public final class ConcurrentTimeFrameCursorImpl implements ConcurrentTimeFrameCursor {
    private final PageFrameMemoryPool frameMemoryPool;
    // Cache for partition timestamps: [tsLo0, tsHi0, tsLo1, tsHi1, ...]
    private final DirectLongList frameTimestampCache;
    private final TimeFrameMemoryRecord record = new TimeFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
    private final TimeFrame timeFrame = new TimeFrame();
    // Page frame tracking for same-page-frame fast paths in recordAt/recordAtRowIndex.
    // Avoids repeated page frame lookups when consecutive calls target the same page frame.
    private int currentCachePartition = -1;
    private int currentPageFrameLocalIndex = -1; // partition-local (each partition has its own cache)
    private long currentPageFrameRowHi;
    private long currentPageFrameRowLo;
    // Cursor's lifecycle is managed externally
    private TablePageFrameCursor frameCursor;
    private ConcurrentTimeFrameState sharedState;
    // Timestamp column index in the address cache; may differ from the original metadata index
    // when the cache is populated with logically-remapped page frames (e.g. SelectedPageFrame).
    // Initialized from metadata in the constructor; may be overridden via of().
    private int timestampIndex;

    public ConcurrentTimeFrameCursorImpl(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata
    ) {
        try {
            this.timestampIndex = metadata.getTimestampIndex();
            this.frameMemoryPool = new PageFrameMemoryPool(configuration.getSqlParquetFrameCacheCapacity());
            this.frameTimestampCache = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT, true);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        Misc.free(frameMemoryPool);
        Misc.free(frameTimestampCache);
    }

    @Override
    public Record getRecord() {
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
        return timestampIndex;
    }

    @Override
    public void jumpTo(int frameIndex) {
        final int partitionCount = sharedState.getPartitionCount();
        if (frameIndex < 0 || frameIndex >= partitionCount) {
            throw CairoException.nonCritical().put("frame index out of bounds [frameIndex=").put(frameIndex)
                    .put(", frameCount=").put(partitionCount).put(']');
        }
        timeFrame.ofEstimate(
                frameIndex,
                sharedState.getPartitionTimestamp(frameIndex),
                sharedState.getPartitionCeiling(frameIndex)
        );
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return frameCursor.newSymbolTable(columnIndex);
    }

    @Override
    public boolean next() {
        int frameIndex = timeFrame.getFrameIndex();
        final int partitionCount = sharedState.getPartitionCount();
        while (++frameIndex < partitionCount) {
            sharedState.ensurePartitionOpened(frameIndex);
            if (sharedState.getPartitionTotalRows(frameIndex) > 0) {
                timeFrame.ofEstimate(
                        frameIndex,
                        sharedState.getPartitionTimestamp(frameIndex),
                        sharedState.getPartitionCeiling(frameIndex)
                );
                return true;
            }
        }
        // Update frame index in case of subsequent prev() call.
        timeFrame.ofEstimate(partitionCount, Long.MIN_VALUE, Long.MIN_VALUE);
        return false;
    }

    @Override
    public ConcurrentTimeFrameCursor of(
            ConcurrentTimeFrameState sharedState,
            TablePageFrameCursor frameCursor,
            int timestampIndex
    ) {
        this.sharedState = sharedState;
        this.frameCursor = frameCursor;
        this.timestampIndex = timestampIndex;
        record.of(frameCursor);
        // Initialize timestamp cache: 2 entries per partition (tsLo, tsHi)
        final int partitionCount = sharedState.getPartitionCount();
        final int cacheSize = 2 * partitionCount;
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
        final int partitionIndex = timeFrame.getFrameIndex();
        final int partitionCount = sharedState.getPartitionCount();
        if (partitionIndex < 0 || partitionIndex >= partitionCount) {
            throw CairoException.nonCritical().put("open call on uninitialized time frame");
        }

        switchToPartition(partitionIndex);

        final long totalRows = sharedState.getPartitionTotalRows(partitionIndex);
        if (totalRows > 0) {
            final int pfCount = sharedState.getPartitionPageFrameCount(partitionIndex);

            final int cacheOffset = partitionIndex * 2;
            long timestampLo = frameTimestampCache.get(cacheOffset);
            long timestampHi;
            if (timestampLo != Numbers.LONG_NULL) {
                // Cache hit — use cached timestamps
                timestampHi = frameTimestampCache.get(cacheOffset + 1);
            } else {
                // Cache miss — read timestamps from first/last page frames
                // (using partition-local frame indices: 0-based)
                final PageFrameMemory firstPF = frameMemoryPool.navigateTo(0);
                timestampLo = Unsafe.getUnsafe().getLong(firstPF.getPageAddress(timestampIndex));

                final PageFrameMemory lastPF = frameMemoryPool.navigateTo(pfCount - 1);
                // Compute row count in the last page frame
                final int pfStart = sharedState.getPartitionPageFrameStart(partitionIndex);
                final int lastPfGlobalIndex = pfStart + pfCount - 1;
                final LongList cumulativeRows = sharedState.getPageFrameCumulativeRows();
                final long lastPfRows = cumulativeRows.getQuick(lastPfGlobalIndex)
                        - (pfCount > 1 ? cumulativeRows.getQuick(lastPfGlobalIndex - 1) : 0);
                timestampHi = Unsafe.getUnsafe().getLong(
                        lastPF.getPageAddress(timestampIndex) + (lastPfRows - 1) * 8);

                frameTimestampCache.set(cacheOffset, timestampLo);
                frameTimestampCache.set(cacheOffset + 1, timestampHi);
            }
            timeFrame.ofOpen(timestampLo, timestampHi + 1, 0, totalRows);
            return totalRows;
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
        while (--frameIndex >= 0) {
            sharedState.ensurePartitionOpened(frameIndex);
            if (sharedState.getPartitionTotalRows(frameIndex) > 0) {
                timeFrame.ofEstimate(
                        frameIndex,
                        sharedState.getPartitionTimestamp(frameIndex),
                        sharedState.getPartitionCeiling(frameIndex)
                );
                return true;
            }
        }
        // Update frame index in case of subsequent next() call.
        timeFrame.ofEstimate(-1, Long.MIN_VALUE, Long.MIN_VALUE);
        return false;
    }

    @Override
    public void recordAt(Record record, long rowId) {
        assert TimeFrameCursor.isTimeFrameRowID(rowId) : "not a time frame row ID";
        final int partitionIndex = TimeFrameCursor.toPartitionIndex(rowId);
        final long rowInPartition = TimeFrameCursor.toLocalRowID(rowId);
        // Same-page-frame fast path: just update the row offset.
        // The >= 0 guard prevents false hits after reset (both fields are -1).
        if (currentPageFrameLocalIndex >= 0 && partitionIndex == currentCachePartition
                && rowInPartition >= currentPageFrameRowLo && rowInPartition < currentPageFrameRowHi
                && ((PageFrameMemoryRecord) record).getFrameIndex() == currentPageFrameLocalIndex) {
            ((TimeFrameMemoryRecord) record).setRowIndex(partitionIndex, rowInPartition, currentPageFrameRowLo);
            return;
        }
        navigateToRow(record, partitionIndex, rowInPartition);
    }

    @Override
    public void recordAt(Record record, int frameIndex, long rowIndex) {
        // Same-page-frame fast path, see recordAt(Record, long) above.
        if (currentPageFrameLocalIndex >= 0 && frameIndex == currentCachePartition
                && rowIndex >= currentPageFrameRowLo && rowIndex < currentPageFrameRowHi
                && ((PageFrameMemoryRecord) record).getFrameIndex() == currentPageFrameLocalIndex) {
            ((TimeFrameMemoryRecord) record).setRowIndex(frameIndex, rowIndex, currentPageFrameRowLo);
            return;
        }
        navigateToRow(record, frameIndex, rowIndex);
    }

    @Override
    public void recordAtRowIndex(Record record, long rowIndex) {
        // Same-page-frame fast path for within-partition linear scans.
        if (rowIndex >= currentPageFrameRowLo && rowIndex < currentPageFrameRowHi) {
            ((TimeFrameMemoryRecord) record).setRowIndex(rowIndex, currentPageFrameRowLo);
            return;
        }
        navigateToRow(record, timeFrame.getFrameIndex(), rowIndex);
    }

    @Override
    public void seekEstimate(long timestamp) {
        final int partitionCount = sharedState.getPartitionCount();
        if (partitionCount == 0) {
            timeFrame.ofEstimate(-1, Long.MIN_VALUE, Long.MIN_VALUE);
            return;
        }
        int lo = 0, hi = partitionCount - 1, result = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (sharedState.getPartitionCeiling(mid) <= timestamp) {
                result = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        if (result == -1) {
            timeFrame.ofEstimate(-1, Long.MIN_VALUE, Long.MIN_VALUE);
        } else {
            timeFrame.ofEstimate(
                    result,
                    sharedState.getPartitionTimestamp(result),
                    sharedState.getPartitionCeiling(result)
            );
        }
    }

    @Override
    public void toTop() {
        timeFrame.clear();
        currentCachePartition = -1;
        currentPageFrameLocalIndex = -1;
        currentPageFrameRowLo = 0;
        currentPageFrameRowHi = 0;
    }

    private void navigateToRow(Record record, int partitionIndex, long rowInPartition) {
        switchToPartition(partitionIndex);

        final int pfStart = sharedState.getPartitionPageFrameStart(partitionIndex);
        final int pfCount = sharedState.getPartitionPageFrameCount(partitionIndex);
        final LongList cumulativeRows = sharedState.getPageFrameCumulativeRows();

        // Adjacent page frame check: before falling back to scan/binary search,
        // check whether the row is in the next or previous page frame. This is an
        // O(1) shortcut for sequential access patterns that cross page frame boundaries.
        int lo;
        final int lastPfIndex = currentPageFrameLocalIndex;
        if (lastPfIndex >= 0 && lastPfIndex < pfCount) {
            final int nextPf = lastPfIndex + 1;
            if (nextPf < pfCount && rowInPartition >= currentPageFrameRowHi
                    && rowInPartition < cumulativeRows.getQuick(pfStart + nextPf)) {
                lo = nextPf;
            } else if (lastPfIndex > 0 && rowInPartition < currentPageFrameRowLo) {
                final long prevPfRowLo = lastPfIndex > 1 ? cumulativeRows.getQuick(pfStart + lastPfIndex - 2) : 0;
                if (rowInPartition >= prevPfRowLo) {
                    lo = lastPfIndex - 1;
                } else {
                    lo = TimeFrameCursor.findPageFrame(pfStart, pfCount, cumulativeRows, rowInPartition);
                }
            } else {
                lo = TimeFrameCursor.findPageFrame(pfStart, pfCount, cumulativeRows, rowInPartition);
            }
        } else {
            lo = TimeFrameCursor.findPageFrame(pfStart, pfCount, cumulativeRows, rowInPartition);
        }

        final long pfRowLo = lo > 0 ? cumulativeRows.getQuick(pfStart + lo - 1) : 0;
        final long pfRowHi = cumulativeRows.getQuick(pfStart + lo);

        // Navigate using partition-local page frame index (the pool has per-partition cache)
        frameMemoryPool.navigateTo(lo, (PageFrameMemoryRecord) record);
        ((TimeFrameMemoryRecord) record).setRowIndex(partitionIndex, rowInPartition, pfRowLo);

        currentPageFrameLocalIndex = lo;
        currentPageFrameRowLo = pfRowLo;
        currentPageFrameRowHi = pfRowHi;
    }

    private void switchToPartition(int partitionIndex) {
        PageFrameAddressCache cache = sharedState.ensurePartitionOpened(partitionIndex);
        if (currentCachePartition != partitionIndex) {
            frameMemoryPool.switchAddressCache(cache);
            record.clear(); // force re-navigation (resets frameIndex to -1)
            currentCachePartition = partitionIndex;
            currentPageFrameLocalIndex = -1;
        }
    }
}
