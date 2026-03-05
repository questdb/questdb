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

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.TimeFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.std.DirectLongList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

import static io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor.populatePartitionTimestamps;

/**
 * Thread-unsafe time frame cursor with lazy partition opening.
 * <p>
 * Each time frame maps to a partition. Page frames within a partition are an
 * internal detail — callers navigate partitions and rows within them.
 * Partitions are opened lazily on the first {@link #open()} call.
 * <p>
 * The only supported partition order is forward, i.e. navigation
 * should start with a {@link #next()} call.
 */
public final class TimeFrameCursorImpl implements TimeFrameCursor {
    private static final int PAGE_FRAME_SCAN_THRESHOLD = 64;
    private final PageFrameAddressCache frameAddressCache;
    private final PageFrameMemoryPool frameMemoryPool;
    // Cache for partition timestamps: [tsLo0, tsHi0, tsLo1, tsHi1, ...]
    private final DirectLongList frameTimestampCache;
    private final RecordMetadata metadata;
    // Cumulative row counts per page frame (for row-to-page-frame resolution)
    private final LongList pageFrameCumulativeRows = new LongList();
    private final LongList partitionCeilings = new LongList();
    private final LongList partitionRowCounts = new LongList();
    private final LongList partitionTimestamps = new LongList();
    private final TimeFrameMemoryRecord recordA = new TimeFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
    private final TimeFrameMemoryRecord recordB = new TimeFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER);
    private final TimeFrame timeFrame = new TimeFrame();
    private int currentPageFrameGlobalIndex = -1;
    private long currentPageFrameRowHi;
    private long currentPageFrameRowLo;
    private int currentPartition = -1;
    private TablePageFrameCursor frameCursor;
    private boolean isFrameMetadataBuilt;
    private int pageFrameCount;
    private int partitionCount;
    private boolean[] partitionOpened;
    private int[] partitionPageFrameCount;
    private int[] partitionPageFrameStart;
    private long[] partitionTotalRows;
    private TableReader reader;

    public TimeFrameCursorImpl(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata
    ) {
        try {
            this.metadata = metadata;
            this.frameAddressCache = new PageFrameAddressCache();
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
        Misc.free(frameAddressCache);
        Misc.free(frameTimestampCache);
        frameCursor = Misc.free(frameCursor);
    }

    @Override
    public BitmapIndexReader getIndexReaderForCurrentFrame(int logicalColumnIndex, int direction) {
        int physicalColumnIndex = frameCursor.getColumnIndexes().getQuick(logicalColumnIndex);
        int partitionIndex = timeFrame.getFrameIndex();
        if (partitionIndex == -1) {
            return null;
        }
        return reader.getBitmapIndexReader(partitionIndex, physicalColumnIndex, direction);
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
    public int getTimestampIndex() {
        return metadata.getTimestampIndex();
    }

    @Override
    public void jumpTo(int frameIndex) {
        buildFrameMetadata();
        if (frameIndex < 0 || frameIndex >= partitionCount) {
            throw CairoException.nonCritical().put("frame index out of bounds [frameIndex=").put(frameIndex)
                    .put(", frameCount=").put(partitionCount).put(']');
        }
        timeFrame.ofEstimate(
                frameIndex,
                partitionTimestamps.getQuick(frameIndex),
                partitionCeilings.getQuick(frameIndex)
        );
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return frameCursor.newSymbolTable(columnIndex);
    }

    @Override
    public boolean next() {
        buildFrameMetadata();
        int frameIndex = timeFrame.getFrameIndex();
        while (++frameIndex < partitionCount) {
            // Eagerly open partition to check if it has rows after interval filtering.
            // Empty partitions (0 rows) are skipped to match the old page-frame-based
            // model where time frames were never empty.
            ensurePartitionOpened(frameIndex);
            if (partitionTotalRows[frameIndex] > 0) {
                timeFrame.ofEstimate(
                        frameIndex,
                        partitionTimestamps.getQuick(frameIndex),
                        partitionCeilings.getQuick(frameIndex)
                );
                return true;
            }
        }
        // Update frame index in case of subsequent prev() call.
        timeFrame.ofEstimate(partitionCount, Long.MIN_VALUE, Long.MIN_VALUE);
        return false;
    }

    public TimeFrameCursor of(TablePageFrameCursor frameCursor) {
        this.frameCursor = frameCursor;
        frameAddressCache.of(metadata, frameCursor.getColumnIndexes(), frameCursor.isExternal());
        frameMemoryPool.of(frameAddressCache);
        reader = frameCursor.getTableReader();
        recordA.of(frameCursor);
        recordB.of(frameCursor);
        populatePartitionTimestamps(frameCursor, partitionTimestamps, partitionCeilings);
        isFrameMetadataBuilt = false;
        toTop();
        return this;
    }

    @Override
    public long open() {
        final int partitionIndex = timeFrame.getFrameIndex();
        if (partitionIndex < 0 || partitionIndex >= partitionCount) {
            throw CairoException.nonCritical().put("open call on uninitialized time frame");
        }

        ensurePartitionOpened(partitionIndex);

        final long totalRows = partitionTotalRows[partitionIndex];
        if (totalRows > 0) {
            final int pageFrameStart = partitionPageFrameStart[partitionIndex];
            final int pageFrameCountInPartition = partitionPageFrameCount[partitionIndex];

            final int cacheOffset = partitionIndex * 2;
            long timestampLo = frameTimestampCache.get(cacheOffset);
            long timestampHi;
            if (timestampLo != Numbers.LONG_NULL) {
                timestampHi = frameTimestampCache.get(cacheOffset + 1);
            } else {
                final int tsColumnIndex = metadata.getTimestampIndex();
                final PageFrameMemory firstPageFrame = frameMemoryPool.navigateTo(pageFrameStart);
                timestampLo = Unsafe.getUnsafe().getLong(firstPageFrame.getPageAddress(tsColumnIndex));
                final int lastPageFrameIndex = pageFrameStart + pageFrameCountInPartition - 1;
                final PageFrameMemory lastPageFrame = frameMemoryPool.navigateTo(lastPageFrameIndex);
                final long lastPageFrameRows = pageFrameCumulativeRows.getQuick(lastPageFrameIndex)
                        - (lastPageFrameIndex > pageFrameStart ? pageFrameCumulativeRows.getQuick(lastPageFrameIndex - 1) : 0);
                timestampHi = Unsafe.getUnsafe().getLong(
                        lastPageFrame.getPageAddress(tsColumnIndex) + (lastPageFrameRows - 1) * 8);
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
        buildFrameMetadata();
        int frameIndex = timeFrame.getFrameIndex();
        while (--frameIndex >= 0) {
            ensurePartitionOpened(frameIndex);
            if (partitionTotalRows[frameIndex] > 0) {
                timeFrame.ofEstimate(
                        frameIndex,
                        partitionTimestamps.getQuick(frameIndex),
                        partitionCeilings.getQuick(frameIndex)
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
        if (currentPageFrameGlobalIndex >= 0
                && partitionIndex == currentPartition
                && rowInPartition >= currentPageFrameRowLo && rowInPartition < currentPageFrameRowHi
                && ((PageFrameMemoryRecord) record).getFrameIndex() == currentPageFrameGlobalIndex) {
            ((TimeFrameMemoryRecord) record).setRowIndex(partitionIndex, rowInPartition, currentPageFrameRowLo);
            return;
        }
        ensurePartitionOpened(partitionIndex);
        navigateToRow(record, partitionIndex, rowInPartition);
    }

    @Override
    public void recordAt(Record record, int frameIndex, long rowIndex) {
        if (currentPageFrameGlobalIndex >= 0
                && frameIndex == currentPartition
                && rowIndex >= currentPageFrameRowLo && rowIndex < currentPageFrameRowHi
                && ((PageFrameMemoryRecord) record).getFrameIndex() == currentPageFrameGlobalIndex) {
            ((TimeFrameMemoryRecord) record).setRowIndex(frameIndex, rowIndex, currentPageFrameRowLo);
            return;
        }
        ensurePartitionOpened(frameIndex);
        navigateToRow(record, frameIndex, rowIndex);
    }

    @Override
    public void recordAtRowIndex(Record record, long rowIndex) {
        if (rowIndex >= currentPageFrameRowLo && rowIndex < currentPageFrameRowHi) {
            ((TimeFrameMemoryRecord) record).setRowIndex(rowIndex, currentPageFrameRowLo);
            return;
        }
        navigateToRow(record, timeFrame.getFrameIndex(), rowIndex);
    }

    @Override
    public void seekEstimate(long timestamp) {
        buildFrameMetadata();
        if (partitionCount == 0) {
            timeFrame.ofEstimate(-1, Long.MIN_VALUE, Long.MIN_VALUE);
            return;
        }
        int lo = 0, hi = partitionCount - 1, result = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (partitionCeilings.getQuick(mid) <= timestamp) {
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
                    partitionTimestamps.getQuick(result),
                    partitionCeilings.getQuick(result)
            );
        }
    }

    @Override
    public void toTop() {
        timeFrame.clear();
        if (!isFrameMetadataBuilt) {
            frameCursor.toTop();
        }
        currentPageFrameGlobalIndex = -1;
        currentPageFrameRowLo = 0;
        currentPageFrameRowHi = 0;
        currentPartition = -1;
    }

    private void buildFrameMetadata() {
        if (isFrameMetadataBuilt) {
            return;
        }

        partitionCount = reader.getPartitionCount();

        partitionRowCounts.clear();
        for (int p = 0; p < partitionCount; p++) {
            partitionRowCounts.add(reader.getPartitionRowCountFromMetadata(p));
        }

        partitionOpened = new boolean[partitionCount];
        partitionPageFrameStart = new int[partitionCount];
        partitionPageFrameCount = new int[partitionCount];
        partitionTotalRows = new long[partitionCount];
        pageFrameCount = 0;
        pageFrameCumulativeRows.clear();

        final int cacheSize = 2 * partitionCount;
        if (cacheSize > 0) {
            frameTimestampCache.setCapacity(cacheSize);
            frameTimestampCache.clear();
            for (int i = 0; i < cacheSize; i++) {
                frameTimestampCache.set(i, Numbers.LONG_NULL);
            }
        }

        isFrameMetadataBuilt = true;
    }

    private void ensurePartitionOpened(int partitionIndex) {
        if (partitionOpened[partitionIndex]) {
            return;
        }

        frameCursor.toPartition(partitionIndex);

        final int pageFrameStart = pageFrameCount;
        long totalRows = 0;
        int pageFramesInPartition = 0;
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            frameAddressCache.add(pageFrameCount, frame);
            long pageFrameRows = frame.getPartitionHi() - frame.getPartitionLo();
            totalRows += pageFrameRows;
            pageFrameCumulativeRows.add(totalRows);
            pageFrameCount++;
            pageFramesInPartition++;
        }

        partitionPageFrameStart[partitionIndex] = pageFrameStart;
        partitionPageFrameCount[partitionIndex] = pageFramesInPartition;
        partitionTotalRows[partitionIndex] = totalRows;
        partitionOpened[partitionIndex] = true;
    }

    private static int findPageFrame(int pfStart, int pfCount, LongList cumulativeRows, long rowInPartition) {
        int lo;
        if (pfCount <= PAGE_FRAME_SCAN_THRESHOLD) {
            lo = 0;
            while (lo < pfCount - 1
                    && cumulativeRows.getQuick(pfStart + lo) <= rowInPartition) {
                lo++;
            }
        } else {
            lo = 0;
            int hi = pfCount - 1;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (cumulativeRows.getQuick(pfStart + mid) <= rowInPartition) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
        }
        return lo;
    }

    private void navigateToRow(Record record, int partitionIndex, long rowInPartition) {
        final int pageFrameStart = partitionPageFrameStart[partitionIndex];
        final int pfCount = partitionPageFrameCount[partitionIndex];

        int lo;
        final int lastLocalPfIndex = currentPageFrameGlobalIndex - pageFrameStart;
        if (lastLocalPfIndex >= 0 && lastLocalPfIndex < pfCount && partitionIndex == currentPartition) {
            final int nextPf = lastLocalPfIndex + 1;
            if (nextPf < pfCount && rowInPartition >= currentPageFrameRowHi
                    && rowInPartition < pageFrameCumulativeRows.getQuick(pageFrameStart + nextPf)) {
                lo = nextPf;
            } else if (lastLocalPfIndex > 0 && rowInPartition < currentPageFrameRowLo) {
                final long prevPfRowLo = lastLocalPfIndex > 1 ? pageFrameCumulativeRows.getQuick(pageFrameStart + lastLocalPfIndex - 2) : 0;
                if (rowInPartition >= prevPfRowLo) {
                    lo = lastLocalPfIndex - 1;
                } else {
                    lo = findPageFrame(pageFrameStart, pfCount, pageFrameCumulativeRows, rowInPartition);
                }
            } else {
                lo = findPageFrame(pageFrameStart, pfCount, pageFrameCumulativeRows, rowInPartition);
            }
        } else {
            lo = findPageFrame(pageFrameStart, pfCount, pageFrameCumulativeRows, rowInPartition);
        }

        final int pageFrameIndex = pageFrameStart + lo;
        final long pageFrameRowLo = lo > 0 ? pageFrameCumulativeRows.getQuick(pageFrameIndex - 1) : 0;
        final long pageFrameRowHi = pageFrameCumulativeRows.getQuick(pageFrameIndex);

        frameMemoryPool.navigateTo(pageFrameIndex, (PageFrameMemoryRecord) record);
        ((TimeFrameMemoryRecord) record).setRowIndex(partitionIndex, rowInPartition, pageFrameRowLo);

        currentPageFrameGlobalIndex = pageFrameIndex;
        currentPageFrameRowLo = pageFrameRowLo;
        currentPageFrameRowHi = pageFrameRowHi;
        currentPartition = partitionIndex;
    }

    // maxTimestampHi is used to handle split partitions correctly as ceil method
    // will return the same value for all split partitions
    static long estimatePartitionHi(TimestampDriver.TimestampCeilMethod partitionCeilMethod, long partitionTimestamp, long maxTimestampHi) {
        // partitionCeilMethod is null in case of partition by NONE
        long partitionHi = partitionCeilMethod != null ? partitionCeilMethod.ceil(partitionTimestamp) : Long.MAX_VALUE;
        return Math.min(partitionHi, maxTimestampHi);
    }
}
