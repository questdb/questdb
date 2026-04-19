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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.std.BitSet;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

import static io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor.populatePartitionTimestamps;

/**
 * Thread-unsafe time frame cursor with lazy partition opening.
 * <p>
 * Pre-computes exact page frame boundaries from table metadata (column tops,
 * row counts, partition formats) WITHOUT opening partitions. Column addresses
 * are patched lazily per partition on first access via
 * {@link #ensurePartitionOpened(int)}.
 * <p>
 * The only supported partition order is forward, i.e. navigation
 * should start with a {@link #next()} call.
 */
public final class TimeFrameCursorImpl implements TimeFrameCursor {
    private final IntList columnIndexes = new IntList();
    private final LongList columnTops = new LongList();
    private final PageFrameAddressCache frameAddressCache;
    private final PageFrameMemoryPool frameMemoryPool;
    // Off-heap because it's per-frame and can be large unlike per-partition lists
    private final DirectIntList framePartitionIndexes;
    private final DirectLongList frameRowCounts;
    // Cache for frame timestamps: [tsLo0, tsHi0, tsLo1, tsHi1, ...] - avoids re-reading on repeated open()
    private final DirectLongList frameTimestampCache;
    private final RecordMetadata metadata;
    private final LongList partitionCeilings = new LongList();
    // Per-partition: first global frame index.
    // Only populated in the non-eager (lazy) path. Not valid after
    // buildFrameCacheEagerly(); safe to read only when partitionOpened is unset.
    private final IntList partitionFirstFrame = new IntList();
    private final BitSet partitionOpened = new BitSet();
    private final LongList partitionTimestamps = new LongList();
    private final PageFrameMemoryRecord recordA = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
    private final PageFrameMemoryRecord recordB = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER);
    private final TimeFrame timeFrame = new TimeFrame();
    private final UninitializedPageFrame uninitializedFrame = new UninitializedPageFrame();
    private int frameCount = 0;
    private TablePageFrameCursor frameCursor;
    private boolean isFrameCacheBuilt;
    private int pageFrameMaxRows;
    private int pageFrameMinRows;
    private int partitionCount;
    private TableReader tableReader;
    private int workerCount;

    public TimeFrameCursorImpl(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata
    ) {
        try {
            this.metadata = metadata;
            this.frameAddressCache = new PageFrameAddressCache();
            this.frameMemoryPool = new PageFrameMemoryPool(configuration.getSqlParquetFrameCacheCapacity());
            this.framePartitionIndexes = new DirectIntList(64, MemoryTag.NATIVE_DEFAULT, true);
            this.frameRowCounts = new DirectLongList(64, MemoryTag.NATIVE_DEFAULT, true);
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
        Misc.free(framePartitionIndexes);
        Misc.free(frameRowCounts);
        Misc.free(frameTimestampCache);
        frameCursor = Misc.free(frameCursor);
    }

    @Override
    public BitmapIndexReader getIndexReaderForCurrentFrame(int logicalColumnIndex, int direction) {
        int physicalColumnIndex = frameCursor.getColumnMapping().getColumnIndex(logicalColumnIndex);
        int frameIndex = timeFrame.getFrameIndex();
        if (frameIndex == -1) {
            return null;
        }
        int partitionIndex = framePartitionIndexes.get(frameIndex);
        assert partitionOpened.get(partitionIndex) : "partition " + partitionIndex + " not opened before getIndexReaderForCurrentFrame";
        return tableReader.getBitmapIndexReader(partitionIndex, physicalColumnIndex, direction);
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
        buildFrameCache();

        if (frameIndex >= frameCount || frameIndex < 0) {
            throw CairoException.nonCritical().put("frame index out of bounds [frameIndex=").put(frameIndex)
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
        buildFrameCache();

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
            int pageFrameMinRows,
            int pageFrameMaxRows,
            int workerCount
    ) {
        this.frameCursor = frameCursor;
        this.pageFrameMinRows = pageFrameMinRows;
        this.pageFrameMaxRows = pageFrameMaxRows;
        this.workerCount = workerCount;
        final ColumnMapping mapping = frameCursor.getColumnMapping();
        frameAddressCache.of(metadata, mapping, frameCursor.isExternal());
        columnIndexes.clear();
        for (int i = 0, n = mapping.getColumnCount(); i < n; i++) {
            columnIndexes.add(mapping.getColumnIndex(i));
        }
        frameMemoryPool.of(frameAddressCache);
        tableReader = frameCursor.getTableReader();
        recordA.of(frameCursor);
        recordB.of(frameCursor);
        populatePartitionTimestamps(frameCursor, partitionTimestamps, partitionCeilings);
        isFrameCacheBuilt = false;
        toTop();
        return this;
    }

    @Override
    public long open() {
        final int frameIndex = timeFrame.getFrameIndex();
        if (frameIndex < 0 || frameIndex >= frameCount) {
            throw CairoException.nonCritical().put("open call on uninitialized time frame");
        }

        final int partitionIndex = framePartitionIndexes.get(frameIndex);
        ensurePartitionOpened(partitionIndex);

        final long rowCount = frameRowCounts.get(frameIndex);
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
        buildFrameCache();

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
        final int frameIndex = Rows.toPartitionIndex(rowId);
        final int partitionIndex = framePartitionIndexes.get(frameIndex);
        ensurePartitionOpened(partitionIndex);
        frameMemoryPool.navigateTo(frameIndex, frameMemoryRecord);
        frameMemoryRecord.setRowIndex(Rows.toLocalRowID(rowId));
    }

    @Override
    public void recordAt(Record record, int frameIndex, long rowIndex) {
        final PageFrameMemoryRecord frameMemoryRecord = (PageFrameMemoryRecord) record;
        final int partitionIndex = framePartitionIndexes.get(frameIndex);
        ensurePartitionOpened(partitionIndex);
        frameMemoryPool.navigateTo(frameIndex, frameMemoryRecord);
        frameMemoryRecord.setRowIndex(rowIndex);
    }

    @Override
    public void recordAtRowIndex(Record record, long rowIndex) {
        final PageFrameMemoryRecord frameMemoryRecord = (PageFrameMemoryRecord) record;
        frameMemoryRecord.setRowIndex(rowIndex);
    }

    @Override
    public void seekEstimate(long timestamp) {
        buildFrameCache();
        TimeFrameCursor.findSeekEstimate(
                timestamp,
                frameCount,
                framePartitionIndexes,
                partitionCeilings,
                partitionTimestamps,
                timeFrame
        );
    }

    @Override
    public void toTop() {
        timeFrame.clear();
        if (!isFrameCacheBuilt) {
            // No need to reset frame lists here — buildFrameCache() resets
            // all state unconditionally before populating the cache.
            frameCursor.toTop();
        }
    }

    /**
     * Pre-computes and adds uninitialized frame entries for a native partition.
     * Replicates the column-top-aware splitting logic from
     * FwdTableReaderPageFrameCursor#computeNativeFrame().
     */
    private void addNativePartitionFrames(
            ColumnVersionReader columnVersionReader,
            IntList columnIndexes,
            int columnCount,
            int partitionIndex,
            long partitionTimestamp,
            long partitionRowCount
    ) {
        FwdTableReaderPageFrameCursor.populateColumnTops(
                columnTops,
                tableReader,
                columnVersionReader,
                columnIndexes,
                columnCount,
                partitionTimestamp,
                partitionRowCount
        );

        final long pageFrameRowLimit = FwdTableReaderPageFrameCursor.calculatePageFrameRowLimit(
                0,
                partitionRowCount,
                pageFrameMinRows,
                pageFrameMaxRows,
                workerCount
        );

        long lo = 0;
        while (lo < partitionRowCount) {
            long adjustedHi = Math.min(partitionRowCount, lo + pageFrameRowLimit);
            // Shrink frame boundary at column top splits
            for (int i = 0; i < columnCount; i++) {
                long top = columnTops.getQuick(i);
                if (top > lo && top < adjustedHi) {
                    adjustedHi = top;
                }
            }
            addUninitializedFrame(partitionIndex, lo, adjustedHi);
            lo = adjustedHi;
        }
    }

    /**
     * Adds fully initialized frame entries for a partition that is already open
     * in the table reader. Iterates the page frame cursor to get real column
     * addresses directly. Marks the partition as opened so that
     * {@link #ensurePartitionOpened(int)} becomes a no-op.
     */
    private void addOpenPartitionFrames(int partitionIndex) {
        frameCursor.toPartition(partitionIndex);
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            frameAddressCache.add(frameCount, frame);
            framePartitionIndexes.add(frame.getPartitionIndex());
            frameRowCounts.add(frame.getPartitionHi() - frame.getPartitionLo());
            frameCount++;
        }
        partitionOpened.set(partitionIndex);
    }

    /**
     * Opens a parquet partition and adds fully initialized frame entries.
     * <p>
     * TODO(puzpuzpuz): read row group count from table metadata instead of opening the
     *  parquet file. Once available, pre-compute uninitialized frames like native partitions.
     */
    private void addParquetPartitionFrames(int partitionIndex) {
        tableReader.openPartition(partitionIndex);
        addOpenPartitionFrames(partitionIndex);
    }

    /**
     * Adds an uninitialized frame entry to the flat cache with zero column
     * addresses. The frame structure (format, size, rowIdOffset) is correct;
     * column addresses will be patched by {@link #ensurePartitionOpened(int)}.
     */
    private void addUninitializedFrame(int partitionIndex, long lo, long hi) {
        frameAddressCache.add(frameCount, uninitializedFrame.of(partitionIndex, lo, hi, PartitionFormat.NATIVE));
        framePartitionIndexes.add(partitionIndex);
        frameRowCounts.add(hi - lo);
        frameCount++;
    }

    /**
     * Pre-computes page frame boundaries from table metadata without opening
     * partitions. Populates the flat {@link PageFrameAddressCache} with
     * uninitialized (zero-address) entries. Column addresses are patched
     * later by {@link #ensurePartitionOpened(int)}.
     */
    private void buildFrameCache() {
        if (isFrameCacheBuilt) {
            return;
        }

        partitionCount = tableReader.getPartitionCount();

        frameCount = 0;
        framePartitionIndexes.reopen();
        framePartitionIndexes.clear();
        frameRowCounts.reopen();
        frameRowCounts.clear();

        if (frameCursor.hasIntervalFilter()) {
            // Interval filtering makes frame counts unpredictable from metadata.
            // Fall back to eager enumeration of all page frames.
            buildFrameCacheEagerly();
        } else {
            final ColumnVersionReader columnVersionReader = tableReader.getColumnVersionReader();
            final IntList columnIndexes = this.columnIndexes;
            final int columnCount = columnIndexes.size();

            partitionFirstFrame.setAll(partitionCount, 0);
            partitionOpened.clear();

            for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                partitionFirstFrame.setQuick(partitionIndex, frameCount);

                final long partitionRowCount = tableReader.getPartitionRowCountFromMetadata(partitionIndex);
                if (partitionRowCount <= 0) {
                    continue;
                }

                if (tableReader.getPartitionRowCount(partitionIndex) != -1) {
                    // Partition is already open — iterate page frames eagerly
                    addOpenPartitionFrames(partitionIndex);
                } else {
                    final byte format = tableReader.getPartitionFormatFromMetadata(partitionIndex);
                    if (format == PartitionFormat.NATIVE) {
                        addNativePartitionFrames(
                                columnVersionReader,
                                columnIndexes,
                                columnCount,
                                partitionIndex,
                                tableReader.getPartitionTimestampByIndex(partitionIndex),
                                partitionRowCount
                        );
                    } else {
                        addParquetPartitionFrames(partitionIndex);
                    }
                }
            }
        }

        isFrameCacheBuilt = true;

        // Initialize timestamp cache (2 entries per frame: tsLo, tsHi)
        final int cacheSize = 2 * frameCount;
        if (cacheSize > 0) {
            frameTimestampCache.setCapacity(cacheSize);
            frameTimestampCache.clear();
            for (int i = 0; i < cacheSize; i++) {
                frameTimestampCache.set(i, Numbers.LONG_NULL);
            }
        }
    }

    /**
     * Eagerly iterates all page frames and adds them with real column
     * addresses. Used when the cursor has interval filtering.
     */
    private void buildFrameCacheEagerly() {
        frameCursor.toTop();
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            frameAddressCache.add(frameCount, frame);
            framePartitionIndexes.add(frame.getPartitionIndex());
            frameRowCounts.add(frame.getPartitionHi() - frame.getPartitionLo());
            frameCount++;
        }
        // Mark all partitions as opened so ensurePartitionOpened is a no-op
        for (int i = 0; i < partitionCount; i++) {
            partitionOpened.set(i);
        }
    }

    /**
     * Opens a partition lazily if not already opened, patching the
     * uninitialized cache entries with real column addresses.
     */
    private void ensurePartitionOpened(int partitionIndex) {
        if (partitionOpened.get(partitionIndex)) {
            return;
        }

        frameCursor.toPartition(partitionIndex);
        int globalFrame = partitionFirstFrame.getQuick(partitionIndex);
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            frameAddressCache.updateAddresses(globalFrame, frame);
            globalFrame++;
        }
        int expectedEnd = (partitionIndex + 1 < partitionCount) ? partitionFirstFrame.getQuick(partitionIndex + 1) : frameCount;
        assert globalFrame == expectedEnd : "frame count mismatch for partition " + partitionIndex + ": expected " + expectedEnd + " but got " + globalFrame;
        partitionOpened.set(partitionIndex);
    }

    // maxTimestampHi is used to handle split partitions correctly as ceil method
    // will return the same value for all split partitions
    static long estimatePartitionHi(TimestampDriver.TimestampCeilMethod partitionCeilMethod, long partitionTimestamp, long maxTimestampHi) {
        // partitionCeilMethod is null in case of partition by NONE
        long partitionHi = partitionCeilMethod != null ? partitionCeilMethod.ceil(partitionTimestamp) : Long.MAX_VALUE;
        return Math.min(partitionHi, maxTimestampHi);
    }
}
