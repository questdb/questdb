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

import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;

import java.util.concurrent.atomic.AtomicIntegerArray;

import static io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor.populatePartitionTimestamps;

/**
 * Shared state across all concurrent time frame cursors for a query.
 * <p>
 * Pre-computes exact page frame boundaries from table metadata (column tops,
 * row counts, partition formats) WITHOUT opening partitions. Builds an
 * uninitialized flat {@link PageFrameAddressCache} with zero column addresses.
 * Column addresses are patched lazily per partition on first access via
 * {@link #ensurePartitionOpened(int)}.
 * <p>
 * The flat cache preserves the master branch's "1 time frame = 1 page frame"
 * model so that per-row operations in the cursor are trivial
 * ({@code record.setRowIndex(rowIndex)}).
 * <p>
 * Thread safety: {@link #ensurePartitionOpened(int)} uses double-checked
 * locking with {@link AtomicIntegerArray} for acquire/release fences.
 */
public class ConcurrentTimeFrameState implements QuietCloseable, Mutable {
    private final PageFrameAddressCache addressCache = new PageFrameAddressCache();
    private final LongList columnTops = new LongList();
    private final DirectIntList framePartitionIndexes;
    private final DirectLongList frameRowCounts;
    private final Object openLock = new Object();
    private final LongList partitionCeilings = new LongList();
    private final LongList partitionTimestamps = new LongList();
    private final UninitializedPageFrame uninitializedFrame = new UninitializedPageFrame();
    private int frameCount;
    private TablePageFrameCursor frameCursor;
    private int partitionCount;
    // Per-partition: first global frame index (populated during of())
    private int[] partitionFirstFrame;
    private AtomicIntegerArray partitionOpened;

    public ConcurrentTimeFrameState() {
        try {
            this.framePartitionIndexes = new DirectIntList(64, MemoryTag.NATIVE_DEFAULT, true);
            this.frameRowCounts = new DirectLongList(64, MemoryTag.NATIVE_DEFAULT, true);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        addressCache.clear();
        frameCursor = null;
    }

    @Override
    public void close() {
        Misc.free(addressCache);
        Misc.free(framePartitionIndexes);
        Misc.free(frameRowCounts);
        frameCursor = null;
    }

    /**
     * Opens a partition lazily if not already opened, patching the
     * uninitialized cache entries with real column addresses.
     */
    public void ensurePartitionOpened(int partitionIndex) {
        if (partitionOpened.get(partitionIndex) != 0) { // acquire fence
            return;
        }
        synchronized (openLock) {
            if (partitionOpened.get(partitionIndex) != 0) {
                return;
            }
            frameCursor.toPartition(partitionIndex);
            int globalFrame = partitionFirstFrame[partitionIndex];
            PageFrame frame;
            while ((frame = frameCursor.next()) != null) {
                addressCache.updateAddresses(globalFrame, frame);
                globalFrame++;
            }
            partitionOpened.set(partitionIndex, 1); // release fence
        }
    }

    public PageFrameAddressCache getAddressCache() {
        return addressCache;
    }

    public int getFrameCount() {
        return frameCount;
    }

    public DirectIntList getFramePartitionIndexes() {
        return framePartitionIndexes;
    }

    public DirectLongList getFrameRowCounts() {
        return frameRowCounts;
    }

    public LongList getPartitionCeilings() {
        return partitionCeilings;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public LongList getPartitionTimestamps() {
        return partitionTimestamps;
    }

    /**
     * Initializes the state by pre-computing page frame boundaries from table
     * metadata and building an uninitialized flat address cache with zero
     * column addresses.
     *
     * @param frameCursor      page frame cursor (used for lazy partition opening)
     * @param metadata         slave table metadata
     * @param columnIndexes    query-to-reader column index mapping
     * @param isExternal       whether the cursor wraps an external data source
     * @param pageFrameMinRows min rows per page frame (from SqlExecutionContext)
     * @param pageFrameMaxRows max rows per page frame (from SqlExecutionContext)
     * @param workerCount      shared query worker count
     */
    public ConcurrentTimeFrameState of(
            TablePageFrameCursor frameCursor,
            RecordMetadata metadata,
            IntList columnIndexes,
            boolean isExternal,
            int pageFrameMinRows,
            int pageFrameMaxRows,
            int workerCount
    ) {
        this.frameCursor = frameCursor;

        populatePartitionTimestamps(frameCursor, partitionTimestamps, partitionCeilings);
        partitionCount = partitionTimestamps.size();

        // Initialize the address cache structure (no frames added yet)
        addressCache.of(metadata, columnIndexes, isExternal);
        framePartitionIndexes.reopen();
        framePartitionIndexes.clear();
        frameRowCounts.reopen();
        frameRowCounts.clear();

        // Allocate per-partition tracking arrays
        if (partitionFirstFrame == null || partitionFirstFrame.length < partitionCount) {
            partitionFirstFrame = new int[partitionCount];
        }
        if (partitionOpened == null || partitionOpened.length() < partitionCount) {
            partitionOpened = new AtomicIntegerArray(partitionCount);
        } else {
            for (int i = 0; i < partitionCount; i++) {
                partitionOpened.set(i, 0);
            }
        }

        final TableReader tableReader = frameCursor.getTableReader();
        frameCount = 0;

        if (frameCursor.hasIntervalFilter()) {
            // Interval filtering makes frame counts unpredictable from metadata.
            // Fall back to eager enumeration of all page frames (like master).
            buildFrameCacheEagerly();
        } else {
            // Pre-compute frame boundaries for all partitions.
            // If a partition is already open in the table reader, iterate its
            // page frames eagerly (zero contention, no lazy open overhead).
            // Otherwise, pre-compute from metadata and add uninitialized entries.
            final ColumnVersionReader columnVersionReader = tableReader.getColumnVersionReader();
            final int columnCount = columnIndexes.size();

            for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                partitionFirstFrame[partitionIndex] = frameCount;

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
                                tableReader,
                                columnVersionReader,
                                columnIndexes,
                                columnCount,
                                partitionIndex,
                                tableReader.getPartitionTimestampByIndex(partitionIndex),
                                partitionRowCount,
                                pageFrameMinRows,
                                pageFrameMaxRows,
                                workerCount
                        );
                    } else {
                        // TODO(puzpuzpuz): read row group count from table metadata instead of
                        //  opening the parquet file. This will be addressed in a follow-up PR.
                        addParquetPartitionFrames(tableReader, partitionIndex);
                    }
                }
            }
        }

        return this;
    }

    /**
     * Pre-computes and adds uninitialized frame entries for a native partition.
     * Replicates the column-top-aware splitting logic from
     * FwdTableReaderPageFrameCursor#computeNativeFrame().
     */
    private void addNativePartitionFrames(
            TableReader tableReader,
            ColumnVersionReader columnVersionReader,
            IntList columnIndexes,
            int columnCount,
            int partitionIndex,
            long partitionTimestamp,
            long partitionRowCount,
            int pageFrameMinRows,
            int pageFrameMaxRows,
            int workerCount
    ) {
        final long pageFrameRowLimit = FwdTableReaderPageFrameCursor.calculatePageFrameRowLimit(
                0,
                partitionRowCount,
                pageFrameMinRows,
                pageFrameMaxRows,
                workerCount
        );

        // Pre-fetch column tops for this partition from column version metadata.
        // A column top > partitionLo and < adjustedHi causes a frame split.
        // Columns that don't exist in this partition have top = partitionRowCount
        // (all-null, no constraint on frame boundary).
        // Use reader metadata (not factory metadata) for writer index lookup,
        // because factory metadata (e.g. SelectedRecordCursorFactory) may not
        // implement getWriterIndex().
        final RecordMetadata readerMetadata = tableReader.getMetadata();
        columnTops.clear();
        for (int i = 0; i < columnCount; i++) {
            final int readerColumnIndex = columnIndexes.getQuick(i);
            final int writerIndex = readerMetadata.getWriterIndex(readerColumnIndex);
            final int recordIndex = columnVersionReader.getRecordIndex(partitionTimestamp, writerIndex);
            if (recordIndex > -1) {
                columnTops.add(columnVersionReader.getColumnTopByIndex(recordIndex));
            } else if (columnVersionReader.getColumnTopPartitionTimestamp(writerIndex) <= partitionTimestamp) {
                columnTops.add(0); // column exists from start, no top
            } else {
                columnTops.add(partitionRowCount); // column doesn't exist — all-null
            }
        }

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
            addressCache.add(frameCount, frame);
            framePartitionIndexes.add(frame.partitionIndex());
            frameRowCounts.add(frame.getPartitionHi() - frame.getPartitionLo());
            frameCount++;
        }
        partitionOpened.set(partitionIndex, 1);
    }

    /**
     * Opens a parquet partition and adds fully initialized frame entries.
     * <p>
     * TODO(puzpuzpuz): read row group count from table metadata instead of opening the
     *  parquet file. Once available, pre-compute uninitialized frames like native partitions.
     */
    private void addParquetPartitionFrames(TableReader reader, int partitionIndex) {
        reader.openPartition(partitionIndex);
        addOpenPartitionFrames(partitionIndex);
    }

    /**
     * Adds an uninitialized frame entry to the flat cache with zero column
     * addresses. The frame structure (format, size, rowIdOffset) is correct;
     * column addresses will be patched by {@link #ensurePartitionOpened(int)}.
     */
    private void addUninitializedFrame(int partitionIndex, long lo, long hi) {
        addressCache.add(frameCount, uninitializedFrame.of(partitionIndex, lo, hi, PartitionFormat.NATIVE));
        framePartitionIndexes.add(partitionIndex);
        frameRowCounts.add(hi - lo);
        frameCount++;
    }

    /**
     * Eagerly iterates all page frames and adds them with real column
     * addresses. Used when the cursor has interval filtering, which
     * makes frame counts unpredictable from metadata alone.
     */
    private void buildFrameCacheEagerly() {
        frameCursor.toTop();
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            addressCache.add(frameCount, frame);
            framePartitionIndexes.add(frame.partitionIndex());
            frameRowCounts.add(frame.getPartitionHi() - frame.getPartitionLo());
            frameCount++;
        }
        // Mark all partitions as opened so ensurePartitionOpened is a no-op
        for (int i = 0; i < partitionCount; i++) {
            partitionOpened.set(i, 1);
        }
    }

}
