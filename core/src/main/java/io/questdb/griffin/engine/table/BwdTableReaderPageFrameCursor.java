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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.PartitionFrameState;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.NullMemoryCMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;


public class BwdTableReaderPageFrameCursor implements TablePageFrameCursor {
    private final int columnCount;
    private final IntList columnIndexes;
    private final ColumnMapping columnMapping = new ColumnMapping();
    private final LongList columnPageAddresses = new LongList();
    private final LongList columnPageTops = new LongList();
    // Reader column indexes of the non-timestamp dedup keys, and their pages for the current
    // custom-decode window as PageFrame.DEDUP_KEY_PAGE_CELLS longs per key.
    private final IntList dedupKeyColumns = new IntList();
    private final LongList dedupKeyPages = new LongList();
    private final IntList columnSizeShifts;
    private final DirectLongList filterList;
    private final MemoryCARWImpl filterValues;
    private final TableReaderPageFrame frame = new TableReaderPageFrame();
    private final LongList pageSizes = new LongList();
    private final @Nullable ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions;
    private final int sharedQueryWorkerCount;
    private int cachedRowGroupIndex = -1;
    private long cachedRowGroupStartRow = 0;
    private long filterBufEnd = -1;
    // Track the highest partition index that has not been released yet
    private int highestOpenPartitionIndex = -1;
    private int pageFrameMaxRows;
    private int pageFrameMinRows;
    private PartitionFrameCursor partitionFrameCursor;
    private int preparedNativeBaseWindow = -1;
    private TableReader reader;
    private long reenterPageFrameRowLimit;
    private ParquetPartitionDecoder reenterParquetDecoder;
    private byte reenterPartitionFormat;
    private boolean reenterPartitionFrame = false;
    private long reenterPartitionFrameState;
    private long reenterPartitionHi;
    private int reenterPartitionIndex;
    private long reenterPartitionLo;
    private long remainingRowsInInterval;
    private long timestampPageAddress;
    private long timestampPageSize;
    private long timestampPageTop;

    public BwdTableReaderPageFrameCursor(
            IntList columnIndexes,
            IntList columnSizeShifts,
            @Nullable ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions,
            int sharedQueryWorkerCount
    ) {
        this.columnIndexes = columnIndexes;
        this.columnSizeShifts = columnSizeShifts;
        this.columnCount = columnIndexes.size();
        this.sharedQueryWorkerCount = sharedQueryWorkerCount;
        this.pushdownFilterConditions = pushdownFilterConditions;
        if (pushdownFilterConditions != null && pushdownFilterConditions.size() > 0) {
            this.filterList = new DirectLongList(
                    (long) pushdownFilterConditions.size() * ParquetRowGroupFilter.LONGS_PER_FILTER,
                    MemoryTag.NATIVE_PARQUET_PARTITION_DECODER,
                    true
            );
            this.filterValues = new MemoryCARWImpl(
                    ParquetRowGroupFilter.FILTER_BUFFER_PAGE_SIZE,
                    ParquetRowGroupFilter.FILTER_BUFFER_MAX_PAGES,
                    MemoryTag.NATIVE_PARQUET_PARTITION_DECODER
            );
        } else {
            this.filterList = null;
            this.filterValues = null;
        }
    }

    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        partitionFrameCursor.calculateSize(counter);
    }

    @Override
    public void close() {
        partitionFrameCursor = Misc.free(partitionFrameCursor);
        Misc.free(filterList);
        Misc.free(filterValues);
    }

    @Override
    public ColumnMapping getColumnMapping() {
        return columnMapping;
    }

    @Override
    public LongList getIntervals() {
        return partitionFrameCursor != null ? partitionFrameCursor.getIntervals() : null;
    }

    @Override
    public long getRemainingRowsInInterval() {
        return remainingRowsInInterval;
    }

    @Override
    public StaticSymbolTable getSymbolTable(int columnIndex) {
        return reader.getSymbolTable(columnIndexes.getQuick(columnIndex));
    }

    @Override
    public TableReader getTableReader() {
        return reader;
    }

    @Override
    public boolean hasActivePushdownFilter() {
        return pushdownFilterConditions != null && pushdownFilterConditions.size() > 0;
    }

    @Override
    public boolean hasCustomFrames() {
        return reader.hasAnyDelta();
    }

    @Override
    public boolean hasInexactFrames() {
        return reader.hasInexactRowCount();
    }

    @Override
    public boolean hasIntervalFilter() {
        return partitionFrameCursor != null && partitionFrameCursor.hasIntervalFilter();
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return reader.newSymbolTable(columnIndexes.getQuick(columnIndex));
    }

    @Override
    public @Nullable PageFrame next(long skipTarget) {
        while (true) {
            if (reenterPartitionFrame) {
                if (reenterParquetDecoder != null || reenterPartitionFrameState != 0) {
                    final TableReaderPageFrame result = reenterPartitionFormat == PartitionFormat.PARQUET
                            ? computeParquetFrame(reenterPartitionLo, reenterPartitionHi)
                            : computeNativeDeltaFrame(reenterPartitionLo, reenterPartitionHi);
                    if (result != null) {
                        return result;
                    }
                    // all remaining row groups in this partition were skipped, try next partition
                    continue;
                } else {
                    return computeNativeFrame(reenterPartitionLo, reenterPartitionHi, skipTarget);
                }
            }

            final PartitionFrame partitionFrame = partitionFrameCursor.next(skipTarget);
            if (partitionFrame != null) {
                reenterPartitionIndex = partitionFrame.getPartitionIndex();
                // Track highest partition index seen (for backward cursor, first partition seen has highest index)
                if (highestOpenPartitionIndex < 0) {
                    highestOpenPartitionIndex = reenterPartitionIndex;
                }
                final long lo = partitionFrame.getRowLo();
                final long hi = partitionFrame.getRowHi();
                final long partitionFrameState = partitionFrame.getPartitionFrameState();
                final boolean hasCustomFrames = partitionFrameState != 0;

                if (hi - lo <= skipTarget && !hasCustomFrames) {
                    frame.partitionIndex = reenterPartitionIndex;
                    frame.partitionLo = lo;
                    frame.partitionHi = hi;
                    frame.pieceShift = 0;
                    frame.skipSkeleton = true;
                    frame.format = partitionFrame.getPartitionFormat();
                    frame.rowGroupIndex = -1;
                    frame.rowGroupLo = -1;
                    frame.rowGroupHi = -1;
                    final ParquetPartitionDecoder partitionDecoder = partitionFrame.getParquetMetaDecoder();
                    frame.parquetMetaDecoder = frame.format == PartitionFormat.PARQUET ? partitionDecoder : null;
                    frame.partitionFrameState = 0;
                    return frame;
                }
                final TableReaderPageFrame result = nextSlow(partitionFrame, lo, hi, skipTarget);
                if (result != null) {
                    return result;
                }
                // all row groups in this parquet partition were skipped, try next partition
                continue;
            }
            return null;
        }
    }

    @Override
    public TablePageFrameCursor of(SqlExecutionContext executionContext, PartitionFrameCursor partitionFrameCursor) throws SqlException {
        this.partitionFrameCursor = partitionFrameCursor;
        this.reader = partitionFrameCursor.getTableReader();
        TablePageFrameCursor.buildColumnMapping(columnMapping, columnIndexes, reader.getMetadata());
        dedupKeyColumns.clear();
        dedupKeyPages.clear();
        this.pageFrameMinRows = executionContext.getPageFrameMinRows();
        this.pageFrameMaxRows = executionContext.getPageFrameMaxRows();
        if (pushdownFilterConditions != null) {
            for (int i = 0, n = pushdownFilterConditions.size(); i < n; i++) {
                pushdownFilterConditions.getQuick(i).init(executionContext);
            }
        }
        toTop();
        return this;
    }

    @Override
    public void releaseOpenPartitions() {
        // Guard against being called before next() or after toTop() when no partitions are open.
        // highestOpenPartitionIndex is -1 until the first partition is opened by next().
        if (highestOpenPartitionIndex < 0 || highestOpenPartitionIndex <= reenterPartitionIndex) {
            return;
        }
        // Close all partitions from highestOpenPartitionIndex down to (but not including) current partition
        // Backward cursor scans from high to low partition indices
        for (int i = highestOpenPartitionIndex; i > reenterPartitionIndex; i--) {
            reader.closePartitionByIndex(i);
        }
        highestOpenPartitionIndex = reenterPartitionIndex;
    }

    @Override
    public long size() {
        return partitionFrameCursor.size();
    }

    @Override
    public boolean supportsSizeCalculation() {
        return partitionFrameCursor.supportsSizeCalculation();
    }

    @Override
    public void toTop() {
        partitionFrameCursor.toTop();
        reenterPartitionFrame = false;
        reenterParquetDecoder = null;
        clearReenterPartitionFrameState();
        highestOpenPartitionIndex = -1;
        remainingRowsInInterval = 0;
        cachedRowGroupIndex = -1;
        cachedRowGroupStartRow = 0;
        filterBufEnd = -1;
        clearAddresses();
    }

    private long adjustNativeFrameLo(int base, long physicalLo, long physicalHi) {
        long adjustedLo = physicalLo;
        for (int i = 0; i < columnCount; i++) {
            final int columnIndex = columnIndexes.getQuick(i);
            final long top = reader.getColumnTop(base, columnIndex);
            if (top > adjustedLo && top < physicalHi) {
                adjustedLo = top;
            }
        }
        return adjustedLo;
    }

    private void clearAddresses() {
        columnPageAddresses.setAll(2 * columnCount, 0);
        columnPageTops.setAll(columnCount, 0);
        pageSizes.setAll(2 * columnCount, -1);
        preparedNativeBaseWindow = -1;
        timestampPageAddress = 0;
        timestampPageSize = 0;
        timestampPageTop = 0;
        clearDedupKeyPages();
    }

    private void clearDedupKeyPages() {
        for (int key = 0, n = dedupKeyColumns.size(); key < n; key++) {
            final int offset = key * PageFrame.DEDUP_KEY_PAGE_CELLS;
            for (int cell = PageFrame.DEDUP_KEY_PAGE_DATA_ADDRESS; cell < PageFrame.DEDUP_KEY_PAGE_CELLS; cell++) {
                dedupKeyPages.setQuick(offset + cell, 0);
            }
        }
    }

    private void clearReenterPartitionFrameState() {
        reenterPartitionFrameState = 0;
    }

    private @Nullable TableReaderPageFrame computeNativeDeltaFrame(long partitionLo, long partitionHi) {
        final int base = reader.getColumnBase(reenterPartitionIndex);
        final int windowCount = PartitionFrameState.getWindowCount(reenterPartitionFrameState);
        long baseStartRow = 0;
        long windowStartRow = 0;
        for (int window = 0; window < windowCount; window++) {
            final long baseRows = PartitionFrameState.getBaseRowCount(reenterPartitionFrameState, window);
            final long logicalRows = PartitionFrameState.getLogicalRowCount(reenterPartitionFrameState, window);
            final long windowEndRow = Math.addExact(windowStartRow, logicalRows);
            if (partitionHi > windowStartRow && partitionHi <= windowEndRow) {
                final long adjustedLo;
                final boolean requiresMaterialization = PartitionFrameState.requiresMaterialization(reenterPartitionFrameState, window);
                if (requiresMaterialization) {
                    if (preparedNativeBaseWindow != window) {
                        prepareNativePartitionFrameBaseColumns(base, window);
                    }
                    final long subframeSize = PartitionFrameState.getSubframeSize(reenterPartitionFrameState);
                    final long localHi = partitionHi - windowStartRow;
                    final long canonicalLo = Math.multiplyExact(
                            Math.floorDiv(localHi - 1, subframeSize),
                            subframeSize
                    );
                    adjustedLo = Math.max(partitionLo, Math.addExact(windowStartRow, canonicalLo));
                } else {
                    preparedNativeBaseWindow = -1;
                    final long frameLimit = reenterPageFrameRowLimit > 0
                            ? partitionHi - reenterPageFrameRowLimit
                            : Long.MIN_VALUE;
                    final long requestedLo = Math.max(Math.max(partitionLo, windowStartRow), frameLimit);
                    final long physicalHi = Math.addExact(baseStartRow, partitionHi - windowStartRow);
                    final long requestedPhysicalLo = physicalHi - (partitionHi - requestedLo);
                    final long adjustedPhysicalLo = adjustNativeFrameLo(base, requestedPhysicalLo, physicalHi);
                    adjustedLo = partitionHi - (physicalHi - adjustedPhysicalLo);
                    fillNativeAddresses(base, adjustedPhysicalLo, physicalHi);
                }
                final int rowGroupLo = TablePageFrameCursor.toWindowRow(reader, reenterPartitionIndex, window, adjustedLo - windowStartRow);
                final int rowGroupHi = TablePageFrameCursor.toWindowRow(reader, reenterPartitionIndex, window, partitionHi - windowStartRow);

                if (adjustedLo > partitionLo) {
                    reenterPartitionLo = partitionLo;
                    reenterPartitionHi = adjustedLo;
                    reenterPartitionFrame = true;
                } else {
                    reenterPartitionFrame = false;
                }

                remainingRowsInInterval = adjustedLo - partitionLo;
                frame.pieceShift = 0;
                frame.skipSkeleton = false;
                frame.format = PartitionFormat.NATIVE;
                frame.parquetMetaDecoder = null;
                setFramePartitionFrameState(requiresMaterialization);
                frame.partitionHi = partitionHi;
                frame.partitionIndex = reenterPartitionIndex;
                frame.partitionLo = adjustedLo;
                frame.rowGroupHi = rowGroupHi;
                frame.rowGroupIndex = window;
                frame.rowGroupLo = rowGroupLo;
                return frame;
            }
            baseStartRow = Math.addExact(baseStartRow, baseRows);
            windowStartRow = windowEndRow;
        }
        reenterPartitionFrame = false;
        return null;
    }

    private TableReaderPageFrame computeNativeFrame(long partitionLo, long partitionHi, long skipTarget) {
        if (skipTarget > 0) {
            return computeNativeSkipFrame(partitionLo, partitionHi, skipTarget);
        }

        final int base = reader.getColumnBase(reenterPartitionIndex);

        // we may need to split this partition frame either along "top" lines, or along
        // max page frame sizes; to do this, we calculate min top value from given position
        long adjustedLo = Math.max(partitionLo, partitionHi - reenterPageFrameRowLimit);

        // A COMPOSITE partition is several PIECES over one set of column files, each sitting at its own place in them,
        // so a frame is cut at piece boundaries and carries that piece's SHIFT - the term that turns a partition row.
        long pieceShift = 0;
        if (reader.getTxFile().isPartitionComposite(reenterPartitionIndex)) {
            final PartitionGeometry geometry = reader.getGeometry();
            final int piece = geometry.findPieceByRow(reenterPartitionIndex, partitionHi - 1);
            pieceShift = geometry.getPieceShift(reenterPartitionIndex, piece);
            final long pieceLo = geometry.getPieceCumulativeLo(reenterPartitionIndex, piece);
            if (pieceLo > adjustedLo && pieceLo < partitionHi) {
                adjustedLo = pieceLo;
            }
        }

        // Split along the column tops, which has to happen AFTER the piece is known: a top is a FILE row and
        // adjustedLo/partitionHi are PARTITION rows, so the two are only comparable once the piece's shift is in hand.
        for (int i = 0; i < columnCount; i++) {
            final int columnIndex = columnIndexes.getQuick(i);
            final long top = reader.getColumnTop(base, columnIndex) - pieceShift;
            if (top > adjustedLo && top < partitionHi) {
                adjustedLo = top;
            }
        }

        for (int i = 0; i < columnCount; i++) {
            final int columnIndex = columnIndexes.getQuick(i);
            final int readerColIndex = TableReader.getPrimaryColumnIndex(base, columnIndex);
            final MemoryR colMem = reader.getColumn(readerColIndex);
            // When the entire column is NULL we make it skip the whole of the partition frame, by handing the
            // arithmetic below a top that cancels the frame's own high row exactly.
            final long top = colMem instanceof NullMemoryCMR
                    ? partitionHi + pieceShift
                    : reader.getColumnTop(base, columnIndex);
            final long partitionLoAdjusted = adjustedLo + pieceShift - top;
            final long partitionHiAdjusted = partitionHi + pieceShift - top;
            final int sh = columnSizeShifts.getQuick(i);

            if (partitionHiAdjusted > 0) {
                if (sh > -1) {
                    // this assumes reader uses single page to map the whole column
                    // non-negative sh means fixed length column
                    final long address = colMem.getPageAddress(0);
                    final long addressSize = partitionHiAdjusted << sh;
                    final long offset = partitionLoAdjusted << sh;
                    columnPageAddresses.setQuick(2 * i, address + offset);
                    pageSizes.setQuick(2 * i, addressSize - offset);
                } else {
                    final int columnType = reader.getMetadata().getColumnType(columnIndex);
                    final ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);
                    final MemoryR auxCol = reader.getColumn(readerColIndex + 1);
                    final long auxAddress = auxCol.getPageAddress(0);
                    final long auxOffsetLo = columnTypeDriver.getAuxVectorOffset(partitionLoAdjusted);
                    final long auxOffsetHi = columnTypeDriver.getAuxVectorOffset(partitionHiAdjusted);

                    final long dataSize = columnTypeDriver.getDataVectorSizeAt(auxAddress, partitionHiAdjusted - 1);
                    // some var-size columns may not have data memory (fully inlined)
                    final long dataAddress = dataSize > 0 ? colMem.getPageAddress(0) : 0;

                    columnPageAddresses.setQuick(2 * i, dataAddress);
                    columnPageAddresses.setQuick(2 * i + 1, auxAddress + auxOffsetLo);
                    pageSizes.setQuick(2 * i, dataSize);
                    pageSizes.setQuick(2 * i + 1, auxOffsetHi - auxOffsetLo);
                }
            } else { // column top
                columnPageAddresses.setQuick(2 * i, 0);
                columnPageAddresses.setQuick(2 * i + 1, 0);
                // data page size is used by VectorAggregateFunction as the size hint
                // in the following way:
                //   size = page_size >>> column_size_hint
                // (for var-sized types column_size_hint is 0)
                pageSizes.setQuick(2 * i, (partitionHiAdjusted - partitionLoAdjusted) << (sh > -1 ? sh : 0));
                pageSizes.setQuick(2 * i + 1, 0);
            }
        }

        // it is possible that all columns in partition frame are empty, but it doesn't mean
        // the partition frame size is 0; sometimes we may want to imply nulls
        if (partitionLo < adjustedLo) {
            this.reenterPartitionLo = partitionLo;
            this.reenterPartitionHi = adjustedLo;
            this.reenterPartitionFrame = true;
        } else {
            this.reenterPartitionFrame = false;
        }

        // remaining rows in the partition = max row number in the frame - min row number of the partition
        remainingRowsInInterval = adjustedLo - partitionLo;

        frame.partitionLo = adjustedLo;
        frame.partitionHi = partitionHi;
        frame.pieceShift = pieceShift;
        frame.skipSkeleton = false;
        frame.partitionFrameState = 0;
        frame.parquetMetaDecoder = null;
        frame.format = PartitionFormat.NATIVE;
        frame.rowGroupIndex = -1;
        frame.rowGroupLo = -1;
        frame.rowGroupHi = -1;
        frame.partitionIndex = reenterPartitionIndex;
        return frame;
    }

    private TableReaderPageFrame computeNativeSkipFrame(long partitionLo, long partitionHi, long skipTarget) {
        final long skipLo = Math.max(partitionLo, partitionHi - skipTarget);
        // Publish no addresses at all rather than the previous frame's: a skip-only frame still reaches
        // PageFrameAddressCache, and a stale-but-plausible address there would read the wrong rows if anything
        // ever navigated to it, whereas a zero address cannot be mistaken for a live one.
        clearAddresses();

        if (partitionLo < skipLo) {
            this.reenterPartitionLo = partitionLo;
            this.reenterPartitionHi = skipLo;
            this.reenterPartitionFrame = true;
        } else {
            this.reenterPartitionFrame = false;
        }
        remainingRowsInInterval = skipLo - partitionLo;

        frame.partitionLo = skipLo;
        frame.partitionHi = partitionHi;
        // A skeleton may span several pieces, so no single shift describes it. Nothing reads a shift off a frame
        // the caller discards, and 0 is what the whole-partition skeleton in next(long) publishes.
        frame.pieceShift = 0;
        frame.skipSkeleton = true;
        frame.partitionFrameState = 0;
        frame.parquetMetaDecoder = null;
        frame.format = PartitionFormat.NATIVE;
        frame.rowGroupIndex = -1;
        frame.rowGroupLo = -1;
        frame.rowGroupHi = -1;
        frame.partitionIndex = reenterPartitionIndex;
        return frame;
    }

    private @Nullable TableReaderPageFrame computeParquetFrame(long partitionLo, long partitionHi) {
        final ParquetMetaFileReader metadata = reenterParquetDecoder.metadata();
        final int rowGroupCount = reenterPartitionFrameState == 0
                ? metadata.getRowGroupCount()
                : PartitionFrameState.getWindowCount(reenterPartitionFrameState);

        if (reenterPartitionFrameState == 0 && partitionHi > metadata.getPartitionRowCount()) {
            throw CairoException.critical(0)
                    .put("parquet partition row count mismatch [partitionHi=").put(partitionHi)
                    .put(", parquetRowCount=").put(metadata.getPartitionRowCount())
                    .put(", partitionIndex=").put(reenterPartitionIndex)
                    .put(']');
        }

        int targetGroup = -1;
        long targetGroupStart = 0;

        // Use cached position if available
        if (cachedRowGroupIndex >= 0 && cachedRowGroupIndex < rowGroupCount) {
            final long cachedBaseGroupSize = metadata.getRowGroupSize(cachedRowGroupIndex);
            final long cachedGroupSize = reenterPartitionFrameState == 0
                    ? cachedBaseGroupSize
                    : PartitionFrameState.getLogicalRowCount(reenterPartitionFrameState, cachedRowGroupIndex);
            final long cachedGroupEndRow = cachedRowGroupStartRow + cachedGroupSize;
            if (partitionHi > cachedRowGroupStartRow && partitionHi <= cachedGroupEndRow) {
                // partitionHi is within the cached row group, use it directly
                targetGroup = cachedRowGroupIndex;
                targetGroupStart = cachedRowGroupStartRow;
            } else if (partitionHi <= cachedRowGroupStartRow) {
                // partitionHi moved to earlier row groups, search backward from cache
                long rowGroupEndRow = cachedRowGroupStartRow;
                for (int i = cachedRowGroupIndex - 1; i >= 0; i--) {
                    final long baseRowGroupSize = metadata.getRowGroupSize(i);
                    final long rowGroupSize = reenterPartitionFrameState == 0
                            ? baseRowGroupSize
                            : PartitionFrameState.getLogicalRowCount(reenterPartitionFrameState, i);
                    final long rowGroupStartRow = rowGroupEndRow - rowGroupSize;
                    if (partitionHi > rowGroupStartRow) {
                        targetGroup = i;
                        targetGroupStart = rowGroupStartRow;
                        cachedRowGroupIndex = i;
                        cachedRowGroupStartRow = rowGroupStartRow;
                        break;
                    }
                    rowGroupEndRow = rowGroupStartRow;
                }
            }
        }

        // fallback
        if (targetGroup < 0) {
            long rowGroupStartRow = 0;
            for (int i = 0; i < rowGroupCount; i++) {
                final long baseRowGroupSize = metadata.getRowGroupSize(i);
                final long rowGroupSize = reenterPartitionFrameState == 0
                        ? baseRowGroupSize
                        : PartitionFrameState.getLogicalRowCount(reenterPartitionFrameState, i);
                final long rowGroupEndRow = rowGroupStartRow + rowGroupSize;
                if (partitionHi <= rowGroupEndRow) {
                    targetGroup = i;
                    targetGroupStart = rowGroupStartRow;
                    cachedRowGroupIndex = i;
                    cachedRowGroupStartRow = rowGroupStartRow;
                    break;
                }
                rowGroupStartRow = rowGroupEndRow;
            }
        }

        if (targetGroup < 0) {
            this.reenterPartitionFrame = false;
            return null;
        }

        while (targetGroup >= 0) {
            if ((reenterPartitionFrameState == 0 || !PartitionFrameState.requiresMaterialization(reenterPartitionFrameState, targetGroup))
                    && filterBufEnd != -1 && ParquetRowGroupFilter.canSkipRowGroup(
                    targetGroup,
                    metadata,
                    filterList,
                    filterBufEnd
            )) {
                partitionHi = targetGroupStart;
                if (partitionHi <= partitionLo) {
                    this.reenterPartitionFrame = false;
                    return null;
                }
                targetGroup--;
                if (targetGroup >= 0) {
                    final long baseRowGroupSize = metadata.getRowGroupSize(targetGroup);
                    targetGroupStart -= reenterPartitionFrameState == 0
                            ? baseRowGroupSize
                            : PartitionFrameState.getLogicalRowCount(reenterPartitionFrameState, targetGroup);
                }
                continue;
            }

            final boolean requiresMaterialization = reenterPartitionFrameState != 0
                    && PartitionFrameState.requiresMaterialization(reenterPartitionFrameState, targetGroup);
            final long adjustedLo;
            if (requiresMaterialization) {
                final long subframeSize = PartitionFrameState.getSubframeSize(reenterPartitionFrameState);
                final long localHi = partitionHi - targetGroupStart;
                final long canonicalLo = Math.multiplyExact(
                        Math.floorDiv(localHi - 1, subframeSize),
                        subframeSize
                );
                adjustedLo = Math.max(partitionLo, Math.addExact(targetGroupStart, canonicalLo));
            } else {
                final long frameLimitedLo = reenterPageFrameRowLimit > 0
                        ? partitionHi - reenterPageFrameRowLimit
                        : Long.MIN_VALUE;
                adjustedLo = Math.max(Math.max(partitionLo, targetGroupStart), frameLimitedLo);
            }
            final int rowGroupLo = TablePageFrameCursor.toWindowRow(reader, reenterPartitionIndex, targetGroup, adjustedLo - targetGroupStart);
            final int rowGroupHi = TablePageFrameCursor.toWindowRow(reader, reenterPartitionIndex, targetGroup, partitionHi - targetGroupStart);
            if (adjustedLo > partitionLo) {
                this.reenterPartitionLo = partitionLo;
                this.reenterPartitionHi = adjustedLo;
                this.reenterPartitionFrame = true;
            } else {
                this.reenterPartitionFrame = false;
            }

            remainingRowsInInterval = adjustedLo - partitionLo;

            frame.partitionLo = adjustedLo;
            frame.partitionHi = partitionHi;
            // Symbol indexes store index rows, see ParquetMetaFileReader#getRowGroupIndexRowLo.
            frame.pieceShift = metadata.getRowGroupIndexRowLo(targetGroup, targetGroupStart) - targetGroupStart;
            frame.skipSkeleton = false;
            frame.format = PartitionFormat.PARQUET;
            setFramePartitionFrameState(requiresMaterialization);
            frame.parquetMetaDecoder = reenterParquetDecoder;
            frame.rowGroupIndex = targetGroup;
            frame.rowGroupLo = rowGroupLo;
            frame.rowGroupHi = rowGroupHi;
            frame.partitionIndex = reenterPartitionIndex;
            // Update cache to avoid re-scanning skipped row groups on next call
            cachedRowGroupIndex = targetGroup;
            cachedRowGroupStartRow = targetGroupStart;
            return frame;
        }

        // All row groups were skipped
        this.reenterPartitionFrame = false;
        return null;
    }

    private void fillNativeAddresses(int base, long physicalLo, long physicalHi) {
        for (int i = 0; i < columnCount; i++) {
            final int columnIndex = columnIndexes.getQuick(i);
            final int readerColIndex = TableReader.getPrimaryColumnIndex(base, columnIndex);
            final MemoryR colMem = reader.getColumn(readerColIndex);
            // when the entire column is NULL we make it skip the whole of the partition frame
            final long top = colMem instanceof NullMemoryCMR ? physicalHi : reader.getColumnTop(base, columnIndex);
            final long partitionLoAdjusted = physicalLo - top;
            final long partitionHiAdjusted = physicalHi - top;
            final int sh = columnSizeShifts.getQuick(i);

            if (partitionHiAdjusted > 0) {
                if (sh > -1) {
                    // this assumes reader uses single page to map the whole column
                    // non-negative sh means fixed length column
                    final long address = colMem.getPageAddress(0);
                    final long addressSize = partitionHiAdjusted << sh;
                    final long offset = partitionLoAdjusted << sh;
                    columnPageAddresses.setQuick(2 * i, address + offset);
                    pageSizes.setQuick(2 * i, addressSize - offset);
                } else {
                    final int columnType = reader.getMetadata().getColumnType(columnIndex);
                    final ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);
                    final MemoryR auxCol = reader.getColumn(readerColIndex + 1);
                    final long auxAddress = auxCol.getPageAddress(0);
                    final long auxOffsetLo = columnTypeDriver.getAuxVectorOffset(partitionLoAdjusted);
                    final long auxOffsetHi = columnTypeDriver.getAuxVectorOffset(partitionHiAdjusted);

                    final long dataSize = columnTypeDriver.getDataVectorSizeAt(auxAddress, partitionHiAdjusted - 1);
                    // some var-size columns may not have data memory (fully inlined)
                    final long dataAddress = dataSize > 0 ? colMem.getPageAddress(0) : 0;

                    columnPageAddresses.setQuick(2 * i, dataAddress);
                    columnPageAddresses.setQuick(2 * i + 1, auxAddress + auxOffsetLo);
                    pageSizes.setQuick(2 * i, dataSize);
                    pageSizes.setQuick(2 * i + 1, auxOffsetHi - auxOffsetLo);
                }
            } else { // column top
                columnPageAddresses.setQuick(2 * i, 0);
                columnPageAddresses.setQuick(2 * i + 1, 0);
                // data page size is used by VectorAggregateFunction as the size hint
                // in the following way:
                //   size = page_size >>> column_size_hint
                // (for var-sized types column_size_hint is 0)
                pageSizes.setQuick(2 * i, (partitionHiAdjusted - partitionLoAdjusted) << (sh > -1 ? sh : 0));
                pageSizes.setQuick(2 * i + 1, 0);
            }
        }
    }

    private @Nullable TableReaderPageFrame nextSlow(PartitionFrame partitionFrame, long lo, long hi, long skipTarget) {
        final byte format = partitionFrame.getPartitionFormat();
        if (format == PartitionFormat.PARQUET) {
            clearAddresses();
            reenterParquetDecoder = partitionFrame.getParquetMetaDecoder();
            setReenterPartitionFrameState(partitionFrame);
            reenterPartitionFormat = PartitionFormat.PARQUET;
            // A row group stays one frame: sub-frames would each decompress its shared parquet pages
            // again. Only a row group above the 24-bit batched GROUP BY row index splits.
            reenterPageFrameRowLimit = Map.BATCH_ROW_INDEX_MASK + 1;
            cachedRowGroupIndex = -1;
            cachedRowGroupStartRow = 0;
            assert reenterParquetDecoder != null;
            filterBufEnd = -1;
            if (filterList != null && ParquetRowGroupFilter.prepareFilterList(
                    reenterParquetDecoder.metadata(),
                    pushdownFilterConditions,
                    filterList,
                    filterValues,
                    // native-table partitions: resolve the Parquet column by stable id so a
                    // renamed column maps correctly despite the frozen Parquet name.
                    true
            )) {
                filterBufEnd = filterValues.getAddress() + filterValues.getAppendOffset();
            }
            return computeParquetFrame(lo, hi);
        }

        assert format == PartitionFormat.NATIVE;
        reenterParquetDecoder = null;
        setReenterPartitionFrameState(partitionFrame);
        reenterPartitionFormat = PartitionFormat.NATIVE;
        reenterPageFrameRowLimit = NativeFrameBoundaries.calculatePageFrameRowLimit(lo, hi, pageFrameMinRows, pageFrameMaxRows, sharedQueryWorkerCount);
        if (reenterPartitionFrameState != 0) {
            clearAddresses();
            return computeNativeDeltaFrame(lo, hi);
        }
        clearReenterPartitionFrameState();
        return computeNativeFrame(lo, hi, skipTarget);
    }

    // Historical keys may no longer be keys in the current table metadata.
    private void initDedupKeyPages(TableReaderMetadata metadata, long state) {
        dedupKeyColumns.clear();
        dedupKeyPages.clear();
        if (state == 0) {
            return;
        }
        for (int key = 0, count = PartitionFrameState.getDedupKeyCount(state); key < count; key++) {
            final int columnId = PartitionFrameState.getDedupKeyColumnId(state, key);
            int columnIndex = -1;
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (metadata.getColumnMetadata(i).getOriginalWriterIndex() == columnId) {
                    columnIndex = i;
                    break;
                }
            }
            dedupKeyColumns.add(columnIndex);
            dedupKeyPages.add(columnId);
            for (int cell = PageFrame.DEDUP_KEY_PAGE_DATA_ADDRESS; cell < PageFrame.DEDUP_KEY_PAGE_CELLS; cell++) {
                dedupKeyPages.add(0);
            }
        }
    }

    private void prepareNativePartitionFrameBaseColumn(int base, int columnIndex, int outputIndex, boolean timestamp) {
        if (columnIndex < 0) {
            final int key = -2 - outputIndex;
            final int offset = key * PageFrame.DEDUP_KEY_PAGE_CELLS;
            final var column = reader.getHistoricalColumn(reenterPartitionIndex,
                    (int) dedupKeyPages.getQuick(offset), PartitionFrameState.getDedupKeyType(reenterPartitionFrameState, key));
            final MemoryR data = column.data();
            final MemoryR aux = column.aux();
            dedupKeyPages.setQuick(offset + PageFrame.DEDUP_KEY_PAGE_DATA_ADDRESS, data == null ? 0 : data.getPageAddress(0));
            dedupKeyPages.setQuick(offset + PageFrame.DEDUP_KEY_PAGE_DATA_SIZE, data == null ? 0 : data.size());
            dedupKeyPages.setQuick(offset + PageFrame.DEDUP_KEY_PAGE_AUX_ADDRESS, aux == null ? 0 : aux.getPageAddress(0));
            dedupKeyPages.setQuick(offset + PageFrame.DEDUP_KEY_PAGE_AUX_SIZE, aux == null ? 0 : aux.size());
            dedupKeyPages.setQuick(offset + PageFrame.DEDUP_KEY_PAGE_TOP, column.top());
            return;
        }
        reader.openPageFrameColumn(reenterPartitionIndex, columnIndex);
        final int primary = TableReader.getPrimaryColumnIndex(base, columnIndex);
        final MemoryR data = reader.getColumn(primary);
        final long top = reader.getColumnTop(base, columnIndex);
        long auxAddress = 0;
        long auxSize = 0;
        long dataAddress = 0;
        long dataSize = 0;
        if (data != null && !(data instanceof NullMemoryCMR)) {
            dataSize = data.size();
            dataAddress = dataSize == 0 ? 0 : data.getPageAddress(0);
            if (ColumnType.isVarSize(reader.getMetadata().getColumnType(columnIndex))) {
                final MemoryR aux = reader.getColumn(primary + 1);
                if (aux != null && !(aux instanceof NullMemoryCMR)) {
                    auxSize = aux.size();
                    auxAddress = auxSize == 0 ? 0 : aux.getPageAddress(0);
                }
            }
        }
        if (timestamp) {
            timestampPageAddress = dataAddress;
            timestampPageSize = dataSize;
            timestampPageTop = top;
        } else if (outputIndex < -1) {
            final int offset = (-2 - outputIndex) * PageFrame.DEDUP_KEY_PAGE_CELLS;
            dedupKeyPages.setQuick(offset + PageFrame.DEDUP_KEY_PAGE_DATA_ADDRESS, dataAddress);
            dedupKeyPages.setQuick(offset + PageFrame.DEDUP_KEY_PAGE_DATA_SIZE, dataSize);
            dedupKeyPages.setQuick(offset + PageFrame.DEDUP_KEY_PAGE_AUX_ADDRESS, auxAddress);
            dedupKeyPages.setQuick(offset + PageFrame.DEDUP_KEY_PAGE_AUX_SIZE, auxSize);
            dedupKeyPages.setQuick(offset + PageFrame.DEDUP_KEY_PAGE_TOP, top);
        } else {
            columnPageAddresses.setQuick(2 * outputIndex, dataAddress);
            columnPageAddresses.setQuick(2 * outputIndex + 1, auxAddress);
            columnPageTops.setQuick(outputIndex, top);
            pageSizes.setQuick(2 * outputIndex, dataSize);
            pageSizes.setQuick(2 * outputIndex + 1, auxSize);
        }
    }

    private void prepareNativePartitionFrameBaseColumns(int base, int window) {
        for (int i = 0; i < columnCount; i++) {
            prepareNativePartitionFrameBaseColumn(base, columnIndexes.getQuick(i), i, false);
        }
        final int timestampIndex = reader.getMetadata().getTimestampIndex();
        if (timestampIndex >= 0) {
            prepareNativePartitionFrameBaseColumn(base, timestampIndex, -1, true);
        }
        // Duplicate resolution compares keys of rows the query may not select.
        for (int key = 0, n = dedupKeyColumns.size(); key < n; key++) {
            prepareNativePartitionFrameBaseColumn(base, dedupKeyColumns.getQuick(key), -2 - key, false);
        }
        preparedNativeBaseWindow = window;
    }

    private void setFramePartitionFrameState(boolean requiresMaterialization) {
        frame.partitionFrameState = requiresMaterialization ? reenterPartitionFrameState : 0;
    }

    private void setReenterPartitionFrameState(PartitionFrame partitionFrame) {
        reenterPartitionFrameState = partitionFrame.getPartitionFrameState();
        initDedupKeyPages(reader.getMetadata(), reenterPartitionFrameState);
    }

    private class TableReaderPageFrame implements PageFrame {
        private byte format;
        private ParquetPartitionDecoder parquetMetaDecoder;
        private long partitionFrameState;
        private long partitionHi;
        private int partitionIndex;
        private long partitionLo;
        private long pieceShift;
        private int rowGroupHi;
        private int rowGroupIndex;
        private int rowGroupLo;
        private boolean skipSkeleton;

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return columnPageAddresses.getQuick(2 * columnIndex + 1);
        }

        @Override
        public long getAuxPageSize(int columnIndex) {
            return pageSizes.getQuick(2 * columnIndex + 1);
        }

        @Override
        public int getColumnCount() {
            return columnCount;
        }

        @Override
        public long getDedupKeyPage(int key, int cell) {
            return dedupKeyPages.getQuick(key * DEDUP_KEY_PAGE_CELLS + cell);
        }

        @Override
        public int getDedupKeyPageCount() {
            return dedupKeyColumns.size();
        }

        @Override
        public long getDesignatedTimestampPageAddress() {
            return timestampPageAddress;
        }

        @Override
        public long getDesignatedTimestampPageSize() {
            return timestampPageSize;
        }

        @Override
        public long getDesignatedTimestampPageTop() {
            return timestampPageTop;
        }

        @Override
        public byte getFormat() {
            return format;
        }

        @Override
        public IndexReader getIndexReader(int columnIndex, int direction) {
            return reader.getIndexReader(partitionIndex, columnIndexes.getQuick(columnIndex), direction);
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return columnPageAddresses.getQuick(2 * columnIndex);
        }

        @Override
        public long getPageSize(int columnIndex) {
            return pageSizes.getQuick(2 * columnIndex);
        }

        @Override
        public long getPageTop(int columnIndex) {
            return columnPageTops.getQuick(columnIndex);
        }

        @Override
        public ParquetPartitionDecoder getParquetDecoder() {
            assert parquetMetaDecoder != null || format != PartitionFormat.PARQUET;
            return parquetMetaDecoder;
        }

        @Override
        public int getParquetRowGroup() {
            return rowGroupIndex;
        }

        @Override
        public int getParquetRowGroupHi() {
            return rowGroupHi;
        }

        @Override
        public int getParquetRowGroupLo() {
            return rowGroupLo;
        }

        @Override
        public long getIndexRowHi() {
            return partitionHi + pieceShift;
        }

        @Override
        public long getIndexRowLo() {
            return partitionLo + pieceShift;
        }

        @Override
        public long getPartitionFrameState() {
            return partitionFrameState;
        }

        @Override
        public long getPartitionHi() {
            return partitionHi;
        }

        @Override
        public int getPartitionIndex() {
            return partitionIndex;
        }

        @Override
        public long getPartitionLo() {
            return partitionLo;
        }

        @Override
        public long getRowCountLo() {
            // Only a window that needs custom materialization can drop rows while decoding;
            // the Rust binding flags such windows, and the frame then promises nothing
            // beyond its declared span. Every other frame is served as is and stays exact.
            if (partitionFrameState != 0
                    && rowGroupIndex >= 0
                    && !PartitionFrameState.isExactRowCount(partitionFrameState, rowGroupIndex)) {
                return 0;
            }
            return partitionHi - partitionLo;
        }

        @Override
        public boolean isSkipSkeleton() {
            return skipSkeleton;
        }
    }
}
