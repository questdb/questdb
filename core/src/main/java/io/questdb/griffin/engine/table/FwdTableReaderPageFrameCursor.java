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
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.PartitionFrameState;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
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

public class FwdTableReaderPageFrameCursor implements TablePageFrameCursor {
    private final int columnCount;
    private final IntList columnIndexes;
    private final ColumnMapping columnMapping = new ColumnMapping();
    private final LongList columnPageAddresses = new LongList();
    private final LongList columnPageTops = new LongList();
    private final IntList columnSizeShifts;
    private final DirectLongList filterList;
    private final MemoryCARWImpl filterValues;
    private final TableReaderPageFrame frame = new TableReaderPageFrame();
    private final LongList pageSizes = new LongList();
    private final @Nullable ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions;
    private final int sharedQueryWorkerCount;
    private int cachedRowGroupIndex;
    private long cachedBaseRowStart;
    private long cachedRowGroupStartRow;
    private long filterBufEnd = -1;
    // Track the lowest partition index that has not been released yet
    private int lowestOpenPartitionIndex = 0;
    private int pageFrameMaxRows;
    private int pageFrameMinRows;
    private PartitionFrameCursor partitionFrameCursor;
    private int preparedNativeBaseWindow = -1;
    private TableReader reader;
    // only native partition frames are reentered
    private long reenterPageFrameRowLimit;
    private ParquetPartitionDecoder reenterParquetDecoder;
    private long reenterPartitionFrameState;
    private boolean reenterPartitionFrame = false; // true when the current Partition Frame is not entirely exhausted
    private byte reenterPartitionFormat;
    private long reenterPartitionHi;
    private int reenterPartitionIndex;
    private long reenterPartitionLo;
    private long remainingRowsInInterval;
    private long timestampPageAddress;
    private long timestampPageSize;
    private long timestampPageTop;

    public FwdTableReaderPageFrameCursor(
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
                    continue;
                } else {
                    return computeNativeFrame(reenterPartitionLo, reenterPartitionHi);
                }
            }

            final PartitionFrame partitionFrame = partitionFrameCursor.next(skipTarget);
            if (partitionFrame != null) {
                reenterPartitionIndex = partitionFrame.getPartitionIndex();
                final long lo = partitionFrame.getRowLo();
                final long hi = partitionFrame.getRowHi();
                final long partitionFrameState = partitionFrame.getPartitionFrameState();
                final boolean hasCustomFrames = partitionFrameState != 0;

                if (hi - lo <= skipTarget && !hasCustomFrames) {
                    frame.partitionIndex = reenterPartitionIndex;
                    frame.partitionLo = lo;
                    frame.partitionHi = hi;
                    frame.format = partitionFrame.getPartitionFormat();
                    frame.rowGroupIndex = -1;
                    frame.rowGroupLo = -1;
                    frame.rowGroupHi = -1;
                    final ParquetPartitionDecoder partitionDecoder = partitionFrame.getParquetMetaDecoder();
                    frame.parquetMetaDecoder = frame.format == PartitionFormat.PARQUET ? partitionDecoder : null;
                    frame.partitionFrameState = 0;

                    return frame;
                }
                final TableReaderPageFrame result = nextSlow(partitionFrame, lo, hi);
                if (result != null) {
                    return result;
                }
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
        // Guard against being called before next() when no partitions need releasing.
        if (lowestOpenPartitionIndex >= reenterPartitionIndex) {
            return;
        }
        // Close all partitions from lowestOpenPartitionIndex up to (but not including) current partition
        for (int i = lowestOpenPartitionIndex; i < reenterPartitionIndex; i++) {
            reader.closePartitionByIndex(i);
        }
        lowestOpenPartitionIndex = reenterPartitionIndex;
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
    public void toPartition(int targetPartitionIndex) {
        partitionFrameCursor.toPartition(targetPartitionIndex);
        reenterPartitionFrame = false;
        reenterParquetDecoder = null;
        clearReenterPartitionFrameState();
        clearAddresses();
    }

    @Override
    public void toTop() {
        partitionFrameCursor.toTop();
        reenterPartitionFrame = false;
        reenterParquetDecoder = null;
        clearReenterPartitionFrameState();
        lowestOpenPartitionIndex = 0;
        cachedRowGroupIndex = 0;
        cachedBaseRowStart = 0;
        cachedRowGroupStartRow = 0;
        filterBufEnd = -1;
        clearAddresses();
    }

    private long adjustNativeFrameHi(int base, long physicalLo, long physicalHi) {
        long adjustedHi = physicalHi;
        for (int i = 0; i < columnCount; i++) {
            final int columnIndex = columnIndexes.getQuick(i);
            final long top = reader.getColumnTop(base, columnIndex);
            if (top > physicalLo && top < adjustedHi) {
                adjustedHi = top;
            }
        }
        return adjustedHi;
    }

    private void clearAddresses() {
        columnPageAddresses.setAll(2 * columnCount, 0);
        columnPageTops.setAll(columnCount, 0);
        pageSizes.setAll(2 * columnCount, -1);
        preparedNativeBaseWindow = -1;
        timestampPageAddress = 0;
        timestampPageSize = 0;
        timestampPageTop = 0;
    }

    private void clearReenterPartitionFrameState() {
        reenterPartitionFrameState = 0;
    }

    private void setFramePartitionFrameState(boolean requiresMaterialization) {
        frame.partitionFrameState = requiresMaterialization ? reenterPartitionFrameState : 0;
    }

    private void setReenterPartitionFrameState(PartitionFrame partitionFrame) {
        reenterPartitionFrameState = partitionFrame.getPartitionFrameState();
    }

    private void prepareNativePartitionFrameBaseColumns(int base, int window) {
        for (int i = 0; i < columnCount; i++) {
            prepareNativePartitionFrameBaseColumn(base, columnIndexes.getQuick(i), i, false);
        }
        final int timestampIndex = reader.getMetadata().getTimestampIndex();
        if (timestampIndex >= 0) {
            prepareNativePartitionFrameBaseColumn(base, timestampIndex, -1, true);
        }
        preparedNativeBaseWindow = window;
    }

    private void prepareNativePartitionFrameBaseColumn(int base, int columnIndex, int outputIndex, boolean timestamp) {
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
        } else {
            columnPageAddresses.setQuick(2 * outputIndex, dataAddress);
            columnPageAddresses.setQuick(2 * outputIndex + 1, auxAddress);
            columnPageTops.setQuick(outputIndex, top);
            pageSizes.setQuick(2 * outputIndex, dataSize);
            pageSizes.setQuick(2 * outputIndex + 1, auxSize);
        }
    }

    private @Nullable TableReaderPageFrame computeNativeDeltaFrame(long partitionLo, long partitionHi) {
        final int base = reader.getColumnBase(reenterPartitionIndex);
        final int windowCount = PartitionFrameState.getWindowCount(reenterPartitionFrameState);
        long baseStartRow = cachedBaseRowStart;
        long windowStartRow = cachedRowGroupStartRow;
        for (int window = cachedRowGroupIndex; window < windowCount; window++) {
            final long baseRows = PartitionFrameState.getBaseRowCount(reenterPartitionFrameState, window);
            final long logicalRows = PartitionFrameState.getLogicalRowCount(reenterPartitionFrameState, window);
            final long windowEndRow = Math.addExact(windowStartRow, logicalRows);
            if (partitionLo < windowEndRow) {
                final long adjustedHi;
                final boolean requiresMaterialization = PartitionFrameState.requiresMaterialization(reenterPartitionFrameState, window);
                if (requiresMaterialization) {
                    if (preparedNativeBaseWindow != window) {
                        prepareNativePartitionFrameBaseColumns(base, window);
                    }
                    final long subframeSize = PartitionFrameState.getSubframeSize(reenterPartitionFrameState);
                    final long localLo = partitionLo - windowStartRow;
                    final long canonicalHi = Math.min(
                            logicalRows,
                            Math.multiplyExact(Math.floorDiv(localLo, subframeSize) + 1, subframeSize)
                    );
                    adjustedHi = Math.min(partitionHi, Math.addExact(windowStartRow, canonicalHi));
                } else {
                    preparedNativeBaseWindow = -1;
                    final long frameLimit = reenterPageFrameRowLimit > 0
                            ? Math.addExact(partitionLo, reenterPageFrameRowLimit)
                            : Long.MAX_VALUE;
                    final long requestedHi = Math.min(Math.min(partitionHi, windowEndRow), frameLimit);
                    final long physicalLo = Math.addExact(baseStartRow, partitionLo - windowStartRow);
                    final long requestedPhysicalHi = Math.addExact(physicalLo, requestedHi - partitionLo);
                    final long adjustedPhysicalHi = adjustNativeFrameHi(base, physicalLo, requestedPhysicalHi);
                    adjustedHi = Math.addExact(partitionLo, adjustedPhysicalHi - physicalLo);
                    fillNativeAddresses(base, physicalLo, adjustedPhysicalHi);
                }

                if (adjustedHi < partitionHi) {
                    reenterPartitionLo = adjustedHi;
                    reenterPartitionHi = partitionHi;
                    reenterPartitionFrame = true;
                } else {
                    reenterPartitionFrame = false;
                }
                if (adjustedHi >= windowEndRow) {
                    cachedBaseRowStart = Math.addExact(baseStartRow, baseRows);
                    cachedRowGroupIndex = window + 1;
                    cachedRowGroupStartRow = windowEndRow;
                } else {
                    cachedBaseRowStart = baseStartRow;
                    cachedRowGroupIndex = window;
                    cachedRowGroupStartRow = windowStartRow;
                }

                remainingRowsInInterval = partitionHi - adjustedHi;
                frame.format = PartitionFormat.NATIVE;
                frame.parquetMetaDecoder = null;
                setFramePartitionFrameState(requiresMaterialization);
                frame.partitionHi = adjustedHi;
                frame.partitionIndex = reenterPartitionIndex;
                frame.partitionLo = partitionLo;
                frame.rowGroupHi = Math.toIntExact(adjustedHi - windowStartRow);
                frame.rowGroupIndex = window;
                frame.rowGroupLo = Math.toIntExact(partitionLo - windowStartRow);
                return frame;
            }
            baseStartRow = Math.addExact(baseStartRow, baseRows);
            windowStartRow = windowEndRow;
        }
        reenterPartitionFrame = false;
        return null;
    }

    private TableReaderPageFrame computeNativeFrame(long partitionLo, long partitionHi) {
        final int base = reader.getColumnBase(reenterPartitionIndex);

        // we may need to split this partition frame either along "top" lines, or along
        // max page frame sizes; to do this, we calculate min top value from given position
        final long requestedHi = Math.min(partitionHi, Math.addExact(partitionLo, reenterPageFrameRowLimit));
        final long adjustedHi = adjustNativeFrameHi(base, partitionLo, requestedHi);
        fillNativeAddresses(base, partitionLo, adjustedHi);

        // it is possible that all columns in partition frame are empty, but it doesn't mean
        // the partition frame size is 0; sometimes we may want to imply nulls
        if (adjustedHi < partitionHi) {
            reenterPartitionLo = adjustedHi;
            reenterPartitionHi = partitionHi;
            reenterPartitionFrame = true;
        } else {
            reenterPartitionFrame = false;
        }

        // remaining rows in the partition = size of the partition - max row number of the frame
        remainingRowsInInterval = partitionHi - adjustedHi;

        frame.partitionLo = partitionLo;
        frame.partitionHi = adjustedHi;
        frame.format = PartitionFormat.NATIVE;
        frame.parquetMetaDecoder = null;
        frame.partitionFrameState = 0;
        frame.rowGroupIndex = -1;
        frame.rowGroupLo = -1;
        frame.rowGroupHi = -1;
        frame.partitionIndex = reenterPartitionIndex;
        return frame;
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

        long rowGroupStartRow = cachedRowGroupStartRow;
        for (int i = cachedRowGroupIndex; i < rowGroupCount; i++) {
            final long baseRowGroupSize = metadata.getRowGroupSize(i);
            final long rowGroupSize = reenterPartitionFrameState == 0
                    ? baseRowGroupSize
                    : PartitionFrameState.getLogicalRowCount(reenterPartitionFrameState, i);
            final long rowGroupEndRow = rowGroupStartRow + rowGroupSize;

            if (partitionLo < rowGroupEndRow) {
                if ((reenterPartitionFrameState == 0 || !PartitionFrameState.requiresMaterialization(reenterPartitionFrameState, i))
                        && filterBufEnd != -1 && ParquetRowGroupFilter.canSkipRowGroup(
                        i,
                        metadata,
                        filterList,
                        filterBufEnd
                )) {
                    partitionLo = rowGroupEndRow;
                    if (partitionLo >= partitionHi) {
                        reenterPartitionFrame = false;
                        return null;
                    }
                    rowGroupStartRow = rowGroupEndRow;
                    continue;
                }

                final boolean requiresMaterialization = reenterPartitionFrameState != 0
                        && PartitionFrameState.requiresMaterialization(reenterPartitionFrameState, i);
                final long adjustedHi;
                if (requiresMaterialization) {
                    final long subframeSize = PartitionFrameState.getSubframeSize(reenterPartitionFrameState);
                    final long localLo = partitionLo - rowGroupStartRow;
                    final long canonicalHi = Math.min(
                            rowGroupSize,
                            Math.multiplyExact(Math.floorDiv(localLo, subframeSize) + 1, subframeSize)
                    );
                    adjustedHi = Math.min(partitionHi, Math.addExact(rowGroupStartRow, canonicalHi));
                } else {
                    final long frameLimitedHi = reenterPageFrameRowLimit > 0
                            ? partitionLo + reenterPageFrameRowLimit
                            : Long.MAX_VALUE;
                    adjustedHi = Math.min(Math.min(partitionHi, rowGroupEndRow), frameLimitedHi);
                }
                if (adjustedHi < partitionHi) {
                    reenterPartitionLo = adjustedHi;
                    reenterPartitionHi = partitionHi;
                    reenterPartitionFrame = true;
                } else {
                    reenterPartitionFrame = false;
                }

                // Advance to the next row group only when this one is exhausted; a cut inside the
                // row group re-enters the same group on the next call.
                if (adjustedHi >= rowGroupEndRow) {
                    cachedRowGroupIndex = i + 1;
                    cachedRowGroupStartRow = rowGroupEndRow;
                } else {
                    cachedRowGroupIndex = i;
                    cachedRowGroupStartRow = rowGroupStartRow;
                }

                remainingRowsInInterval = partitionHi - adjustedHi;

                frame.parquetMetaDecoder = reenterParquetDecoder;
                setFramePartitionFrameState(requiresMaterialization);
                frame.partitionLo = partitionLo;
                frame.partitionHi = adjustedHi;
                frame.format = PartitionFormat.PARQUET;
                frame.rowGroupIndex = i;
                frame.rowGroupLo = (int) (partitionLo - rowGroupStartRow);
                frame.rowGroupHi = (int) (adjustedHi - rowGroupStartRow);
                frame.partitionIndex = reenterPartitionIndex;
                return frame;
            }
            rowGroupStartRow = rowGroupEndRow;
        }

        // partitionLo is beyond all row groups
        reenterPartitionFrame = false;
        return null;
    }

    private @Nullable TableReaderPageFrame nextSlow(PartitionFrame partitionFrame, long lo, long hi) {
        final byte format = partitionFrame.getPartitionFormat();
        if (format == PartitionFormat.PARQUET) {
            clearAddresses();
            reenterParquetDecoder = partitionFrame.getParquetMetaDecoder();
            setReenterPartitionFrameState(partitionFrame);
            reenterPartitionFormat = PartitionFormat.PARQUET;
            // Honour the page-frame row limit on parquet too, so a row group larger than
            // pageFrameMaxRows is split into bounded sub-frames (matching the native path).
            reenterPageFrameRowLimit = calculatePageFrameRowLimit(lo, hi, pageFrameMinRows, pageFrameMaxRows, sharedQueryWorkerCount);
            cachedRowGroupIndex = 0;
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
        reenterPageFrameRowLimit = calculatePageFrameRowLimit(lo, hi, pageFrameMinRows, pageFrameMaxRows, sharedQueryWorkerCount);
        if (reenterPartitionFrameState != 0) {
            cachedBaseRowStart = 0;
            cachedRowGroupIndex = 0;
            cachedRowGroupStartRow = 0;
            clearAddresses();
            return computeNativeDeltaFrame(lo, hi);
        }
        clearReenterPartitionFrameState();
        return computeNativeFrame(lo, hi);
    }

    static long calculatePageFrameRowLimit(
            long partitionLo,
            long partitionHi,
            long pageFrameMinRows,
            long pageFrameMaxRows,
            int sharedQueryWorkerCount
    ) {
        final int workerCount = Math.max(sharedQueryWorkerCount, 1);
        long rowsPerFrame = Math.min(pageFrameMaxRows, Math.max(pageFrameMinRows, (partitionHi - partitionLo) / workerCount));
        final long lastFrameSize = (partitionHi - partitionLo) % rowsPerFrame;
        if (lastFrameSize > 0 && lastFrameSize < pageFrameMinRows) {
            // Adjust the limit, so that we don't have tiny trailing frames.
            final long frameCount = Math.max((partitionHi - partitionLo) / rowsPerFrame, 1);
            rowsPerFrame += (lastFrameSize + frameCount - 1) / frameCount;
        }
        return rowsPerFrame;
    }

    /**
     * Populates column tops for a partition from column version metadata.
     * A column top value indicates the first row where the column has data;
     * rows before the top are NULL. Columns that don't exist in this
     * partition get top = partitionRowCount (all-null).
     *
     * @param columnTops          output list, cleared and populated with one entry per column
     * @param tableReader         table reader (used for reader metadata)
     * @param columnVersionReader column version reader
     * @param columnIndexes       query-to-reader column index mapping
     * @param columnCount         number of columns
     * @param partitionTimestamp  partition timestamp
     * @param partitionRowCount   partition row count
     */
    static void populateColumnTops(
            LongList columnTops,
            TableReader tableReader,
            ColumnVersionReader columnVersionReader,
            IntList columnIndexes,
            int columnCount,
            long partitionTimestamp,
            long partitionRowCount
    ) {
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
    }

    private class TableReaderPageFrame implements PageFrame {
        private byte format;
        private ParquetPartitionDecoder parquetMetaDecoder;
        private long partitionFrameState;
        private long partitionHi;
        private int partitionIndex;
        private long partitionLo;
        private int rowGroupHi;
        private int rowGroupIndex;
        private int rowGroupLo;

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
            if (partitionFrameState != 0
                    && rowGroupIndex >= 0
                    && PartitionFrameState.requiresMaterialization(partitionFrameState, rowGroupIndex)) {
                throw CairoException.nonCritical()
                        .put("direct index access is unavailable for a cold delta frame [partitionIndex=")
                        .put(partitionIndex)
                        .put(", window=").put(rowGroupIndex)
                        .put(']');
            }
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
        public long getPartitionFrameState() {
            return partitionFrameState;
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
    }
}
