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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.NullMemoryCMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.table.FwdTableReaderPageFrameCursor.calculatePageFrameRowLimit;

public class BwdTableReaderPageFrameCursor implements TablePageFrameCursor {
    private final int columnCount;
    private final IntList columnIndexes;
    private final LongList columnPageAddresses = new LongList();
    private final IntList columnSizeShifts;
    private final DirectLongList filterList;
    private final MemoryCARWImpl filterValues;
    private final TableReaderPageFrame frame = new TableReaderPageFrame();
    private final LongList pageSizes = new LongList();
    private final @Nullable ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions;
    private final int sharedQueryWorkerCount;
    private int pageFrameMaxRows;
    private int pageFrameMinRows;
    private PartitionFrameCursor partitionFrameCursor;
    private TableReader reader;
    private long reenterPageFrameRowLimit;
    private PartitionDecoder reenterParquetDecoder;
    private boolean reenterPartitionFrame = false;
    private long reenterPartitionHi;
    private int reenterPartitionIndex;
    private long reenterPartitionLo;
    private long remainingRowsInInterval;

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
    public IntList getColumnIndexes() {
        return columnIndexes;
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
    public SymbolTable newSymbolTable(int columnIndex) {
        return reader.newSymbolTable(columnIndexes.getQuick(columnIndex));
    }

    @Override
    public @Nullable PageFrame next(long skipTarget) {
        while (true) {
            if (reenterPartitionFrame) {
                if (reenterParquetDecoder != null) {
                    final TableReaderPageFrame result = computeParquetFrame(reenterPartitionLo, reenterPartitionHi);
                    if (result != null) {
                        return result;
                    }
                    // all remaining row groups in this partition were skipped, try next partition
                    continue;
                }
                return computeNativeFrame(reenterPartitionLo, reenterPartitionHi);
            }

            final PartitionFrame partitionFrame = partitionFrameCursor.next(skipTarget);
            if (partitionFrame != null) {
                reenterPartitionIndex = partitionFrame.getPartitionIndex();
                final long lo = partitionFrame.getRowLo();
                final long hi = partitionFrame.getRowHi();

                if (hi - lo <= skipTarget) {
                    frame.partitionIndex = reenterPartitionIndex;
                    frame.partitionLo = lo;
                    frame.partitionHi = hi;
                    return frame;
                }
                final TableReaderPageFrame result = nextSlow(partitionFrame, lo, hi);
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
    public TablePageFrameCursor of(SqlExecutionContext executionContext, PartitionFrameCursor partitionFrameCursor, int pageFrameMinRows, int pageFrameMaxRows) throws SqlException {
        this.partitionFrameCursor = partitionFrameCursor;
        reader = partitionFrameCursor.getTableReader();
        this.pageFrameMinRows = pageFrameMinRows;
        this.pageFrameMaxRows = pageFrameMaxRows;
        if (pushdownFilterConditions != null) {
            for (int i = 0, n = pushdownFilterConditions.size(); i < n; i++) {
                pushdownFilterConditions.getQuick(i).init(executionContext);

            }
        }
        toTop();
        return this;
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
        clearAddresses();
    }

    private void clearAddresses() {
        columnPageAddresses.setAll(2 * columnCount, 0);
        pageSizes.setAll(2 * columnCount, -1);
    }

    private TableReaderPageFrame computeNativeFrame(long partitionLo, long partitionHi) {
        final int base = reader.getColumnBase(reenterPartitionIndex);

        // we may need to split this partition frame either along "top" lines, or along
        // max page frame sizes; to do this, we calculate min top value from given position
        long adjustedLo = Math.max(partitionLo, partitionHi - reenterPageFrameRowLimit);
        for (int i = 0; i < columnCount; i++) {
            final int columnIndex = columnIndexes.getQuick(i);
            long top = reader.getColumnTop(base, columnIndex);
            if (top > adjustedLo && top < partitionHi) {
                adjustedLo = top;
            }
        }

        for (int i = 0; i < columnCount; i++) {
            final int columnIndex = columnIndexes.getQuick(i);
            final int readerColIndex = TableReader.getPrimaryColumnIndex(base, columnIndex);
            final MemoryR colMem = reader.getColumn(readerColIndex);
            // when the entire column is NULL we make it skip the whole of the partition frame
            final long top = colMem instanceof NullMemoryCMR ? partitionHi : reader.getColumnTop(base, columnIndex);
            final long partitionLoAdjusted = adjustedLo - top;
            final long partitionHiAdjusted = partitionHi - top;
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
        frame.format = PartitionFormat.NATIVE;
        frame.rowGroupIndex = -1;
        frame.rowGroupLo = -1;
        frame.rowGroupHi = -1;
        frame.partitionIndex = reenterPartitionIndex;
        return frame;
    }

    private @Nullable TableReaderPageFrame computeParquetFrame(long partitionLo, long partitionHi) {
        final PartitionDecoder.Metadata metadata = reenterParquetDecoder.metadata();
        final int rowGroupCount = metadata.getRowGroupCount();

        long rowGroupStartRow = 0;
        int targetGroup = -1;
        long targetGroupStart = 0;

        for (int i = 0; i < rowGroupCount; i++) {
            final long rowGroupSize = metadata.getRowGroupSize(i);
            final long rowGroupEndRow = rowGroupStartRow + rowGroupSize;
            if (partitionHi <= rowGroupEndRow) {
                targetGroup = i;
                targetGroupStart = rowGroupStartRow;
                break;
            }
            rowGroupStartRow = rowGroupEndRow;
        }

        if (targetGroup < 0) {
            this.reenterPartitionFrame = false;
            return null;
        }

        while (targetGroup >= 0) {
            if (filterList != null && ParquetRowGroupFilter.canSkipRowGroup(
                    targetGroup, reenterParquetDecoder, pushdownFilterConditions, filterList, filterValues)) {
                partitionHi = targetGroupStart;
                if (partitionHi <= partitionLo) {
                    this.reenterPartitionFrame = false;
                    return null;
                }
                targetGroup--;
                if (targetGroup >= 0) {
                    targetGroupStart -= metadata.getRowGroupSize(targetGroup);
                }
                continue;
            }

            final long adjustedLo = Math.max(partitionLo, targetGroupStart);
            if (adjustedLo > partitionLo) {
                this.reenterPartitionLo = partitionLo;
                this.reenterPartitionHi = adjustedLo;
                this.reenterPartitionFrame = true;
            } else {
                this.reenterPartitionFrame = false;
            }

            frame.partitionLo = adjustedLo;
            frame.partitionHi = partitionHi;
            frame.format = PartitionFormat.PARQUET;
            frame.rowGroupIndex = targetGroup;
            frame.rowGroupLo = (int) (adjustedLo - targetGroupStart);
            frame.rowGroupHi = (int) (partitionHi - targetGroupStart);
            frame.partitionIndex = reenterPartitionIndex;
            return frame;
        }

        // All row groups were skipped
        this.reenterPartitionFrame = false;
        return null;
    }

    private @Nullable TableReaderPageFrame nextSlow(PartitionFrame partitionFrame, long lo, long hi) {
        final byte format = partitionFrame.getPartitionFormat();
        if (format == PartitionFormat.PARQUET) {
            clearAddresses();
            reenterParquetDecoder = partitionFrame.getParquetDecoder();
            reenterPageFrameRowLimit = 0;
            return computeParquetFrame(lo, hi);
        }

        assert format == PartitionFormat.NATIVE;
        reenterParquetDecoder = null;
        reenterPageFrameRowLimit = calculatePageFrameRowLimit(lo, hi, pageFrameMinRows, pageFrameMaxRows, sharedQueryWorkerCount);
        return computeNativeFrame(lo, hi);
    }

    private class TableReaderPageFrame implements PageFrame {
        private byte format;
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
        public BitmapIndexReader getBitmapIndexReader(int columnIndex, int direction) {
            return reader.getBitmapIndexReader(partitionIndex, columnIndexes.getQuick(columnIndex), direction);
        }

        @Override
        public int getColumnCount() {
            return columnCount;
        }

        @Override
        public byte getFormat() {
            return format;
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
        public PartitionDecoder getParquetPartitionDecoder() {
            assert reenterParquetDecoder != null || format != PartitionFormat.PARQUET;
            return reenterParquetDecoder;
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
