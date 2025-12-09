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
import io.questdb.cairo.vm.NullMemoryCMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.Nullable;

public class FwdTableReaderPageFrameCursor implements TablePageFrameCursor {
    private final int columnCount;
    private final IntList columnIndexes;
    private final LongList columnPageAddresses = new LongList();
    private final IntList columnSizeShifts;
    private final TableReaderPageFrame frame = new TableReaderPageFrame();
    private final int pageFrameMaxRows;
    private final int pageFrameMinRows;
    private final LongList pageSizes = new LongList();
    private final int sharedQueryWorkerCount;
    private PartitionFrameCursor partitionFrameCursor;
    private TableReader reader;
    // only native partition frames are reentered
    private long reenterPageFrameRowLimit;
    private PartitionDecoder reenterParquetDecoder;
    private boolean reenterPartitionFrame = false; // true when the current Partition Frame is not entirely exhausted
    private long reenterPartitionHi;
    private int reenterPartitionIndex;
    private long reenterPartitionLo;
    private long remainingRowsInInterval;

    public FwdTableReaderPageFrameCursor(
            IntList columnIndexes,
            IntList columnSizeShifts,
            int sharedQueryWorkerCount,
            int pageFrameMinRows,
            int pageFrameMaxRows
    ) {
        this.columnIndexes = columnIndexes;
        this.columnSizeShifts = columnSizeShifts;
        columnCount = columnIndexes.size();
        this.sharedQueryWorkerCount = sharedQueryWorkerCount;
        this.pageFrameMinRows = pageFrameMinRows;
        this.pageFrameMaxRows = pageFrameMaxRows;
    }

    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        partitionFrameCursor.calculateSize(counter);
    }

    @Override
    public void close() {
        partitionFrameCursor = Misc.free(partitionFrameCursor);
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
        if (reenterPartitionFrame) {
            if (reenterParquetDecoder != null) {
                return computeParquetFrame(reenterPartitionLo, reenterPartitionHi);
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
            return nextSlow(partitionFrame, lo, hi);
        }
        return null;
    }

    @Override
    public TablePageFrameCursor of(PartitionFrameCursor partitionFrameCursor) {
        reader = partitionFrameCursor.getTableReader();
        this.partitionFrameCursor = partitionFrameCursor;
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
        long adjustedHi = Math.min(partitionHi, partitionLo + reenterPageFrameRowLimit);
        for (int i = 0; i < columnCount; i++) {
            final int columnIndex = columnIndexes.getQuick(i);
            long top = reader.getColumnTop(base, columnIndex);
            if (top > partitionLo && top < adjustedHi) {
                adjustedHi = top;
            }
        }

        for (int i = 0; i < columnCount; i++) {
            final int columnIndex = columnIndexes.getQuick(i);
            final int readerColIndex = TableReader.getPrimaryColumnIndex(base, columnIndex);
            final MemoryR colMem = reader.getColumn(readerColIndex);
            // when the entire column is NULL we make it skip the whole of the partition frame
            final long top = colMem instanceof NullMemoryCMR ? adjustedHi : reader.getColumnTop(base, columnIndex);
            final long partitionLoAdjusted = partitionLo - top;
            final long partitionHiAdjusted = adjustedHi - top;
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
        frame.parquetAddr = -1;
        frame.parquetFileSize = 0;
        frame.rowGroupIndex = -1;
        frame.rowGroupLo = -1;
        frame.rowGroupHi = -1;
        frame.partitionIndex = reenterPartitionIndex;
        return frame;
    }

    private TableReaderPageFrame computeParquetFrame(long partitionLo, long partitionHi) {
        final PartitionDecoder.Metadata metadata = reenterParquetDecoder.metadata();
        final int rowGroupCount = metadata.getRowGroupCount();

        long rowCount = 0;
        long rowGroupSize = 0;
        int rowGroupIndex = 0;
        for (int i = 0; i < rowGroupCount; i++) {
            rowGroupIndex = i;
            rowGroupSize = metadata.getRowGroupSize(i);
            if (partitionLo < rowCount + rowGroupSize) {
                break;
            }
            rowCount += rowGroupSize;
        }

        // We may add rowGroupSize to rowCount second time here if we scanned to the last row group.
        // This is fine since then we're going to proceed with the partitionHi value.
        final long adjustedHi = Math.min(partitionHi, rowCount + rowGroupSize);
        if (adjustedHi < partitionHi) {
            reenterPartitionLo = adjustedHi;
            reenterPartitionHi = partitionHi;
            reenterPartitionFrame = true;
        } else {
            reenterPartitionFrame = false;
        }

        frame.partitionLo = partitionLo;
        frame.partitionHi = adjustedHi;
        frame.format = PartitionFormat.PARQUET;
        frame.parquetAddr = reenterParquetDecoder.getFileAddr();
        frame.parquetFileSize = reenterParquetDecoder.getFileSize();
        frame.rowGroupIndex = rowGroupIndex;
        frame.rowGroupLo = (int) (partitionLo - rowCount);
        frame.rowGroupHi = (int) (adjustedHi - rowCount);
        frame.partitionIndex = reenterPartitionIndex;
        return frame;
    }

    private TableReaderPageFrame nextSlow(PartitionFrame partitionFrame, long lo, long hi) {
        final byte format = partitionFrame.getPartitionFormat();
        if (format == PartitionFormat.PARQUET) {
            clearAddresses();
            reenterParquetDecoder = partitionFrame.getParquetDecoder();
            reenterPageFrameRowLimit = 0;
            return computeParquetFrame(lo, hi);
        }

        assert format == PartitionFormat.NATIVE;
        reenterParquetDecoder = null;
        reenterPageFrameRowLimit = Math.min(
                pageFrameMaxRows,
                Math.max(pageFrameMinRows, (hi - lo) / Math.max(sharedQueryWorkerCount, 1))
        );
        return computeNativeFrame(lo, hi);
    }

    private class TableReaderPageFrame implements PageFrame {
        private byte format;
        private long parquetAddr;
        private long parquetFileSize;
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
        public long getParquetAddr() {
            return parquetAddr;
        }

        @Override
        public long getParquetFileSize() {
            assert parquetFileSize > 0 || format == PartitionFormat.NATIVE;
            return parquetFileSize;
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
