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
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.NullMemoryCMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.*;
import org.jetbrains.annotations.Nullable;

public class BwdTableReaderPageFrameCursor implements PageFrameCursor {
    private final int columnCount;
    private final IntList columnIndexes;
    private final LongList columnPageAddress = new LongList();
    private final LongList columnPageNextAddress = new LongList();
    private final IntList columnSizeShifts;
    // Holds PageFrame#*_FORMAT per each partition.
    private final ByteList formats = new ByteList();
    private final TableReaderPageFrame frame = new TableReaderPageFrame();
    private final int pageFrameMaxRows;
    private final int pageFrameMinRows;
    private final LongList pageRowsRemaining = new LongList();
    private final LongList pageSizes = new LongList();
    private final IntList pages = new IntList();
    private final LongList topsRemaining = new LongList();
    private final int workerCount;
    private long currentPageFrameRowLimit;
    private PartitionFrameCursor partitionFrameCursor;
    private TableReader reader;
    private boolean reenterPartitionFrame = false;
    private long reenterPartitionHi;
    private int reenterPartitionIndex;
    private long reenterPartitionLo;

    public BwdTableReaderPageFrameCursor(
            IntList columnIndexes,
            IntList columnSizeShifts,
            int workerCount,
            int pageFrameMinRows,
            int pageFrameMaxRows
    ) {
        this.columnIndexes = columnIndexes;
        this.columnSizeShifts = columnSizeShifts;
        this.columnCount = columnIndexes.size();
        this.workerCount = workerCount;
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
    public StaticSymbolTable getSymbolTable(int columnIndex) {
        return reader.getSymbolTable(columnIndexes.getQuick(columnIndex));
    }

    @Override
    public TableReader getTableReader() {
        return reader;
    }

    @Override
    public long getUpdateRowId(long rowIndex) {
        return Rows.toRowID(frame.getPartitionIndex(), frame.getPartitionLo() + rowIndex);
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return reader.newSymbolTable(columnIndexes.getQuick(columnIndex));
    }

    @Override
    public @Nullable PageFrame next() {
        if (reenterPartitionFrame) {
            return computeFrame(reenterPartitionLo, reenterPartitionHi);
        }
        PartitionFrame partitionFrame = partitionFrameCursor.next();
        if (partitionFrame != null) {
            reenterPartitionIndex = partitionFrame.getPartitionIndex();
            final long lo = partitionFrame.getRowLo();
            final long hi = partitionFrame.getRowHi();
            currentPageFrameRowLimit = Math.min(
                    pageFrameMaxRows,
                    Math.max(pageFrameMinRows, (hi - lo) / workerCount)
            );
            return computeFrame(lo, hi);
        }
        return null;
    }

    @Override
    public PageFrameCursor of(PartitionFrameCursor partitionFrameCursor) {
        this.partitionFrameCursor = partitionFrameCursor;
        reader = partitionFrameCursor.getTableReader();
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
        pages.setAll(columnCount, 0);
        topsRemaining.setAll(columnCount, 0);
        columnPageAddress.setAll(2 * columnCount, 0);
        columnPageNextAddress.setAll(2 * columnCount, 0);
        pageRowsRemaining.setAll(columnCount, -1L);
        pageSizes.setAll(2 * columnCount, -1L);
        formats.setAll(formats.size(), (byte) -1);
        formats.clear();
        reenterPartitionFrame = false;
    }

    private TableReaderPageFrame computeFrame(final long partitionLo, final long partitionHi) {
        final int base = reader.getColumnBase(reenterPartitionIndex);

        // we may need to split this partition frame either along "top" lines, or along
        // max page frame sizes; to do this, we calculate min top value from given position
        long adjustedLo = Math.max(partitionLo, partitionHi - currentPageFrameRowLimit);
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
                    columnPageAddress.setQuick(2 * i, address + offset);
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

                    columnPageAddress.setQuick(2 * i, dataAddress);
                    columnPageAddress.setQuick(2 * i + 1, auxAddress + auxOffsetLo);
                    pageSizes.setQuick(2 * i, dataSize);
                    pageSizes.setQuick(2 * i + 1, auxOffsetHi - auxOffsetLo);
                }
            } else { // column top
                columnPageAddress.setQuick(2 * i, 0);
                columnPageAddress.setQuick(2 * i + 1, 0);
                // data page size is used by VectorAggregateFunction as the size hint
                // in the following way:
                //   size = page_size >>> column_size_hint
                // (for var-sized types column_size_hint is 0)
                pageSizes.setQuick(2 * i, (partitionHiAdjusted - partitionLoAdjusted) << (sh > -1 ? sh : 0));
                pageSizes.setQuick(2 * i + 1, 0);
            }
        }

        // TODO(puzpuzpuz): we should get the format from table reader
        formats.extendAndSet(reenterPartitionIndex, PageFrame.NATIVE_FORMAT);

        // it is possible that all columns in partition frame are empty, but it doesn't mean
        // the partition frame size is 0; sometimes we may want to imply nulls
        if (partitionLo < adjustedLo) {
            this.reenterPartitionLo = partitionLo;
            this.reenterPartitionHi = adjustedLo;
            this.reenterPartitionFrame = true;
        } else {
            this.reenterPartitionFrame = false;
        }

        frame.partitionLo = adjustedLo;
        frame.partitionHi = partitionHi;
        frame.partitionIndex = reenterPartitionIndex;
        return frame;
    }

    private class TableReaderPageFrame implements PageFrame {
        private long partitionHi;
        private int partitionIndex;
        private long partitionLo;

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return columnPageAddress.getQuick(2 * columnIndex + 1);
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
            return formats.getQuick(partitionIndex);
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return columnPageAddress.getQuick(2 * columnIndex);
        }

        @Override
        public long getPageSize(int columnIndex) {
            return pageSizes.getQuick(2 * columnIndex);
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
