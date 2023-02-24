/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.NullMemoryMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.*;
import org.jetbrains.annotations.Nullable;

public class FwdTableReaderPageFrameCursor implements PageFrameCursor {
    private final int columnCount;
    private final IntList columnIndexes;
    private final LongList columnPageAddress = new LongList();
    private final LongList columnPageNextAddress = new LongList();
    private final IntList columnSizes;
    private final TableReaderPageFrame frame = new TableReaderPageFrame();
    private final int pageFrameMaxRows;
    private final int pageFrameMinRows;
    private final LongList pageRowsRemaining = new LongList();
    private final LongList pageSizes = new LongList();
    private final IntList pages = new IntList();
    private final LongList topsRemaining = new LongList();
    private final int workerCount;
    private long currentPageFrameRowLimit;
    private DataFrameCursor dataFrameCursor;
    private TableReader reader;
    private boolean reenterDataFrame = false;
    private long reenterPartitionHi;
    private int reenterPartitionIndex;
    private long reenterPartitionLo;

    public FwdTableReaderPageFrameCursor(
            IntList columnIndexes,
            IntList columnSizes,
            int workerCount,
            int pageFrameMinRows,
            int pageFrameMaxRows
    ) {
        this.columnIndexes = columnIndexes;
        this.columnSizes = columnSizes;
        columnCount = columnIndexes.size();
        this.workerCount = workerCount;
        this.pageFrameMinRows = pageFrameMinRows;
        this.pageFrameMaxRows = pageFrameMaxRows;
    }

    @Override
    public void close() {
        dataFrameCursor = Misc.free(dataFrameCursor);
    }

    @Override
    public StaticSymbolTable getSymbolTable(int columnIndex) {
        return reader.getSymbolTable(columnIndexes.getQuick(columnIndex));
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
        if (reenterDataFrame) {
            return computeFrame(reenterPartitionLo, reenterPartitionHi);
        }
        DataFrame dataFrame = dataFrameCursor.next();
        if (dataFrame != null) {
            reenterPartitionIndex = dataFrame.getPartitionIndex();
            final long lo = dataFrame.getRowLo();
            final long hi = dataFrame.getRowHi();
            currentPageFrameRowLimit = Math.min(
                    pageFrameMaxRows,
                    Math.max(pageFrameMinRows, (hi - lo) / workerCount)
            );
            return computeFrame(lo, hi);
        }
        return null;
    }

    public FwdTableReaderPageFrameCursor of(DataFrameCursor dataFrameCursor) {
        reader = dataFrameCursor.getTableReader();
        this.dataFrameCursor = dataFrameCursor;
        toTop();
        return this;
    }

    @Override
    public long size() {
        return reader.size();
    }

    @Override
    public void toTop() {
        dataFrameCursor.toTop();
        pages.setAll(columnCount, 0);
        topsRemaining.setAll(columnCount, 0);
        columnPageAddress.setAll(columnCount * 2, 0);
        columnPageNextAddress.setAll(columnCount * 2, 0);
        pageRowsRemaining.setAll(columnCount, -1L);
        pageSizes.setAll(columnCount * 2, -1L);
        reenterDataFrame = false;
    }

    private TableReaderPageFrame computeFrame(final long partitionLo, final long partitionHi) {
        final int base = reader.getColumnBase(reenterPartitionIndex);

        // we may need to split this data frame either along "top" lines, or along
        // max page frame sizes; to do this, we calculate min top value from given position
        long adjustedHi = Math.min(partitionHi, partitionLo + currentPageFrameRowLimit);
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
            final MemoryR col = reader.getColumn(readerColIndex);
            // when the entire column is NULL we make it skip the whole of the data frame
            final long top = col instanceof NullMemoryMR ? adjustedHi : reader.getColumnTop(base, columnIndex);
            final long partitionLoAdjusted = partitionLo - top;
            final long partitionHiAdjusted = adjustedHi - top;
            final int sh = columnSizes.getQuick(i);

            if (partitionHiAdjusted > 0) {
                if (sh > -1) {
                    // this assumes reader uses single page to map the whole column
                    // non-negative sh means fixed length column
                    long address = col.getPageAddress(0);
                    long addressSize = partitionHiAdjusted << sh;
                    long offset = partitionLoAdjusted << sh;
                    columnPageAddress.setQuick(i * 2, address + offset);
                    pageSizes.setQuick(i * 2, addressSize - offset);
                } else {
                    final MemoryR fixCol = reader.getColumn(readerColIndex + 1);
                    long fixAddress = fixCol.getPageAddress(0);
                    long fixAddressSize = partitionHiAdjusted << 3;
                    long fixOffset = partitionLoAdjusted << 3;

                    long varAddress = col.getPageAddress(0);
                    long varAddressSize = Unsafe.getUnsafe().getLong(fixAddress + fixAddressSize);

                    columnPageAddress.setQuick(i * 2, varAddress);
                    columnPageAddress.setQuick(i * 2 + 1, fixAddress + fixOffset);
                    pageSizes.setQuick(i * 2, varAddressSize);
                    pageSizes.setQuick(i * 2 + 1, fixAddressSize - fixOffset);
                }
            } else {
                columnPageAddress.setQuick(i * 2, 0);
                columnPageAddress.setQuick(i * 2 + 1, 0);
                pageSizes.setQuick(i * 2, (partitionHiAdjusted - partitionLoAdjusted) << (sh > -1 ? sh : 3));
                pageSizes.setQuick(i * 2 + 1, 0);
            }
        }

        // it is possible that all columns in data frame are empty, but it doesn't mean
        // the data frame size is 0; sometimes we may want to imply nulls
        if (adjustedHi < partitionHi) {
            reenterPartitionLo = adjustedHi;
            reenterPartitionHi = partitionHi;
            reenterDataFrame = true;
        } else {
            reenterDataFrame = false;
        }

        frame.partitionLo = partitionLo;
        frame.partitionHi = adjustedHi;
        frame.partitionIndex = reenterPartitionIndex;
        return frame;
    }

    private class TableReaderPageFrame implements PageFrame {
        private long partitionHi;
        private int partitionIndex;
        private long partitionLo;

        @Override
        public BitmapIndexReader getBitmapIndexReader(int columnIndex, int direction) {
            return reader.getBitmapIndexReader(partitionIndex, columnIndexes.getQuick(columnIndex), direction);
        }

        @Override
        public int getColumnShiftBits(int columnIndex) {
            return columnSizes.getQuick(columnIndex);
        }

        @Override
        public long getIndexPageAddress(int columnIndex) {
            return columnPageAddress.getQuick(columnIndex * 2 + 1);
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return columnPageAddress.getQuick(columnIndex * 2);
        }

        @Override
        public long getPageSize(int columnIndex) {
            return pageSizes.getQuick(columnIndex * 2);
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
