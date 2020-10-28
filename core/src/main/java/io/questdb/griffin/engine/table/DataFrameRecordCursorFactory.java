/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.NullColumn;
import io.questdb.cairo.ReadOnlyColumn;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class DataFrameRecordCursorFactory extends AbstractDataFrameRecordCursorFactory {
    private final DataFrameRecordCursor cursor;
    private final boolean followsOrderByAdvice;
    private final Function filter;
    private final boolean framingSupported;
    private final IntList columnIndexes;
    private final IntList columnSizes;
    private TableReaderPageFrameCursor pageFrameCursor;

    public DataFrameRecordCursorFactory(
            RecordMetadata metadata,
            DataFrameCursorFactory dataFrameCursorFactory,
            RowCursorFactory rowCursorFactory,
            boolean followsOrderByAdvice,
            // filter included here only for lifecycle management of the latter
            @Nullable Function filter,
            boolean framingSupported,
            @NotNull IntList columnIndexes,
            @Nullable IntList columnSizes
    ) {
        super(metadata, dataFrameCursorFactory);
        this.cursor = new DataFrameRecordCursor(rowCursorFactory, rowCursorFactory.isEntity(), filter, columnIndexes);
        this.followsOrderByAdvice = followsOrderByAdvice;
        this.filter = filter;
        this.framingSupported = framingSupported;
        this.columnIndexes = columnIndexes;
        this.columnSizes = columnSizes;
    }

    @Override
    public boolean followedOrderByAdvice() {
        return followsOrderByAdvice;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    protected RecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) {
        cursor.of(dataFrameCursor, executionContext);
        if (filter != null) {
            filter.init(cursor, executionContext);
        }
        return cursor;
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext) {
        DataFrameCursor dataFrameCursor = dataFrameCursorFactory.getCursor(executionContext.getCairoSecurityContext());
        if (pageFrameCursor != null) {
            return pageFrameCursor.of(dataFrameCursor);
        } else if (framingSupported) {
            pageFrameCursor = new TableReaderPageFrameCursor(columnIndexes, columnSizes);
            return pageFrameCursor.of(dataFrameCursor);
        } else {
            return null;
        }
    }

    @Override
    public boolean supportPageFrameCursor() {
        return framingSupported;
    }

    @Override
    public void close() {
        Misc.free(filter);
    }

    private static class TableReaderPageFrameCursor implements PageFrameCursor {
        private final LongList columnPageNextAddress = new LongList();
        private final LongList columnPageAddress = new LongList();
        private final TableReaderPageFrameCursor.TableReaderPageFrame frame = new TableReaderPageFrameCursor.TableReaderPageFrame();
        private final LongList topsRemaining = new LongList();
        private final IntList pages = new IntList();
        private final int columnCount;
        private final IntList columnIndexes;
        private final IntList columnSizes;
        private final LongList pageNRowsRemaining = new LongList();
        private final LongList pageSizes = new LongList();
        private TableReader reader;
        private int partitionIndex;
        private long partitionRemaining = 0L;
        private DataFrameCursor dataFrameCursor;

        public TableReaderPageFrameCursor(IntList columnIndexes, IntList columnSizes) {
            this.columnIndexes = columnIndexes;
            this.columnSizes = columnSizes;
            this.columnCount = columnIndexes.size();
        }

        @Override
        public void close() {
            dataFrameCursor = Misc.free(dataFrameCursor);
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return reader.getSymbolMapReader(columnIndexes.getQuick(columnIndex));
        }

        @Override
        public @Nullable PageFrame next() {

            if (partitionIndex > -1) {
                final long m = computePageMin(reader.getColumnBase(partitionIndex));
                if (m < Long.MAX_VALUE) {
                    return computeFrame(m);
                }
            }

            DataFrame dataFrame;
            while ((dataFrame = dataFrameCursor.next()) != null) {
                this.partitionIndex = dataFrame.getPartitionIndex();
                long partitionSize = reader.openPartition(partitionIndex);
                final long partitionLo = dataFrame.getRowLo();
                final long partitionHi = dataFrame.getRowHi();

                this.partitionRemaining = partitionHi - partitionLo;

                if (partitionRemaining > 0) {
                    final int base = reader.getColumnBase(dataFrame.getPartitionIndex());
                    // copy table tops
                    for (int i = 0; i < columnCount; i++) {
                        final int columnIndex = columnIndexes.getQuick(i);
                        topsRemaining.setQuick(i, reader.getColumnTop(base, columnIndex));
                        pages.setQuick(i, 0);
                        pageNRowsRemaining.setQuick(i, -1L);
                    }

                    // reduce
                    for (int i = 0; i < columnCount; i++) {
                        long loRemaining = partitionLo;
                        long top = topsRemaining.getQuick(i);
                        if (top >= partitionLo) {
                            loRemaining = 0;
                            top -= partitionLo;
                            topsRemaining.setQuick(i, top);
                        } else {
                            topsRemaining.setQuick(i, 0);
                            loRemaining -= top;
                        }

                        if (loRemaining > 0) {
                            final ReadOnlyColumn col = reader.getColumn(TableReader.getPrimaryColumnIndex(base, columnIndexes.getQuick(i)));
                            if (col instanceof NullColumn) {
                                columnPageNextAddress.setQuick(i, 0);
                                pageNRowsRemaining.setQuick(i, partitionSize - partitionLo);
                            } else {
                                int page = pages.getQuick(i);
                                while (true) {
                                    long pageSize = col.getPageSize(page) >> columnSizes.getQuick(i);
                                    if (pageSize > loRemaining) {
                                        long addr = col.getPageAddress(page);
                                        addr += partitionLo << columnSizes.getQuick(i);
                                        columnPageNextAddress.setQuick(i, addr);
                                        pageNRowsRemaining.setQuick(i, pageSize - partitionLo);
                                        pages.setQuick(i, page);
                                        break;
                                    }
                                    loRemaining -= pageSize;
                                    page++;
                                }
                            }
                        }
                    }
                    return computeFrame(computePageMin(base));
                }
            }
            return null;
        }

        @Override
        public void toTop() {
            this.partitionIndex = -1;
            this.dataFrameCursor.toTop();
            pages.setAll(columnCount, 0);
            topsRemaining.setAll(columnCount, 0);
            columnPageAddress.setAll(columnCount, 0);
            columnPageNextAddress.setAll(columnCount, 0);
            pageNRowsRemaining.setAll(columnCount, -1L);
            pageSizes.setAll(columnCount, -1L);
        }

        @Override
        public long size() {
            return reader.size();
        }

        public TableReaderPageFrameCursor of(DataFrameCursor dataFrameCursor) {
            this.reader = dataFrameCursor.getTableReader();
            this.dataFrameCursor = dataFrameCursor;
            toTop();
            return this;
        }

        private PageFrame computeFrame(long min) {
            for (int i = 0; i < columnCount; i++) {
                final int columnIndex = columnIndexes.getQuick(i);
                final long top = topsRemaining.getQuick(i);
                if (top > 0) {
                    assert min <= top;
                    topsRemaining.setQuick(i, top - min);
                    columnPageAddress.setQuick(columnIndex, 0);
                    pageSizes.setQuick(i, min);
                } else {
                    long addr = columnPageNextAddress.getQuick(i);
                    long psz = pageNRowsRemaining.getQuick(i);
                    pageNRowsRemaining.setQuick(i, psz - min);
                    columnPageAddress.setQuick(i, addr);
                    if (addr != 0) {
                        long pageSize = min << columnSizes.getQuick(i);
                        pageSizes.setQuick(i, pageSize);
                        columnPageNextAddress.setQuick(i, addr + pageSize);
                    } else {
                        pageSizes.setQuick(i, min);
                    }
                }
            }
            partitionRemaining -= min;
            return frame;
        }

        private long computePageMin(int base) {
            // find min frame length
            long min = Long.MAX_VALUE;
            for (int i = 0; i < columnCount; i++) {
                final long top = topsRemaining.getQuick(i);
                if (top > 0) {
                    if (min > top) {
                        min = top;
                    }
                } else {
                    long psz = pageNRowsRemaining.getQuick(i);
                    if (psz > 0) {
                        if (min > psz) {
                            min = psz;
                        }
                    } else if (partitionRemaining > 0) {
                        final int page = pages.getQuick(i);
                        pages.setQuick(i, page + 1);
                        final ReadOnlyColumn col = reader.getColumn(TableReader.getPrimaryColumnIndex(base, columnIndexes.getQuick(i)));
                        // page size is liable to change after it is mapped
                        // it is important to map page first and call pageSize() after
                        columnPageNextAddress.setQuick(i, col.getPageAddress(page));
                        psz = !(col instanceof NullColumn) ? col.getPageSize(page) >> columnSizes.getQuick(i) : partitionRemaining;
                        final long m = Math.min(psz, partitionRemaining);
                        pageNRowsRemaining.setQuick(i, m);
                        if (min > m) {
                            min = m;
                        }
                    }
                }
            }
            return min;
        }

        private class TableReaderPageFrame implements PageFrame {

            @Override
            public long getPageAddress(int columnIndex) {
                return columnPageAddress.getQuick(columnIndex);
            }

            @Override
            public long getPageSize(int columnIndex) {
                return pageSizes.getQuick(columnIndex);
            }
        }
    }

}
