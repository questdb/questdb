/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
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
    public void close() {
        Misc.free(filter);
        Misc.free(dataFrameCursorFactory);
    }

    @Override
    public boolean followedOrderByAdvice() {
        return followsOrderByAdvice;
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext) throws SqlException {
        DataFrameCursor dataFrameCursor = dataFrameCursorFactory.getCursor(executionContext);
        if (pageFrameCursor != null) {
            return pageFrameCursor.of(dataFrameCursor);
        } else if (framingSupported) {
            pageFrameCursor = new TableReaderPageFrameCursor(columnIndexes, columnSizes, getMetadata().getTimestampIndex());
            return pageFrameCursor.of(dataFrameCursor);
        } else {
            return null;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public boolean supportPageFrameCursor() {
        return framingSupported;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put("{\"name\":\"DataFrameRecordCursorFactory\", \"cursorFactory\":");
        dataFrameCursorFactory.toSink(sink);
        sink.put('}');
    }

    @Override
    protected RecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        cursor.of(dataFrameCursor, executionContext);
        if (filter != null) {
            filter.init(cursor, executionContext);
        }
        return cursor;
    }

    public static class TableReaderPageFrameCursor implements PageFrameCursor {
        private final LongList columnPageNextAddress = new LongList();
        private final LongList columnPageAddress = new LongList();
        private final TableReaderPageFrameCursor.TableReaderPageFrame frame = new TableReaderPageFrameCursor.TableReaderPageFrame();
        private final LongList topsRemaining = new LongList();
        private final IntList pages = new IntList();
        private final int columnCount;
        private final IntList columnIndexes;
        private final IntList columnSizes;
        private final LongList pageRowsRemaining = new LongList();
        private final LongList pageSizes = new LongList();
        private final int timestampIndex;
        private TableReader reader;
        private int partitionIndex;
        private long partitionRemaining = 0L;
        private DataFrameCursor dataFrameCursor;
        private long frameTopRowIndex = -1;
        private long pageMin;
        private long prevMin;

        public TableReaderPageFrameCursor(IntList columnIndexes, IntList columnSizes, int timestampIndex) {
            this.columnIndexes = columnIndexes;
            this.columnSizes = columnSizes;
            this.columnCount = columnIndexes.size();
            this.timestampIndex = timestampIndex;
        }

        @Override
        public void close() {
            dataFrameCursor = Misc.free(dataFrameCursor);
        }

        @Override
        public @Nullable PageFrame next() {

            if (partitionIndex > -1) {
                computePageMin(reader.getColumnBase(partitionIndex));
                if (pageMin < Long.MAX_VALUE) {
                    // Offset next frame lowest RowId with the count of rows returned in previous frame.
                    frameTopRowIndex += prevMin;
                    return computeFrame();
                }
            }

            DataFrame dataFrame;
            while ((dataFrame = dataFrameCursor.next()) != null) {
                this.partitionIndex = dataFrame.getPartitionIndex();
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
                        pageRowsRemaining.setQuick(i, -1L);
                    }

                    // reduce
                    for (int i = 0; i < columnCount; i++) {
                        long loRemaining = partitionLo;
                        long top = topsRemaining.getQuick(i);
                        if (top >= partitionLo) {
                            loRemaining = 0;
                            top -= partitionLo;
                            topsRemaining.setQuick(i, Math.min(top, partitionHi - partitionLo));
                        } else {
                            topsRemaining.setQuick(i, 0);
                            loRemaining -= top;
                        }

                        if (loRemaining > 0) {
                            final int readerColIndex = TableReader.getPrimaryColumnIndex(base, columnIndexes.getQuick(i));
                            final MemoryR col = reader.getColumn(readerColIndex);
                            if (col instanceof NullColumn) {
                                columnPageNextAddress.setQuick(i * 2, 0);
                                pageRowsRemaining.setQuick(i, 0);
                            } else {
                                final int sh = columnSizes.getQuick(i);
                                final int page = pages.getQuick(i);
                                if (sh > -1) {
                                    long pageRowCount = col.getPageSize() >> sh;
                                    if (pageRowCount < loRemaining) {
                                        throw CairoException.instance(0).put("partition is not mapped as single page, cannot perform vector calculation");
                                    }
                                    long addr = col.getPageAddress(page);
                                    addr += loRemaining << sh;
                                    columnPageNextAddress.setQuick(i * 2, addr);
                                    long pageHi = Math.min(partitionHi, pageRowCount);
                                    pageRowsRemaining.setQuick(i, pageHi - partitionLo);
                                    // todo: why are we setting pages to the same value?
                                    //     should this be + 1?
                                    pages.setQuick(i, page);
                                } else {
                                    // this is index column
                                    final MemoryR col2 = reader.getColumn(readerColIndex + 1);
                                    long pageRowCount = col2.getPageSize() >> 3;
                                    if (pageRowCount < loRemaining) {
                                        throw CairoException.instance(0).put("partition is not mapped as single page, cannot perform vector calculation");
                                    }

                                    long fixAddr = col2.getPageAddress(page);
                                    fixAddr += loRemaining << 3;
                                    columnPageNextAddress.setQuick(i * 2 + 1, fixAddr);
                                    long pageHi = Math.min(partitionHi, pageRowCount);
                                    pageRowsRemaining.setQuick(i, pageHi - partitionLo);
                                    // todo: why are we setting pages to the same value?
                                    pages.setQuick(i, page + 1);
                                }
                            }
                        }
                    }
                    frameTopRowIndex = dataFrame.getRowLo();
                    computePageMin(base);
                    return computeFrame();
                }
            }
            frameTopRowIndex = 0;
            return null;
        }

        @Override
        public void toTop() {
            this.partitionIndex = -1;
            this.dataFrameCursor.toTop();
            pages.setAll(columnCount, 0);
            topsRemaining.setAll(columnCount, 0);
            columnPageAddress.setAll(columnCount * 2, 0);
            columnPageNextAddress.setAll(columnCount * 2, 0);
            pageRowsRemaining.setAll(columnCount, -1L);
            pageSizes.setAll(columnCount * 2, -1L);
            pageMin = 0;
            prevMin = 0;
        }

        @Override
        public long size() {
            return reader.size();
        }

        @Override
        public SymbolMapReader getSymbolMapReader(int columnIndex) {
            return reader.getSymbolMapReader(columnIndexes.getQuick(columnIndex));
        }

        public TableReaderPageFrameCursor of(DataFrameCursor dataFrameCursor) {
            this.reader = dataFrameCursor.getTableReader();
            this.dataFrameCursor = dataFrameCursor;
            toTop();
            return this;
        }

        private TableReaderPageFrameCursor.TableReaderPageFrame computeFrame() {
            for (int i = 0; i < columnCount; i++) {
                final long top = topsRemaining.getQuick(i);
                if (top > 0) {
                    assert pageMin <= top;
                    topsRemaining.setQuick(i, top - pageMin);
                    columnPageAddress.setQuick(i * 2, 0);
                    pageSizes.setQuick(i * 2, pageMin);
                } else {
                    long addr = columnPageNextAddress.getQuick(i * 2);
                    long psz = pageRowsRemaining.getQuick(i);
                    pageRowsRemaining.setQuick(i, psz - pageMin);
                    columnPageAddress.setQuick(i * 2, addr);
                    if (addr != 0) {
                        final int shl = columnSizes.getQuick(i);
                        if (shl > -1) {
                            long pageSize = pageMin << shl;
                            pageSizes.setQuick(i * 2, pageSize);
                            columnPageNextAddress.setQuick(i * 2, addr + pageSize);
                        } else {
                            final long addr2 = columnPageNextAddress.getQuick(i * 2 + 1);
                            final long fixPageSize = pageMin << 3;
                            final long varPageSize = Unsafe.getUnsafe().getLong(addr2 + fixPageSize);
                            columnPageNextAddress.setQuick(i * 2, addr + Unsafe.getUnsafe().getLong(addr2 + varPageSize));
                            pageSizes.setQuick(i * 2, varPageSize);
                            columnPageNextAddress.setQuick(i * 2 + 1, addr2 + fixPageSize);
                            pageSizes.setQuick(i * 2 + 1, fixPageSize);
                        }
                    } else {
                        pageSizes.setQuick(i * 2, pageMin);
                    }
                }
            }
            partitionRemaining -= pageMin;
            if (partitionRemaining < 0) {
                throw CairoException.instance(0).put("incorrect frame built for vector calculation");
            }
            return frame;
        }

        private void computePageMin(int base) {
            // find min frame length
            prevMin = pageMin;
            pageMin = Long.MAX_VALUE;
            for (int i = 0; i < columnCount; i++) {
                final long top = topsRemaining.getQuick(i);
                if (top > 0) {
                    if (pageMin > top) {
                        pageMin = top;
                    }
                } else {
                    long psz = pageRowsRemaining.getQuick(i);
                    if (psz > 0) {
                        if (pageMin > psz) {
                            pageMin = psz;
                        }
                    } else if (partitionRemaining > 0) {
                        final int page = pages.getQuick(i);
                        pages.setQuick(i, page + 1);
                        final int readerColIndex = TableReader.getPrimaryColumnIndex(base, columnIndexes.getQuick(i));
                        final MemoryR col = reader.getColumn(readerColIndex);
                        final int shr = columnSizes.getQuick(i);
                        // page size is liable to change after it is mapped
                        // it is important to map page first and call pageSize() after
                        columnPageNextAddress.setQuick(i * 2, col.getPageAddress(page));
                        if (col instanceof NullColumn) {
                            psz = partitionRemaining;
                        } else if (shr > -1) {
                            psz = col.getPageSize() >> shr;
                        } else {
                            final MemoryR col2 = reader.getColumn(readerColIndex + 1);
                            columnPageNextAddress.setQuick(i * 2 + 1, col2.getPageAddress(page));
                            psz = (col2.getPageSize() >> 3) - 1;
                        }
                        final long m = Math.min(psz, partitionRemaining);
                        pageRowsRemaining.setQuick(i, m);
                        if (pageMin > m) {
                            pageMin = m;
                        }
                    }
                }
            }
        }

        private class TableReaderPageFrame implements PageFrame {
            @Override
            public BitmapIndexReader getBitmapIndexReader(int columnIndex, int direction) {
                return reader.getBitmapIndexReader(partitionIndex, columnIndexes.getQuick(columnIndex), direction);
            }

            @Override
            public long getTopRowIndex() {
                assert frameTopRowIndex >= 0;
                return frameTopRowIndex;
            }

            @Override
            public int getPartitionIndex() {
                return partitionIndex;
            }

            @Override
            public long getFirstTimestamp() {
                if (timestampIndex != -1) {
                    return Unsafe.getUnsafe().getLong(columnPageAddress.getQuick(timestampIndex * 2));
                }
                return Long.MIN_VALUE;
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
            public int getColumnSize(int columnIndex) {
                return columnSizes.getQuick(columnIndex);
            }

        }
    }

}
