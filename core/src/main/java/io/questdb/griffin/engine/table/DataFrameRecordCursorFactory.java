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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.ReadOnlyVirtualMemory;
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
        private final LongList pageNRowsRemaining = new LongList();
        private final LongList pageSizes = new LongList();
        private TableReader reader;
        private int partitionIndex;
        private long partitionRemaining = 0L;
        private DataFrameCursor dataFrameCursor;
        private final int timestampIndex;
        private long rowLo = -1;

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
                final long m = computePageMin(reader.getColumnBase(partitionIndex));
                if (m < Long.MAX_VALUE) {
                    // Offset next frame lowest RowId with the count of rows returned in previous frame.
                    rowLo += pageSizes.get(timestampIndex) >> columnSizes.get(timestampIndex);
                    return computeFrame(m);
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
                        pageNRowsRemaining.setQuick(i, -1L);
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
                            final ReadOnlyVirtualMemory col = reader.getColumn(TableReader.getPrimaryColumnIndex(base, columnIndexes.getQuick(i)));
                            if (col instanceof NullColumn) {
                                columnPageNextAddress.setQuick(i, 0);
                                pageNRowsRemaining.setQuick(i, 0);
                            } else {
                                int page = pages.getQuick(i);
                                long pageSize = col.getPageSize(page) >> columnSizes.getQuick(i);
                                if (pageSize < loRemaining) {
                                    throw CairoException.instance(0).put("partition is not mapped as single page, cannot perform vector calculation");
                                }
                                long addr = col.getPageAddress(page);
                                addr += loRemaining << columnSizes.getQuick(i);
                                columnPageNextAddress.setQuick(i, addr);
                                long pageHi = Math.min(partitionHi, pageSize);
                                pageNRowsRemaining.setQuick(i, pageHi - partitionLo);
                                pages.setQuick(i, page);
                            }
                        }
                    }
                    rowLo = dataFrame.getRowLo();
                    TableReaderPageFrameCursor.TableReaderPageFrame pageFrame = computeFrame(computePageMin(base));
                    return pageFrame;
                }
            }
            rowLo = 0;
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

        private TableReaderPageFrameCursor.TableReaderPageFrame computeFrame(long min) {
            for (int i = 0; i < columnCount; i++) {
                final long top = topsRemaining.getQuick(i);
                if (top > 0) {
                    assert min <= top;
                    topsRemaining.setQuick(i, top - min);
                    columnPageAddress.setQuick(i, 0);
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
            if (partitionRemaining < 0) {
                throw CairoException.instance(0).put("incorrect frame built for vector calculation");
            }
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
                        final ReadOnlyVirtualMemory col = reader.getColumn(TableReader.getPrimaryColumnIndex(base, columnIndexes.getQuick(i)));
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
            public BitmapIndexReader getBitmapIndexReader(int columnIndex, int direction) {
                return reader.getBitmapIndexReader(partitionIndex, columnIndexes.getQuick(columnIndex), direction);
            }

            @Override
            public long getFirstRowId() {
                assert rowLo >= 0;
                return rowLo;
            }

            @Override
            public int getPartitionIndex() {
                return partitionIndex;
            }

            @Override
            public long getFirstTimestamp() {
                if (timestampIndex != -1) {
                    return Unsafe.getUnsafe().getLong(columnPageAddress.getQuick(timestampIndex));
                }
                return Long.MIN_VALUE;
            }

            @Override
            public long getPageAddress(int columnIndex) {
                return columnPageAddress.getQuick(columnIndex);
            }

            @Override
            public long getPageSize(int columnIndex) {
                return pageSizes.getQuick(columnIndex);
            }

            @Override
            public int getColumnSize(int columnIndex) {
                return columnSizes.getQuick(columnIndex);
            }

        }
    }

}
