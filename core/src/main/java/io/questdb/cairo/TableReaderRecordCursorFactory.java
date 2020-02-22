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

package io.questdb.cairo;

import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.Nullable;

public class TableReaderRecordCursorFactory extends AbstractRecordCursorFactory {
    private final TableReaderRecordCursor cursor = new TableReaderRecordCursor();
    private final CairoEngine engine;
    private final String tableName;
    private final long tableVersion;
    private final IntList columnIndexes;
    private final IntList columnSizes;
    private TableReaderPageFrameCursor pageFrameCursor = null;

    public TableReaderRecordCursorFactory(
            RecordMetadata metadata,
            CairoEngine engine,
            String tableName,
            long tableVersion,
            @Nullable IntList columnIndexes,
            @Nullable IntList columnSizes
    ) {
        super(metadata);
        this.engine = engine;
        this.tableName = tableName;
        this.tableVersion = tableVersion;
        this.columnIndexes = columnIndexes;
        this.columnSizes = columnSizes;
    }

    @Override
    public void close() {
        Misc.free(cursor);
        Misc.free(pageFrameCursor);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        cursor.of(engine.getReader(executionContext.getCairoSecurityContext(), tableName, tableVersion));
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext) {
        if (pageFrameCursor != null) {
            return pageFrameCursor.of(engine.getReader(executionContext.getCairoSecurityContext(), tableName));
        } else if (columnIndexes != null) {
            pageFrameCursor = new TableReaderPageFrameCursor(columnIndexes, columnSizes);
            return pageFrameCursor.of(engine.getReader(executionContext.getCairoSecurityContext(), tableName));
        } else {
            return null;
        }
    }

    @Override
    public boolean supportPageFrameCursor() {
        return columnIndexes != null;
    }

    private static class TableReaderPageFrameCursor implements PageFrameCursor {
        private final LongList columnPageNextAddress = new LongList();
        private final LongList columnPageAddress = new LongList();
        private final TableReaderPageFrame frame = new TableReaderPageFrame();
        private final LongList topsRemaining = new LongList();
        private final IntList pages = new IntList();
        private TableReader reader;
        private final int columnCount;
        private IntList columnIndexes;
        private IntList columnSizes;
        private int partitionIndex;
        private int partitionCount;
        private LongList pageSizes = new LongList();
        private long pageValueCount;

        public TableReaderPageFrameCursor(IntList columnIndexes, IntList columnSizes) {
            this.columnIndexes = columnIndexes;
            this.columnSizes = columnSizes;
            this.columnCount = columnIndexes.size();
        }

        @Override
        public void close() {
            reader = Misc.free(reader);
        }

        @Override
        public @Nullable PageFrame next() {

            // find min frame length
            long min = Long.MAX_VALUE;
            for (int i = 0; i < columnCount; i++) {
                final long top = topsRemaining.getQuick(i);
                if (top > 0) {
                    if (min > top) {
                        min = top;
                    }
                } else {
                    long psz = pageSizes.getQuick(i);
                    if (psz > 0) {
                        if (min > psz) {
                            min = psz;
                        }
                    }
                }
            }
            final long m = min;
            if (m < Long.MAX_VALUE) {
                return computeFrame(m);
            }

            while (++partitionIndex < partitionCount) {
                long size = reader.openPartition(partitionIndex);
                if (size > 0) {
                    final int base = reader.getColumnBase(partitionIndex);
                    // copy table tops
                    for (int i = 0, n = columnIndexes.size(); i < n; i++) {
                        final int columnIndex = columnIndexes.getQuick(i);
                        topsRemaining.setQuick(i, reader.getColumnTop(base, columnIndex));
                        pages.setQuick(i, 0);
                        pageSizes.setQuick(i, -1L);
                    }

                    return computeFrame(computePageMin(base));
                }
            }
            return null;
        }

        @Override
        public void toTop() {
            this.partitionIndex = -1;
            this.partitionCount = reader.getPartitionCount();
            pages.setAll(columnCount, 0);
            topsRemaining.setAll(columnCount, 0);
            columnPageAddress.setAll(columnCount, 0);
            columnPageNextAddress.setAll(columnCount, 0);
            pageSizes.setAll(columnCount, -1L);
        }

        public TableReaderPageFrameCursor of(TableReader reader) {
            this.reader = reader;
            toTop();
            return this;
        }

        private PageFrame computeFrame(long min) {
            for (int i = 0; i < columnCount; i++) {
                final int columnIndex = columnIndexes.getQuick(i);
                final long top = topsRemaining.getQuick(i);
                if (top > 0) {
                    topsRemaining.setQuick(i, top - min);
                    columnPageAddress.setQuick(columnIndex, 0);
                } else {
                    long addr = columnPageNextAddress.getQuick(i);
                    long psz = pageSizes.getQuick(i);
                    pageSizes.setQuick(i, psz - min);
                    columnPageAddress.setQuick(i, addr);
                    columnPageNextAddress.setQuick(i, addr + (psz * columnSizes.getQuick(i)));
                }
            }
            pageValueCount = min;
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
                    long psz = pageSizes.getQuick(i);
                    if (psz > 0) {
                        if (min > psz) {
                            min = psz;
                        }
                    } else {
                        final int page = pages.getQuick(i);
                        pages.setQuick(i, page + 1);
                        final ReadOnlyColumn col = reader.getColumn(TableReader.getPrimaryColumnIndex(base, columnIndexes.getQuick(i)));
                        long m = col.getPageSize(page) / columnSizes.getQuick(i);
                        columnPageNextAddress.setQuick(i, col.getPageAddress(page));
                        pageSizes.setQuick(i, m);
                        if (min > m) {
                            min = m;
                        }
                    }
                }
            }
            return min;
        }

        @Override
        public long size() {
            return reader.size();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return reader.getSymbolMapReader(columnIndex);
        }

        private class TableReaderPageFrame implements PageFrame {

            @Override
            public long getPageAddress(int columnIndex) {
                return columnPageAddress.getQuick(columnIndex);
            }

            @Override
            public long getPageValueCount(int columnIndex) {
                return pageValueCount;
            }
        }
    }
}
