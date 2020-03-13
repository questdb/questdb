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
import org.jetbrains.annotations.Nullable;

public class TableReaderRecordCursorFactory extends AbstractRecordCursorFactory {
    private final TableReaderRecordCursor cursor = new TableReaderRecordCursor();
    private final CairoEngine engine;
    private final String tableName;
    private final long tableVersion;

    public TableReaderRecordCursorFactory(RecordMetadata metadata, CairoEngine engine, String tableName, long tableVersion) {
        super(metadata);
        this.engine = engine;
        this.tableName = tableName;
        this.tableVersion = tableVersion;
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
        return null;
    }

    @Override
    public boolean supportPageFrameCursor() {
        return true;
    }

    private static class TableReaderPageFrameCursor implements PageFrameCursor {
        private TableReader reader;


        @Override
        public void close() {

        }

        @Override
        public @Nullable PageFrame next() {

//            double result = 0;
//            for (int i = 0; i < partitionCount; i++) {
//                openPartition(i);
//                final int base = getColumnBase(i);
//                final int index = getPrimaryColumnIndex(base, columnIndex);
//                final ReadOnlyColumn column = columns.getQuick(index);
//                for (int pageIndex = 0, pageCount = column.getPageCount(); pageIndex < pageCount; pageIndex++) {
//                    long a = column.getPageAddress(pageIndex);
//                    long count = column.getPageSize(pageIndex) / Double.BYTES;
//                    result += Vect.sumDouble(a, count);
//                }
//            }
//
//            return result;
//

            return null;
        }

        @Override
        public void toTop() {

        }

        @Override
        public long size() {
            return reader.size();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return reader.getSymbolMapReader(columnIndex);
        }
    }

    private static class TableReaderPageFrame implements PageFrame {

        @Override
        public long getPageAddress(int columnIndex) {
            return 0;
        }

        @Override
        public long getPageValueCount(int columnIndex) {
            return 0;
        }
    }
}
