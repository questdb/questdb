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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public class TableListRecordCursorFactory extends AbstractRecordCursorFactory {

    public static final String TABLE_NAME_COLUMN = "table";
    private static final RecordMetadata METADATA;
    private final TableListRecordCursor cursor;

    public TableListRecordCursorFactory() {
        super(METADATA);
        cursor = new TableListRecordCursor();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        return cursor.of(executionContext.getCairoEngine());
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    protected void _close() {
    }

    private static class TableListRecordCursor implements RecordCursor {
        private final TableListRecord record = new TableListRecord();
        private CairoEngine engine;
        private String tableName = null;
        private Iterator<TableToken> tableTokens;

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            if (tableTokens == null) {
                tableTokens = engine.getTableTokens().iterator();
            }

            do {
                boolean hasNext = tableTokens.hasNext();
                if (!hasNext) {
                    tableTokens = null;
                } else {
                    TableToken tableToken = tableTokens.next();
                    if (engine.isLiveTable(tableToken)) {
                        tableName = tableToken.getLoggingName();
                    } else {
                        tableName = null;
                    }
                }
            } while (tableTokens != null && tableName == null);

            return tableTokens != null;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            tableTokens = null;
        }

        private TableListRecordCursor of(@NotNull CairoEngine cairoEngine) {
            this.engine = cairoEngine;
            toTop();
            return this;
        }

        public class TableListRecord implements Record {
            @Override
            public CharSequence getStr(int col) {
                if (col == 0) {
                    return tableName;
                }
                return null;
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStr(col);
            }

            @Override
            public int getStrLen(int col) {
                return getStr(col).length();
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata(TABLE_NAME_COLUMN, ColumnType.STRING));
        METADATA = metadata;
    }
}
