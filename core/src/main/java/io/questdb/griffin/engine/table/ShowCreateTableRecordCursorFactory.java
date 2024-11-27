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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoColumn;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

public class ShowCreateTableRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final int N_DDL_COL = 0;
    private static final RecordMetadata METADATA;
    private final ShowCreateTableCursor cursor = new ShowCreateTableCursor();
    private final TableToken tableToken;
    private final int tokenPosition;

    public ShowCreateTableRecordCursorFactory(TableToken tableToken, int tokenPosition) {
        super(METADATA);
        this.tableToken = tableToken;
        this.tokenPosition = tokenPosition;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        return cursor.of(executionContext, tableToken, tokenPosition);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("show_create_table");
        sink.meta("of").val(tableToken.getTableName());
    }

    public static class ShowCreateTableCursor implements NoRandomAccessRecordCursor {
        private final ShowCreateTableRecord record = new ShowCreateTableRecord();
        private final StringSink sink = new StringSink();
        private boolean hasRun;
        private CairoTable table;
        private TableToken tableToken;

        @Override
        public void close() {
            sink.clear();
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (!hasRun) {
                sink.clear();
                // CREATE TABLE table_name
                sink.put("CREATE TABLE ")
                        .put(tableToken.getTableName())
                        .put(" ( ")
                        .put('\n');

                // column_name TYPE CACHE/NOCACHE INDEX etc.
                for (int i = 0, n = (int) table.getColumnCount(); i < n; i++) {
                    final CairoColumn column = table.getColumnQuiet(i);
                    sink.put('\t')
                            .put(column.getName())
                            .put(' ')
                            .put(ColumnType.nameOf(column.getType()));

                    if (column.getType() == ColumnType.SYMBOL) {
                        sink.put(" CAPACITY ").put(column.getSymbolCapacity());
                        sink.put(column.getSymbolCached() ? " CACHE" : " NOCACHE");
                        if (column.getIsIndexed()) {
                            sink.put(" INDEX CAPACITY ")
                                    .put(column.getIndexBlockCapacity());
                        }
                    }

                    if (i < n - 1) {
                        sink.put(',');
                    }
                    sink.put('\n');
                }
                sink.put(')');

                // timestamp(ts)
                if (table.getTimestampIndex() != -1) {
                    sink.put(" timestamp(")
                            .put(table.getTimestampName())
                            .put(')');

                    // PARTITION BY unit
                    if (table.getPartitionBy() != PartitionBy.NONE) {
                        sink.put(" PARTITION BY ").put(table.getPartitionByName());
                    }

                    // (BYPASS) WAL
                    if (!table.getWalEnabled()) {
                        sink.put(" BYPASS ");
                    }
                    sink.put(" WAL");

                    // DEDUP UPSERT(key1, key2)
                    if (table.getIsDedup()) {
                        sink.put('\n');
                        sink.put("DEDUP UPSERT KEYS(");
                        for (int i = 0, n = (int) table.getColumnCount(); i < n; i++) {
                            final CairoColumn column = table.getColumnQuiet(i);
                            if (column.getIsDedupKey()) {
                                sink.put(column.getName());
                                sink.put(',');
                            }
                        }
                        // drop the last comma
                        sink.clear(sink.length() - 1);
                        sink.put(')');
                    }
                }
                // todo - owned by/other enterprise needs?
                sink.put(';');
                hasRun = true;
                return true;
            }
            return false;
        }

        public ShowCreateTableCursor of(SqlExecutionContext executionContext, TableToken tableToken, int tokenPosition) throws SqlException {
            this.tableToken = tableToken;
            try (MetadataCacheReader metadataRO = executionContext.getCairoEngine().getMetadataCache().readLock()) {
                this.table = metadataRO.getTable(tableToken);
                if (this.table == null) {
                    throw SqlException.$(tokenPosition, "table does not exist [table=")
                            .put(tableToken.getTableName()).put(']');
                }
            }
            toTop();
            return this;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            sink.clear();
            hasRun = false;
        }

        public class ShowCreateTableRecord implements Record {

            @Override
            @NotNull
            public CharSequence getStrA(int col) {
                if (col == N_DDL_COL) {
                    return sink.toString();
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStrA(col);
            }

            @Override
            public int getStrLen(int col) {
                return getStrA(col).length();
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("ddl", ColumnType.STRING));
        METADATA = metadata;
    }
}
