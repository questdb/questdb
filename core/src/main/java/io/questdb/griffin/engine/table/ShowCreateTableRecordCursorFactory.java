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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
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
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
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
        private final Utf8StringSink sink = new Utf8StringSink();

        private SqlExecutionContext executionContext;
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

                // column_name TYPE
                for (int i = 0, n = (int) table.getColumnCount(); i < n; i++) {
                    final CairoColumn column = table.getColumnQuiet(i);
                    sink.put('\t')
                            .put(column.getName())
                            .put(' ')
                            .put(ColumnType.nameOf(column.getType()));

                    if (column.getType() == ColumnType.SYMBOL) {
                        // CAPACITY value (NO)CACHE
                        sink.put(" CAPACITY ").put(column.getSymbolCapacity());
                        sink.put(column.getSymbolCached() ? " CACHE" : " NOCACHE");

                        if (column.getIsIndexed()) {
                            // INDEX CAPACITY value
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
                        sink.put(" BYPASS");
                    }
                    sink.put(" WAL");
                }

                final CairoConfiguration config = executionContext.getCairoEngine().getConfiguration();
                final boolean withMaxUncommittedRows = table.getMaxUncommittedRows() != config.getMaxUncommittedRows();
                final boolean withO3MaxLag = table.getO3MaxLag() != config.getO3MaxLag();
                final boolean withRequired = withMaxUncommittedRows || withO3MaxLag;

                // WITH maxUncommittedRows=123, o3MaxLag=456s
                if (withRequired) {
                    sink.put('\n').put("WITH ");
                    if (withMaxUncommittedRows) {
                        sink.put("maxUncommittedRows=").put(table.getMaxUncommittedRows());
                    }
                    if (withO3MaxLag) {
                        if (withMaxUncommittedRows) {
                            sink.put(", ");
                        }
                        sink.put("o3MaxLag=").put(table.getO3MaxLag()).put("us");
                    }
                }

                // IN VOLUME OTHER_VOLUME
                if (table.getIsSoftLink()) {
                    if (withRequired) {
                        sink.put(',');
                    }
                    sink.put(" IN VOLUME ");

                    FilesFacade ff = config.getFilesFacade();
                    try (Path softLinkPath = new Path().of(config.getRoot()).concat(table.getDirectoryName())) {
                        try (Path otherVolumePath = new Path()) {
                            ff.readLink(softLinkPath, otherVolumePath);
                            otherVolumePath.trimTo(otherVolumePath.size()
                                    - table.getDirectoryName().length()  // look for directory
                                    - 1 // get rid of trailing slash
                            );
                            CharSequence alias = config.getVolumeDefinitions().resolvePath(otherVolumePath.asAsciiCharSequence());

                            if (alias == null) {
                                throw CairoException.nonCritical().put("could not find volume alias for table [table=").put(tableToken).put(']');
                            } else {
                                sink.put(alias);
                            }
                        }
                    }
                }

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
                    sink.clear(sink.size() - 1);
                    sink.put(')');
                }
                sink.put(';');
                hasRun = true;
                return true;
            }
            return false;
        }

        public ShowCreateTableCursor of(SqlExecutionContext executionContext, TableToken tableToken, int tokenPosition) throws SqlException {
            this.tableToken = tableToken;
            this.executionContext = executionContext;
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
            public Utf8Sequence getVarcharA(int col) {
                if (col == N_DDL_COL) {
                    return sink;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public Utf8Sequence getVarcharB(int col) {
                return getVarcharA(col);
            }

            @Override
            public int getVarcharSize(int col) {
                return getVarcharA(col).size();
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("ddl", ColumnType.VARCHAR));
        METADATA = metadata;
    }
}
