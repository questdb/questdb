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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.TelemetryJob;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.META_FILE_NAME;

public class TableListFunctionFactory implements FunctionFactory {
    private static final Log LOG = LogFactory.getLog(TableListFunctionFactory.class);
    private static final RecordMetadata METADATA;
    private static final int designatedTimestampColumn;
    private static final int idColumn;
    private static final int maxUncommittedRowsColumn;
    private static final int nameColumn;
    private static final int o3MaxLagColumn;
    private static final int partitionByColumn;
    private static final int systemNameColumn;
    private static final int writeModeColumn;

    @Override
    public String getSignature() {
        return "tables()";
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new CursorFunction(new TableListCursorFactory(configuration)) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    private static class TableListCursorFactory extends AbstractRecordCursorFactory {
        private final TableListRecordCursor cursor;
        private final boolean hideTelemetryTables;
        private final CharSequence sysTablePrefix;
        private CairoEngine engine;
        private Path path;
        private TableReaderMetadata tableReaderMetadata;

        public TableListCursorFactory(CairoConfiguration configuration) {
            super(METADATA);
            path = new Path().of(configuration.getRoot()).$();
            this.sysTablePrefix = configuration.getSystemTableNamePrefix();
            cursor = new TableListRecordCursor();
            this.hideTelemetryTables = configuration.getTelemetryConfiguration().hideTables();
            this.tableReaderMetadata = new TableReaderMetadata(configuration);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            this.engine = executionContext.getCairoEngine();
            cursor.toTop();
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        protected void _close() {
            path = Misc.free(path);
            tableReaderMetadata = Misc.free(tableReaderMetadata);
        }

        private class TableListRecordCursor implements RecordCursor {
            private final TableListRecord record = new TableListRecord();
            private final ObjList<TableToken> tableBucket = new ObjList<>();
            private int tableIndex = -1;
            private TableToken tableToken;

            @Override
            public void close() {
                tableIndex = -1;
                tableReaderMetadata.clear();//release FD of last table on the list
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
                if (tableIndex < 0) {
                    engine.getTableTokens(tableBucket, false);
                    tableIndex = -1;
                }

                tableIndex++;
                int n = tableBucket.size();
                for (; tableIndex < n; tableIndex++) {
                    tableToken = tableBucket.get(tableIndex);
                    if (record.open(tableToken)) {
                        break;
                    }
                }

                return tableIndex < n;
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
                close();
            }

            public class TableListRecord implements Record {
                private int maxUncommittedRows;
                private long o3MaxLag;
                private int partitionBy;
                private int tableId;

                @Override
                public boolean getBool(int col) {
                    if (col == writeModeColumn) {
                        return tableReaderMetadata.isWalEnabled();
                    }
                    return false;
                }

                @Override
                public int getInt(int col) {
                    if (col == idColumn) {
                        return tableId;
                    }
                    if (col == maxUncommittedRowsColumn) {
                        return maxUncommittedRows;
                    }
                    return Numbers.INT_NaN;
                }

                @Override
                public long getLong(int col) {
                    if (col == o3MaxLagColumn) {
                        return o3MaxLag;
                    }
                    return Numbers.LONG_NaN;
                }

                @Override
                public CharSequence getStr(int col) {
                    if (col == nameColumn) {
                        return tableToken.getTableName();
                    }
                    if (col == partitionByColumn) {
                        return PartitionBy.toString(partitionBy);
                    }
                    if (col == designatedTimestampColumn) {
                        if (tableReaderMetadata.getTimestampIndex() > -1) {
                            return tableReaderMetadata.getColumnName(tableReaderMetadata.getTimestampIndex());
                        }
                    }
                    if (col == systemNameColumn) {
                        return tableToken.getDirName();
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

                public boolean open(TableToken tableToken) {

                    if (hideTelemetryTables && (Chars.equals(tableToken.getTableName(), TelemetryJob.tableName)
                            || Chars.equals(tableToken.getTableName(), TelemetryJob.configTableName)
                            || Chars.startsWith(tableToken.getTableName(), sysTablePrefix))) {
                        return false;
                    }

                    int pathLen = path.length();
                    try {
                        path.concat(tableToken).concat(META_FILE_NAME).$();
                        tableReaderMetadata.load(path.$());

                        // Pre-read as much as possible to skip record instead of failing on column fetch
                        tableId = tableReaderMetadata.getTableId();
                        maxUncommittedRows = tableReaderMetadata.getMaxUncommittedRows();
                        o3MaxLag = tableReaderMetadata.getO3MaxLag();
                        partitionBy = tableReaderMetadata.getPartitionBy();
                    } catch (CairoException e) {
                        // perhaps this folder is not a table
                        // remove it from the result set
                        LOG.info()
                                .$("cannot query table metadata [table=").$(tableToken)
                                .$(", error=").$(e.getFlyweightMessage())
                                .$(", errno=").$(e.getErrno())
                                .I$();
                        return false;
                    } finally {
                        path.trimTo(pathLen).$();
                    }

                    return true;
                }
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("id", ColumnType.INT));
        idColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("name", ColumnType.STRING));
        nameColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("designatedTimestamp", ColumnType.STRING));
        designatedTimestampColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("partitionBy", ColumnType.STRING));
        partitionByColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("maxUncommittedRows", ColumnType.INT));
        maxUncommittedRowsColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("o3MaxLag", ColumnType.LONG));
        o3MaxLagColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("walEnabled", ColumnType.BOOLEAN));
        writeModeColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("directoryName", ColumnType.STRING));
        systemNameColumn = metadata.getColumnCount() - 1;
        METADATA = metadata;
    }
}
