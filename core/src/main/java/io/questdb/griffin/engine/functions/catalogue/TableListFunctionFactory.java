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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.TelemetryConfigLogger;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.tasks.TelemetryTask;

import static io.questdb.cairo.TableUtils.META_FILE_NAME;

public class TableListFunctionFactory implements FunctionFactory {
    private static final int DESIGNATED_TIMESTAMP_COLUMN;
    private static final int DIRECTORY_NAME_COLUMN;
    private static final int ID_COLUMN;
    private static final Log LOG = LogFactory.getLog(TableListFunctionFactory.class);
    private static final int MAX_UNCOMMITTED_ROWS_COLUMN;
    private static final RecordMetadata METADATA;
    private static final int NAME_COLUMN;
    private static final int O3MAXLAG_COLUMN;
    private static final int PARTITION_BY_COLUMN;
    private static final int WAL_ENABLED_COLUMN;

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
            sysTablePrefix = configuration.getSystemTableNamePrefix();
            cursor = new TableListRecordCursor();
            hideTelemetryTables = configuration.getTelemetryConfiguration().hideTables();
            tableReaderMetadata = new TableReaderMetadata(configuration);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            engine = executionContext.getCairoEngine();
            cursor.toTop();
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("tables()");
        }

        @Override
        protected void _close() {
            path = Misc.free(path);
            tableReaderMetadata = Misc.free(tableReaderMetadata);
        }

        private class TableListRecordCursor implements RecordCursor {
            private final TableListRecord record = new TableListRecord();
            private final ObjHashSet<TableToken> tableBucket = new ObjHashSet<>();
            private int tableIndex = -1;
            private TableToken tableToken;

            @Override
            public void close() {
                tableIndex = -1;
                tableReaderMetadata.clear(); // release FD of last table on the list
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
                private boolean isSoftLink;
                private int maxUncommittedRows;
                private long o3MaxLag;
                private int partitionBy;
                private int tableId;

                @Override
                public boolean getBool(int col) {
                    if (col == WAL_ENABLED_COLUMN) {
                        return tableReaderMetadata.isWalEnabled();
                    }
                    return false;
                }

                @Override
                public int getInt(int col) {
                    if (col == ID_COLUMN) {
                        return tableId;
                    }
                    if (col == MAX_UNCOMMITTED_ROWS_COLUMN) {
                        return maxUncommittedRows;
                    }
                    return Numbers.INT_NaN;
                }

                @Override
                public long getLong(int col) {
                    if (col == O3MAXLAG_COLUMN) {
                        return o3MaxLag;
                    }
                    return Numbers.LONG_NaN;
                }

                @Override
                public CharSequence getStr(int col) {
                    if (col == NAME_COLUMN) {
                        return tableToken.getTableName();
                    }
                    if (col == PARTITION_BY_COLUMN) {
                        return PartitionBy.toString(partitionBy);
                    }
                    if (col == DESIGNATED_TIMESTAMP_COLUMN) {
                        if (tableReaderMetadata.getTimestampIndex() > -1) {
                            return tableReaderMetadata.getColumnName(tableReaderMetadata.getTimestampIndex());
                        }
                    }
                    if (col == DIRECTORY_NAME_COLUMN) {
                        if (isSoftLink) {
                            return tableToken.getDirName() + " (->)";
                        }
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
                    if (hideTelemetryTables && (Chars.equals(tableToken.getTableName(), TelemetryTask.TABLE_NAME)
                            || Chars.equals(tableToken.getTableName(), TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME)
                            || Chars.startsWith(tableToken.getTableName(), sysTablePrefix))) {
                        return false;
                    }

                    int pathLen = path.length();
                    try {
                        path.concat(tableToken).$();
                        isSoftLink = Files.isSoftLink(path);
                        path.concat(META_FILE_NAME).$();
                        tableReaderMetadata.load(path);

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
        ID_COLUMN = 0;
        NAME_COLUMN = 1;
        DESIGNATED_TIMESTAMP_COLUMN = 2;
        PARTITION_BY_COLUMN = 3;
        MAX_UNCOMMITTED_ROWS_COLUMN = 4;
        O3MAXLAG_COLUMN = 5;
        WAL_ENABLED_COLUMN = 6;
        DIRECTORY_NAME_COLUMN = 7;
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("id", ColumnType.INT));
        metadata.add(new TableColumnMetadata("name", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("designatedTimestamp", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("partitionBy", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("maxUncommittedRows", ColumnType.INT));
        metadata.add(new TableColumnMetadata("o3MaxLag", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("walEnabled", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("directoryName", ColumnType.STRING));
        METADATA = metadata;
    }
}
