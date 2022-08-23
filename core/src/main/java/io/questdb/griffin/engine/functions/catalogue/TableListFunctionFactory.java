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
import io.questdb.std.str.StringSink;

import static io.questdb.cairo.TableUtils.META_FILE_NAME;

public class TableListFunctionFactory implements FunctionFactory {
    private static final RecordMetadata METADATA;
    private static final int idColumn;
    private static final int nameColumn;
    private static final int partitionByColumn;
    private static final int maxUncommittedRowsColumn;
    private static final int commitLagColumn;
    private static final int designatedTimestampColumn;
    private static final Log LOG = LogFactory.getLog(TableListFunctionFactory.class);
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
        return new CursorFunction(
                new TableListCursorFactory(
                        configuration.getFilesFacade(),
                        configuration.getRoot(),
                        configuration.getTelemetryConfiguration().hideTables(),
                        configuration.getSystemTableNamePrefix())
        ) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    private static class TableListCursorFactory extends AbstractRecordCursorFactory {
        private final FilesFacade ff;
        private final TableListRecordCursor cursor;
        private final boolean hideTelemetryTables;
        private final CharSequence sysTablePrefix;
        private Path path;
        private TableReaderMetadata metaReader;

        public TableListCursorFactory(FilesFacade ff, CharSequence dbRoot, boolean hideTelemetryTables, CharSequence sysTablePrefix) {
            super(METADATA);
            this.ff = ff;
            path = new Path().of(dbRoot).$();
            this.sysTablePrefix = sysTablePrefix;
            cursor = new TableListRecordCursor();
            this.hideTelemetryTables = hideTelemetryTables;
            this.metaReader = new TableReaderMetadata(ff);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
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
            metaReader = Misc.free(metaReader);
        }

        private class TableListRecordCursor implements RecordCursor {
            private final StringSink sink = new StringSink();
            private final TableListRecord record = new TableListRecord();
            private long findPtr = 0;

            @Override
            public void close() {
                findPtr = ff.findClose(findPtr);
                metaReader.clear();//release FD of last table on the list
            }

            @Override
            public Record getRecord() {
                return record;
            }

            @Override
            public boolean hasNext() {
                while (true) {
                    if (findPtr == 0) {
                        findPtr = ff.findFirst(path);
                        if (findPtr <= 0) {
                            return false;
                        }
                    } else {
                        if (ff.findNext(findPtr) <= 0) {
                            return false;
                        }
                    }
                    if (Files.isDir(ff.findName(findPtr), ff.findType(findPtr), sink)) {
                        if (record.open(sink)) {
                            return true;
                        }
                    }
                }
            }

            @Override
            public Record getRecordB() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void recordAt(Record record, long atRowId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void toTop() {
                close();
            }

            @Override
            public long size() {
                return -1;
            }

            public class TableListRecord implements Record {
                private int tableId;
                private int maxUncommittedRows;
                private long commitLag;
                private int partitionBy;

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
                    if (col == commitLagColumn) {
                        return commitLag;
                    }
                    return Numbers.LONG_NaN;
                }

                @Override
                public CharSequence getStr(int col) {
                    if (col == nameColumn) {
                        return sink;
                    }
                    if (col == partitionByColumn) {
                        return PartitionBy.toString(partitionBy);
                    }
                    if (col == designatedTimestampColumn) {
                        if (metaReader.getTimestampIndex() > -1) {
                            return metaReader.getColumnName(metaReader.getTimestampIndex());
                        }
                    }
                    return null;
                }

                @Override
                public boolean getBool(int col) {
                    if (col == writeModeColumn) {
                        return metaReader.isWalEnabled();
                    }
                    return false;
                }

                @Override
                public CharSequence getStrB(int col) {
                    return getStr(col);
                }

                @Override
                public int getStrLen(int col) {
                    return getStr(col).length();
                }

                public boolean open(CharSequence tableName) {

                    if (hideTelemetryTables && (Chars.equals(tableName, TelemetryJob.tableName) || Chars.equals(tableName, TelemetryJob.configTableName) || Chars.startsWith(tableName, sysTablePrefix))) {
                        return false;
                    }

                    int pathLen = path.length();
                    try {
                        path.chop$().concat(tableName).concat(META_FILE_NAME).$();
                        metaReader.deferredInit(path.$(), ColumnType.VERSION);

                        // Pre-read as much as possible to skip record instead of failing on column fetch
                        tableId = metaReader.getId();
                        maxUncommittedRows = metaReader.getMaxUncommittedRows();
                        commitLag = metaReader.getCommitLag();
                        partitionBy = metaReader.getPartitionBy();
                    } catch (CairoException e) {
                        // perhaps this folder is not a table
                        // remove it from the result set
                        LOG.info()
                                .$("cannot query table metadata [table=").$(tableName)
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
        metadata.add(new TableColumnMetadata("id", 1, ColumnType.INT));
        idColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("name", 2, ColumnType.STRING));
        nameColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("designatedTimestamp", 3, ColumnType.STRING));
        designatedTimestampColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("partitionBy", 4, ColumnType.STRING));
        partitionByColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("maxUncommittedRows", 5, ColumnType.INT));
        maxUncommittedRowsColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("commitLag", 6, ColumnType.LONG));
        commitLagColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("walEnabled", 7, ColumnType.BOOLEAN));
        writeModeColumn = metadata.getColumnCount() - 1;
        METADATA = metadata;
    }
}
