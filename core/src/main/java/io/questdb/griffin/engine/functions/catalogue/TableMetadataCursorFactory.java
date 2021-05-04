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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;

public class TableMetadataCursorFactory implements FunctionFactory {
    private static final RecordMetadata METADATA;
    private static final int idColumn;
    private static final int nameColumn;
    private static final int partitionByColumn;
    private static final int maxUncommittedRows;
    private static final int o3CommitHysteresisMicroSec;
    private static final int designatedTimestampColumn;
    private static final Log LOG = LogFactory.getLog(TableMetadataCursorFactory.class);

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("id", ColumnType.INT, null));
        idColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("name", ColumnType.STRING, null));
        nameColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("designatedTimestamp", ColumnType.STRING, null));
        designatedTimestampColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("partitionBy", ColumnType.STRING, null));
        partitionByColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("maxUncommittedRows", ColumnType.INT, null));
        maxUncommittedRows = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("o3CommitHysteresisMicros", ColumnType.LONG, null));
        o3CommitHysteresisMicroSec = metadata.getColumnCount() - 1;
        METADATA = metadata;
    }

    @Override
    public String getSignature() {
        return "tables()";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        return new CursorFunction(
                position,
                new TableMetadataCursor(configuration.getFilesFacade(), configuration.getRoot())
        );
    }

    private static class TableMetadataCursor implements RecordCursorFactory {
        private final FilesFacade ff;
        private Path path;
        private final TableListRecordCursor cursor;

        public TableMetadataCursor(FilesFacade ff, CharSequence dbRoot) {
            this.ff = ff;
            path = new Path().of(dbRoot).$();
            cursor = new TableListRecordCursor();
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            return cursor.of(executionContext);
        }

        @Override
        public RecordMetadata getMetadata() {
            return METADATA;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void close() {
            if (null != path) {
                path.close();
                path = null;
            }
        }

        private class TableListRecordCursor implements RecordCursor {
            private final NativeLPSZ nativeLPSZ = new NativeLPSZ();
            private final TableListRecord record = new TableListRecord();
            private long findPtr = 0;
            private SqlExecutionContext executionContext;

            @Override
            public void close() {
                if (findPtr > 0) {
                    ff.findClose(findPtr);
                    findPtr = 0;
                }
                if (record.reader != null) {
                    record.reader.close();
                }
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
                    nativeLPSZ.of(ff.findName(findPtr));
                    int type = ff.findType(findPtr);
                    if (type == Files.DT_DIR && nativeLPSZ.charAt(0) != '.') {
                        if (record.open(executionContext, nativeLPSZ)) {
                            return true;
                        }
                    }
                }
            }

            public TableListRecordCursor of(SqlExecutionContext executionContext) {
                this.executionContext = executionContext;
                toTop();
                return this;
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
                TableReaderMetadata reader;

                @Override
                public CharSequence getStr(int col) {
                    if (col == nameColumn) {
                        return nativeLPSZ;
                    }
                    if (col == partitionByColumn) {
                        return PartitionBy.toString(reader.getPartitionBy());
                    }
                    if (col == designatedTimestampColumn) {
                        if (reader.getTimestampIndex() > -1) {
                            return reader.getColumnName(reader.getTimestampIndex());
                        }
                    }
                    return null;
                }

                @Override
                public CharSequence getStrB(int col) {
                    return getStr(col);
                }

                @Override
                public int getInt(int col) {
                    if (col == idColumn) {
                        return reader.getId();
                    }
                    if (col == maxUncommittedRows) {
                        return reader.getMaxUncommittedRows();
                    }
                    return Numbers.INT_NaN;
                }

                @Override
                public long getLong(int col) {
                    if (col == o3CommitHysteresisMicroSec) {
                        return reader.getO3CommitHysteresisMicros();
                    }
                    return Numbers.LONG_NaN;
                }

                @Override
                public int getStrLen(int col) {
                    return getStr(col).length();
                }

                public boolean open(SqlExecutionContext executionContext, NativeLPSZ tableName) {
                    if (reader != null) {
                        reader.close();
                        reader = null;
                    }
                    int pathLen = path.length();
                    try {
                        reader = new TableReaderMetadata(executionContext.getCairoEngine().getConfiguration().getFilesFacade(), path.chop$().concat(tableName).concat(TableUtils.META_FILE_NAME).$());
                    } catch (CairoException e) {
                        // perhaps this folder is not a table
                        // remove it from the result set
                        LOG.info().$("cannot query table metadata [table=").$(tableName).$(", error=").$(e.getFlyweightMessage()).I$();
                        return false;
                    } finally {
                        path.trimTo(pathLen);
                    }

                    return true;
                }
            }
        }
    }
}
