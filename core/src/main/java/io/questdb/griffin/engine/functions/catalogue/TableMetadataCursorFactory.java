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
import io.questdb.std.*;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.META_FILE_NAME;

public class TableMetadataCursorFactory implements FunctionFactory {
    private static final RecordMetadata METADATA;
    private static final int idColumn;
    private static final int nameColumn;
    private static final int partitionByColumn;
    private static final int maxUncommittedRowsColumn;
    private static final int commitLagColumn;
    private static final int designatedTimestampColumn;
    private static final Log LOG = LogFactory.getLog(TableMetadataCursorFactory.class);

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
                new TableMetadataCursor(configuration.getFilesFacade(), configuration.getRoot())
        ) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    private static class TableMetadataCursor implements RecordCursorFactory {
        private final FilesFacade ff;
        private final TableListRecordCursor cursor;
        private Path path;

        public TableMetadataCursor(FilesFacade ff, CharSequence dbRoot) {
            this.ff = ff;
            path = new Path().of(dbRoot).$();
            cursor = new TableListRecordCursor();
        }

        @Override
        public void close() {
            if (null != path) {
                path.close();
                path = null;
            }
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
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

        private class TableListRecordCursor implements RecordCursor {
            private final NativeLPSZ nativeLPSZ = new NativeLPSZ();
            private final TableListRecord record = new TableListRecord();
            private long findPtr = 0;
            private TableReaderMetadata metaReader;

            @Override
            public void close() {
                if (findPtr > 0) {
                    ff.findClose(findPtr);
                    findPtr = 0;
                }
                if (metaReader != null) {
                    metaReader.close();
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
                        if (record.open(nativeLPSZ)) {
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

            public TableListRecordCursor of(SqlExecutionContext executionContext) {
                toTop();
                if (metaReader == null) {
                    // Assuming FilesFacade does not chang in real execution
                    metaReader = new TableReaderMetadata(executionContext.getCairoEngine().getConfiguration().getFilesFacade());
                }
                return this;
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
                        return nativeLPSZ;
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
                public CharSequence getStrB(int col) {
                    return getStr(col);
                }

                @Override
                public int getStrLen(int col) {
                    return getStr(col).length();
                }

                public boolean open(NativeLPSZ tableName) {
                    int pathLen = path.length();
                    try {
                        path.chop$().concat(tableName).concat(META_FILE_NAME).$();
                        metaReader.of(path.$());

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
        metadata.add(new TableColumnMetadata("id", ColumnType.INT, null));
        idColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("name", ColumnType.STRING, null));
        nameColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("designatedTimestamp", ColumnType.STRING, null));
        designatedTimestampColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("partitionBy", ColumnType.STRING, null));
        partitionByColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("maxUncommittedRows", ColumnType.INT, null));
        maxUncommittedRowsColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("commitLag", ColumnType.LONG, null));
        commitLagColumn = metadata.getColumnCount() - 1;
        METADATA = metadata;
    }
}
