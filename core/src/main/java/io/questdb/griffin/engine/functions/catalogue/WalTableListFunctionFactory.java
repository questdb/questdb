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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

public class WalTableListFunctionFactory implements FunctionFactory {
    private static final Log LOG = LogFactory.getLog(WalTableListFunctionFactory.class);
    private static final RecordMetadata METADATA;
    private static final int nameColumn;
    private static final int sequencerTxnColumn;
    private static final int suspendedColumn;
    private static final int writerTxnColumn;

    @Override
    public String getSignature() {
        return "wal_tables()";
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
    ) throws SqlException {
        return new CursorFunction(new WalTableListCursorFactory(configuration, sqlExecutionContext)) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    private static class WalTableListCursorFactory extends AbstractRecordCursorFactory {
        private final CairoConfiguration configuration;
        private final FilesFacade ff;
        private final SqlExecutionContext sqlExecutionContext;
        TableListRecordCursor cursor = new TableListRecordCursor();
        private Path rootPath;

        public WalTableListCursorFactory(CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
            super(METADATA);
            this.configuration = configuration;
            this.ff = configuration.getFilesFacade();
            this.rootPath = new Path().of(configuration.getRoot());
            this.sqlExecutionContext = sqlExecutionContext;
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
            this.rootPath = Misc.free(this.rootPath);
        }

        private class TableListRecordCursor implements RecordCursor {
            private final TableListRecord record = new TableListRecord();
            private final StringSink tableNameSink = new StringSink();
            private long findPtr = 0;

            @Override
            public void close() {
                findPtr = ff.findClose(findPtr);
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
                int rootLen = rootPath.length();
                while (true) {
                    if (findPtr == 0) {
                        findPtr = ff.findFirst(rootPath.$());
                        if (findPtr <= 0) {
                            return false;
                        }
                    } else {
                        if (ff.findNext(findPtr) <= 0) {
                            return false;
                        }
                    }
                    boolean isDir = Files.isDir(ff.findName(findPtr), ff.findType(findPtr), tableNameSink);
                    if (isDir) {
                        boolean isWalTable = TableSequencerAPI.isWalTable(tableNameSink, rootPath, ff);
                        rootPath.trimTo(rootLen);
                        if (isWalTable) {
                            if (record.switchTo(tableNameSink)) {
                                return true;
                            }
                        }
                    }
                }
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
                private long sequencerTxn;
                private boolean suspendedFlag;
                private long writerTxn;

                @Override
                public boolean getBool(int col) {
                    if (col == suspendedColumn) {
                        return suspendedFlag;
                    }
                    return false;
                }

                @Override
                public long getLong(int col) {
                    if (col == writerTxnColumn) {
                        return writerTxn;
                    }
                    if (col == sequencerTxnColumn) {
                        return sequencerTxn;
                    }
                    return Numbers.LONG_NaN;
                }

                @Override
                public CharSequence getStr(int col) {
                    if (col == nameColumn) {
                        return tableNameSink;
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

                public boolean switchTo(final CharSequence tableName) {
                    final CairoEngine cairoEngine = sqlExecutionContext.getCairoEngine();
                    final TableSequencerAPI sequencerAPI = cairoEngine.getTableSequencerAPI();

                    suspendedFlag = sequencerAPI.isSuspended(tableName);
                    sequencerTxn = sequencerAPI.lastTxn(tableName);
                    writerTxn = -1;
                    try (TableReader reader = cairoEngine.getReader(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                        writerTxn = reader.getTxn();
                    }
                    return true;
                }
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("name", ColumnType.STRING));
        nameColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("suspended", ColumnType.BOOLEAN));
        suspendedColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("writerTxn", ColumnType.LONG));
        writerTxnColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("sequencerTxn", ColumnType.LONG));
        sequencerTxnColumn = metadata.getColumnCount() - 1;
        METADATA = metadata;
    }
}
