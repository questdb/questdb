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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.META_FILE_NAME;
import static io.questdb.cairo.wal.WalUtils.*;
import static io.questdb.cairo.wal.seq.TableTransactionLog.MAX_TXN_OFFSET;

public class WalTableListFunctionFactory implements FunctionFactory {
    private static final Log LOG = LogFactory.getLog(WalTableListFunctionFactory.class);
    private static final RecordMetadata METADATA;
    private static final String SIGNATURE = "wal_tables()";
    private static final int nameColumn;
    private static final int sequencerTxnColumn;
    private static final int suspendedColumn;
    private static final int writerTxnColumn;

    @Override
    public String getSignature() {
        return SIGNATURE;
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
        private final TableListRecordCursor cursor;
        private final FilesFacade ff;
        private final SqlExecutionContext sqlExecutionContext;
        private Path rootPath;
        private CairoEngine engine;

        public WalTableListCursorFactory(CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
            super(METADATA);
            this.ff = configuration.getFilesFacade();
            this.rootPath = new Path().of(configuration.getRoot());
            this.sqlExecutionContext = sqlExecutionContext;
            this.cursor = new TableListRecordCursor();
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
            sink.val(SIGNATURE);
        }

        @Override
        protected void _close() {
            this.rootPath = Misc.free(this.rootPath);
        }

        private class TableListRecordCursor implements RecordCursor {
            private final TableListRecord record = new TableListRecord();
            private final TxReader txReader = new TxReader(ff);
            private final ObjHashSet<TableToken> tableBucket = new ObjHashSet<>();
            private int tableIndex = -1;

            @Override
            public void close() {
                tableIndex = -1;
                txReader.close();
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
                    TableToken tableToken = tableBucket.get(tableIndex);
                    if (engine.isWalTable(tableToken) && record.switchTo(tableToken)) {
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
                private long sequencerTxn;
                private boolean suspendedFlag;
                private String tableName;
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

                private boolean switchTo(final TableToken tableToken) {
                    tableName = tableToken.getTableName();
                    int rootLen = rootPath.length();
                    rootPath.concat(tableToken).concat(SEQ_DIR);
                    int metaFd = -1;
                    int txnFd = -1;
                    try {
                        metaFd = TableUtils.openRO(ff, rootPath, META_FILE_NAME, LOG);
                        txnFd = TableUtils.openRO(ff, rootPath, TXNLOG_FILE_NAME, LOG);
                        suspendedFlag = ff.readNonNegativeByte(metaFd, SEQ_META_SUSPENDED) > 0;
                        sequencerTxn = ff.readNonNegativeLong(txnFd, MAX_TXN_OFFSET);
                    } finally {
                        rootPath.trimTo(rootLen);
                        ff.close(metaFd);
                        ff.close(txnFd);
                    }

                    rootPath.concat(tableToken).concat(TableUtils.TXN_FILE_NAME).$();
                    txReader.ofRO(rootPath, PartitionBy.NONE);
                    rootPath.trimTo(rootLen);

                    final CairoEngine engine = sqlExecutionContext.getCairoEngine();
                    MillisecondClock millisecondClock = engine.getConfiguration().getMillisecondClock();
                    long spinLockTimeout = engine.getConfiguration().getSpinLockTimeout();
                    TableUtils.safeReadTxn(txReader, millisecondClock, spinLockTimeout);
                    writerTxn = txReader.getSeqTxn();
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
