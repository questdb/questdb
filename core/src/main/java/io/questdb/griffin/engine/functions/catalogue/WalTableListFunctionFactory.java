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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;

import static io.questdb.cairo.wal.WalUtils.SEQ_DIR;
import static io.questdb.cairo.wal.WalUtils.TXNLOG_FILE_NAME;

public class WalTableListFunctionFactory implements FunctionFactory {
    private static final Log LOG = LogFactory.getLog(WalTableListFunctionFactory.class);
    private static final RecordMetadata METADATA;
    private static final String SIGNATURE = "wal_tables()";
    private static final int bufferedTxnSizeColumn;
    private static final int errorMessageColumn;
    private static final int errorTagColumn;
    private static final int memoryPressureLevelColumn;
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
    ) {
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
        private CairoEngine engine;
        private Path rootPath;

        public WalTableListCursorFactory(CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
            super(METADATA);
            this.ff = configuration.getFilesFacade();
            this.rootPath = new Path();
            rootPath.of(configuration.getDbRoot());
            this.sqlExecutionContext = sqlExecutionContext;
            this.cursor = new TableListRecordCursor();
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            engine = executionContext.getCairoEngine();
            cursor.init();
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

        private class TableListRecordCursor implements NoRandomAccessRecordCursor {
            private final TableListRecord record = new TableListRecord();
            private final ObjHashSet<TableToken> tableBucket = new ObjHashSet<>();
            private final TxReader txReader = new TxReader(ff);
            private int tableIndex = -1;

            @Override
            public void close() {
                tableBucket.clear();
                txReader.close();
            }

            @Override
            public Record getRecord() {
                return record;
            }

            @Override
            public boolean hasNext() {
                tableIndex++;
                final int n = tableBucket.size();
                for (; tableIndex < n; tableIndex++) {
                    final TableToken tableToken = tableBucket.get(tableIndex);
                    if (engine.isWalTable(tableToken) && !engine.isTableDropped(tableToken) && record.switchTo(tableToken)) {
                        break;
                    }
                }
                return tableIndex < n;
            }

            @Override
            public long preComputedStateSize() {
                return tableBucket.size();
            }

            @Override
            public long size() {
                return -1;
            }

            @Override
            public void toTop() {
                tableIndex = -1;
            }

            private void init() {
                tableBucket.clear();
                engine.getTableTokens(tableBucket, false);
                toTop();
            }

            public class TableListRecord implements Record {
                private long bufferedTxnSize;
                private String errorMessage;
                private String errorTag;
                private int memoryPressureLevel;
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
                public int getInt(int col) {
                    if (col == memoryPressureLevelColumn) {
                        return memoryPressureLevel;
                    }
                    return Numbers.INT_NULL;
                }

                @Override
                public long getLong(int col) {
                    if (col == writerTxnColumn) {
                        return writerTxn;
                    }
                    if (col == bufferedTxnSizeColumn) {
                        return bufferedTxnSize;
                    }
                    if (col == sequencerTxnColumn) {
                        return sequencerTxn;
                    }
                    return Numbers.LONG_NULL;
                }

                @Override
                public CharSequence getStrA(int col) {
                    if (col == nameColumn) {
                        return tableName;
                    }
                    if (col == errorTagColumn) {
                        return errorTag;
                    }
                    if (col == errorMessageColumn) {
                        return errorMessage;
                    }
                    return null;
                }

                @Override
                public CharSequence getStrB(int col) {
                    return getStrA(col);
                }

                @Override
                public int getStrLen(int col) {
                    return TableUtils.lengthOf(getStrA(col));
                }

                private boolean switchTo(final TableToken tableToken) {
                    try {
                        long txnFd = -1;
                        int rootLen = -1;
                        SeqTxnTracker seqTxnTracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);
                        memoryPressureLevel = seqTxnTracker.getMemPressureControl().getMemoryPressureLevel();
                        tableName = tableToken.getTableName();

                        if (seqTxnTracker.isInitialised()) {
                            suspendedFlag = seqTxnTracker.isSuspended();
                            sequencerTxn = seqTxnTracker.getSeqTxn();
                            writerTxn = seqTxnTracker.getWriterTxn();
                            bufferedTxnSize = seqTxnTracker.getLagTxnCount();
                            if (suspendedFlag) {
                                // only read error details from seqTxnTracker if the table is suspended
                                // when the table is not suspended, it is not guaranteed that error details are immediately cleared
                                errorTag = seqTxnTracker.getErrorTag().text();
                                errorMessage = seqTxnTracker.getErrorMessage();
                            } else {
                                errorTag = "";
                                errorMessage = "";
                            }
                            return true;
                        }

                        try {
                            // We used to have suspended flag saved in the sequencer metadata file
                            // but we no longer need it since we ignore suspended flag on the restart
                            // and try to apply transactions once any way.

                            // Not initialized means there will be an attempt to apply
                            // meaning the table is not suspended
                            suspendedFlag = false;

                            rootLen = rootPath.size();
                            rootPath.concat(tableToken).concat(SEQ_DIR);

                            txnFd = TableUtils.openRO(ff, rootPath, TXNLOG_FILE_NAME, LOG);
                            sequencerTxn = ff.readNonNegativeLong(txnFd, TableTransactionLogFile.MAX_TXN_OFFSET_64);
                            rootPath.trimTo(rootLen).concat(tableToken).concat(TableUtils.TXN_FILE_NAME).$();
                            if (!ff.exists(rootPath.$())) {
                                return false;
                            }

                            try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
                                txReader.ofRO(rootPath.$(), metadata.getTimestampType(), metadata.getPartitionBy());
                                final CairoEngine engine = sqlExecutionContext.getCairoEngine();
                                final MillisecondClock millisecondClock = engine.getConfiguration().getMillisecondClock();
                                final long spinLockTimeout = engine.getConfiguration().getSpinLockTimeout();
                                TableUtils.safeReadTxn(txReader, millisecondClock, spinLockTimeout);
                                bufferedTxnSize = txReader.getLagTxnCount();
                                return true;
                            }
                        } finally {
                            if (txnFd > -1) {
                                ff.close(txnFd);
                                txReader.close();
                            }
                            if (rootLen > -1) {
                                rootPath.trimTo(rootLen);
                            }
                        }
                    } catch (CairoException ex) {
                        if (ex.isFileCannotRead()) {
                            return false;
                        }
                        throw ex;
                    }
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
        metadata.add(new TableColumnMetadata("bufferedTxnSize", ColumnType.LONG));
        bufferedTxnSizeColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("sequencerTxn", ColumnType.LONG));
        sequencerTxnColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("errorTag", ColumnType.STRING));
        errorTagColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("errorMessage", ColumnType.STRING));
        errorMessageColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("memoryPressure", ColumnType.INT));
        memoryPressureLevelColumn = metadata.getColumnCount() - 1;
        METADATA = metadata;
    }
}
