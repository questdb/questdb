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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

import static io.questdb.cairo.wal.WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2;

public class WalTransactionsFunctionFactory implements FunctionFactory {
    private static final RecordMetadata METADATA;
    private static final String SIGNATURE = "wal_transactions(s)";
    private static final int alterCommandTypeColumn;
    private static final int maxTimestampColumn;
    private static final int minTimestampColumn;
    private static final int rowCountColumn;
    private static final int segmentIdColumn;
    private static final int segmentTxnColumn;
    private static final int sequencerTxnColumn;
    private static final int structureVersionColumn;
    private static final int timestampColumn;
    private static final int walIdColumn;

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
        CharSequence tableName = args.get(0).getStrA(null);
        TableToken tableToken = sqlExecutionContext.getCairoEngine().getTableTokenIfExists(tableName);
        if (tableToken == null) {
            throw SqlException.$(argPositions.get(0), "table does not exist: ").put(tableName);
        }
        if (!sqlExecutionContext.getCairoEngine().isWalTable(tableToken)) {
            throw SqlException.$(argPositions.get(0), "table is not a WAL table: ").put(tableName);
        }
        int timestampType = 0;
        try (TableMetadata metadata = sqlExecutionContext.getCairoEngine().getTableMetadata(tableToken)) {
            if (metadata != null) {
                timestampType = metadata.getTimestampType();
            }
        }
        return new CursorFunction(new WalTransactionsCursorFactory(tableToken, timestampType));
    }

    private static class WalTransactionsCursorFactory extends AbstractRecordCursorFactory {
        private final TableListRecordCursor cursor;
        private final TableToken tableToken;

        public WalTransactionsCursorFactory(TableToken tableToken, int timestampType) {
            super(METADATA);
            this.tableToken = tableToken;
            this.cursor = new TableListRecordCursor(ColumnType.getTimestampDriver(timestampType));
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            cursor.close();
            long txnLo = 0;
            while (true) {
                TransactionLogCursor cursor = null;
                try {
                    cursor = executionContext.getCairoEngine().getTableSequencerAPI().getCursor(tableToken, txnLo);
                    cursor.toMinTxn();
                    this.cursor.logCursor = cursor;
                    break;
                } catch (CairoException e) {
                    Misc.free(cursor);
                    if (e.isFileCannotRead()) {
                        // Txn sequencer can have its parts deleted due to housekeeping
                        // Need to keep scanning until we find a valid part
                        if (txnLo == 0) {
                            long writerTxn = executionContext.getCairoEngine().getTableSequencerAPI().getTxnTracker(tableToken).getWriterTxn();
                            if (writerTxn > 0) {
                                txnLo = writerTxn;
                                continue;
                            }
                        }
                    }
                    throw e;
                }
            }
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("wal_transactions of: ").val(tableToken.getTableName());
        }


        private static class TableListRecordCursor implements NoRandomAccessRecordCursor {
            private final TimestampDriver timestampDriver;
            TransactionRecord record = new TransactionRecord();
            private TransactionLogCursor logCursor;

            private TableListRecordCursor(TimestampDriver timestampDriver) {
                this.timestampDriver = timestampDriver;
            }

            @Override
            public void close() {
                logCursor = Misc.free(logCursor);
            }

            @Override
            public Record getRecord() {
                return record;
            }

            @Override
            public boolean hasNext() {
                return logCursor.hasNext();
            }

            @Override
            public long preComputedStateSize() {
                return 0;
            }

            @Override
            public long size() {
                return -1;
            }

            @Override
            public void toTop() {
                logCursor.toTop();
            }

            public class TransactionRecord implements Record {
                @Override
                public int getInt(int col) {
                    if (col == walIdColumn) {
                        return logCursor.getWalId();
                    }
                    if (col == segmentIdColumn) {
                        return logCursor.getSegmentId();
                    }
                    if (col == segmentTxnColumn) {
                        return logCursor.getSegmentTxn();
                    }
                    return Numbers.INT_NULL;
                }

                @Override
                public long getLong(int col) {
                    if (col == structureVersionColumn) {
                        return logCursor.getStructureVersion();
                    }
                    if (col == sequencerTxnColumn) {
                        return logCursor.getTxn();
                    }
                    if (col == rowCountColumn) {
                        if (logCursor.getVersion() == WAL_SEQUENCER_FORMAT_VERSION_V2
                                && logCursor.getTxnRowCount() > 0) {
                            return logCursor.getTxnRowCount();
                        } else {
                            return Numbers.LONG_NULL;
                        }
                    }
                    return Numbers.LONG_NULL;
                }

                @Override
                public short getShort(int col) {
                    if (col == alterCommandTypeColumn && logCursor.getVersion() == WAL_SEQUENCER_FORMAT_VERSION_V2
                            && logCursor.getTxnRowCount() == 0) {
                        return (short) logCursor.getTxnMinTimestamp();
                    }

                    return 0;
                }

                @Override
                public long getTimestamp(int col) {
                    if (col == timestampColumn) {
                        return timestampDriver.toMicros(logCursor.getCommitTimestamp());
                    }
                    if (col == minTimestampColumn) {
                        if (logCursor.getVersion() == WAL_SEQUENCER_FORMAT_VERSION_V2
                                && logCursor.getTxnRowCount() > 0) {
                            return timestampDriver.toMicros(logCursor.getTxnMinTimestamp());
                        } else {
                            return Numbers.LONG_NULL;
                        }
                    }
                    if (col == maxTimestampColumn) {
                        if (logCursor.getVersion() == WAL_SEQUENCER_FORMAT_VERSION_V2
                                && logCursor.getTxnRowCount() > 0) {
                            return timestampDriver.toMicros(logCursor.getTxnMaxTimestamp());
                        } else {
                            return Numbers.LONG_NULL;
                        }
                    }
                    return Numbers.LONG_NULL;
                }
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("sequencerTxn", ColumnType.LONG));
        sequencerTxnColumn = metadata.getColumnCount() - 1;
        // todo, maybe we should use ColumnType.String here?, same as `minTimestamp` and `maxTimestamp`.
        metadata.add(new TableColumnMetadata("timestamp", ColumnType.TIMESTAMP_MICRO));
        timestampColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("walId", ColumnType.INT));
        walIdColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("segmentId", ColumnType.INT));
        segmentIdColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("segmentTxn", ColumnType.INT));
        segmentTxnColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("structureVersion", ColumnType.LONG));
        structureVersionColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("minTimestamp", ColumnType.TIMESTAMP_MICRO));
        minTimestampColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("maxTimestamp", ColumnType.TIMESTAMP_MICRO));
        maxTimestampColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("rowCount", ColumnType.LONG));
        rowCountColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("alterCommandType", ColumnType.SHORT));
        alterCommandTypeColumn = metadata.getColumnCount() - 1;
        METADATA = metadata;
    }
}
