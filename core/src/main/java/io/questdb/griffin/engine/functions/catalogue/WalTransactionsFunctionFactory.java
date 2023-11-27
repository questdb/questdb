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

public class WalTransactionsFunctionFactory implements FunctionFactory {
    private static final RecordMetadata METADATA;
    private static final String SIGNATURE = "wal_transactions(s)";
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
        CharSequence tableName = args.get(0).getStr(null);
        TableToken tableToken = sqlExecutionContext.getCairoEngine().getTableTokenIfExists(tableName);
        if (tableToken == null) {
            throw SqlException.$(argPositions.get(0), "table does not exist: ").put(tableName);
        }
        if (!sqlExecutionContext.getCairoEngine().isWalTable(tableToken)) {
            throw SqlException.$(argPositions.get(0), "table is not a WAL table: ").put(tableName);
        }
        return new CursorFunction(new WalTransactionsCursorFactory(tableToken));
    }

    private static class WalTransactionsCursorFactory extends AbstractRecordCursorFactory {
        private final TableListRecordCursor cursor;
        private final TableToken tableToken;

        public WalTransactionsCursorFactory(TableToken tableToken) {
            super(METADATA);
            this.tableToken = tableToken;
            this.cursor = new TableListRecordCursor();
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            cursor.close();
            cursor.logCursor = executionContext.getCairoEngine().getTableSequencerAPI().getCursor(tableToken, 0);
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


        private static class TableListRecordCursor implements RecordCursor {
            TransactionRecord record = new TransactionRecord();
            private TransactionLogCursor logCursor;

            @Override
            public void close() {
                logCursor = Misc.free(logCursor);
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
                return logCursor.hasNext();
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
                    return Numbers.INT_NaN;
                }

                @Override
                public long getLong(int col) {
                    if (col == structureVersionColumn) {
                        return logCursor.getStructureVersion();
                    }
                    if (col == sequencerTxnColumn) {
                        return logCursor.getTxn();
                    }
                    return Numbers.LONG_NaN;
                }

                @Override
                public long getTimestamp(int col) {
                    if (col == timestampColumn) {
                        return logCursor.getCommitTimestamp();
                    }
                    return Numbers.LONG_NaN;
                }
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("sequencerTxn", ColumnType.LONG));
        sequencerTxnColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("timestamp", ColumnType.TIMESTAMP));
        timestampColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("walId", ColumnType.INT));
        walIdColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("segmentId", ColumnType.INT));
        segmentIdColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("segmentTxn", ColumnType.INT));
        segmentTxnColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("structureVersion", ColumnType.LONG));
        structureVersionColumn = metadata.getColumnCount() - 1;
        METADATA = metadata;
    }
}
