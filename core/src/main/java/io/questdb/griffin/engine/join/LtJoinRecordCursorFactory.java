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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.*;
import io.questdb.cairo.map.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Transient;

public class LtJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private final LtJoinRecordCursor cursor;
    private final JoinContext joinContext;
    private final RecordCursorFactory masterFactory;
    private final RecordSink masterKeySink;
    private final IntList slaveColumnIndex; // maps columns after the split to columns in the slave cursor
    private final RecordCursorFactory slaveFactory;
    private final RecordSink slaveKeySink;

    public LtJoinRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes mapKeyTypes,
            @Transient ColumnTypes mapValueTypes,
            @Transient ColumnTypes slaveColumnTypes,
            RecordSink masterKeySink,
            RecordSink slaveKeySink,
            int columnSplit,
            RecordValueSink slaveValueSink,
            IntList columnIndex, // this column index will be used to retrieve symbol tables from underlying slave
            JoinContext joinContext,
            ColumnFilter masterTableKeyColumns) {
        super(metadata);
        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;
        Map joinKeyMap = MapFactory.createMap(configuration, mapKeyTypes, mapValueTypes);
        this.masterKeySink = masterKeySink;
        this.slaveKeySink = slaveKeySink;
        int slaveWrappedOverMaster = slaveColumnTypes.getColumnCount() - masterTableKeyColumns.getColumnCount();
        this.cursor = new LtJoinRecordCursor(
                columnSplit,
                joinKeyMap,
                NullRecordFactory.getInstance(slaveColumnTypes),
                masterFactory.getMetadata().getTimestampIndex(),
                slaveFactory.getMetadata().getTimestampIndex(),
                slaveValueSink,
                masterTableKeyColumns,
                slaveWrappedOverMaster
        );
        this.slaveColumnIndex = columnIndex;
        this.joinContext = joinContext;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        RecordCursor slaveCursor = null;
        try {
            slaveCursor = slaveFactory.getCursor(executionContext);
            cursor.of(masterCursor, slaveCursor);
            return cursor;
        } catch (Throwable e) {
            Misc.free(slaveCursor);
            Misc.free(masterCursor);
            throw e;
        }
    }

    @Override
    public boolean hasDescendingOrder() {
        return masterFactory.hasDescendingOrder();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Lt Join");
        sink.attr("condition").val(joinContext);
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        ((JoinRecordMetadata) getMetadata()).close();
        masterFactory.close();
        slaveFactory.close();
        cursor.close();
    }

    private class LtJoinRecordCursor implements NoRandomAccessRecordCursor {
        protected final int columnSplit;
        private final Map joinKeyMap;
        private final ColumnFilter masterTableKeyColumns;
        private final int masterTimestampIndex;
        private final SymbolWrapOverJoinRecord record;
        private final int slaveTimestampIndex;
        private final int slaveWrappedOverMaster;
        private final RecordValueSink valueSink;
        protected RecordCursor masterCursor;
        protected RecordCursor slaveCursor;
        private boolean danglingSlaveRecord = false;
        private boolean isMasterHasNextPending;
        private boolean isOpen;
        private boolean masterHasNext;
        private Record masterRecord;
        private Record slaveRecord;
        private long slaveTimestamp = Long.MIN_VALUE;

        public LtJoinRecordCursor(
                int columnSplit,
                Map joinKeyMap,
                Record nullRecord,
                int masterTimestampIndex,
                int slaveTimestampIndex,
                RecordValueSink valueSink,
                ColumnFilter masterTableKeyColumns, int slaveWrappedOverMaster) {
            this.record = new SymbolWrapOverJoinRecord(columnSplit, nullRecord, masterTableKeyColumns, slaveWrappedOverMaster);
            this.columnSplit = columnSplit;
            this.joinKeyMap = joinKeyMap;
            this.masterTimestampIndex = masterTimestampIndex;
            this.slaveTimestampIndex = slaveTimestampIndex;
            this.valueSink = valueSink;
            this.isOpen = true;
            this.slaveWrappedOverMaster = slaveWrappedOverMaster;
            this.masterTableKeyColumns = masterTableKeyColumns;
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                joinKeyMap.close();
                masterCursor = Misc.free(masterCursor);
                slaveCursor = Misc.free(slaveCursor);
            }
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (columnIndex < columnSplit) {
                return masterCursor.getSymbolTable(columnIndex);
            }
            int slaveCol = columnIndex - columnSplit;
            if (slaveCol >= slaveWrappedOverMaster) {
                slaveCol -= slaveWrappedOverMaster;
                int masterCol = masterTableKeyColumns.getColumnIndexFactored(slaveCol);
                return masterCursor.getSymbolTable(masterCol);
            }
            slaveCol = slaveColumnIndex.getQuick(slaveCol);
            return slaveCursor.getSymbolTable(slaveCol);
        }

        @Override
        public boolean hasNext() {
            if (isMasterHasNextPending) {
                masterHasNext = masterCursor.hasNext();
                isMasterHasNextPending = false;
            }
            if (masterHasNext) {
                final long masterTimestamp = masterRecord.getTimestamp(masterTimestampIndex);
                MapKey key;
                MapValue value;
                long slaveTimestamp = this.slaveTimestamp;
                if (slaveTimestamp < masterTimestamp) {
                    if (danglingSlaveRecord) {
                        key = joinKeyMap.withKey();
                        key.put(slaveRecord, slaveKeySink);
                        value = key.createValue();
                        valueSink.copy(slaveRecord, value);
                        danglingSlaveRecord = false;
                    }

                    while (slaveCursor.hasNext()) {
                        slaveTimestamp = slaveRecord.getTimestamp(slaveTimestampIndex);
                        if (slaveTimestamp < masterTimestamp) {
                            key = joinKeyMap.withKey();
                            key.put(slaveRecord, slaveKeySink);
                            value = key.createValue();
                            valueSink.copy(slaveRecord, value);
                        } else {
                            danglingSlaveRecord = true;
                            break;
                        }
                    }

                    this.slaveTimestamp = slaveTimestamp;
                }
                key = joinKeyMap.withKey();
                key.put(masterRecord, masterKeySink);
                value = key.findValue();
                if (value != null) {
                    value.setMapRecordHere();
                    record.hasSlave(true);
                } else {
                    record.hasSlave(false);
                }

                isMasterHasNextPending = true;
                return true;
            }
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            // everything before columnSplit is from master. all columns are mapped 1:1.
            if (columnIndex < columnSplit) {
                return masterCursor.newSymbolTable(columnIndex);
            }

            // everything after columnSplit is from slave
            int slaveCol = columnIndex - columnSplit;
            if (slaveCol >= slaveWrappedOverMaster) {
                // ok, this is technically a slave column, but we get the symbol table from master cursor.
                // why? key symbols from slave cursors are converted to strings. that's undisiarable.
                // so let's use the fact it's a join key column. keys are present in both master and slave cursors
                // this is a bit of a hack, but it works
                // LtJoinRecord getInt() and getSym() and getSym() methods are aware of this
                slaveCol -= slaveWrappedOverMaster;
                int masterCol = masterTableKeyColumns.getColumnIndexFactored(slaveCol);
                return masterCursor.newSymbolTable(masterCol);
            }
            slaveCol = slaveColumnIndex.getQuick(slaveCol);
            return slaveCursor.newSymbolTable(slaveCol);
        }

        @Override
        public long size() {
            return masterCursor.size();
        }

        @Override
        public void toTop() {
            joinKeyMap.clear();
            slaveTimestamp = Long.MIN_VALUE;
            danglingSlaveRecord = false;
            masterCursor.toTop();
            slaveCursor.toTop();
            isMasterHasNextPending = true;
        }

        private void of(RecordCursor masterCursor, RecordCursor slaveCursor) {
            if (!isOpen) {
                isOpen = true;
                joinKeyMap.reopen();
            }
            slaveTimestamp = Long.MIN_VALUE;
            danglingSlaveRecord = false;
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            masterRecord = masterCursor.getRecord();
            slaveRecord = slaveCursor.getRecord();
            MapRecord mapRecord = joinKeyMap.getRecord();
            mapRecord.setSymbolTableResolver(slaveCursor, slaveColumnIndex);
            record.of(masterRecord, mapRecord);
            isMasterHasNextPending = true;
        }
    }
}
