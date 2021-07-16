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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.Transient;

/**
 * Splice join compares time series that do not always align on timestamp. Consider
 * the following: we have two engines, X and Y, that produce FX prices. We need to verify
 * that prices are the same or similar. When engine X produces price A we need to tell
 * what was the last price produced by Y at the time of A and vice versa, when engine Y
 * produced price B what was the last price produced by X at the time of B.
 * <p>
 * Splice join does exactly that. If engines produce prices for multiple currency pairs
 * splice join optionally join on "tags", which in above example is currency pair field.
 * The join result will match rows where value of currency pair is the same.
 */
public class SpliceJoinLightRecordCursorFactory extends AbstractRecordCursorFactory {

    private static final int VAL_MASTER_PREV = 0;
    private static final int VAL_MASTER_NEXT = 1;
    private static final int VAL_SLAVE_PREV = 2;
    private static final int VAL_SLAVE_NEXT = 3;
    private static final long NULL_ROWID = -1L;
    private final RecordCursorFactory slaveFactory;
    private final RecordCursorFactory masterFactory;
    private final Map joinKeyMap;
    private final RecordSink masterKeySink;
    private final RecordSink slaveKeySink;
    private final SpliceJoinLightRecordCursor cursor;

    public SpliceJoinLightRecordCursorFactory(
            CairoConfiguration cairoConfiguration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes joinColumnTypes,
            @Transient ColumnTypes valueTypes,
            RecordSink masterSink,
            RecordSink slaveSink,
            int columnSplit
    ) {
        super(metadata);
        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;
        this.joinKeyMap = MapFactory.createMap(
                cairoConfiguration,
                joinColumnTypes,
                valueTypes
        );
        this.masterKeySink = masterSink;
        this.slaveKeySink = slaveSink;
        this.cursor = new SpliceJoinLightRecordCursor(
                joinKeyMap,
                columnSplit,
                masterFactory.getMetadata().getTimestampIndex(),
                slaveFactory.getMetadata().getTimestampIndex(),
                NullRecordFactory.getInstance(masterFactory.getMetadata()),
                NullRecordFactory.getInstance(slaveFactory.getMetadata())
        );
    }

    @Override
    public void close() {
        joinKeyMap.close();
        ((JoinRecordMetadata) getMetadata()).close();
        masterFactory.close();
        slaveFactory.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.of(
                masterFactory.getCursor(executionContext),
                slaveFactory.getCursor(executionContext));
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    private class SpliceJoinLightRecordCursor implements NoRandomAccessRecordCursor {
        private final JoinRecord record;
        private final Map joinKeyMap;
        private final int columnSplit;
        private final int masterTimestampIndex;
        private final int slaveTimestampIndex;
        private final Record nullMasterRecord;
        private final Record nullSlaveRecord;
        private Record masterRecord2;
        private Record slaveRecord2;
        private RecordCursor masterCursor;
        private RecordCursor slaveCursor;
        private Record masterRecord;
        private Record slaveRecord;
        private long masterKeyAddress = -1L;
        private long slaveKeyAddress = -1L;
        private long masterTimestamp = -1L;
        private long slaveTimestamp = -1L;
        private boolean fetchMaster = true;
        private boolean fetchSlave = true;
        private boolean hasMaster = true;
        private boolean hasSlave = true;
        private boolean dualRecord = false;

        public SpliceJoinLightRecordCursor(
                Map joinKeyMap,
                int columnSplit,
                int masterTimestampIndex,
                int slaveTimestampIndex,
                Record nullMasterRecord,
                Record nullSlaveRecord
        ) {
            this.record = new JoinRecord(columnSplit);
            this.joinKeyMap = joinKeyMap;
            this.columnSplit = columnSplit;
            this.masterTimestampIndex = masterTimestampIndex;
            this.slaveTimestampIndex = slaveTimestampIndex;
            this.nullMasterRecord = nullMasterRecord;
            this.nullSlaveRecord = nullSlaveRecord;
        }

        @Override
        public void close() {
            masterCursor = Misc.free(masterCursor);
            slaveCursor = Misc.free(slaveCursor);
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
            return slaveCursor.getSymbolTable(columnIndex - columnSplit);
        }

        @Override
        public boolean hasNext() {
            if (dualRecord) {
                slaveRecordLeads();
                dualRecord = false;
            }

            if (fetchMaster && hasMaster && masterCursor.hasNext()) {
                final MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterKeySink);
                final MapValue value = key.createValue();
                masterKeyAddress = value.getAddress();
                if (value.isNew()) {
                    value.putLong(VAL_MASTER_PREV, NULL_ROWID);
                    value.putLong(VAL_SLAVE_PREV, NULL_ROWID);
                    value.putLong(VAL_SLAVE_NEXT, NULL_ROWID);
                } else {
                    // copy current to previous
                    value.putLong(VAL_MASTER_PREV, value.getLong(VAL_MASTER_NEXT));
                }
                value.putLong(VAL_MASTER_NEXT, masterRecord.getRowId());
                masterTimestamp = masterRecord.getTimestamp(masterTimestampIndex);
            } else if (fetchMaster && hasMaster) {
                hasMaster = false;
                masterTimestamp = Long.MAX_VALUE;
            }

            if (fetchSlave && hasSlave && slaveCursor.hasNext()) {
                final MapKey key = joinKeyMap.withKey();
                key.put(slaveRecord, slaveKeySink);
                final MapValue value = key.createValue();
                slaveKeyAddress = value.getAddress();
                if (value.isNew()) {
                    value.putLong(VAL_MASTER_PREV, NULL_ROWID);
                    value.putLong(VAL_MASTER_NEXT, NULL_ROWID);
                    value.putLong(VAL_SLAVE_PREV, NULL_ROWID);
                } else {
                    value.putLong(VAL_SLAVE_PREV, value.getLong(VAL_SLAVE_NEXT));
                }
                value.putLong(VAL_SLAVE_NEXT, slaveRecord.getRowId());
                slaveTimestamp = slaveRecord.getTimestamp(slaveTimestampIndex);
            } else if (fetchSlave && hasSlave) {
                hasSlave = false;
                slaveTimestamp = Long.MAX_VALUE;
            }

            if (masterTimestamp < slaveTimestamp) {
                masterRecordLeads();
                fetchMaster = true;
                fetchSlave = false;
            } else if (masterTimestamp > slaveTimestamp) {
                slaveRecordLeads();
                fetchMaster = false;
                fetchSlave = true;
            } else {
                if (slaveTimestamp == Long.MAX_VALUE) {
                    return false;
                }

                if (masterKeyAddress == slaveKeyAddress) {
                    record.of(masterRecord, slaveRecord);
                } else {
                    masterRecordLeads();
                    dualRecord = true;
                }

                fetchMaster = true;
                fetchSlave = true;
            }

            return true;
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            slaveCursor.toTop();
            resetState();
        }

        @Override
        public long size() {
            return -1L;
        }

        private void masterRecordLeads() {
            // lookup previous slave
            final long rowid = joinKeyMap.valueAt(masterKeyAddress).getLong(hasSlave ? VAL_SLAVE_PREV : VAL_SLAVE_NEXT);
            if (rowid == NULL_ROWID) {
                record.of(masterRecord, nullSlaveRecord);
            } else {
                slaveCursor.recordAt(slaveRecord2, rowid);
                record.of(masterRecord, slaveRecord2);
            }
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor) {
            // avoid resetting these
            if (this.masterCursor == null) {
                this.masterCursor = masterCursor;
                this.slaveCursor = slaveCursor;
                this.masterRecord = masterCursor.getRecord();
                this.slaveRecord = slaveCursor.getRecord();
                this.masterRecord2 = masterCursor.getRecordB();
                this.slaveRecord2 = slaveCursor.getRecordB();
            }
            resetState();
        }

        private void resetState() {
            joinKeyMap.clear();
            masterKeyAddress = -1L;
            slaveKeyAddress = -1L;
            masterTimestamp = -1L;
            slaveTimestamp = -1L;
            fetchMaster = true;
            fetchSlave = true;
            hasMaster = true;
            hasSlave = true;
            // wasn't there originally
            dualRecord = false;
        }

        private void slaveRecordLeads() {
            // lookup previous master
            final long rowid = joinKeyMap.valueAt(slaveKeyAddress).getLong(hasMaster ? VAL_MASTER_PREV : VAL_MASTER_NEXT);
            if (rowid == NULL_ROWID) {
                record.of(nullMasterRecord, slaveRecord);
            } else {
                masterCursor.recordAt(masterRecord2, rowid);
                record.of(masterRecord2, slaveRecord);
            }
        }
    }
}
