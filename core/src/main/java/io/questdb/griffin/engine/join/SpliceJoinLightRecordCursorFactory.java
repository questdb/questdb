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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.Misc;
import io.questdb.std.Transient;

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;

/**
 * Splice join compares time series that do not always align on timestamp. Consider
 * the following: we have two engines, X and Y, that produce FX prices. We need to verify
 * that prices are the same or similar. When engine X produces price A, we need to tell
 * what was the last price produced by Y at the time of A and vice versa, when engine Y
 * produced price B what was the last price produced by X at the time of B.
 * <p>
 * Splice join does exactly that. If engines produce prices for multiple currency pairs
 * splice join optionally join on "tags", which in above example is currency pair field.
 * The join result will match rows where value of currency pair is the same.
 */
public class SpliceJoinLightRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private static final long NULL_ROWID = -1;
    private static final int VAL_MASTER_NEXT = 1;
    private static final int VAL_MASTER_PREV = 0;
    private static final int VAL_SLAVE_NEXT = 3;
    private static final int VAL_SLAVE_PREV = 2;
    private final SpliceJoinLightRecordCursor cursor;
    private final RecordSink masterKeySink;
    private final RecordSink slaveKeySink;

    public SpliceJoinLightRecordCursorFactory(
            CairoConfiguration cairoConfiguration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes joinColumnTypes,
            @Transient ColumnTypes valueTypes,
            RecordSink masterSink,
            RecordSink slaveSink,
            int columnSplit,
            JoinContext joinContext
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        try {
            this.masterKeySink = masterSink;
            this.slaveKeySink = slaveSink;
            Map joinKeyMap = MapFactory.createUnorderedMap(
                    cairoConfiguration,
                    joinColumnTypes,
                    valueTypes
            );
            cursor = new SpliceJoinLightRecordCursor(
                    joinKeyMap,
                    columnSplit,
                    masterFactory.getMetadata().getTimestampIndex(),
                    slaveFactory.getMetadata().getTimestampIndex(),
                    masterFactory.getMetadata().getTimestampType(),
                    slaveFactory.getMetadata().getTimestampType(),
                    NullRecordFactory.getInstance(masterFactory.getMetadata()),
                    NullRecordFactory.getInstance(slaveFactory.getMetadata())
            );
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public boolean followedOrderByAdvice() {
        return masterFactory.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        // Forcefully disable column pre-touch for nested filter queries.
        executionContext.setColumnPreTouchEnabled(false);
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
    public int getScanDirection() {
        return masterFactory.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Splice Join");
        if (joinContext != null && !joinContext.isEmpty()) {
            sink.optAttr("condition", joinContext);
        }
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
        Misc.free(cursor);
    }

    private class SpliceJoinLightRecordCursor extends AbstractJoinCursor {
        private final Map joinKeyMap;
        private final int masterTimestampIndex;
        private final long masterTimestampScale;
        private final Record nullMasterRecord;
        private final Record nullSlaveRecord;
        private final JoinRecord record;
        private final int slaveTimestampIndex;
        private final long slaveTimestampScale;
        private boolean dualRecord;
        private boolean fetchMaster = true;
        private boolean fetchSlave = true;
        private boolean hasMaster;
        private boolean hasMasterPending = true;
        private boolean hasSlave;
        private boolean hasSlavePending = true;
        private boolean isOpen;
        private long masterKeyValueAddress = -1;
        private Record masterRecord;
        private Record masterRecord2;
        private long masterTimestamp = -1;
        private long slaveKeyValueAddress = -1;
        private Record slaveRecord;
        private Record slaveRecord2;
        private long slaveTimestamp = -1;

        public SpliceJoinLightRecordCursor(
                Map joinKeyMap,
                int columnSplit,
                int masterTimestampIndex,
                int slaveTimestampIndex,
                int masterTimestampType,
                int slaveTimestampType,
                Record nullMasterRecord,
                Record nullSlaveRecord
        ) {
            super(columnSplit);
            record = new JoinRecord(columnSplit);
            this.joinKeyMap = joinKeyMap;
            this.masterTimestampIndex = masterTimestampIndex;
            this.slaveTimestampIndex = slaveTimestampIndex;
            this.nullMasterRecord = nullMasterRecord;
            this.nullSlaveRecord = nullSlaveRecord;
            isOpen = true;
            if (masterTimestampType == slaveTimestampType) {
                masterTimestampScale = slaveTimestampScale = 1L;
            } else {
                masterTimestampScale = ColumnType.getTimestampDriver(masterTimestampType).toNanosScale();
                slaveTimestampScale = ColumnType.getTimestampDriver(slaveTimestampType).toNanosScale();
            }
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                joinKeyMap.close();
                super.close();
            }
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (dualRecord) {
                slaveRecordLeads();
                dualRecord = false;
            }

            if (fetchMaster) {
                if (hasMasterPending) {
                    hasMaster = masterCursor.hasNext();
                    hasMasterPending = false;
                }
                if (hasMaster) {
                    final MapKey key = joinKeyMap.withKey();
                    key.put(masterRecord, masterKeySink);
                    final MapValue value = key.createValue();
                    masterKeyValueAddress = value.getStartAddress();
                    if (value.isNew()) {
                        value.putLong(VAL_MASTER_PREV, NULL_ROWID);
                        value.putLong(VAL_SLAVE_PREV, NULL_ROWID);
                        value.putLong(VAL_SLAVE_NEXT, NULL_ROWID);
                    } else {
                        // copy current to previous
                        value.putLong(VAL_MASTER_PREV, value.getLong(VAL_MASTER_NEXT));
                    }
                    value.putLong(VAL_MASTER_NEXT, masterRecord.getRowId());
                    masterTimestamp = scaleTimestamp(masterRecord.getTimestamp(masterTimestampIndex), masterTimestampScale);
                } else {
                    masterTimestamp = Long.MAX_VALUE;
                }
            }

            if (fetchSlave) {
                if (hasSlavePending) {
                    hasSlave = slaveCursor.hasNext();
                    hasSlavePending = false;
                }
                if (hasSlave) {
                    final MapKey key = joinKeyMap.withKey();
                    key.put(slaveRecord, slaveKeySink);
                    final MapValue value = key.createValue();
                    slaveKeyValueAddress = value.getStartAddress();
                    if (value.isNew()) {
                        value.putLong(VAL_MASTER_PREV, NULL_ROWID);
                        value.putLong(VAL_MASTER_NEXT, NULL_ROWID);
                        value.putLong(VAL_SLAVE_PREV, NULL_ROWID);
                    } else {
                        value.putLong(VAL_SLAVE_PREV, value.getLong(VAL_SLAVE_NEXT));
                    }
                    value.putLong(VAL_SLAVE_NEXT, slaveRecord.getRowId());
                    slaveTimestamp = scaleTimestamp(slaveRecord.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                } else {
                    slaveTimestamp = Long.MAX_VALUE;
                }
            }

            // all suspendable calls are done, so we can reset the pending flags
            hasMasterPending = true;
            hasSlavePending = true;

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
                if (masterKeyValueAddress == slaveKeyValueAddress) {
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
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            slaveCursor.toTop();
            resetState();
        }

        private void masterRecordLeads() {
            // lookup previous slave
            final long rowid = joinKeyMap.valueAt(masterKeyValueAddress).getLong(hasSlave ? VAL_SLAVE_PREV : VAL_SLAVE_NEXT);
            if (rowid == NULL_ROWID) {
                record.of(masterRecord, nullSlaveRecord);
            } else {
                slaveCursor.recordAt(slaveRecord2, rowid);
                record.of(masterRecord, slaveRecord2);
            }
        }

        private void resetState() {
            joinKeyMap.clear();
            masterKeyValueAddress = -1L;
            slaveKeyValueAddress = -1L;
            masterTimestamp = -1L;
            slaveTimestamp = -1L;
            fetchMaster = true;
            hasMasterPending = true;
            fetchSlave = true;
            hasSlavePending = true;
            // wasn't there originally
            dualRecord = false;
        }

        private void slaveRecordLeads() {
            // lookup previous master
            final long rowid = joinKeyMap.valueAt(slaveKeyValueAddress).getLong(hasMaster ? VAL_MASTER_PREV : VAL_MASTER_NEXT);
            if (rowid == NULL_ROWID) {
                record.of(nullMasterRecord, slaveRecord);
            } else {
                masterCursor.recordAt(masterRecord2, rowid);
                record.of(masterRecord2, slaveRecord);
            }
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor) {
            if (!isOpen) {
                isOpen = true;
                joinKeyMap.reopen();
            }
            // avoid resetting these
            if (this.masterCursor == null) {
                this.masterCursor = masterCursor;
                this.slaveCursor = slaveCursor;
                masterRecord = masterCursor.getRecord();
                slaveRecord = slaveCursor.getRecord();
                masterRecord2 = masterCursor.getRecordB();
                slaveRecord2 = slaveCursor.getRecordB();
            }
            resetState();
        }
    }
}
