/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.join;

import com.questdb.cairo.AbstractRecordCursorFactory;
import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.ColumnTypes;
import com.questdb.cairo.RecordSink;
import com.questdb.cairo.map.Map;
import com.questdb.cairo.map.MapFactory;
import com.questdb.cairo.map.MapKey;
import com.questdb.cairo.map.MapValue;
import com.questdb.cairo.sql.*;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.std.BinarySequence;
import com.questdb.std.Misc;
import com.questdb.std.Transient;
import com.questdb.std.str.CharSink;

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
    private final FullAslOfJoinCursor cursor;

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
        this.cursor = new FullAslOfJoinCursor(
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
    public RecordCursor getCursor(BindVariableService bindVariableService) {
        cursor.of(
                masterFactory.getCursor(bindVariableService),
                slaveFactory.getCursor(bindVariableService));
        return cursor;
    }

    @Override
    public boolean isRandomAccessCursor() {
        return false;
    }

    private class FullAslOfJoinCursor implements NoRandomAccessRecordCursor {
        private final FullJoinRecord record;
        private final Map joinKeyMap;
        private final int columnSplit;
        private final int masterTimestampIndex;
        private final int slaveTimestampIndex;
        private final Record nullMasterRecord;
        private final Record nullSlaveRecord;
        private RecordCursor masterCursor;
        private RecordCursor slaveCursor;
        private Record masterRecord;
        private Record slaveRecord;
        private Record masterRecord2;
        private Record slaveRecord2;
        private long masterKeyAddress = -1L;
        private long slaveKeyAddress = -1L;
        private long masterTimestamp = -1L;
        private long slaveTimestamp = -1L;
        private boolean fetchMaster = true;
        private boolean fetchSlave = true;
        private boolean hasMaster = true;
        private boolean hasSlave = true;
        private boolean dualRecord = false;

        public FullAslOfJoinCursor(
                Map joinKeyMap,
                int columnSplit,
                int masterTimestampIndex,
                int slaveTimestampIndex,
                Record nullMasterRecord,
                Record nullSlaveRecord
        ) {
            this.record = new FullJoinRecord(columnSplit);
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
                this.masterRecord2 = masterCursor.newRecord();
                this.slaveRecord2 = slaveCursor.newRecord();
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

    public class FullJoinRecord implements Record {
        private final int split;
        private Record master;
        private Record slave;

        public FullJoinRecord(int split) {
            this.split = split;
        }

        @Override
        public BinarySequence getBin(int col) {
            if (col < split) {
                return master.getBin(col);
            }
            return slave.getBin(col - split);
        }

        @Override
        public long getBinLen(int col) {
            if (col < split) {
                return master.getBinLen(col);
            }
            return slave.getBinLen(col - split);
        }

        @Override
        public boolean getBool(int col) {
            if (col < split) {
                return master.getBool(col);
            }
            return slave.getBool(col - split);
        }

        @Override
        public byte getByte(int col) {
            if (col < split) {
                return master.getByte(col);
            }
            return slave.getByte(col - split);
        }

        @Override
        public long getDate(int col) {
            if (col < split) {
                return master.getDate(col - split);
            }
            return slave.getDate(col - split);
        }

        @Override
        public double getDouble(int col) {
            if (col < split) {
                return master.getDouble(col);
            }
            return slave.getDouble(col - split);
        }

        @Override
        public float getFloat(int col) {
            if (col < split) {
                return master.getFloat(col);
            }
            return slave.getFloat(col - split);
        }

        @Override
        public int getInt(int col) {
            if (col < split) {
                return master.getInt(col);
            }
            return slave.getInt(col - split);
        }

        @Override
        public long getLong(int col) {
            if (col < split) {
                return master.getLong(col);
            }
            return slave.getLong(col - split);
        }


        @Override
        public long getRowId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public short getShort(int col) {
            if (col < split) {
                return master.getShort(col);
            }
            return slave.getShort(col - split);
        }

        @Override
        public CharSequence getStr(int col) {
            if (col < split) {
                return master.getStr(col);
            }
            return slave.getStr(col - split);
        }

        @Override
        public void getStr(int col, CharSink sink) {
            if (col < split) {
                master.getStr(col, sink);
            } else {
                slave.getStr(col - split, sink);
            }
        }

        @Override
        public CharSequence getStrB(int col) {
            if (col < split) {
                return master.getStrB(col);
            }
            return slave.getStrB(col - split);
        }

        @Override
        public int getStrLen(int col) {
            if (col < split) {
                return master.getStrLen(col);
            }
            return slave.getStrLen(col - split);
        }

        @Override
        public CharSequence getSym(int col) {
            if (col < split) {
                return master.getSym(col);
            }
            return slave.getSym(col - split);
        }

        @Override
        public long getTimestamp(int col) {
            if (col < split) {
                return master.getTimestamp(col);
            }
            return slave.getTimestamp(col - split);
        }

        void of(Record master, Record slave) {
            this.master = master;
            this.slave = slave;
        }
    }
}
