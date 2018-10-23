/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.cairo.*;
import com.questdb.cairo.map.Map;
import com.questdb.cairo.map.MapFactory;
import com.questdb.cairo.map.MapKey;
import com.questdb.cairo.map.MapValue;
import com.questdb.cairo.sql.*;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.std.Misc;
import com.questdb.std.Transient;

public class HashJoinLightRecordCursorFactory extends AbstractRecordCursorFactory {
    private final Map joinKeyMap;
    private final LongChain slaveChain;
    private final RecordCursorFactory masterFactory;
    private final RecordCursorFactory slaveFactory;
    private final RecordSink masterSink;
    private final RecordSink slaveSink;
    private final HashJoinRecordCursor cursor;

    public HashJoinLightRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes joinColumnTypes,
            @Transient ColumnTypes valueTypes, // this expected to be just LONG, we store chain references in map
            RecordSink masterSink,
            RecordSink slaveSink,
            int columnSplit

    ) {
        super(metadata);
        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;
        joinKeyMap = MapFactory.createMap(configuration, joinColumnTypes, valueTypes);
        slaveChain = new LongChain(configuration.getSqlHashJoinLightValuePageSize());
        this.masterSink = masterSink;
        this.slaveSink = slaveSink;
        this.cursor = new HashJoinRecordCursor(columnSplit, joinKeyMap, slaveChain);
    }

    @Override
    public void close() {
        joinKeyMap.close();
        slaveChain.close();
        ((JoinRecordMetadata) getMetadata()).close();
        masterFactory.close();
        slaveFactory.close();
    }

    @Override
    public RecordCursor getCursor(BindVariableService bindVariableService) {
        RecordCursor slaveCursor = slaveFactory.getCursor(bindVariableService);
        try {
            buildMapOfSlaveRecords(slaveCursor);
        } catch (CairoException e) {
            slaveCursor.close();
            throw e;
        }
        cursor.of(masterFactory.getCursor(bindVariableService), slaveCursor);
        return cursor;
    }

    @Override
    public boolean isRandomAccessCursor() {
        return false;
    }

    private void buildMapOfSlaveRecords(RecordCursor slaveCursor) {
        slaveChain.clear();
        joinKeyMap.clear();
        final Record record = slaveCursor.getRecord();
        while (slaveCursor.hasNext()) {
            MapKey key = joinKeyMap.withKey();
            key.put(record, slaveSink);
            MapValue value = key.createValue();
            if (value.isNew()) {
                final long offset = slaveChain.put(-1, record.getRowId());
                value.putLong(0, offset);
                value.putLong(1, offset);
            } else {
                value.putLong(1, slaveChain.put(value.getLong(1), record.getRowId()));
            }
        }
    }

    private class HashJoinRecordCursor implements RecordCursor {
        private final JoinRecord record;
        private final LongChain slaveChain;
        private final Map joinKeyMap;
        private final int columnSplit;
        private RecordCursor masterCursor;
        private RecordCursor slaveCursor;
        private Record masterRecord;
        private Record slaveRecord;
        private LongChain.TreeCursor slaveChainCursor;

        public HashJoinRecordCursor(int columnSplit, Map joinKeyMap, LongChain slaveChain) {
            this.record = new JoinRecord(columnSplit);
            this.joinKeyMap = joinKeyMap;
            this.slaveChain = slaveChain;
            this.columnSplit = columnSplit;
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
            if (slaveChainCursor != null && slaveChainCursor.hasNext()) {
                slaveCursor.recordAt(slaveChainCursor.next());
                return true;
            }

            while (masterCursor.hasNext()) {
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterSink);
                MapValue value = key.findValue();
                if (value != null) {
                    slaveChainCursor = slaveChain.getCursor(value.getLong(0));
                    // we know cursor has values
                    // advance to get first value
                    slaveChainCursor.hasNext();
                    slaveCursor.recordAt(slaveChainCursor.next());
                    return true;
                }
            }
            return false;
        }

        @Override
        public Record newRecord() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void recordAt(long rowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            slaveChainCursor = null;
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor) {
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            this.masterRecord = masterCursor.getRecord();
            this.slaveRecord = slaveCursor.getRecord();
            record.of(masterRecord, slaveRecord);
            slaveChainCursor = null;
        }
    }
}
