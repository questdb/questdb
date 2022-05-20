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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

class IntersectRecordCursor implements RecordCursor {
    private final Map map;
    private final RecordSink recordSink;
    private final boolean convertSymbolsAsStrings;
    private RecordCursor masterCursor;
    private RecordCursor slaveCursor;
    private Record record;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private UnionDelegatingRecordImpl delegatingRecord;

    public IntersectRecordCursor(Map map, RecordSink recordSink, boolean convertSymbolsAsStrings) {
        this.map = map;
        this.recordSink = recordSink;
        this.convertSymbolsAsStrings = convertSymbolsAsStrings;
        if (convertSymbolsAsStrings) {
            this.delegatingRecord = new UnionDelegatingRecordImpl();
            record = delegatingRecord;
        }
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return masterCursor.getSymbolTable(columnIndex);
    }

    @Override
    public Record getRecordB() {
        return masterCursor.getRecordB();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        masterCursor.recordAt(record, atRowId);
    }

    @Override
    public void close() {
        Misc.free(this.masterCursor);
        Misc.free(this.slaveCursor);
        circuitBreaker = null;
    }

    @Override
    public boolean hasNext() {
        while (masterCursor.hasNext()) {
            MapKey key = map.withKey();
            key.put(record, recordSink);
            if (key.findValue() != null) {
                return true;
            }
            circuitBreaker.statefulThrowExceptionIfTripped();
        }
        return false;
    }

    void of(RecordCursor masterCursor, RecordMetadata masterMetadata, RecordCursor slaveCursor, RecordMetadata slaveMetadata, SqlExecutionContext executionContext) {
        this.masterCursor = masterCursor;
        this.slaveCursor = slaveCursor;
        circuitBreaker = executionContext.getCircuitBreaker();

        map.clear();
        populateSlaveMap(slaveCursor, slaveMetadata);


        if (convertSymbolsAsStrings) {
            delegatingRecord.of(masterCursor.getRecord(), masterMetadata);
        } else {
            record = masterCursor.getRecord();
        }

        toTop();
    }

    private void populateSlaveMap(RecordCursor cursor, RecordMetadata slaveMetadata) {
        Record record = cursor.getRecord();
        if (convertSymbolsAsStrings) {
            delegatingRecord.of(record, slaveMetadata);
            record = delegatingRecord;
        }

        while (cursor.hasNext()) {
            MapKey key = map.withKey();
            key.put(record, recordSink);
            key.createValue();
            circuitBreaker.statefulThrowExceptionIfTripped();
        }
    }

    @Override
    public void toTop() {
        masterCursor.toTop();
    }

    @Override
    public long size() {
        return -1;
    }
}
