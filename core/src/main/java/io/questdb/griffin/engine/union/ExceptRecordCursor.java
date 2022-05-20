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

class ExceptRecordCursor implements RecordCursor {
    private final Map map;
    private final RecordSink recordSink;
    private final boolean convertSymbolsToStrings;
    private RecordCursor masterCursor;
    private RecordMetadata masterMetadata;
    private RecordCursor slaveCursor;
    private Record record;
    private RecordMetadata slaveMetadata;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private UnionDelegatingRecordImpl unionDelegatingRecord;

    public ExceptRecordCursor(Map map, RecordSink recordSink, boolean convertSymbolsToStrings) {
        this.map = map;
        this.recordSink = recordSink;
        this.convertSymbolsToStrings = convertSymbolsToStrings;
        if (convertSymbolsToStrings) {
            unionDelegatingRecord = new UnionDelegatingRecordImpl();
            record = unionDelegatingRecord;
        }
    }

    @Override
    public void close() {
        Misc.free(this.masterCursor);
        Misc.free(this.slaveCursor);
        circuitBreaker = null;
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
    public SymbolTable newSymbolTable(int columnIndex) {
        return masterCursor.newSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        while (masterCursor.hasNext()) {
            MapKey key = map.withKey();
            key.put(record, recordSink);
            if (key.notFound()) {
                return true;
            }
            circuitBreaker.statefulThrowExceptionIfTripped();
        }
        return false;
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
    public void toTop() {
        masterCursor.toTop();

        if (!convertSymbolsToStrings) {
            this.record = masterCursor.getRecord();
        } else {
            this.unionDelegatingRecord.of(masterCursor.getRecord(), masterMetadata);
        }
    }

    @Override
    public long size() {
        return -1;
    }

    void of(RecordCursor masterCursor, RecordMetadata masterMetadata, RecordCursor slaveCursor, RecordMetadata slaveMetadata, SqlExecutionContext executionContext) {
        this.masterCursor = masterCursor;
        this.masterMetadata = masterMetadata;
        this.slaveCursor = slaveCursor;
        this.slaveMetadata = slaveMetadata;
        circuitBreaker = executionContext.getCircuitBreaker();
        map.clear();
        populateSlaveMap();
        toTop();
    }

    private void populateSlaveMap() {
        if (!convertSymbolsToStrings) {
            this.record = slaveCursor.getRecord();
        } else {
            this.unionDelegatingRecord.of(slaveCursor.getRecord(), slaveMetadata);
        }

        while (slaveCursor.hasNext()) {
            MapKey key = map.withKey();
            key.put(record, recordSink);
            key.createValue();
            circuitBreaker.statefulThrowExceptionIfTripped();
        }
    }
}
