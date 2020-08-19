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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionInterruptor;
import io.questdb.std.Misc;

class ExceptRecordCursor implements RecordCursor {
    private final Map map;
    private final RecordSink recordSink;
    private RecordCursor masterCursor;
    private RecordCursor slaveCursor;
    private Record masterRecord;
    private RecordCursor symbolCursor;
    private SqlExecutionInterruptor interruptor;

    public ExceptRecordCursor(Map map, RecordSink recordSink) {
        this.map = map;
        this.recordSink = recordSink;
    }

    void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext executionContext) {
        this.masterCursor = masterCursor;
        this.slaveCursor = slaveCursor;
        this.masterRecord = masterCursor.getRecord();
        interruptor = executionContext.getSqlExecutionInterruptor();
        map.clear();
        populateSlaveMap(slaveCursor);
        toTop();
    }

    private void populateSlaveMap(RecordCursor cursor) {
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            MapKey key = map.withKey();
            key.put(record, recordSink);
            key.createValue();
            interruptor.checkInterrupted();
        }
    }

    @Override
    public void close() {
        Misc.free(this.masterCursor);
        Misc.free(this.slaveCursor);
        interruptor = null;
    }

    @Override
    public Record getRecord() {
        return masterRecord;
    }

    @Override
    public boolean hasNext() {
        while (masterCursor.hasNext()) {
            MapKey key = map.withKey();
            key.put(masterRecord, recordSink);
            if (key.notFound()) {
                return true;
            }
            interruptor.checkInterrupted();
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
    public SymbolTable getSymbolTable(int columnIndex) {
        return symbolCursor.getSymbolTable(columnIndex);
    }

    @Override
    public void toTop() {
        symbolCursor = masterCursor;
        masterCursor.toTop();
    }

    @Override
    public long size() {
        return -1;
    }
}
