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

import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.Record;
import io.questdb.std.Misc;

class UnionAllRecordCursor implements NoRandomAccessRecordCursor {
    private final DelegatingRecordImpl record = new DelegatingRecordImpl();
    private RecordCursor masterCursor;
    private RecordCursor slaveCursor;
    private final NextMethod nextSlave = this::nextSlave;
    private Record masterRecord;
    private Record slaveRecord;
    private NextMethod nextMethod;
    private RecordCursor symbolCursor;
    private final NextMethod nextMaster = this::nextMaster;

    void of(RecordCursor masterCursor, RecordCursor slaveCursor) {
        this.masterCursor = masterCursor;
        this.slaveCursor = slaveCursor;
        this.masterRecord = masterCursor.getRecord();
        this.slaveRecord = slaveCursor.getRecord();
        toTop();
    }

    @Override
    public void close() {
        Misc.free(this.masterCursor);
        Misc.free(this.slaveCursor);
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public boolean hasNext() {
        return nextMethod.next();
    }

    private boolean nextSlave() {
        return slaveCursor.hasNext();
    }

    @Override
    public long size() {
        final long masterSize = masterCursor.size();
        final long slaveSize = slaveCursor.size();
        if (masterSize == -1 || slaveSize == -1) {
            return -1;
        }
        return masterSize + slaveSize;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return symbolCursor.getSymbolTable(columnIndex);
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return symbolCursor.newSymbolTable(columnIndex);
    }

    private boolean nextMaster() {
        return masterCursor.hasNext() || switchToSlaveCursor();
    }

    private boolean switchToSlaveCursor() {
        record.of(slaveRecord);
        nextMethod = nextSlave;
        symbolCursor = slaveCursor;
        return nextMethod.next();
    }

    @Override
    public void toTop() {
        record.of(masterRecord);
        nextMethod = nextMaster;
        symbolCursor = masterCursor;
        masterCursor.toTop();
        slaveCursor.toTop();
    }

    interface NextMethod {
        boolean next();
    }
}
