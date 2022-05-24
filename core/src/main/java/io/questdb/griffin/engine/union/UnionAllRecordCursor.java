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
import io.questdb.griffin.SqlException;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

class UnionAllRecordCursor implements NoRandomAccessRecordCursor {
    private final UnionRecord record;
    private RecordCursor cursorA;
    private RecordCursor cursorB;
    private final NextMethod nextSlave = this::nextSlave;
    private NextMethod nextMethod;
    private RecordCursor symbolCursor;
    private final NextMethod nextMaster = this::nextMaster;

    public UnionAllRecordCursor(ObjList<Function> castFunctionsA, ObjList<Function> castFunctionsB) {
        this.record = new UnionRecord(castFunctionsA, castFunctionsB);
    }

    @Override
    public void toTop() {
        record.setAb(true);
        nextMethod = nextMaster;
        symbolCursor = cursorA;
        cursorA.toTop();
        cursorB.toTop();
    }

    @Override
    public void close() {
        Misc.free(this.cursorA);
        Misc.free(this.cursorB);
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
        return cursorB.hasNext();
    }

    @Override
    public long size() {
        final long masterSize = cursorA.size();
        final long slaveSize = cursorB.size();
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
        return cursorA.hasNext() || switchToSlaveCursor();
    }

    void of(RecordCursor cursorA, RecordCursor cursorB) throws SqlException {
        this.cursorA = cursorA;
        this.cursorB = cursorB;
        record.of(cursorA.getRecord(), cursorB.getRecord());
        toTop();
    }

    private boolean switchToSlaveCursor() {
        record.setAb(false);
        nextMethod = nextSlave;
        symbolCursor = cursorB;
        return nextMethod.next();
    }

    interface NextMethod {
        boolean next();
    }
}
