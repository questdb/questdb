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
import io.questdb.griffin.SqlException;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

class UnionAllRecordCursor implements NoRandomAccessRecordCursor {
    private final AbstractUnionRecord record;
    private RecordCursor cursorA;
    private RecordCursor cursorB;
    private final NextMethod nextB = this::nextB;
    private NextMethod nextMethod;
    private RecordCursor symbolCursor;
    private final NextMethod nextA = this::nextA;

    public UnionAllRecordCursor(ObjList<Function> castFunctionsA, ObjList<Function> castFunctionsB) {
        if (castFunctionsA != null && castFunctionsB != null) {
            this.record = new UnionCastRecord(castFunctionsA, castFunctionsB);
        } else {
            this.record = new UnionDirectRecord();
        }
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
    public SymbolTable getSymbolTable(int columnIndex) {
        return symbolCursor.getSymbolTable(columnIndex);
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return symbolCursor.newSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        return nextMethod.next();
    }

    @Override
    public void toTop() {
        record.setAb(true);
        nextMethod = nextA;
        symbolCursor = cursorA;
        cursorA.toTop();
        cursorB.toTop();
    }

    @Override
    public long size() {
        final long sizeA = cursorA.size();
        final long sizeB = cursorB.size();
        if (sizeA == -1 || sizeB == -1) {
            return -1;
        }
        return sizeA + sizeB;
    }

    private boolean nextA() {
        return cursorA.hasNext() || switchToSlaveCursor();
    }

    private boolean nextB() {
        return cursorB.hasNext();
    }

    void of(RecordCursor cursorA, RecordCursor cursorB) throws SqlException {
        this.cursorA = cursorA;
        this.cursorB = cursorB;
        record.of(cursorA.getRecord(), cursorB.getRecord());
        toTop();
    }

    private boolean switchToSlaveCursor() {
        record.setAb(false);
        nextMethod = nextB;
        symbolCursor = cursorB;
        return nextMethod.next();
    }

    interface NextMethod {
        boolean next();
    }
}
