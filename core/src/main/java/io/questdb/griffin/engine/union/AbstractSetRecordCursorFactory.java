/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

abstract class AbstractSetRecordCursorFactory extends AbstractRecordCursorFactory {
    protected final RecordCursorFactory factoryA;
    protected final RecordCursorFactory factoryB;
    private final ObjList<Function> castFunctionsA;
    private final ObjList<Function> castFunctionsB;
    protected AbstractSetRecordCursor cursor;

    public AbstractSetRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory factoryA,
            RecordCursorFactory factoryB,
            ObjList<Function> castFunctionsA,
            ObjList<Function> castFunctionsB
    ) {
        super(metadata);
        this.factoryA = factoryA;
        this.factoryB = factoryB;
        this.castFunctionsB = castFunctionsB;
        this.castFunctionsA = castFunctionsA;
    }

    @Override
    public String getBaseColumnName(int idx) {
        if (idx < factoryA.getMetadata().getColumnCount()) {
            return factoryA.getMetadata().getColumnName(idx);
        } else {
            return factoryB.getMetadata().getColumnName(idx);
        }
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor cursorA = null;
        RecordCursor cursorB = null;
        try {
            cursorA = factoryA.getCursor(executionContext);
            cursorB = factoryB.getCursor(executionContext);
            Function.initNc(castFunctionsA, cursorA, executionContext, null);
            Function.initNc(castFunctionsB, cursorB, executionContext, null);
            cursor.of(cursorA, cursorB, executionContext.getCircuitBreaker());
            return cursor;
        } catch (Throwable ex) {
            Misc.free(cursorA);
            Misc.free(cursorB);
            throw ex;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return factoryA.recordCursorSupportsRandomAccess();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type(getOperation());
        sink.child(factoryA);
        if (isSecondFactoryHashed()) {
            sink.child("Hash", factoryB);
        } else {
            sink.child(factoryB);
        }
    }

    @Override
    protected void _close() {
        Misc.free(factoryA);
        Misc.free(factoryB);
        Misc.freeObjListAndClear(castFunctionsA);
        Misc.freeObjListAndClear(castFunctionsB);
    }

    protected abstract CharSequence getOperation();

    protected boolean isSecondFactoryHashed() {
        return false;
    }
}
