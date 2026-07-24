/*+*****************************************************************************
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
import io.questdb.cairo.CairoException;
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
    protected AbstractSetRecordCursor cursor;
    protected RecordCursorFactory factoryA;
    protected RecordCursorFactory factoryB;
    private ObjList<Function> castFunctionsA;
    private ObjList<Function> castFunctionsB;

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
            cursor.of(cursorA, cursorB, executionContext);
            return cursor;
        } catch (Throwable ex) {
            Misc.free(cursorA);
            Misc.free(cursorB);
            // of() may breach after reopening tracker-charged maps; close() frees them and resets isOpen for reuse
            Misc.free(cursor);
            throw ex;
        }
    }

    @Override
    public boolean isColumnIntWidthStable(int columnIndex) {
        // EXCEPT / INTERSECT (and their ALL variants) emit only leg A's live record - getRecord()
        // returns cursorA.getRecord(), and the maps hold nothing but dedup/membership keys - so the
        // width answer depends solely on leg A, exactly like the master columns of a join delegate to
        // the master factory. An overflowing INT projection on leg A widens through getLong() and must
        // stay width-unstable on store; a real INT column on leg A keeps INT width so getLong() does not
        // over-read its 4-byte slot. UNION and UNION ALL emit either leg and override this with the
        // both-legs rule.
        //
        // CAVEAT - the cast path, the same one UNION ALL documents: when a sibling column forces
        // castIsRequired the cursor becomes ExceptCastRecordCursor / IntersectCastRecordCursor, whose
        // record still tracks leg A but routes an INT->INT column through IntColumn.getLong() = a
        // re-wrap. A column this reports unstable then STORES the wrapped value on that path. Safe (no
        // over-read either way), so it is left documented rather than risked.
        return factoryA.isColumnIntWidthStable(columnIndex);
    }

    @Override
    public boolean isColumnRowStable(int columnIndex) {
        // Paired with isColumnIntWidthStable above: only leg A's record is emitted, so the answer is
        // leg A's for this question too.
        return factoryA.isColumnRowStable(columnIndex);
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
        final AbstractSetRecordCursor cursor = this.cursor;
        this.cursor = null;
        closeSetOwnersBestEffort(cursor);
    }

    protected final void closeSetOwnersBestEffort(AbstractSetRecordCursor cursor) {
        final RecordCursorFactory factoryA = this.factoryA;
        this.factoryA = null;
        final RecordCursorFactory factoryB = this.factoryB;
        this.factoryB = null;
        final ObjList<Function> castFunctionsA = this.castFunctionsA;
        this.castFunctionsA = null;
        final ObjList<Function> castFunctionsB = this.castFunctionsB;
        this.castFunctionsB = null;

        Throwable failure = Misc.freeBestEffort(null, cursor);
        failure = Misc.freeBestEffort(failure, factoryA);
        if (factoryB != factoryA) {
            failure = Misc.freeBestEffort(failure, factoryB);
        }
        failure = Misc.freeObjListBestEffort(failure, castFunctionsA);
        if (castFunctionsB != castFunctionsA) {
            failure = Misc.freeObjListBestEffort(failure, castFunctionsB);
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    protected abstract CharSequence getOperation();

    protected boolean isSecondFactoryHashed() {
        return false;
    }
}
