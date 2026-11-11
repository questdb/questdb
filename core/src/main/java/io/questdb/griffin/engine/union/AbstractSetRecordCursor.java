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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractSetRecordCursor implements RecordCursor {
    protected SqlExecutionCircuitBreaker circuitBreaker;
    protected RecordCursor cursorA;
    protected RecordCursor cursorB;
    private ObjList<Function> castFunctionsA;
    private ObjList<Function> castFunctionsB;

    @Override
    public void close() {
        this.cursorA = Misc.free(this.cursorA);
        this.cursorB = Misc.free(this.cursorB);
        this.circuitBreaker = null;
        // the factory frees the cast functions only after it closed this cursor
        castFunctionsA = cursorClosed(castFunctionsA);
        castFunctionsB = cursorClosed(castFunctionsB);
    }

    @Override
    public void setParquetDecodeHint(ParquetDecodeHint hint) {
        if (cursorA != null) {
            cursorA.setParquetDecodeHint(hint);
        }
        if (cursorB != null) {
            cursorB.setParquetDecodeHint(hint);
        }
    }

    void of(RecordCursor cursorA, RecordCursor cursorB, SqlExecutionContext executionContext) throws SqlException {
        this.cursorA = cursorA;
        this.cursorB = cursorB;
        this.circuitBreaker = executionContext.getCircuitBreaker();
    }

    /**
     * Hands over the cast functions the factory is about to initialize for this execution, so
     * that {@link #close()} frees what they build for it.
     */
    void ofCastFunctions(@Nullable ObjList<Function> castFunctionsA, @Nullable ObjList<Function> castFunctionsB) {
        this.castFunctionsA = castFunctionsA;
        this.castFunctionsB = castFunctionsB;
    }

    private static ObjList<Function> cursorClosed(@Nullable ObjList<Function> functions) {
        if (functions != null) {
            Function.cursorClosed(functions);
        }
        return null;
    }

    protected long sumBranchSizes() {
        final long sizeA = cursorA.size();
        final long sizeB = cursorB.size();
        if (sizeA == -1 || sizeB == -1) {
            return -1;
        }
        return sizeA + sizeB;
    }
}
