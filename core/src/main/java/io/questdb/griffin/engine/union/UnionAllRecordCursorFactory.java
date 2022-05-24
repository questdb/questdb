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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class UnionAllRecordCursorFactory implements RecordCursorFactory {
    private final RecordMetadata metadata;
    private final RecordCursorFactory factoryA;
    private final RecordCursorFactory factoryB;
    private final UnionAllRecordCursor cursor;
    private final ObjList<Function> castFunctionsA;
    private final ObjList<Function> castFunctionsB;

    public UnionAllRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            ObjList<Function> castFunctionsA,
            ObjList<Function> castFunctionsB
    ) {
        this.metadata = metadata;
        this.factoryA = masterFactory;
        this.factoryB = slaveFactory;
        this.cursor = new UnionAllRecordCursor(castFunctionsA, castFunctionsB);
        this.castFunctionsA = castFunctionsA;
        this.castFunctionsB = castFunctionsB;
    }

    @Override
    public void close() {
        Misc.free(factoryA);
        Misc.free(factoryB);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor cursorA = factoryA.getCursor(executionContext);
        RecordCursor cursorB = null;
        try {
            cursorB = factoryB.getCursor(executionContext);
            Function.init(castFunctionsA, cursorA, executionContext);
            Function.init(castFunctionsB, cursorB, executionContext);
            cursor.of(cursorA, cursorB);
            return cursor;
        } catch (Throwable e) {
            Misc.free(cursorB);
            Misc.free(cursorA);
            throw e;
        }
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public boolean fragmentedSymbolTables() {
        return true;
    }
}
