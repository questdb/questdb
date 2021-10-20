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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class VirtualRecordCursorFactory extends AbstractRecordCursorFactory {
    private final VirtualFunctionDirectSymbolRecordCursor cursor;
    private final ObjList<Function> functions;
    private final RecordCursorFactory baseFactory;
    private final boolean supportsRandomAccess;

    public VirtualRecordCursorFactory(
            RecordMetadata metadata,
            ObjList<Function> functions,
            RecordCursorFactory baseFactory) {
        super(metadata);
        this.functions = functions;
        boolean supportsRandomAccess = baseFactory.recordCursorSupportsRandomAccess();
        for (int i = 0, n = functions.size(); i < n; i++) {
            if (!functions.getQuick(i).supportsRandomAccess()) {
                supportsRandomAccess = false;
                break;
            }
        }
        this.supportsRandomAccess = supportsRandomAccess;
        this.cursor = new VirtualFunctionDirectSymbolRecordCursor(functions, supportsRandomAccess);
        this.baseFactory = baseFactory;
    }

    @Override
    public void close() {
        Misc.freeObjList(functions);
        Misc.free(baseFactory);
    }

    public RecordCursorFactory getBaseFactory() {
        return baseFactory;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor cursor = baseFactory.getCursor(executionContext);
        Function.init(functions, cursor, executionContext);
        this.cursor.of(cursor);
        return this.cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return supportsRandomAccess;
    }
}
