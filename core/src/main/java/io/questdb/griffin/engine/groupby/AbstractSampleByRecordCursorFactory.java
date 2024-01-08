/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public abstract class AbstractSampleByRecordCursorFactory extends AbstractRecordCursorFactory {

    protected final RecordCursorFactory base;
    protected final ObjList<Function> recordFunctions;

    public AbstractSampleByRecordCursorFactory(
            RecordCursorFactory base,
            RecordMetadata metadata,
            ObjList<Function> recordFunctions
    ) {
        super(metadata);
        this.base = base;
        this.recordFunctions = recordFunctions;
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    @Override
    protected void _close() {
        Misc.freeObjList(recordFunctions);
        Misc.free(base);
    }

    protected abstract AbstractNoRecordSampleByCursor getRawCursor();

    protected RecordCursor initFunctionsAndCursor(
            SqlExecutionContext executionContext,
            RecordCursor baseCursor
    ) throws SqlException {
        try {
            // init all record functions for this cursor, in case functions require metadata and/or symbol tables
            Function.init(recordFunctions, baseCursor, executionContext);
            AbstractNoRecordSampleByCursor cursor = getRawCursor();
            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (Throwable ex) {
            Misc.free(baseCursor);
            throw ex;
        }
    }
}
