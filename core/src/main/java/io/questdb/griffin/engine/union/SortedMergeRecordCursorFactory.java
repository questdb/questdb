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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.std.Misc;

public final class SortedMergeRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory factoryA;
    private final RecordCursorFactory factoryB;
    private final SortedMergeRecordCursor cursor;
    private final RecordComparator comparator;

    public SortedMergeRecordCursorFactory(RecordMetadata metadata, RecordCursorFactory factoryA, RecordCursorFactory factoryB, RecordComparator comparator) {
        super(metadata);
        this.factoryA = factoryA;
        this.factoryB = factoryB;
        this.cursor = new SortedMergeRecordCursor();
        this.comparator = comparator;
    }

    @Override
    protected void _close() {
        Misc.free(factoryA);
        Misc.free(factoryB);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor cursorA = null;
        RecordCursor cursorB = null;
        try {
            cursorA = factoryA.getCursor(executionContext);
            cursorB = factoryB.getCursor(executionContext);
            cursor.of(cursorA, cursorB, comparator, executionContext.getCircuitBreaker());
            return cursor;
        } catch (Throwable ex) {
            Misc.free(cursorA);
            Misc.free(cursorB);
            throw ex;
        }
    }

    @Override
    public boolean fragmentedSymbolTables() {
        return true;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }
}
