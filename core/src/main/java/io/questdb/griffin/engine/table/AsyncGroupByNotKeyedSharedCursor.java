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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class AsyncGroupByNotKeyedSharedCursor implements NoRandomAccessRecordCursor {
    private final ObjList<Function> groupByFunctions;
    private final AsyncGroupByNotKeyedRecordCursor primaryCursor;
    private final VirtualRecord record;
    private boolean isExhausted;

    AsyncGroupByNotKeyedSharedCursor(
            AsyncGroupByNotKeyedRecordCursor cursor,
            ObjList<Function> functions,
            SimpleMapValue mapValue
    ) {
        this.primaryCursor = cursor;
        this.groupByFunctions = functions;
        this.record = new VirtualRecord(functions);
        this.record.of(mapValue);
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        if (!isExhausted) {
            counter.inc();
            isExhausted = true;
        }
    }

    @Override
    public void close() {
        Misc.clearObjList(groupByFunctions);
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return (SymbolTable) groupByFunctions.getQuick(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (isExhausted) {
            return false;
        }
        primaryCursor.buildValueConditionally();
        isExhausted = true;
        return true;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return ((SymbolFunction) groupByFunctions.getQuick(columnIndex)).newSymbolTable();
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public long size() {
        return 1;
    }

    @Override
    public void toTop() {
        isExhausted = false;
        GroupByUtils.toTop(groupByFunctions);
    }

    void of(SqlExecutionContext executionContext, UnorderedPageFrameSequence<AsyncGroupByNotKeyedAtom> frameSequence) throws SqlException {
        Function.init(groupByFunctions, frameSequence.getSymbolTableSource(), executionContext, null);
        isExhausted = false;
    }
}
