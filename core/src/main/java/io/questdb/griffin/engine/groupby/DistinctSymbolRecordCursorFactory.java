/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BitSet;
import io.questdb.std.Misc;

import static io.questdb.cairo.sql.SymbolTable.VALUE_IS_NULL;

public class DistinctSymbolRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final DistinctSymbolRecordCursor cursor;

    public DistinctSymbolRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base
    ) {
        super(base.getMetadata());
        this.base = base;
        this.cursor = new DistinctSymbolRecordCursor(configuration.getCountDistinctCapacity());
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        cursor.of(baseCursor, executionContext);
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("DistinctSymbol");
        sink.child(base);
    }

    @Override
    protected void _close() {
        Misc.free(base);
    }

    private static class DistinctSymbolRecordCursor implements RecordCursor {
        private final BitSet seenKeySet;
        private RecordCursor baseCursor;
        private Record baseRecord;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private int count = 0;
        private int knownSymbolCount = -1;
        // BitSet doesn't support negative values, so we track null key separately.
        private boolean seenNull;

        public DistinctSymbolRecordCursor(int bitSetCapacity) {
            this.seenKeySet = new BitSet(bitSetCapacity);
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
        }

        @Override
        public Record getRecord() {
            return baseCursor.getRecord();
        }

        @Override
        public Record getRecordB() {
            return baseCursor.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (count == knownSymbolCount) {
                return false;
            }
            while (baseCursor.hasNext()) {
                final int symbolKey = baseRecord.getInt(0); // single column query
                if (symbolKey != VALUE_IS_NULL && !seenKeySet.get(symbolKey)) {
                    seenKeySet.set(symbolKey);
                    count++;
                    return true;
                } else if (symbolKey == VALUE_IS_NULL && !seenNull) {
                    seenNull = true;
                    count++;
                    return true;
                }
                circuitBreaker.statefulThrowExceptionIfTripped();
            }
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
        }

        public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) {
            this.baseCursor = baseCursor;
            baseRecord = baseCursor.getRecord();
            circuitBreaker = executionContext.getCircuitBreaker();
            // Single column query, hence 0 column index.
            final SymbolTable symbolTable = baseCursor.getSymbolTable(0);
            if (symbolTable instanceof StaticSymbolTable) {
                final StaticSymbolTable staticSymbolTable = (StaticSymbolTable) symbolTable;
                knownSymbolCount = staticSymbolTable.containsNullValue()
                        ? staticSymbolTable.getSymbolCount() + 1
                        : staticSymbolTable.getSymbolCount();
            } else {
                knownSymbolCount = -1;
            }
            toTop();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            baseCursor.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
            seenKeySet.clear();
            seenNull = false;
            count = 0;
        }
    }
}
