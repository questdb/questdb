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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class UnnestRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory baseFactory;
    private final ObjList<CharSequence> columnNames;
    private final UnnestRecordCursor cursor;
    private final ObjList<Function> functions;
    private final boolean hasOrdinality;
    private final ObjList<UnnestSource> sources;

    public UnnestRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory baseFactory,
            ObjList<Function> functions,
            ObjList<UnnestSource> sources,
            int columnSplit,
            boolean hasOrdinality,
            ObjList<CharSequence> columnNames
    ) {
        super(metadata);
        this.baseFactory = baseFactory;
        this.functions = functions;
        this.sources = sources;
        this.hasOrdinality = hasOrdinality;
        this.columnNames = columnNames;
        if (hasOrdinality) {
            sources.add(new OrdinalityUnnestSource());
        }
        this.cursor = new UnnestRecordCursor(columnSplit, sources);
    }

    @Override
    public boolean followedOrderByAdvice() {
        return baseFactory.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(
            SqlExecutionContext executionContext
    ) throws SqlException {
        RecordCursor baseCursor = baseFactory.getCursor(executionContext);
        try {
            Function.init(
                    functions, baseCursor, executionContext, null
            );
            cursor.of(
                    baseCursor,
                    executionContext.getCircuitBreaker()
            );
            return cursor;
        } catch (Throwable ex) {
            Misc.free(baseCursor);
            throw ex;
        }
    }

    @Override
    public int getScanDirection() {
        return baseFactory.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Unnest");
        sink.attr("columns").val(columnNames);
        if (hasOrdinality) {
            sink.attr("ordinality").val(true);
        }
        sink.child(baseFactory);
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(cursor);
        Misc.free(baseFactory);
        Misc.freeObjList(functions);
        Misc.freeObjListIfCloseable(sources);
    }

    private static class UnnestRecordCursor implements NoRandomAccessRecordCursor {
        private final UnnestRecord record;
        private final ObjList<UnnestSource> sources;
        private int arrayIndex;
        private RecordCursor baseCursor;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isMasterPending;
        private int maxArrayLen;

        public UnnestRecordCursor(
                int columnSplit,
                ObjList<UnnestSource> sources
        ) {
            this.sources = sources;
            this.record = new UnnestRecord(columnSplit, sources);
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            while (hasNext()) {
                counter.inc();
            }
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (columnIndex < record.getSplit()) {
                return baseCursor.getSymbolTable(columnIndex);
            }
            return null;
        }

        @Override
        public boolean hasNext() {
            while (true) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                if (isMasterPending) {
                    if (!baseCursor.hasNext()) {
                        return false;
                    }
                    initSources();
                    isMasterPending = false;
                    arrayIndex = 0;
                    if (maxArrayLen == 0) {
                        isMasterPending = true;
                        continue;
                    }
                }
                if (arrayIndex < maxArrayLen) {
                    record.setArrayIndex(arrayIndex);
                    arrayIndex++;
                    return true;
                }
                isMasterPending = true;
            }
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            if (columnIndex < record.getSplit()) {
                return baseCursor.newSymbolTable(columnIndex);
            }
            return null;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
            isMasterPending = true;
            arrayIndex = 0;
            maxArrayLen = 0;
        }

        private void initSources() {
            Record baseRecord = baseCursor.getRecord();
            maxArrayLen = 0;
            for (int i = 0, n = sources.size(); i < n; i++) {
                int len = sources.getQuick(i).init(baseRecord);
                if (len > maxArrayLen) {
                    maxArrayLen = len;
                }
            }
        }

        void of(
                RecordCursor baseCursor,
                SqlExecutionCircuitBreaker circuitBreaker
        ) {
            this.baseCursor = baseCursor;
            this.circuitBreaker = circuitBreaker;
            this.isMasterPending = true;
            this.arrayIndex = 0;
            this.maxArrayLen = 0;
            Record baseRecord = baseCursor.getRecord();
            record.of(baseRecord);
        }
    }
}
