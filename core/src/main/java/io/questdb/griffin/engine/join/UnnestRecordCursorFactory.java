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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
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
    private final int arrayColumnCount;
    private final ObjList<Function> arrayFunctions;
    private final RecordCursorFactory baseFactory;
    private final int columnSplit;
    private final UnnestRecordCursor cursor;
    private final boolean hasOrdinality;

    public UnnestRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory baseFactory,
            ObjList<Function> arrayFunctions,
            int columnSplit,
            boolean hasOrdinality
    ) {
        super(metadata);
        this.baseFactory = baseFactory;
        this.arrayFunctions = arrayFunctions;
        this.columnSplit = columnSplit;
        this.arrayColumnCount = arrayFunctions.size();
        this.hasOrdinality = hasOrdinality;
        this.cursor = new UnnestRecordCursor(columnSplit, arrayColumnCount, hasOrdinality);
    }

    @Override
    public boolean followedOrderByAdvice() {
        return baseFactory.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor baseCursor = baseFactory.getCursor(executionContext);
        try {
            Function.init(arrayFunctions, baseCursor, executionContext, null);
            cursor.of(baseCursor, arrayFunctions, executionContext.getCircuitBreaker());
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
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Unnest");
        sink.child(baseFactory);
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(baseFactory);
        Misc.freeObjList(arrayFunctions);
    }

    private static class UnnestRecordCursor implements RecordCursor {
        private final int arrayColumnCount;
        private final int[] arrayLengths;
        private final ArrayView[] arrayViews;
        private final UnnestRecord record;
        private int arrayIndex;
        private RecordCursor baseCursor;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private ObjList<Function> functions;
        private boolean isMasterPending;
        private int maxArrayLen;

        public UnnestRecordCursor(int columnSplit, int arrayColumnCount, boolean hasOrdinality) {
            this.record = new UnnestRecord(columnSplit, arrayColumnCount, hasOrdinality);
            this.arrayColumnCount = arrayColumnCount;
            this.arrayViews = new ArrayView[arrayColumnCount];
            this.arrayLengths = new int[arrayColumnCount];
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            // cannot pre-calculate size efficiently
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
        public Record getRecordB() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (isMasterPending) {
                    if (!baseCursor.hasNext()) {
                        return false;
                    }
                    loadArrays();
                    isMasterPending = false;
                    arrayIndex = 0;
                    if (maxArrayLen == 0) {
                        isMasterPending = true;
                        continue;
                    }
                }
                if (arrayIndex < maxArrayLen) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    record.setArrayIndex(arrayIndex);
                    arrayIndex++;
                    return true;
                }
                isMasterPending = true;
            }
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
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

        void of(RecordCursor baseCursor, ObjList<Function> functions, SqlExecutionCircuitBreaker circuitBreaker) {
            this.baseCursor = baseCursor;
            this.functions = functions;
            this.circuitBreaker = circuitBreaker;
            this.isMasterPending = true;
            this.arrayIndex = 0;
            this.maxArrayLen = 0;
            Record baseRecord = baseCursor.getRecord();
            record.of(baseRecord, arrayViews, arrayLengths);
        }

        private void loadArrays() {
            Record baseRecord = baseCursor.getRecord();
            maxArrayLen = 0;
            for (int i = 0; i < arrayColumnCount; i++) {
                ArrayView view = functions.getQuick(i).getArray(baseRecord);
                if (view == null || view.isNull()) {
                    arrayViews[i] = null;
                    arrayLengths[i] = 0;
                } else {
                    arrayViews[i] = view;
                    int len = view.isEmpty() ? 0 : view.getDimLen(0);
                    arrayLengths[i] = len;
                    if (len > maxArrayLen) {
                        maxArrayLen = len;
                    }
                }
            }
        }
    }
}
