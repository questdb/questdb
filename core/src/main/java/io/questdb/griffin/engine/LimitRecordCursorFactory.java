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

package io.questdb.griffin.engine;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.Nullable;

public class LimitRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final LimitRecordCursor cursor;

    public LimitRecordCursorFactory(RecordCursorFactory base, Function loFunction, @Nullable Function hiFunction) {
        super(base.getMetadata());
        this.base = base;
        this.cursor = new LimitRecordCursor(loFunction, hiFunction);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (cursor.hiFunction != null) {
            // Forcefully disable column pre-touch for LIMIT K,N queries for all downstream
            // async filtered factories to avoid redundant disk reads.
            executionContext.setColumnPreTouchEnabled(false);
        }
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public boolean implementsLimit() {
        return true;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Limit");
        Function loFunc = cursor.loFunction;
        Function hiFunc = cursor.hiFunction;
        if (loFunc != null) {
            sink.meta("lo").val(loFunc);
            if (loFunc.isRuntimeConstant()) {
                sink.val('[').val(loFunc.getLong(null)).val(']');
            }
        }
        if (hiFunc != null) {
            sink.meta("hi").val(hiFunc);
            if (hiFunc.isRuntimeConstant()) {
                sink.val('[').val(hiFunc.getLong(null)).val(']');
            }
        }

        // cursor has to be open to calculate the limit details.
        if (cursor.base != null && loFunc != null && loFunc.getLong(null) != Numbers.LONG_NULL) {
            cursor.countLimit();
            sink.meta("skip-over-rows").val(cursor.skippedRows);
            sink.meta("limit").val(cursor.limit);
        }
        sink.child(base);
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
        base.close();
    }

    private static class LimitRecordCursor implements RecordCursor {
        private final RecordCursor.Counter counter = new Counter();
        private final Function hiFunction;
        private final Function loFunction;
        private boolean areRowsCounted;
        private RecordCursor base;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private long hi;
        private boolean isLimitCounted;
        private long limit;
        private long lo;
        private long rowCount;
        private long size;
        private long skipToRows;
        // number of rows cursor will skip, it is used to display correct
        // query execution plan only
        private long skippedRows;

        public LimitRecordCursor(Function loFunction, Function hiFunction) {
            this.loFunction = loFunction;
            this.hiFunction = hiFunction;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            if (areRowsCounted && limit > 0) {
                counter.add(size);
                limit = 0;
                return;
            }

            countLimitIfNotCounted();

            while (limit > 0 && base.hasNext()) {
                limit--;
                counter.inc();
            }
        }

        @Override
        public void close() {
            base = Misc.free(base);
        }

        @Override
        public Record getRecord() {
            return base.getRecord();
        }

        @Override
        public Record getRecordB() {
            return base.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return base.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            countLimitIfNotCounted();
            if (limit <= 0) {
                return false;
            }
            if (base.hasNext()) {
                limit--;
                return true;
            }
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return base.newSymbolTable(columnIndex);
        }

        public void of(RecordCursor base, SqlExecutionContext executionContext) throws SqlException {
            this.base = base;
            loFunction.init(base, executionContext);
            // swap lo and hi if lo > hi
            if (hiFunction != null) {
                hiFunction.init(base, executionContext);
            }
            this.circuitBreaker = executionContext.getCircuitBreaker();
            toTop();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            base.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            countLimitIfNotCounted();
            return size;
        }

        @Override
        public void toTop() {
            base.toTop();
            rowCount = -1;
            size = -1;
            skipToRows = -1;
            lo = loFunction.getLong(null);
            hi = hiFunction != null ? hiFunction.getLong(null) : -1;
            if (hi != -1 && hi < lo && Numbers.sameSign(lo, hi)) {
                final long l = hi;
                hi = lo;
                lo = l;
            }
            isLimitCounted = false;
            areRowsCounted = false;
            counter.clear();
            skippedRows = 0;
        }

        private void countLimit() {
            if (lo < 0 && hiFunction == null) {
                // last N rows
                countRows();

                // lo is negative, -5 for example
                // if we have 12 records, we need to skip 12-5 = 7
                // if we have 4 records, return all of them
                if (rowCount > -lo) {
                    skipRows(rowCount + lo);
                } else {
                    base.toTop();
                }
                // set limit to return remaining rows
                limit = Math.min(rowCount, -lo);
                size = limit;
            } else if (lo > -1 && hiFunction == null) {
                // first N rows
                long baseRowCount = base.size();
                if (baseRowCount > -1) { // we don't want to cause a pass-through whole data set
                    limit = Math.min(baseRowCount, lo);
                    areRowsCounted = true;
                } else {
                    limit = lo;
                    areRowsCounted = false;
                }
                countRows();
                size = Math.min(rowCount, limit);
            } else {
                // at this stage we have 'hi'
                if (lo < 0) {
                    // right, here we are looking for something like
                    // -10,-5 five rows away from tail

                    if (lo < hi) {
                        countRows();
                        // when count < -hi we have empty cursor
                        if (rowCount >= -hi) {
                            if (rowCount < -lo) {
                                base.toTop();
                                // if we asked for -9,-4 but there are 7 records in cursor
                                // we would first ignore last 4 and return first 3
                                limit = rowCount + hi;
                            } else {
                                skipRows(rowCount + lo);
                                limit = Math.min(rowCount, -lo + hi);
                            }
                            size = limit;
                        } else {
                            limit = size = 0;
                        }
                    } else {
                        // this is invalid bottom range, for example -3, -10
                        limit = 0;
                        size = 0;
                    }
                } else {
                    if (hi < 0) {
                        countRows();
                        limit = Math.max(rowCount - lo + hi, 0);
                        size = limit;

                        if (lo > 0 && limit > 0) {
                            skipRows(lo);
                        } else {
                            base.toTop();
                        }
                    } else {
                        long baseRowCount = base.size();
                        if (baseRowCount > -1L) { // we don't want to cause a pass-through whole data set
                            limit = Math.max(0, Math.min(baseRowCount, hi) - lo);
                            areRowsCounted = true;
                        } else {
                            limit = Math.max(0, hi - lo); // doesn't handle hi exceeding number of rows
                            areRowsCounted = false;
                        }
                        size = limit;

                        if (lo > 0 && limit > 0) {
                            skipRows(lo);
                        }
                    }
                }
            }
        }

        private void countLimitIfNotCounted() {
            if (!isLimitCounted) {
                countLimit();
                isLimitCounted = true;
            }
        }

        private void countRows() {
            if (rowCount == -1) {
                rowCount = base.size();
                if (rowCount > -1) {
                    areRowsCounted = true;
                    return;
                }
                rowCount = 0;
            }

            if (!areRowsCounted) {
                base.calculateSize(circuitBreaker, counter);
                base.toTop();
                rowCount = counter.get();
                areRowsCounted = true;
                counter.clear();
            }
        }

        private void skipRows(long rowCount) {
            if (skipToRows == -1) {
                skipToRows = Math.max(0, rowCount);
                counter.set(skipToRows);
                skippedRows = skipToRows;
                base.toTop();
            }
            if (skipToRows > 0) {
                base.skipRows(counter);
                skipToRows = 0;
                counter.clear();
            }
        }
    }
}
