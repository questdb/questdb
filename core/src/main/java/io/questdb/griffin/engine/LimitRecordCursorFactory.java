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

    public LimitRecordCursorFactory(
            RecordCursorFactory base,
            Function loFunction,
            @Nullable Function hiFunction,
            int argPos
    ) {
        super(base.getMetadata());
        this.base = base;
        this.cursor = new LimitRecordCursor(loFunction, hiFunction, argPos);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
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
        Function loFunc = cursor.leftFunction;
        Function hiFunc = cursor.rightFunction;
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

        // cursor must be open to calculate the limit details.
        if (cursor.base != null && loFunc != null && loFunc.getLong(null) != Numbers.LONG_NULL) {
            cursor.resolveSize();
            sink.meta("skip-over-rows").val(cursor.skippedRows);
            sink.meta("limit").val(cursor.size);
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
        private final int argPos;
        private final RecordCursor.Counter counter = new Counter();
        private final Function leftFunction;
        private final Function rightFunction;
        private boolean areRowsCounted;
        private RecordCursor base;
        private long baseRowCount;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private long hi;
        private boolean isSizeResolved;
        private long lo;
        private long remaining;
        private long rowsToSkip;
        private long size;
        // number of rows the cursor will skip, only used to display correct query execution plan
        private long skippedRows;

        public LimitRecordCursor(Function leftFunction, Function rightFunction, int argPos) {
            this.leftFunction = leftFunction;
            this.rightFunction = rightFunction;
            this.argPos = argPos;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            if (areRowsCounted && remaining > 0) {
                counter.add(size);
                remaining = 0;
                return;
            }

            ensureSizeResolved();

            while (remaining > 0 && base.hasNext()) {
                remaining--;
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
            ensureSizeResolved();
            if (remaining <= 0) {
                return false;
            }
            if (base.hasNext()) {
                remaining--;
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
            this.circuitBreaker = executionContext.getCircuitBreaker();

            leftFunction.init(base, executionContext);
            if (rightFunction != null) {
                rightFunction.init(base, executionContext);
            }
            long leftArg = leftFunction.getLong(null);
            if (rightFunction == null) {
                if (leftArg >= 0) {
                    lo = 0;
                    hi = leftArg;
                } else {
                    lo = leftArg;
                    hi = 0;
                }
            } else {
                lo = leftArg;
                hi = rightFunction.getLong(null);
                if (lo < 0 && hi > 0) {
                    throw SqlException.$(argPos, "LIMIT <negative>, <positive> is not allowed");
                }
                if (lo > hi && Numbers.sameSign(lo, hi)) {
                    final long l = hi;
                    hi = lo;
                    lo = l;
                }
            }

            baseRowCount = -1;
            size = -1;
            rowsToSkip = -1;
            skippedRows = 0;
            isSizeResolved = false;
            areRowsCounted = false;
            counter.clear();
        }

        @Override
        public long preComputedStateSize() {
            return RecordCursor.fromBool(areRowsCounted) + RecordCursor.fromBool(isSizeResolved) + base.preComputedStateSize();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            base.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            ensureSizeResolved();
            return size;
        }

        @Override
        public void toTop() {
            base.toTop();
            remaining = size;
            rowsToSkip = -1;
            skipRows(skippedRows);
            /*
             Now set rowsToSkip back to -1.
             skipRows() needs it at -1 to function correctly.
             In toTop(), we skipRows() in the base cursor, which is fine.
             But in resolveLimitRange(), we have to do the same thing.
             If rowsToSkip == 0, instead of taking the correct count,
             it will set up to return 0 rows and the wrong answer.
             Example query that will break without this:
             (SELECT timestamp FROM trades LIMIT 1)
             UNION ALL
             (SELECT timestamp FROM trades LIMIT -1)

             In the above example, the query acts like LIMIT 1 in both branches.
             */
            rowsToSkip = -1;
        }

        private void countRows() {
            if (baseRowCount == -1) {
                baseRowCount = base.size();
                if (baseRowCount > -1) {
                    areRowsCounted = true;
                    return;
                }
                baseRowCount = 0;
            }

            if (!areRowsCounted) {
                base.calculateSize(circuitBreaker, counter);
                base.toTop();
                baseRowCount = counter.get();
                areRowsCounted = true;
                counter.clear();
            }
        }

        private void ensureSizeResolved() {
            if (!isSizeResolved) {
                resolveSize();
                remaining = size;
                isSizeResolved = true;
            }
        }

        private void resolveSize() {
            if (lo == hi) {
                // There's either a single zero argument (LIMIT 0) or two equal arguments (LIMIT n, n).
                // In both cases the result is an empty cursor.
                size = 0;
            } else if (rightFunction == null) {
                // There's only one LIMIT argument, could be positive or negative.
                // We must first get the actual row count to resolve the LIMIT range.
                countRows();
                if (lo == 0) {
                    // arg is positive, it's the upper bound (hi) and specifies how many rows to take from the start
                    size = Math.min(baseRowCount, hi);
                } else {
                    // arg is negative, it's the lower bound (lo) and specifies how many rows to take from the end
                    long takeFromBack = -lo;
                    if (takeFromBack < baseRowCount) {
                        skipRows(baseRowCount - takeFromBack);
                        size = takeFromBack;
                    } else {
                        base.toTop();
                        size = baseRowCount;
                    }
                }
            } else {
                // There are two LIMIT arguments, and they aren't equal.
                // We must first get the actual row count to resolve the LIMIT range.
                countRows();
                if (lo >= 0) {
                    // There are two LIMIT arguments, and the left one is non-negative.
                    if (hi >= 0) {
                        // Both arguments are non-negative.
                        // The code in of() already ensured that lo <= hi for same-signed arguments.
                        // Therefore, we have lo < hi.
                        // They denote a range counting from start, 0-based.
                        // Lower bound is inclusive, upper bound is exclusive.
                        size = Math.max(0, Math.min(baseRowCount, hi) - lo);
                        if (lo > 0 && size > 0) {
                            skipRows(lo);
                        }
                    } else {
                        // Mixed-sign arguments, like `LIMIT 2, -2` or `LIMIT 0, -2`.
                        // Left is lower bound (inclusive) counting from start, zero-based.
                        // Right is upper bound (exclusive) counting from end. Last row is numbered -1.
                        size = Math.max(baseRowCount - lo + hi, 0);
                        if (lo > 0 && size > 0) {
                            skipRows(lo);
                        } else {
                            base.toTop();
                        }
                    }
                } else {
                    // There are two LIMIT arguments, and the left one is negative.
                    // We already validated against the (negative, positive) combination.
                    // Therefore, both arguments are negative.
                    // The code in of() already ensured that lo <= hi for same-signed arguments.
                    // Therefore, we have lo < hi.
                    // They denote a range counting from the end. Last row is numbered -1.
                    // Lower bound is inclusive, upper bound is exclusive.
                    long start = baseRowCount + lo;
                    long end = baseRowCount + hi;
                    if (end < 0) {
                        size = 0;
                    } else if (start < 0) {
                        base.toTop();
                        size = end;
                    } else {
                        skipRows(start);
                        size = Math.min(baseRowCount, end - start);
                    }
                }
            }
        }

        private void skipRows(long rowCount) {
            if (rowsToSkip == -1) {
                rowsToSkip = Math.max(0, rowCount);
                counter.set(rowsToSkip);
                skippedRows = rowsToSkip;
                base.toTop();
            }
            if (rowsToSkip > 0) {
                base.skipRows(counter);
                rowsToSkip = 0;
                counter.clear();
            }
        }
    }
}
