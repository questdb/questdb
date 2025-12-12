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
        Function leftFunc = cursor.leftFunction;
        Function rightFunc = cursor.rightFunction;
        boolean isCursorOpen = cursor.base != null;
        if (leftFunc != null) {
            sink.meta(rightFunc != null ? "left" : "value");
            sink.val(leftFunc);
            if (leftFunc.isConstant() || isCursorOpen && leftFunc.isRuntimeConstant()) {
                sink.val('[').val(leftFunc.getLong(null)).val(']');
            }
        }
        if (rightFunc != null) {
            sink.meta("right").val(rightFunc);
            if (rightFunc.isConstant() || isCursorOpen && rightFunc.isRuntimeConstant()) {
                sink.val('[').val(rightFunc.getLong(null)).val(']');
            }
        }

        if (isCursorOpen && leftFunc != null && leftFunc.getLong(null) != Numbers.LONG_NULL) {
            cursor.ensureBoundsResolved();
            String skipLabel, takeLabel;
            if (cursor.areRowsCounted) {
                skipLabel = "skip-rows";
                takeLabel = "take-rows";
            } else {
                skipLabel = "skip-rows-max";
                takeLabel = "take-rows-max";
            }
            sink.meta(skipLabel).val(cursor.baseRowsToSkip);
            sink.meta(takeLabel).val(cursor.baseRowsToTake);
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
        private long baseRowsToSkip;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private long hi;
        private boolean isSizeResolved;
        private boolean isToTopInProgress;
        private long lo;
        private long remaining;
        private long size;

        public LimitRecordCursor(Function leftFunction, Function rightFunction, int argPos) {
            this.leftFunction = leftFunction;
            this.rightFunction = rightFunction;
            this.argPos = argPos;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            if (isSizeResolved) {
                counter.add(remaining);
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
            isToTopInProgress = false;
            baseRowsToSkip = 0;
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
            remaining = size;
            if (!isToTopInProgress) {
                isToTopInProgress = true;
                base.toTop();
                counter.set(baseRowsToSkip);
            }
            if (isToTopInProgress) {
                if (counter.get() > 0) {
                    base.skipRows(counter);
                }
                isToTopInProgress = false;
                counter.clear();
            }
        }

        private void ensureRowsCounted() {
            if (baseRowCount == -1) {
                baseRowCount = base.size();
                if (baseRowCount > -1) {
                    areRowsCounted = true;
                    return;
                }
                baseRowCount = 0;
            }

            if (!areRowsCounted) {
                base.toTop();
                base.calculateSize(circuitBreaker, counter);
                baseRowCount = counter.get();
                areRowsCounted = true;
                counter.clear();
            }
        }

        private void ensureSizeResolved() {
            if (isSizeResolved) {
                return;
            }
            resolveSizeAndGotoTop();
            remaining = size;
            isSizeResolved = true;
        }

        private void resolveSizeAndGotoTop() {
            if (lo == hi) {
                // There's either a single zero argument (LIMIT 0) or two equal arguments (LIMIT n, n).
                // In both cases the result is an empty cursor.
                size = 0;
                return;
            }
            // Compute base cursor's row count and then resolve this cursor's row count,
            // and how many base cursor's rows we must skip.
            ensureRowsCounted();

            long startInclusive, endExclusive;
            if (rightFunction == null) {
                // There's only one LIMIT argument, could be positive or negative.
                if (lo == 0) {
                    // arg is positive, it's the upper bound (hi) and specifies how many rows to take from the start
                    startInclusive = 0;
                    endExclusive = hi;
                } else {
                    // arg is negative, it's the lower bound (lo) and specifies how many rows to take from the end
                    startInclusive = baseRowCount + lo;
                    endExclusive = baseRowCount;
                }
            } else if (lo >= 0) {
                // There are two LIMIT arguments, and the left one is non-negative.
                if (hi >= 0) {
                    // Both arguments are non-negative.
                    // The code in of() already ensured that lo <= hi for same-signed arguments,
                    // and we eliminated lo == hi at the start of this method. Therefore, lo < hi.
                    // They denote a range counting from start, 0-based.
                    // Lower bound is inclusive, upper bound is exclusive.
                    startInclusive = lo;
                    endExclusive = hi;
                } else {
                    // Mixed-sign arguments, like `LIMIT 2, -2` or `LIMIT 0, -2`.
                    // Left arg is lower bound (inclusive) counting from start, 0-based.
                    // Right arg is upper bound (exclusive) counting from end. Last row is numbered -1.
                    startInclusive = lo;
                    endExclusive = baseRowCount + hi;
                }
            } else {
                // There are two LIMIT arguments, and the left one is negative.
                // We already validated against the (negative, positive) combination.
                // Therefore, both arguments are negative.
                // The code in of() already ensured that lo <= hi for same-signed arguments,
                // and we eliminated lo == hi at the start of this method. Therefore, lo < hi.
                // They denote a range counting from the end. Last row is numbered -1.
                // Lower bound is inclusive, upper bound is exclusive.
                startInclusive = baseRowCount + lo;
                endExclusive = baseRowCount + hi;
            }

            startInclusive = Math.max(0, startInclusive);
            endExclusive = Math.min(baseRowCount, endExclusive);
            if (startInclusive >= endExclusive) {
                size = 0;
                return;
            }
            size = endExclusive - startInclusive;
            baseRowsToSkip = startInclusive;
            toTop();
        }
    }
}
