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
        private boolean areBoundsResolved;
        private boolean areRowsCounted;
        private RecordCursor base;
        private long baseRowCount;
        private long baseRowsToSkip;
        private long baseRowsToTake;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private long hi;
        private boolean isSuspendableOpInProgress;
        private long lo;
        private long remaining;
        private long size;

        public LimitRecordCursor(Function leftFunction, Function rightFunction, int argPos) {
            this.leftFunction = leftFunction;
            this.rightFunction = rightFunction;
            this.argPos = argPos;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter sizeCounter) {
            if (areBoundsResolved && areRowsCounted && !isSuspendableOpInProgress) {
                sizeCounter.add(remaining);
            } else {
                ensureBoundsResolved();
                if (areRowsCounted) {
                    sizeCounter.add(remaining);
                } else {
                    // TODO [mtopol]: the commented-out code would be more efficient, a single call to base.skipRows()
                    // instead of a loop around base.hasNext(). But, skipRows() is broken in PageFrameRecordCursorImpl
                    // and potentially other places too. It malfunctions when called after partial consumption with
                    // hasNext().
//                    if (!isSuspendableOpInProgress) {
//                        counter.set(remaining);
//                        isSuspendableOpInProgress = true;
//                    }
                    while (remaining > 0 && base.hasNext()) {
                        remaining--;
                        sizeCounter.inc();
                    }
//                    base.skipRows(counter);
//                    sizeCounter.add(remaining - counter.get());
//                    counter.clear();
//                    isSuspendableOpInProgress = false;
                }
            }
            remaining = 0;
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
            ensureBoundsResolved();
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
            isSuspendableOpInProgress = false;
            baseRowsToSkip = 0;
            areBoundsResolved = false;
            areRowsCounted = false;
            counter.clear();
        }

        @Override
        public long preComputedStateSize() {
            return RecordCursor.fromBool(areRowsCounted) + RecordCursor.fromBool(areBoundsResolved) + base.preComputedStateSize();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            base.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            ensureBoundsResolved();
            return size;
        }

        @Override
        public void toTop() {
            remaining = baseRowsToTake;
            if (!isSuspendableOpInProgress) {
                isSuspendableOpInProgress = true;
                base.toTop();
                counter.set(baseRowsToSkip);
            }
            if (isSuspendableOpInProgress) {
                if (counter.get() > 0) {
                    base.skipRows(counter);
                }
                isSuspendableOpInProgress = false;
                counter.clear();
            }
        }

        private void ensureBoundsResolved() {
            if (!areBoundsResolved) {
                resolveBoundsAndGotoTop();
                remaining = baseRowsToTake;
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

        private void resolveBoundsAndGotoTop() {
            if (lo == hi) {
                // There's either a single zero argument (LIMIT 0) or two equal arguments (LIMIT n, n).
                // In both cases the result is an empty cursor.
                size = baseRowsToTake = 0;
                return;
            }
            long startInclusive, endExclusive;
            if (lo < 0 || hi < 0) {
                // At least one argument is negative. We must compute base cursor's
                // row count to know how many base cursor's rows we'll skip.
                ensureRowsCounted();
                if (lo < 0) {
                    startInclusive = baseRowCount + lo;
                    if (hi <= 0) {
                        endExclusive = baseRowCount + hi;
                    } else {
                        endExclusive = hi;
                    }
                } else {
                    // (lo < 0) is false, therefore (hi < 0) is true
                    startInclusive = lo;
                    endExclusive = baseRowCount + hi;
                }
            } else {
                startInclusive = lo;
                endExclusive = hi;
            }
            startInclusive = Math.max(0, startInclusive);
            if (areRowsCounted) {
                endExclusive = Math.min(baseRowCount, endExclusive);
            }
            if (startInclusive >= endExclusive) {
                size = baseRowsToTake = 0;
                return;
            }
            baseRowsToTake = endExclusive - startInclusive;
            baseRowsToSkip = startInclusive;
            if (areRowsCounted) {
                size = baseRowsToTake;
            }
            areBoundsResolved = true;
            toTop();
        }
    }
}
