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

package io.questdb.griffin.engine;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.MasterSuffixLimitFunction;
import io.questdb.std.IntHashSet;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.Nullable;

public class LimitRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final LimitRecordCursor cursor;

    /**
     * {@code masterSuffixLimit}: the LIMIT count of the master filter of the join the base runs, which the base factory
     * owns. When the join emits fewer rows than a negative LIMIT selects and older master rows may pass the filter, the
     * cursor rewinds the join over a longer suffix of master rows, which keeps the join's slave side, and past the most
     * rows the filter holds, opens the join again over all master rows. The rows of each master row come out together
     * and in master order, so the last rows of the join over a suffix of the master rows are its last rows over all of
     * them once there are as many as the LIMIT selects.
     */
    public LimitRecordCursorFactory(
            RecordCursorFactory base,
            Function loFunction,
            @Nullable Function hiFunction,
            int argPos,
            @Nullable MasterSuffixLimitFunction masterSuffixLimit
    ) {
        super(base.getMetadata());
        this.base = base;
        this.cursor = new LimitRecordCursor(base, loFunction, hiFunction, argPos, masterSuffixLimit);
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

    // Stable iff the limit expressions (which may be arbitrary functions) and the base are stable.
    @Override
    public boolean isNonDeterministic() {
        final Function leftFunc = cursor.leftFunction;
        if (leftFunc != null && leftFunc.isNonDeterministic()) {
            return true;
        }
        final Function rightFunc = cursor.rightFunction;
        if (rightFunc != null && rightFunc.isNonDeterministic()) {
            return true;
        }
        return base.isNonDeterministic();
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
            if (leftFunc.isRuntimeConstant() && isCursorOpen) {
                sink.val('[').val(leftFunc.getLong(null)).val(']');
            }
        }
        if (rightFunc != null) {
            sink.meta("right").val(rightFunc);
            if (rightFunc.isRuntimeConstant() && isCursorOpen) {
                sink.val('[').val(rightFunc.getLong(null)).val(']');
            }
        }

        if (isCursorOpen && leftFunc != null && leftFunc.getLong(null) != Numbers.LONG_NULL) {
            if (cursor.isBaseSizeKnown()) {
                sink.meta("skip-rows").val(cursor.baseRowsToSkip);
                sink.meta("take-rows").val(cursor.baseRowsToTake);
            } else if (cursor.areBoundsResolved()) {
                sink.meta("skip-rows-max").val(cursor.baseRowsToSkip);
                sink.meta("take-rows-max").val(cursor.baseRowsToTake);
            } else {
                long lo = cursor.lo;
                long hi = cursor.hi;
                if (lo < 0) {
                    sink.meta("skip-rows").val("baseRows").val(lo);
                    // if lo < 0, hi should always be <= 0, but guard it just in case.
                    // We don't want any exceptions in toPlan(), so just silently skip unexpected value.
                    if (hi <= 0) {
                        sink.meta("take-rows-max").val(hi - lo);
                    }
                } else {
                    // lo >= 0
                    // If both lo and hi were >= 0, bounds would already have been resolved in cursor.of().
                    // But cursor bounds aren't resolved, therefore hi < 0.
                    sink.meta("skip-rows-max").val(lo);
                    sink.meta("take-rows").val("baseRows").val(hi - lo);
                }
            }
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
        Throwable failure = Misc.freeBestEffort(null, base);
        failure = Misc.freeBestEffort(failure, cursor.leftFunction);
        failure = Misc.freeBestEffort(failure, cursor.rightFunction);
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Applies a LIMIT over a base cursor; a negative LIMIT counts the base rows first, then skips to the last ones.
     */
    public static class LimitRecordCursor implements RecordCursor {
        private final int argPos;
        private final RecordCursorFactory baseFactory;
        private final RecordCursor.Counter counter = new Counter();
        private final Function leftFunction;
        private final MasterSuffixLimitFunction masterSuffixLimit;
        private final Function rightFunction;
        private RecordCursor base;
        private long baseRowsToSkip;
        private long baseRowsToTake;
        private long baseSize;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private SqlExecutionContext executionContext;
        private long hi;
        // The base sits at the first row the LIMIT returns, where toTop() left it.
        private boolean isBaseAtFirstRow;
        private long lo;
        private long remaining;
        private long size;

        public LimitRecordCursor(
                RecordCursorFactory baseFactory,
                Function leftFunction,
                Function rightFunction,
                int argPos,
                MasterSuffixLimitFunction masterSuffixLimit
        ) {
            this.baseFactory = baseFactory;
            this.leftFunction = leftFunction;
            this.rightFunction = rightFunction;
            this.argPos = argPos;
            this.masterSuffixLimit = masterSuffixLimit;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter sizeCounter) {
            ensureReadyToConsume();
            if (isBaseSizeKnown()) {
                sizeCounter.add(remaining);
            } else {
                counter.set(remaining);
                // No further reads after the skip -- max=0 lets the base
                // skip the landing-frame decode altogether.
                isBaseAtFirstRow = false;
                base.skipRows(counter, 0);
                sizeCounter.add(remaining - counter.get());
                counter.clear();
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
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            ensureReadyToConsume();
            if (remaining <= 0) {
                return false;
            }
            isBaseAtFirstRow = false;
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

        @Override
        public long preComputedStateSize() {
            return RecordCursor.fromBool(isBaseSizeKnown()) + RecordCursor.fromBool(areBoundsResolved()) + base.preComputedStateSize();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            base.recordAt(record, atRowId);
        }

        @Override
        public void setParentUsedColumns(@Nullable IntHashSet columnIndexes) {
            base.setParentUsedColumns(columnIndexes);
        }

        @Override
        public void setParquetDecodeHint(ParquetDecodeHint hint) {
            base.setParquetDecodeHint(hint);
        }

        @Override
        public void setRecordAtRows(@Nullable RowIdSource source) {
            base.setRecordAtRows(source);
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public void skipRows(Counter skipCounter, long maxRowsAfterSkip) {
            ensureReadyToConsume();
            long rowsToSkip = skipCounter.get();
            long excessCount = Math.max(0, rowsToSkip - remaining);
            rowsToSkip -= excessCount;
            skipCounter.dec(excessCount);
            final long baseMax = Math.min(maxRowsAfterSkip, remaining - rowsToSkip);
            isBaseAtFirstRow = false;
            base.skipRows(skipCounter, baseMax);
            long counterAfterSkip = skipCounter.get();
            if (counterAfterSkip > 0) {
                remaining = 0;
            } else {
                // counterAfterSkip should never be negative, so normally it will be zero here.
                // However, if the base cursor is broken and makes it negative, it's better
                // not to erase the trace of that bug, so we preserve its effect.
                remaining -= (rowsToSkip - counterAfterSkip);
                if (remaining < 0) {
                    remaining = 0;
                }
            }
            skipCounter.add(excessCount);
        }

        @Override
        public void toTop() {
            ensureBoundsResolved();
            if (!isBaseAtFirstRow) {
                base.toTop();
                counter.set(baseRowsToSkip);
                // Unconditional call: even with baseRowsToSkip == 0, this pushes
                // baseRowsToTake as the post-skip cap so the base can clamp
                // decoding of the first frames.
                base.skipRows(counter, baseRowsToTake);
                counter.clear();
                isBaseAtFirstRow = true;
            }
            remaining = baseRowsToTake;
        }

        private boolean areBoundsResolved() {
            return baseRowsToTake != -1;
        }

        private void calculateBaseSize() {
            isBaseAtFirstRow = false;
            base.toTop();
            base.calculateSize(circuitBreaker, counter);
            baseSize = counter.get();
            counter.clear();
        }

        private void ensureBoundsResolved() {
            if (!areBoundsResolved()) {
                // If baseSize was cheap to get, bounds would already have been sorted out in of().
                // Now it's time to use the heavy-handed approach to getting baseSize.
                calculateBaseSize();
                if (masterSuffixLimit != null) {
                    readMasterSuffix();
                }
                // If both LIMIT args were non-negative, we would have resolved the bounds without
                // needing to know baseSize. Since we're here, one of the args must be negative.
                resolveBoundsFromNegativeArgs();
            }
        }

        private void ensureReadyToConsume() {
            if (remaining != -1) {
                return;
            }
            ensureBoundsResolved();
            toTop();
        }

        private boolean isBaseSizeKnown() {
            return baseSize >= 0;
        }

        private void readAllMasterRows() {
            masterSuffixLimit.setReadingAllRows();
            try {
                base = Misc.free(base);
                base = baseFactory.getCursor(executionContext);
            } catch (SqlException e) {
                throw CairoException.nonCritical().position(e.getPosition()).put(e.getFlyweightMessage());
            } finally {
                masterSuffixLimit.clearReadingAllRows();
            }
        }

        private void readMasterSuffix() {
            while (baseSize < -lo && masterSuffixLimit.hasMoreRows()) {
                if (!masterSuffixLimit.readMoreRows()) {
                    readAllMasterRows();
                }
                calculateBaseSize();
            }
        }

        private void resolveBoundsCheap() {
            if (lo == hi) {
                // There's either a single zero argument (LIMIT 0) or two equal arguments (LIMIT n, n).
                // In both cases the result is an empty cursor.
                size = baseRowsToSkip = baseRowsToTake = 0;
                return;
            }
            baseSize = base.size();
            if (lo >= 0 && hi >= 0) {
                resolveBoundsTail(lo, hi);
            } else if (baseSize >= 0) {
                resolveBoundsFromNegativeArgs();
            }
        }

        private void resolveBoundsFromNegativeArgs() {
            assert baseSize >= 0 : "baseSize < 0";
            long startInclusive, endExclusive;
            if (lo < 0) {
                startInclusive = baseSize + lo;
                if (hi <= 0) {
                    endExclusive = baseSize + hi;
                } else {
                    // "LIMIT <negative>, <positive> is validated against in of() because it's confusing.
                    // We handle it here anyway, in case this decision changes.
                    endExclusive = hi;
                }
            } else {
                // This method is called only when lo < 0 || hi < 0.
                // In this branch, we know that lo >= 0, therefore hi < 0.
                startInclusive = lo;
                endExclusive = baseSize + hi;
            }
            startInclusive = Math.max(0, startInclusive);
            resolveBoundsTail(startInclusive, endExclusive);
        }

        private void resolveBoundsTail(long startInclusive, long endExclusive) {
            if (baseSize >= 0) {
                endExclusive = Math.min(baseSize, endExclusive);
            }
            if (startInclusive >= endExclusive) {
                size = baseRowsToSkip = baseRowsToTake = 0;
                return;
            }
            baseRowsToSkip = startInclusive;
            baseRowsToTake = endExclusive - startInclusive;
            if (baseSize >= 0) {
                size = baseRowsToTake;
            }
        }

        public void of(RecordCursor base, SqlExecutionContext executionContext) throws SqlException {
            this.base = base;
            this.executionContext = executionContext;
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

            baseSize = -1;
            size = -1;
            baseRowsToSkip = -1;
            baseRowsToTake = -1;
            remaining = -1;
            isBaseAtFirstRow = false;
            counter.clear();
            // A negative LIMIT over a master suffix resolves its bounds once the join runs.
            if (masterSuffixLimit == null || lo >= 0) {
                resolveBoundsCheap();
            }
            if (lo >= 0 && hi > lo) {
                base.expectLimitedIteration();
            }
        }
    }
}
