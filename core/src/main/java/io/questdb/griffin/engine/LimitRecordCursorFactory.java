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
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

public class LimitRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final LimitBounds bounds;
    private final LimitRecordCursor cursor;

    /**
     * {@code masterSuffixLimit}: the LIMIT count of the master filter of the join the base runs, which the base factory
     * owns. When the join emits fewer rows than a negative LIMIT selects over the last master rows, the cursor rewinds
     * the join, which keeps its slave side, over older master rows only, until the join emits as many rows as the
     * LIMIT selects or reads all master rows. The rows of each master row come out together and in master order, so
     * the last rows of the join are those of the master rows it emitted them for, and the cursor rewinds the join over
     * just those master rows to return them.
     */
    public LimitRecordCursorFactory(
            RecordCursorFactory base,
            LimitBounds bounds,
            @Nullable MasterSuffixLimitFunction masterSuffixLimit
    ) {
        super(base.getMetadata());
        this.base = base;
        this.bounds = bounds;
        this.cursor = new LimitRecordCursor(bounds, masterSuffixLimit);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @TestOnly
    public long getWalkedRowCount() {
        return cursor.walkedRowCount;
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
        return bounds.isNonDeterministic() || base.isNonDeterministic();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Limit");
        final Function leftFunc = bounds.getLoFunction();
        final Function rightFunc = bounds.getHiFunction();
        final boolean isCursorOpen = cursor.base != null;
        sink.meta(rightFunc != null ? "left" : "value");
        sink.val(leftFunc);
        if (leftFunc.isRuntimeConstant() && isCursorOpen) {
            sink.val('[').val(leftFunc.getLong(null)).val(']');
        }
        if (rightFunc != null) {
            sink.meta("right").val(rightFunc);
            if (rightFunc.isRuntimeConstant() && isCursorOpen) {
                sink.val('[').val(rightFunc.getLong(null)).val(']');
            }
        }

        if (isCursorOpen && leftFunc.getLong(null) != Numbers.LONG_NULL) {
            if (cursor.isBaseSizeKnown()) {
                sink.meta("skip-rows").val(cursor.baseRowsToSkip);
                sink.meta("take-rows").val(cursor.baseRowsToTake);
            } else if (cursor.areBoundsResolved()) {
                sink.meta("skip-rows-max").val(cursor.baseRowsToSkip);
                sink.meta("take-rows-max").val(cursor.baseRowsToTake);
            } else {
                final long lo = bounds.getLo();
                final long hi = bounds.getHi();
                if (lo < 0) {
                    sink.meta("skip-rows").val("baseRows").val(lo);
                    sink.meta("take-rows-max").val(hi - lo);
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
        failure = bounds.close(failure);
        failure = Misc.freeBestEffort(failure, cursor.masterRowRing);
        failure = Misc.freeBestEffort(failure, cursor.masterRows);
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Applies a LIMIT over a base cursor; a negative LIMIT counts the base rows first, then skips to the last ones.
     */
    public static class LimitRecordCursor implements RecordCursor {
        private final LimitBounds bounds;
        private final RecordCursor.Counter counter = new Counter();
        private final MasterSuffixLimitFunction masterSuffixLimit;
        private RecordCursor base;
        private long baseRowsToSkip;
        private long baseRowsToTake;
        private long baseSize;
        private SqlExecutionCircuitBreaker circuitBreaker;
        // The base sits at the first row the LIMIT returns, where toTop() left it.
        private boolean isBaseAtFirstRow;
        // The trailing master rows of one read of the master filter whose joined rows cover the rows the LIMIT still
        // misses, as a ring of (row id, joined row count) pairs.
        private DirectLongList masterRowRing;
        // The master rows whose joined rows the LIMIT returns, newest first.
        private DirectLongList masterRows;
        private long remaining;
        private long ringCapacity;
        private long ringHead;
        private long ringRowCount;
        private long ringSize;
        private long ringTail;
        private long size;
        // The joined rows the last read of older master rows walked one by one, see remainingMasterRowCount().
        private long walkedRowCount;

        public LimitRecordCursor(LimitBounds bounds, @Nullable MasterSuffixLimitFunction masterSuffixLimit) {
            this.bounds = bounds;
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
                final long lo = bounds.getLo();
                if (masterSuffixLimit != null && masterSuffixLimit.isReadingLastRows(lo) && baseSize < -lo) {
                    readOlderMasterRows();
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

        /**
         * The join emitted fewer rows than the LIMIT selects over the last master rows the filter returns, so it goes
         * on over older master rows until it emits the {@code -lo} rows the LIMIT selects or runs out of master rows.
         * Each read of the filter returns master rows the join has not read. The join emits the rows of a master row
         * while its master cursor stands on that row, so the filter's cursor names the master row of each joined row,
         * and the cursor keeps the master rows whose joined rows are among the last {@code -lo}, starting with those
         * of the last master rows. The filter then returns just those master rows, and the join emits their rows,
         * which the bounds trim to the last {@code -lo}.
         */
        private void readOlderMasterRows() {
            final long rowCount = -bounds.getLo();
            final long ringCapacity = rowCount + 1;
            if (masterRows == null) {
                masterRows = new DirectLongList(rowCount, MemoryTag.NATIVE_DEFAULT);
            }
            if (masterRowRing == null) {
                masterRowRing = new DirectLongList(2 * ringCapacity, MemoryTag.NATIVE_DEFAULT);
            } else if (masterRowRing.getCapacity() < 2 * ringCapacity) {
                masterRowRing.setCapacity(2 * ringCapacity);
            }
            masterRows.clear();
            this.ringCapacity = ringCapacity;
            walkedRowCount = 0;
            long keptRowCount = 0;
            long missingRowCount = rowCount;
            boolean hasRows = baseSize > 0 || masterSuffixLimit.readOlderRows();
            while (hasRows) {
                isBaseAtFirstRow = false;
                base.toTop();
                ringHead = 0;
                ringTail = ringCapacity - 1;
                ringSize = 0;
                ringRowCount = 0;
                long rowId = -1;
                long rowIdCount = 0;
                boolean hasNext;
                do {
                    hasNext = base.hasNext();
                    final long nextRowId = hasNext ? masterSuffixLimit.getRowId() : -1;
                    if (nextRowId != rowId) {
                        circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                        keepMasterRow(rowId, rowIdCount, missingRowCount);
                        rowId = nextRowId;
                        rowIdCount = 0;
                    }
                    if (hasNext) {
                        walkedRowCount++;
                        rowIdCount++;
                        final long rowsLeft = base.remainingMasterRowCount();
                        if (rowsLeft > 0) {
                            counter.set(rowsLeft);
                            base.skipRows(counter, UNBOUNDED_ROW_COUNT);
                            rowIdCount += rowsLeft - counter.get();
                            counter.clear();
                        }
                    }
                } while (hasNext);
                for (long i = ringSize - 1; i > -1; i--) {
                    masterRows.add(masterRowRing.get(2 * ((ringHead + i) % ringCapacity)));
                }
                keptRowCount += ringRowCount;
                if (ringRowCount >= missingRowCount) {
                    break;
                }
                missingRowCount -= ringRowCount;
                hasRows = masterSuffixLimit.readOlderRows();
            }
            masterSuffixLimit.readRows(masterRows);
            baseSize = keptRowCount;
        }

        /**
         * Adds the master row and the count of its joined rows to the ring, then drops the oldest master rows while
         * the newer ones alone cover the missing rows.
         */
        private void keepMasterRow(long rowId, long rowIdCount, long missingRowCount) {
            if (rowIdCount == 0) {
                return;
            }
            ringTail = (ringTail + 1) % ringCapacity;
            masterRowRing.set(2 * ringTail, rowId);
            masterRowRing.set(2 * ringTail + 1, rowIdCount);
            ringSize++;
            ringRowCount += rowIdCount;
            while (ringRowCount - masterRowRing.get(2 * ringHead + 1) >= missingRowCount) {
                ringRowCount -= masterRowRing.get(2 * ringHead + 1);
                ringHead = (ringHead + 1) % ringCapacity;
                ringSize--;
            }
        }

        private void resolveBoundsCheap() {
            final long lo = bounds.getLo();
            final long hi = bounds.getHi();
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
            final long lo = bounds.getLo();
            final long hi = bounds.getHi();
            long startInclusive, endExclusive;
            if (lo < 0) {
                startInclusive = baseSize + lo;
                endExclusive = baseSize + hi;
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
            this.circuitBreaker = executionContext.getCircuitBreaker();

            bounds.init(base, executionContext);

            baseSize = -1;
            size = -1;
            baseRowsToSkip = -1;
            baseRowsToTake = -1;
            remaining = -1;
            isBaseAtFirstRow = false;
            counter.clear();
            final long lo = bounds.getLo();
            // A negative LIMIT over a master suffix resolves its bounds once the join runs.
            if (masterSuffixLimit == null || lo >= 0) {
                resolveBoundsCheap();
            }
            if (lo >= 0 && bounds.getHi() > lo) {
                base.expectLimitedIteration();
            }
        }
    }
}
