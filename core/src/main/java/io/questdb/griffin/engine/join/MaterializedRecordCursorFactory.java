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

package io.questdb.griffin.engine.join;

import io.questdb.PropertyKey;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordArray;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.DelegatingRecord;
import io.questdb.cairo.sql.DelegatingRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Copies the rows of its base into a record array as a cursor first reads them and replays the copy after every
 * rewind, so a parent that rewinds its input computes the base rows once per execution. A strict factory gives random
 * access to the copied rows to a parent that relies on their row ids, fails with a limit error when the copy does not
 * fit, and can copy rows in batches ahead of the reads, for a parent that reads every row. A lax factory only saves
 * work: when the copy does not fit, or another allocation of the query needs its memory, it frees the copy and streams
 * the base, see {@link LaxRecordCursor}.
 */
public class MaterializedRecordCursorFactory extends AbstractRecordCursorFactory {
    private final boolean isStrict;
    private RecordCursorFactory base;
    private DelegatingRecordCursor cursor;

    /**
     * Consumes the base on entry, including on failure. A copy with the join budget holds what a full-fat join's
     * value pages may hold; one without it stands in for the storage of a stream GROUP BY and, like the map of a
     * hash GROUP BY, only the query memory limit bounds it.
     */
    public MaterializedRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            RecordSink recordSink,
            boolean isStrict,
            boolean hasJoinBudget,
            boolean isCopiedInBatches
    ) {
        super(base.getMetadata());
        this.base = base;
        this.isStrict = isStrict;
        RecordArray rows = null;
        try {
            // The copy grows from the light join's value page, so a small base costs little memory.
            final long pageSize = configuration.getSqlHashJoinValuePageSize();
            final long maxBytes = hasJoinBudget ? Numbers.ceilPow2(pageSize) * configuration.getSqlHashJoinValueMaxPages() : Long.MAX_VALUE;
            rows = new CachedRecordArray(
                    base.getMetadata(),
                    recordSink,
                    Math.min(configuration.getSqlHashJoinLightValuePageSize(), pageSize),
                    maxBytes,
                    isStrict
            );
            cursor = isStrict ? new MaterializedRecordCursor(rows, maxBytes, isCopiedInBatches) : new LaxRecordCursor(rows);
        } catch (Throwable th) {
            Misc.free(rows, th);
            Misc.free(this, th);
            throw th;
        }
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
            Misc.free(baseCursor, th);
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public boolean isNonDeterministic() {
        return base.isNonDeterministic();
    }

    // A strict copy replays its rows on every rewind. A lax one streams its base once it frees its copy, so a rewind
    // then runs the base again.
    @Override
    public boolean isRewindRecomputing() {
        return !isStrict && base.isRewindRecomputing();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return isStrict;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Materialize");
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
        final RecordCursorFactory base = this.base;
        this.base = null;
        final DelegatingRecordCursor cursor = this.cursor;
        this.cursor = null;
        Throwable failure = Misc.freeBestEffort(null, base);
        failure = Misc.freeBestEffort(failure, cursor);
        CairoException.rethrowCleanupFailure(failure);
    }

    private static class CacheLimitException extends CairoException {
        private CacheLimitException() {
            setOutOfMemory(true);
        }
    }

    /**
     * Memory that grows as {@link MemoryCARWImpl} does and fails with a {@link CacheLimitException} when it and its
     * sibling would together exceed {@code maxBytes}, or, unless strict, when memory runs out.
     */
    private static class CacheMemory extends MemoryCARWImpl {
        private final boolean isStrict;
        private final CacheLimitException limitExceeded = new CacheLimitException();
        private final long maxBytes;
        private CacheMemory sibling;

        private CacheMemory(long pageSize, long maxBytes, boolean isStrict, @Nullable CacheMemory sibling) {
            super(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_RECORD_CHAIN);
            this.maxBytes = maxBytes;
            this.isStrict = isStrict;
            if (sibling != null) {
                this.sibling = sibling;
                sibling.sibling = this;
            }
        }

        @Override
        protected long reallocateMemory(long currentBaseAddress, long currentSize, long newSize) {
            if (newSize + sibling.size() > maxBytes) {
                // The allocator retries an exact page count before giving up its speculative doubling.
                throw limitExceeded;
            }
            try {
                return super.reallocateMemory(currentBaseAddress, currentSize, newSize);
            } catch (CairoException e) {
                if (e.isOutOfMemory() && !isStrict) {
                    throw limitExceeded;
                }
                throw e;
            }
        }
    }

    private static class CachedRecordArray extends RecordArray {
        private CachedRecordArray(ColumnTypes columnTypes, RecordSink recordSink, long pageSize, long maxBytes, boolean isStrict) {
            this(columnTypes, recordSink, new CacheMemory(Math.max(pageSize >> 4, Long.BYTES), maxBytes, isStrict, null), pageSize, maxBytes, isStrict);
        }

        private CachedRecordArray(ColumnTypes columnTypes, RecordSink recordSink, CacheMemory offsets, long pageSize, long maxBytes, boolean isStrict) {
            super(columnTypes, recordSink, new CacheMemory(pageSize, maxBytes, isStrict, offsets), offsets);
        }
    }

    /**
     * Streams its base on the first pass, copying each row as a side effect, and replays the copy after a rewind, so a
     * parent that reads its input twice, such as a negative LIMIT or a keyed FILL, computes the base once. The parent
     * reads the base's record on the first pass and never the copy, so any thread may free the copy then: while the
     * cursor copies, it is a {@link MemoryTracker.Reclaimer} of the query's tracker, and an allocation that would pass a
     * memory limit frees the copy before it fails. Once the copy is freed or reaches its page budget, the cursor streams
     * the base, and a rewind runs the base again.
     */
    private static class LaxRecordCursor implements DelegatingRecordCursor, MemoryTracker.Reclaimer {
        // The cursor reads the base and copies its rows; a thread that runs out of memory may free the copy.
        private static final int STATE_COPYING = 0;
        // The cursor reads the copy, which no thread may free meanwhile, and then the base again, unless the copy holds
        // every row of the base.
        private static final int STATE_REPLAYING = 1;
        // The cursor reads the base and holds no copy.
        private static final int STATE_STREAMING = 2;
        // The thread that copies a row or frees the copy, or null. A spin lock, since either holds it only briefly and
        // the owner must not hold a monitor in a fiber.
        private final AtomicReference<Thread> copyHolder = new AtomicReference<>();
        private final DelegatingRecord record = new DelegatingRecord();
        private final RecordArray rows;
        private RecordCursor baseCursor;
        private Record baseRecord;
        private boolean isBaseExhausted;
        // Whether the cursor is a reclaimer of memoryTracker.
        private boolean isReclaimable;
        private @Nullable MemoryTracker memoryTracker;
        // A thread that frees the copy writes it under copyHolder, as the owner does while the cursor is a reclaimer.
        // The owner reads it without the lock to choose its path, and again under the lock before it touches the copy.
        private volatile int state;

        private LaxRecordCursor(RecordArray rows) {
            this.rows = rows;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            if (state == STATE_REPLAYING) {
                rows.calculateSize(circuitBreaker, counter);
                if (isBaseExhausted) {
                    return;
                }
                copyAgain();
            }
            while (state == STATE_COPYING) {
                if (!copyNext()) {
                    return;
                }
                if (circuitBreaker != null) {
                    circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                }
                counter.inc();
            }
            stopReclaims();
            baseCursor.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            final RecordCursor baseCursor = this.baseCursor;
            this.baseCursor = null;
            baseRecord = null;
            record.of(null);
            stopReclaims();
            memoryTracker = null;
            Throwable failure = Misc.freeBestEffort(null, baseCursor);
            failure = Misc.freeBestEffort(failure, rows);
            CairoException.rethrowCleanupFailure(failure);
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
            if (state == STATE_REPLAYING) {
                if (rows.hasNext()) {
                    return true;
                }
                if (isBaseExhausted) {
                    return false;
                }
                copyAgain();
            }
            if (state == STATE_COPYING) {
                return copyNext();
            }
            // A thread may have freed the copy since the last row.
            stopReclaims();
            return baseCursor.hasNext();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
        }

        @Override
        public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) {
            memoryTracker = executionContext.getMemoryTracker();
            rows.clear();
            rows.setMemoryTracker(memoryTracker);
            rows.setSymbolTableResolver(baseCursor);
            this.baseCursor = baseCursor;
            baseRecord = baseCursor.getRecord();
            record.of(baseRecord);
            isBaseExhausted = false;
            state = STATE_COPYING;
            startReclaims();
        }

        @Override
        public long preComputedStateSize() {
            return state == STATE_STREAMING ? baseCursor.preComputedStateSize() : rows.size();
        }

        // Frees the copy for a thread whose allocation would pass a memory limit. The owner that copies a row frees the
        // copy itself when the row passes the limit, see copyNext(), so it skips the copy here.
        @Override
        public void reclaim() {
            final Thread thread = Thread.currentThread();
            if (copyHolder.get() == thread) {
                return;
            }
            lockCopy(thread);
            try {
                if (state == STATE_COPYING) {
                    freeCopy();
                }
            } finally {
                copyHolder.set(null);
            }
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            return baseCursor.size();
        }

        @Override
        public void skipRows(Counter rowCount, long maxRowsAfterSkip) {
            while (state != STATE_STREAMING && rowCount.get() > 0 && hasNext()) {
                rowCount.dec();
            }
            if (state == STATE_STREAMING) {
                stopReclaims();
                baseCursor.skipRows(rowCount, maxRowsAfterSkip);
            }
        }

        @Override
        public void toTop() {
            stopReclaims();
            if (state == STATE_STREAMING) {
                baseCursor.toTop();
                isBaseExhausted = false;
            } else {
                // The copied rows first, then the base from where it stopped, see hasNext().
                state = STATE_REPLAYING;
                rows.toTop();
                record.of(rows.getRecord());
            }
        }

        // Goes on copying the base after a replay of the rows copied so far.
        private void copyAgain() {
            state = STATE_COPYING;
            record.of(baseRecord);
            startReclaims();
        }

        // Reads the next base row and copies it, unless a thread freed the copy or the row does not fit, when the cursor
        // streams from then on.
        private boolean copyNext() {
            if (!baseCursor.hasNext()) {
                isBaseExhausted = true;
                stopReclaims();
                return false;
            }
            final Thread thread = Thread.currentThread();
            lockCopy(thread);
            try {
                if (state == STATE_COPYING) {
                    rows.put(baseRecord);
                }
            } catch (CacheLimitException e) {
                freeCopy();
            } finally {
                copyHolder.set(null);
            }
            if (state == STATE_STREAMING) {
                stopReclaims();
            }
            return true;
        }

        // Frees the copy under copyHolder. A thread that waits for the lock to free the copy itself then tries its
        // allocation again, so the free counts before the lock goes: a Resource Group tracker sees it once published.
        private void freeCopy() {
            rows.clear();
            MemoryTracker.publishResourceMemoryCurrentThread();
            state = STATE_STREAMING;
        }

        private void lockCopy(Thread thread) {
            while (!copyHolder.compareAndSet(null, thread)) {
                Os.pause();
            }
        }

        private void startReclaims() {
            assert !isReclaimable;
            isReclaimable = memoryTracker != null && memoryTracker.addReclaimer(this);
        }

        // No thread frees the copy once this returns: one that is freeing it finishes first.
        private void stopReclaims() {
            if (isReclaimable) {
                isReclaimable = false;
                memoryTracker.removeReclaimer(this);
                lockCopy(Thread.currentThread());
                copyHolder.set(null);
            }
        }
    }

    /**
     * Copies each base row as the parent first reads it, the parent reading the copy, and replays the copy after every
     * rewind. A copy past its page budget fails the query, as one past the memory limit does.
     */
    private static class MaterializedRecordCursor implements DelegatingRecordCursor {
        private static final int COPY_BATCH_ROWS = 256;
        private final boolean isCopiedInBatches;
        private final long maxBytes;
        private final RecordArray rows;
        private RecordCursor baseCursor;
        private Record baseRecord;
        private boolean isBaseExhausted;

        private MaterializedRecordCursor(RecordArray rows, long maxBytes, boolean isCopiedInBatches) {
            this.rows = rows;
            this.maxBytes = maxBytes;
            this.isCopiedInBatches = isCopiedInBatches;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            rows.calculateSize(circuitBreaker, counter);
            while (copyNext()) {
                if (circuitBreaker != null) {
                    circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                }
                counter.inc();
                rows.hasNext();
            }
        }

        @Override
        public void close() {
            final RecordCursor baseCursor = this.baseCursor;
            this.baseCursor = null;
            baseRecord = null;
            Throwable failure = Misc.freeBestEffort(null, baseCursor);
            failure = Misc.freeBestEffort(failure, rows);
            CairoException.rethrowCleanupFailure(failure);
        }

        @Override
        public Record getRecord() {
            return rows.getRecord();
        }

        @Override
        public Record getRecordB() {
            return rows.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            return rows.hasNext() || hasNextFromBase();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
        }

        @Override
        public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) {
            rows.clear();
            rows.setMemoryTracker(executionContext.getMemoryTracker());
            rows.setSymbolTableResolver(baseCursor);
            baseRecord = baseCursor.getRecord();
            isBaseExhausted = false;
            this.baseCursor = baseCursor;
        }

        @Override
        public long preComputedStateSize() {
            return rows.size();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            rows.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            return baseCursor.size();
        }

        @Override
        public void skipRows(Counter rowCount, long maxRowsAfterSkip) {
            while (rowCount.get() > 0 && hasNext()) {
                rowCount.dec();
            }
        }

        @Override
        public void toTop() {
            rows.toTop();
        }

        private boolean copyNext() {
            if (isBaseExhausted || !baseCursor.hasNext()) {
                isBaseExhausted = true;
                return false;
            }
            try {
                rows.put(baseRecord);
            } catch (CacheLimitException e) {
                throw LimitOverflowException.instance()
                        .put("materialized rows exceed the memory limit [maxBytes=").put(maxBytes)
                        .put("] (raise ").put(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_MAX_PAGES.getPropertyPath()).put(')');
            }
            return true;
        }

        private boolean hasNextFromBase() {
            if (isCopiedInBatches) {
                //noinspection StatementWithEmptyBody
                for (int i = 0; i < COPY_BATCH_ROWS && copyNext(); i++) {
                }
                return rows.hasNext();
            }
            if (!copyNext()) {
                return false;
            }
            // Positions the record at the row that copyNext() appended.
            final boolean hasRow = rows.hasNext();
            assert hasRow;
            return true;
        }
    }
}
