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
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
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
import io.questdb.std.Misc;
import io.questdb.std.Numbers;

/**
 * Copies the rows of its base into a record array as a cursor first reads them and replays the copy after every
 * rewind, so a parent that rewinds its input computes the base rows once per execution. If the copy reaches its
 * page budget or the query memory limit, it releases the copy and streams the base. A strict factory fails with a
 * limit error instead, for a parent that relies on the row ids of the copy.
 */
public class MaterializedRecordCursorFactory extends AbstractRecordCursorFactory {
    private RecordCursorFactory base;
    private MaterializedRecordCursor cursor;

    /**
     * Consumes the base on entry, including on failure.
     */
    public MaterializedRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            RecordSink recordSink,
            boolean isStrict
    ) {
        super(base.getMetadata());
        this.base = base;
        RecordArray rows = null;
        try {
            // The copy holds what a full-fat join's value pages may hold and grows from the light join's
            // value page, so a small base costs little memory.
            final long pageSize = configuration.getSqlHashJoinValuePageSize();
            final long maxBytes = Numbers.ceilPow2(pageSize) * configuration.getSqlHashJoinValueMaxPages();
            rows = new CachedRecordArray(
                    base.getMetadata(),
                    recordSink,
                    Math.min(configuration.getSqlHashJoinLightValuePageSize(), pageSize),
                    maxBytes,
                    isStrict
            );
            cursor = new MaterializedRecordCursor(rows, maxBytes, isStrict);
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

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
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
        final MaterializedRecordCursor cursor = this.cursor;
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
     * Memory that grows as {@link MemoryCARWImpl} does and fails with a {@link CacheLimitException} when it would
     * exceed {@code maxBytes}, or, unless strict, when memory runs out.
     */
    private static class CacheMemory extends MemoryCARWImpl {
        private final boolean isStrict;
        private final CacheLimitException limitExceeded = new CacheLimitException();
        private final long maxBytes;

        private CacheMemory(long pageSize, long maxBytes, boolean isStrict) {
            super(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_RECORD_CHAIN);
            this.maxBytes = maxBytes;
            this.isStrict = isStrict;
        }

        @Override
        protected long reallocateMemory(long currentBaseAddress, long currentSize, long newSize) {
            if (newSize > maxBytes) {
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
            super(
                    columnTypes,
                    recordSink,
                    new CacheMemory(pageSize, maxBytes, isStrict),
                    new CacheMemory(pageSize >> 4, maxBytes >> 4, isStrict)
            );
        }
    }

    /**
     * Joins bind {@link #getRecord()} once per cursor; the record delegates to the copy or, after the fallback,
     * to the base record, so the parent needs no rebinding.
     */
    private static class MaterializedRecordCursor implements DelegatingRecordCursor, NoRandomAccessRecordCursor {
        private final boolean isStrict;
        private final long maxBytes;
        private final DelegatingRecord record = new DelegatingRecord();
        private final RecordArray rows;
        private RecordCursor baseCursor;
        private Record baseRecord;
        private boolean isBaseExhausted;
        private boolean isStreaming;

        private MaterializedRecordCursor(RecordArray rows, long maxBytes, boolean isStrict) {
            this.rows = rows;
            this.maxBytes = maxBytes;
            this.isStrict = isStrict;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            if (isStreaming) {
                baseCursor.calculateSize(circuitBreaker, counter);
                return;
            }
            rows.calculateSize(circuitBreaker, counter);
            while (copyNext()) {
                if (circuitBreaker != null) {
                    circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                }
                counter.inc();
                if (isStreaming) {
                    baseCursor.calculateSize(circuitBreaker, counter);
                    return;
                }
                rows.hasNext();
            }
        }

        @Override
        public void close() {
            final RecordCursor baseCursor = this.baseCursor;
            this.baseCursor = null;
            baseRecord = null;
            record.of(null);
            Throwable failure = Misc.freeBestEffort(null, baseCursor);
            failure = Misc.freeBestEffort(failure, rows);
            CairoException.rethrowCleanupFailure(failure);
        }

        @Override
        public Record getRecord() {
            return record;
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
            record.of(rows.getRecord());
            isBaseExhausted = false;
            isStreaming = false;
            this.baseCursor = baseCursor;
        }

        @Override
        public long preComputedStateSize() {
            return isStreaming ? baseCursor.preComputedStateSize() : rows.size();
        }

        @Override
        public long size() {
            return baseCursor.size();
        }

        @Override
        public void skipRows(Counter rowCount, long maxRowsAfterSkip) {
            while (!isStreaming && rowCount.get() > 0 && hasNext()) {
                rowCount.dec();
            }
            if (isStreaming) {
                baseCursor.skipRows(rowCount, maxRowsAfterSkip);
            }
        }

        @Override
        public void toTop() {
            if (isStreaming) {
                baseCursor.toTop();
            } else {
                rows.toTop();
            }
        }

        private boolean copyNext() {
            if (isBaseExhausted || !baseCursor.hasNext()) {
                isBaseExhausted = true;
                return false;
            }
            try {
                rows.put(baseRecord);
            } catch (CacheLimitException e) {
                if (isStrict) {
                    throw LimitOverflowException.instance()
                            .put("materialized rows exceed the memory limit [maxBytes=").put(maxBytes)
                            .put("] (raise ").put(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_MAX_PAGES.getPropertyPath()).put(')');
                }
                rows.clear();
                record.of(baseRecord);
                isStreaming = true;
            }
            return true;
        }

        private boolean hasNextFromBase() {
            if (isStreaming) {
                return baseCursor.hasNext();
            }
            return copyNext() && (isStreaming || rows.hasNext());
        }
    }
}
