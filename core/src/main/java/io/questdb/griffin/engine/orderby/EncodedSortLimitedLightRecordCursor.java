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

package io.questdb.griffin.engine.orderby;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.DelegatingRecordCursor;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

class EncodedSortLimitedLightRecordCursor implements DelegatingRecordCursor {
    private static final long MAX_HEAP_SIZE_LIMIT = (Integer.toUnsignedLong(-1) - 1) << 3;
    protected final IntHashSet buildReadColumns;
    private final SortKeyEncoder encoder;
    private final DirectLongList entryMem;
    private final long maxEntryMemBytes;
    private final long parallelThreshold;
    protected RecordCursor baseCursor;
    protected Record baseRecord;
    protected SqlExecutionCircuitBreaker circuitBreaker;
    protected long limit;
    private long count;
    private long currentAddr;
    private long endAddr;
    private int entrySize;
    private boolean isBuilt;
    private boolean isFirstN;
    private boolean isOpen;
    private int keyLongs;
    private int longsPerEntry;
    private long maxEntries;
    private int rowIdOffset;
    private long rowsLeft;
    private long skipFirst;
    private long skipLast;

    EncodedSortLimitedLightRecordCursor(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            IntList sortColumnFilter
    ) {
        SortKeyEncoder encoderInit = null;
        DirectLongList entryMemInit = null;
        try {
            encoderInit = new SortKeyEncoder(metadata, sortColumnFilter);
            entryMemInit = new DirectLongList(16 * 1024, MemoryTag.NATIVE_DEFAULT, true);
        } catch (Throwable th) {
            Misc.free(encoderInit);
            Misc.free(entryMemInit);
            throw th;
        }
        this.encoder = encoderInit;
        this.entryMem = entryMemInit;
        this.buildReadColumns = extractSortKeyColumnIndexes(sortColumnFilter);
        final long keyCap = Math.min(configuration.getSqlSortKeyMaxBytes(), MAX_HEAP_SIZE_LIMIT);
        final long valueCap = Math.min(configuration.getSqlSortLightValueMaxBytes(), MAX_HEAP_SIZE_LIMIT);
        this.maxEntryMemBytes = Math.min(keyCap + valueCap, MAX_HEAP_SIZE_LIMIT);
        this.parallelThreshold = configuration.getSqlSortEncodedParallelThreshold();
        this.isOpen = true;
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            Misc.free(entryMem);
            Misc.free(encoder);
            baseCursor = Misc.free(baseCursor);
            baseRecord = null;
        }
    }

    @Override
    public Record getRecord() {
        return baseRecord;
    }

    @Override
    public Record getRecordB() {
        return baseCursor.getRecordB();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return baseCursor.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (!isBuilt) {
            buildAndSort();
            isBuilt = true;
        }
        if (rowsLeft <= 0 || currentAddr >= endAddr) {
            return false;
        }
        circuitBreaker.statefulThrowExceptionIfTripped();
        final long rowId = Unsafe.getLong(currentAddr);
        currentAddr += entrySize;
        rowsLeft--;
        baseCursor.recordAt(baseRecord, rowId);
        return true;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return baseCursor.newSymbolTable(columnIndex);
    }

    @Override
    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        this.baseCursor = baseCursor;
        this.baseRecord = baseCursor.getRecord();
        if (!isOpen) {
            isOpen = true;
            entryMem.reopen();
        }
        baseCursor.setParentUsedColumns(buildReadColumns);
        final SortKeyType keyType = encoder.init(baseCursor);
        assert keyType != SortKeyType.UNSUPPORTED;
        entrySize = keyType.entrySize();
        rowIdOffset = keyType.rowIdOffset();
        keyLongs = keyType.keyLength() / Long.BYTES;
        longsPerEntry = entrySize / Long.BYTES;
        maxEntries = maxEntryMemBytes / entrySize;
        circuitBreaker = executionContext.getCircuitBreaker();
        isBuilt = false;
        count = 0;
        entryMem.clear();
    }

    @Override
    public long preComputedStateSize() {
        return RecordCursor.fromBool(isBuilt) + baseCursor.preComputedStateSize();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        baseCursor.recordAt(record, atRowId);
    }

    @Override
    public void setParquetDecodeHint(ParquetDecodeHint hint) {
        baseCursor.setParquetDecodeHint(hint);
    }

    public void setSelection(boolean isFirstN, long limit, long skipFirst, long skipLast) {
        this.isFirstN = isFirstN;
        // A NULL lo/hi (Numbers.LONG_NULL == Long.MIN_VALUE) survives the negation in
        // computeLimits() as a negative limit. LimitedSizeLongTreeChain treats negative
        // limits as unbounded, so map them to Long.MAX_VALUE; clamping to 0 would return
        // an empty result instead of the full sorted set.
        this.limit = limit < 0 ? Long.MAX_VALUE : limit;
        this.skipFirst = Math.max(skipFirst, 0);
        this.skipLast = Math.max(skipLast, 0);
    }

    @Override
    public long size() {
        return isBuilt ? emitCount() : -1;
    }

    @Override
    public void toTop() {
        final long sliceStart = isFirstN ? 0 : Math.max(count - limit, 0);
        final long sliceEnd = isFirstN ? Math.min(limit, count) : count;
        final long start = Math.min(sliceStart + skipFirst, sliceEnd);
        final long end = Math.max(sliceEnd - skipLast, start);
        currentAddr = entryMem.getAddress() + rowIdOffset + start * entrySize;
        endAddr = entryMem.getAddress() + rowIdOffset + end * entrySize;
        rowsLeft = end - start;
    }

    private static IntHashSet extractSortKeyColumnIndexes(IntList sortColumnFilter) {
        final IntHashSet indexes = new IntHashSet(sortColumnFilter.size());
        for (int i = 0, n = sortColumnFilter.size(); i < n; i++) {
            final int encoded = sortColumnFilter.getQuick(i);
            indexes.add((encoded > 0 ? encoded : -encoded) - 1);
        }
        return indexes;
    }

    private void buildAndSort() {
        try {
            if (limit > 0) {
                runBuild();
            }
        } finally {
            Misc.free(encoder);
        }
        if (count > 1) {
            Vect.sortEncodedEntries(entryMem.getAddress(), count, keyLongs, parallelThreshold);
            circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
        }
        toTop();
    }

    private long emitCount() {
        final long sliceStart = isFirstN ? 0 : Math.max(count - limit, 0);
        final long sliceEnd = isFirstN ? Math.min(limit, count) : count;
        return Math.max(sliceEnd - sliceStart - skipFirst - skipLast, 0);
    }

    private void throwLimitOverflow() {
        throw LimitOverflowException.instance()
                .put("limit of ").put(maxEntryMemBytes)
                .put(" memory exceeded in EncodedSort (raise ")
                .put(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath())
                .put(" or ")
                .put(PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath())
                .put(')');
    }

    protected final void encodeAndAppendCurrentRow() {
        if (count >= maxEntries) {
            throwLimitOverflow();
        }
        entryMem.ensureCapacity(longsPerEntry);
        encoder.encode(baseRecord, entryMem.getAppendAddress(), baseRecord.getRowId());
        entryMem.skip(longsPerEntry);
        count++;
    }

    protected void runBuild() {
        while (baseCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            encodeAndAppendCurrentRow();
        }
    }
}
