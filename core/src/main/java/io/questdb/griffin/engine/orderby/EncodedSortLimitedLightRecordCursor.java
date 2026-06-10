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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.DelegatingRecordCursor;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * Encoded-key top-K cursor. The build phase encodes each scanned row's sort key
 * into a flat native buffer and never calls {@code recordAt} on the base cursor;
 * once a key threshold is known, non-qualifying rows are rejected with a native
 * key compare and the buffer is kept at O(limit) by sort-and-truncate compaction.
 * After the scan, the surviving entries are sorted natively and the requested
 * slice is emitted through {@code recordAt} in sort order.
 * <p>
 * For a FIRST N selection over a base presorted by its designated timestamp,
 * the scan additionally stops once a completed timestamp group pushes the
 * cumulative row count past the limit.
 */
class EncodedSortLimitedLightRecordCursor implements DelegatingRecordCursor, RecordCursor.RowIdSource {
    // Compacting every `limit` rows would sort tiny batches for small limits;
    // batch at least this many entries between compactions.
    private static final long MIN_COMPACTION_TRIGGER = 4096;
    private final IntHashSet buildReadColumns;
    private final SortKeyEncoder encoder;
    private final DirectLongList entryMem;
    private final long keyCapBytes;
    private final long parallelThreshold;
    // A copy, not a buffer address: entryMem may reallocate on growth.
    private final long[] thresholdEntry = new long[SortKeyType.MAX_ENTRY_LONGS];
    private final int timestampIndex;
    private final long valueCapBytes;
    private RecordCursor baseCursor;
    private Record baseRecord;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private long compactionTrigger;
    private long count;
    private long currentAddr;
    private long emitEndAddr;
    private long emitStartAddr;
    private int entrySize;
    private boolean hasThreshold;
    private boolean isBuilt;
    private boolean isEarlyStopEnabled;
    private boolean isFirstN;
    private boolean isOpen;
    private int keyLongs;
    private long limit;
    private int longsPerEntry;
    private long maxEntries;
    private long maxEntryMemBytes;
    private int rowIdOffset;
    private long skipFirst;
    private long skipLast;

    EncodedSortLimitedLightRecordCursor(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            IntList sortColumnFilter,
            int timestampIndex
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
        this.timestampIndex = timestampIndex;
        this.buildReadColumns = SortKeyEncoder.extractSortKeyColumnIndexes(sortColumnFilter);
        this.keyCapBytes = configuration.getSqlSortKeyMaxBytes();
        this.valueCapBytes = configuration.getSqlSortLightValueMaxBytes();
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
    public void copyParquetRowIdsTo(DirectLongList target, PageFrameAddressCache addressCache) {
        target.ensureCapacity((emitEndAddr - emitStartAddr) / entrySize);
        for (long addr = emitStartAddr; addr < emitEndAddr; addr += entrySize) {
            final long rowId = Unsafe.getLong(addr);
            if (addressCache.getFrameFormat(Rows.toPartitionIndex(rowId)) == PartitionFormat.PARQUET) {
                target.add(rowId);
            }
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
        if (currentAddr >= emitEndAddr) {
            return false;
        }
        circuitBreaker.statefulThrowExceptionIfTripped();
        final long rowId = Unsafe.getLong(currentAddr);
        currentAddr += entrySize;
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
        if (isEarlyStopEnabled) {
            baseCursor.expectLimitedIteration();
        }
        baseCursor.setParentUsedColumns(buildReadColumns);
        final SortKeyType keyType = encoder.init(baseCursor);
        assert keyType != SortKeyType.UNSUPPORTED;
        entrySize = keyType.entrySize();
        rowIdOffset = keyType.rowIdOffset();
        keyLongs = keyType.keyLength() / Long.BYTES;
        longsPerEntry = entrySize / Long.BYTES;
        maxEntries = SortKeyEncoder.maxEntries(keyCapBytes, valueCapBytes, keyType);
        maxEntryMemBytes = maxEntries * entrySize;
        // The trigger stays within maxEntries so compaction fires before the overflow check can.
        compactionTrigger = limit > 0 && limit < maxEntries
                ? Math.min(Math.max(limit << 1, MIN_COMPACTION_TRIGGER), maxEntries)
                : Long.MAX_VALUE;
        hasThreshold = false;
        circuitBreaker = executionContext.getCircuitBreaker();
        isBuilt = false;
        count = 0;
        currentAddr = 0;
        emitStartAddr = 0;
        emitEndAddr = 0;
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
        // the factory as a negative limit. LimitedSizeLongTreeChain treats negative
        // limits as unbounded, so map them to Long.MAX_VALUE; clamping to 0 would
        // return an empty result instead of the full sorted set.
        this.limit = limit < 0 ? Long.MAX_VALUE : limit;
        this.skipFirst = Math.max(skipFirst, 0);
        this.skipLast = Math.max(skipLast, 0);
        this.isEarlyStopEnabled = isFirstN && timestampIndex != -1;
    }

    @Override
    public long size() {
        return isBuilt ? (emitEndAddr - emitStartAddr) / entrySize : -1;
    }

    @Override
    public void toTop() {
        currentAddr = emitStartAddr;
    }

    private void buildAndSort() {
        entryMem.clear();
        count = 0;
        hasThreshold = false;
        if (limit > 0) {
            if (limit == Long.MAX_VALUE) {
                // Unbounded build keeps every scanned row, so pre-size the buffer the
                // way EncodedSortLightRecordCursor does instead of growing per row.
                final long estimatedSize = baseCursor.size();
                if (estimatedSize > 0) {
                    if (estimatedSize > maxEntries) {
                        SortKeyEncoder.throwSortHeapOverflow(maxEntryMemBytes);
                    }
                    entryMem.setCapacity(estimatedSize * longsPerEntry);
                }
            }
            runBuild();
        }
        if (count > 1) {
            Vect.sortEncodedEntries(entryMem.getAddress(), count, keyLongs, parallelThreshold);
            circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
        }
        computeEmitWindow();
        toTop();
        if (emitStartAddr < emitEndAddr) {
            baseCursor.setRecordAtRows(this);
        }
        // No finally: a retry after a mid-build throw must not see freed rank maps.
        Misc.free(encoder);
    }

    /**
     * Sorts the buffer, keeps the {@code limit} entries the emit window selects,
     * and captures the boundary entry so that subsequent rows can be rejected
     * with a key compare instead of growing the buffer. This bounds build memory
     * at O(limit) instead of O(scanned rows).
     */
    private void compact() {
        Vect.sortEncodedEntries(entryMem.getAddress(), count, keyLongs, parallelThreshold);
        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
        if (!isFirstN) {
            // LAST N keeps the tail of the sorted buffer.
            Vect.memmove(entryMem.getAddress(), entryMem.getAddress() + (count - limit) * entrySize, limit * entrySize);
        }
        count = limit;
        entryMem.setPos(limit * longsPerEntry);
        final long boundaryAddr = entryMem.getAddress() + (isFirstN ? limit - 1 : 0) * entrySize;
        for (int i = 0; i < longsPerEntry; i++) {
            thresholdEntry[i] = Unsafe.getLong(boundaryAddr + 8L * i);
        }
        hasThreshold = true;
    }

    private void computeEmitWindow() {
        // The slice the LIMIT selects before skips: head for FIRST N, tail for LAST N.
        final long sliceStart = isFirstN ? 0 : Math.max(count - limit, 0);
        final long sliceEnd = isFirstN ? Math.min(limit, count) : count;
        // Stepwise min/max keeps the bounds exact when the skips carry large
        // user-supplied values; a plain subtraction chain can wrap.
        final long start = Math.min(sliceStart + skipFirst, sliceEnd);
        final long end = Math.max(sliceEnd - skipLast, start);
        emitStartAddr = entryMem.getAddress() + rowIdOffset + start * entrySize;
        emitEndAddr = entryMem.getAddress() + rowIdOffset + end * entrySize;
    }

    // Entries are unique by their trailing rowId word, so word-wise unsigned
    // comparison is a strict total order matching Vect.sortEncodedEntries.
    private boolean isBeyondThreshold(long entryAddr) {
        for (int i = 0; i < longsPerEntry; i++) {
            final int cmp = Long.compareUnsigned(Unsafe.getLong(entryAddr + 8L * i), thresholdEntry[i]);
            if (cmp != 0) {
                return isFirstN ? cmp > 0 : cmp < 0;
            }
        }
        return true;
    }

    private void runBuild() {
        if (isEarlyStopEnabled) {
            runBuildTimestampEarlyStop();
            return;
        }
        while (baseCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            tryAppendCurrentRow();
        }
    }

    /**
     * The base is presorted by its designated timestamp and the selection is
     * FIRST N, so once a completed timestamp group pushes the cumulative row
     * count past the limit, every further row carries a strictly larger leading
     * key than every collected row and cannot enter the top-K slice.
     */
    private void runBuildTimestampEarlyStop() {
        if (!baseCursor.hasNext()) {
            return;
        }
        tryAppendCurrentRow();
        long groupTimestamp = baseRecord.getTimestamp(timestampIndex);
        long rowsInGroup = 1;
        long rowsSoFar = 0;
        while (baseCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            final long currentTimestamp = baseRecord.getTimestamp(timestampIndex);
            if (groupTimestamp == currentTimestamp) {
                rowsInGroup++;
            } else {
                rowsSoFar += rowsInGroup;
                if (rowsSoFar > limit) {
                    return;
                }
                groupTimestamp = currentTimestamp;
                rowsInGroup = 1;
            }
            tryAppendCurrentRow();
        }
    }

    private void tryAppendCurrentRow() {
        if (count >= maxEntries) {
            SortKeyEncoder.throwSortHeapOverflow(maxEntryMemBytes);
        }
        entryMem.ensureCapacity(longsPerEntry);
        final long addr = entryMem.getAppendAddress();
        encoder.encode(baseRecord, addr, baseRecord.getRowId());
        if (hasThreshold && isBeyondThreshold(addr)) {
            // The next candidate overwrites the rejected entry.
            return;
        }
        entryMem.skip(longsPerEntry);
        if (++count == compactionTrigger) {
            compact();
        }
    }
}
