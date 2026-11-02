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
import io.questdb.cairo.RecordArray;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.DelegatingRecordCursor;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

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
 * cumulative row count past the limit. When the base scans its timestamp
 * against the order, or the leading key word of the copied rows never
 * increases, the newest groups of equal leading key that cover the limit
 * hold the top-K: the copying build keeps only those groups and encodes them
 * once the scan ends. A row that breaks that order hands the kept rows, itself
 * included, and the rest of the scan to the regular copying build.
 * <p>
 * Given a record sink, the cursor serves a base without random access: it copies
 * each row the buffer keeps into a record array and encodes the row's index in
 * that array instead of the base rowId, so a row the threshold rejects is never
 * copied. Once the copies reach four times the limit, the next compaction of the
 * buffer re-stores the surviving rows in entry order and renumbers the entries,
 * which keeps the copies at O(limit) rows as well. Emit reads the copies back and
 * never re-reads the base.
 */
class EncodedSortLimitedLightRecordCursor implements DelegatingRecordCursor, RecordCursor.RowIdSource {
    private static final long MIN_TAIL_ROWS = 4096;
    private final IntHashSet buildReadColumns;
    private final SortKeyEncoder encoder;
    private final EncodedTopKBuffer entries;
    private final boolean isScanAgainstOrder;
    // Copies of the kept rows when the base lacks random access, null otherwise.
    private final RecordArray records;
    // Compaction re-stores the surviving rows here, then copies them back to records.
    private final RecordArray spareRecords;
    private final long[] tailKeyWords = new long[4];
    private final int timestampIndex;
    private RecordCursor baseCursor;
    private Record baseRecord;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private long currentAddr;
    private long emitEndAddr;
    private long emitStartAddr;
    private int entrySize;
    private boolean isBuilt;
    private boolean isEarlyStopEnabled;
    private boolean isFirstN;
    private boolean isOpen;
    private MemoryCARW keyHeap;
    private int keyLongs;
    private long limit;
    private int rowIdOffset;
    private long skipFirst;
    private long skipLast;
    private long tailKeyLen;

    EncodedSortLimitedLightRecordCursor(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            IntList sortColumnFilter,
            int timestampIndex,
            boolean isScanAgainstOrder,
            @Nullable RecordSink recordSink
    ) {
        assert recordSink != null || !isScanAgainstOrder;
        SortKeyEncoder encoderInit = null;
        EncodedTopKBuffer entriesInit = null;
        RecordArray recordsInit = null;
        RecordArray spareRecordsInit = null;
        try {
            encoderInit = new SortKeyEncoder(metadata, sortColumnFilter);
            entriesInit = new EncodedTopKBuffer(configuration, true, recordSink != null);
            if (recordSink != null) {
                final long pageSize = Math.min(configuration.getSqlSmallMapPageSize(), configuration.getSqlSortValuePageSize());
                // RecordArray ceilPow2's the page before allocating; divide by the rounded unit to honor the cap.
                final int maxPages = (int) Math.min(
                        Math.max(1L, configuration.getSqlSortValueMaxBytes() / Numbers.ceilPow2(pageSize)),
                        Integer.MAX_VALUE
                );
                final String maxPagesConfigKey = PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_BYTES.getPropertyPath();
                recordsInit = new RecordArray(metadata, recordSink, pageSize, maxPages, maxPagesConfigKey);
                spareRecordsInit = new RecordArray(metadata, recordSink, pageSize, maxPages, maxPagesConfigKey);
            }
        } catch (Throwable th) {
            Misc.free(encoderInit);
            Misc.free(entriesInit);
            Misc.free(recordsInit);
            Misc.free(spareRecordsInit);
            throw th;
        }
        this.encoder = encoderInit;
        this.entries = entriesInit;
        this.records = recordsInit;
        this.spareRecords = spareRecordsInit;
        this.timestampIndex = timestampIndex;
        this.isScanAgainstOrder = isScanAgainstOrder;
        this.buildReadColumns = SortKeyEncoder.extractSortKeyColumnIndexes(sortColumnFilter);
        this.isOpen = true;
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            Misc.free(entries);
            Misc.free(encoder);
            Misc.free(records);
            Misc.free(spareRecords);
            baseCursor = Misc.free(baseCursor);
            baseRecord = null;
        }
    }

    @Override
    public void copyParquetRowIdsTo(DirectLongList target, PageFrameAddressCache addressCache) {
        long parquetRowCount = 0;
        for (long addr = emitStartAddr; addr < emitEndAddr; addr += entrySize) {
            if (addressCache.getFrameFormat(Rows.toPartitionIndex(Unsafe.getLong(addr))) == PartitionFormat.PARQUET) {
                parquetRowCount++;
            }
        }
        if (parquetRowCount == 0) {
            return;
        }
        target.ensureCapacity(parquetRowCount);
        for (long addr = emitStartAddr; addr < emitEndAddr; addr += entrySize) {
            final long rowId = Unsafe.getLong(addr);
            if (addressCache.getFrameFormat(Rows.toPartitionIndex(rowId)) == PartitionFormat.PARQUET) {
                target.add(rowId);
            }
        }
    }

    @TestOnly
    long getCompactionCount() {
        return entries.getCompactionCount();
    }

    @Override
    public Record getRecord() {
        return records != null ? records.getRecord() : baseRecord;
    }

    @Override
    public Record getRecordB() {
        return records != null ? records.getRecordB() : baseCursor.getRecordB();
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
        circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
        final long rowId = Unsafe.getLong(currentAddr);
        currentAddr += entrySize;
        if (records != null) {
            records.recordAtRowIndex(records.getRecord(), rowId);
        } else {
            baseCursor.recordAt(baseRecord, rowId);
        }
        return true;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return baseCursor.newSymbolTable(columnIndex);
    }

    @Override
    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        // The tracker is rebound unconditionally below (outside the !isOpen guard) because
        // the ctor opens eagerly with isOpen=true; binding inside the guard would leave the
        // first query untracked. A second of() without an intervening close() would rebind
        // onto still-charged backing and underflow the per-query counter on free. close()
        // nulls baseCursor, so a null field here means fresh-or-closed.
        assert this.baseCursor == null : "of() without intervening close(): rebinding the memory tracker would underflow the per-query counter";
        // Take ownership before reopen() can throw: on a reopen OOM, close()
        // must find baseCursor here to free it instead of leaking it.
        this.baseCursor = baseCursor;
        this.baseRecord = baseCursor.getRecord();
        // Bind the tracker before any entry-buffer allocation: the first open grows the buffer
        // lazily in beginAppend (reopen is skipped as isOpen starts true), a reuse re-allocates via reopen.
        entries.setMemoryTracker(executionContext.getMemoryTracker());
        if (!isOpen) {
            isOpen = true;
            entries.reopen();
        }
        if (records != null) {
            // The build copies every column of the kept rows, and emit never calls recordAt on the base.
            records.setMemoryTracker(executionContext.getMemoryTracker());
            spareRecords.setMemoryTracker(executionContext.getMemoryTracker());
            records.setSymbolTableResolver(baseCursor);
            spareRecords.setSymbolTableResolver(baseCursor);
        } else {
            baseCursor.setParquetDecodeHint(ParquetDecodeHint.SCATTERED);
            baseCursor.setParentUsedColumns(buildReadColumns);
        }
        if (isEarlyStopEnabled) {
            baseCursor.expectLimitedIteration();
        }
        final SortKeyType keyType = encoder.init(baseCursor);
        assert keyType != SortKeyType.UNSUPPORTED;
        entrySize = keyType.entrySize();
        rowIdOffset = keyType.rowIdOffset();
        entries.of(keyType, isFirstN, limit);
        // Variable-length keys spill into the buffer's key heap; the encoder
        // writes the full key bytes there during the build pass.
        keyHeap = keyType.isVariable() ? entries.getKeyHeap() : null;
        keyLongs = keyType.isVariable() ? 0 : keyType.keyLength() / Long.BYTES;
        if (keyHeap != null) {
            encoder.setKeyHeap(keyHeap);
        }
        circuitBreaker = executionContext.getCircuitBreaker();
        isBuilt = false;
        currentAddr = 0;
        emitStartAddr = 0;
        emitEndAddr = 0;
    }

    @Override
    public long preComputedStateSize() {
        return RecordCursor.fromBool(isBuilt) + (records != null ? 0 : baseCursor.preComputedStateSize());
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        if (records != null) {
            records.recordAt(record, atRowId);
        } else {
            baseCursor.recordAt(record, atRowId);
        }
    }

    @Override
    public void setParquetDecodeHint(ParquetDecodeHint hint) {
        // We emit out of order, so of() pins the base to SCATTERED. An outer MONOTONIC push
        // (e.g. an ASOF light join slave) must not downgrade it and force base re-decodes.
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
        this.isEarlyStopEnabled = isFirstN && timestampIndex != -1 && !isScanAgainstOrder;
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
        entries.clear();
        if (limit > 0) {
            if (limit == Long.MAX_VALUE) {
                // Unbounded build keeps every scanned row, so pre-size the buffer the
                // way EncodedSortLightRecordCursor does instead of growing per row.
                final long estimatedSize = baseCursor.size();
                if (estimatedSize > 0) {
                    entries.presize(estimatedSize);
                }
            }
            runBuild();
        }
        entries.sort();
        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottleOrYield();
        computeEmitWindow();
        toTop();
        if (records == null && emitStartAddr < emitEndAddr) {
            baseCursor.setRecordAtRows(this);
        }
        // Success-path free of the encoder's rank maps; a mid-build throw leaves them
        // for close(). The cursor is not retryable: buildAndSort resets state at entry.
        Misc.free(encoder);
    }

    /**
     * Re-stores the surviving rows in entry order after a compaction and renumbers
     * the entries to match. The append that triggered the compaction has no copy
     * yet, so its row comes from the base record.
     */
    private void compactRecords(long pendingRowId) {
        final Record source = records.getRecordB();
        for (long addr = entries.getAddress() + rowIdOffset, hi = addr + entries.getCount() * entrySize; addr < hi; addr += entrySize) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            final long rowId = Unsafe.getLong(addr);
            if (rowId == pendingRowId) {
                spareRecords.put(baseRecord);
            } else {
                records.recordAtRowIndex(source, rowId);
                spareRecords.put(source);
            }
        }
        entries.renumberRowIds();
        // Copy back rather than swap the arrays: getRecord() hands out the Record of
        // records, and a consumer may hold it before the first hasNext(). reset() keeps
        // the memory of both arrays across compactions.
        records.reset();
        copyRows(spareRecords, 0, records);
        spareRecords.reset();
    }

    private int compareTailKey(Record record, long scratch) {
        if (keyHeap == null) {
            encoder.encode(record, scratch, 0);
            for (int i = 0; i < keyLongs; i++) {
                final int cmp = Long.compareUnsigned(Unsafe.getLong(scratch + 8L * i), tailKeyWords[i]);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }
        keyHeap.jumpTo(tailKeyLen);
        encoder.encode(record, scratch, 0);
        final long len = Unsafe.getLong(scratch + EncodedTopKBuffer.VAR_ENTRY_LEN_OFFSET);
        final int cmp = EncodedTopKBuffer.compareMemory(keyHeap.addressOf(tailKeyLen), keyHeap.addressOf(0), Math.min(len, tailKeyLen));
        return cmp != 0 ? cmp : Long.compare(len, tailKeyLen);
    }

    private void computeEmitWindow() {
        final long count = entries.getCount();
        // The slice the LIMIT selects before skips: head for FIRST N, tail for LAST N.
        final long sliceStart = isFirstN ? 0 : Math.max(count - limit, 0);
        final long sliceEnd = isFirstN ? Math.min(limit, count) : count;
        // Stepwise min/max keeps the bounds exact when the skips carry large
        // user-supplied values; a plain subtraction chain can wrap.
        final long start = Math.min(sliceStart + skipFirst, sliceEnd);
        final long end = Math.max(sliceEnd - skipLast, start);
        emitStartAddr = entries.getAddress() + rowIdOffset + start * entrySize;
        emitEndAddr = entries.getAddress() + rowIdOffset + end * entrySize;
    }

    private void copyRows(RecordArray source, long from, RecordArray target) {
        final Record record = source.getRecordB();
        for (long i = from, n = source.size(); i < n; i++) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            source.recordAtRowIndex(record, i);
            target.put(record);
        }
    }

    private void encodeCopies(long from) {
        final Record record = records.getRecordB();
        for (long i = from, n = records.size(); i < n; i++) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            records.recordAtRowIndex(record, i);
            encoder.encodeTopK(record, i, entries);
        }
    }

    private long encodeCurrentRow() {
        final long addr = entries.beginAppend();
        encoder.encode(baseRecord, addr, records != null ? records.size() : baseRecord.getRowId());
        return addr;
    }

    private void endAppend() {
        final long compactions = entries.getCompactionCount();
        entries.endAppend();
        if (records != null) {
            final long rowId = records.size();
            if (entries.getCompactionCount() != compactions && rowId >> 2 >= limit) {
                compactRecords(rowId);
            } else {
                records.put(baseRecord);
            }
        }
    }

    private long findTailStart(RecordArray rows, long scratch) {
        final Record record = rows.getRecordB();
        long start = rows.size() - limit;
        rows.recordAtRowIndex(record, start);
        final long timestamp = isScanAgainstOrder ? record.getTimestamp(timestampIndex) : 0;
        if (!isScanAgainstOrder) {
            compareTailKey(record, scratch);
            saveTailKey(scratch);
        }
        while (start > 0) {
            rows.recordAtRowIndex(record, start - 1);
            if (isScanAgainstOrder ? record.getTimestamp(timestampIndex) != timestamp : compareTailKey(record, scratch) != 0) {
                break;
            }
            start--;
        }
        return start;
    }

    // Fixed keys encode from the base row; var-size keys encode from the copy, so a computed string is read once.
    private boolean putInOrder(RecordArray rows, long scratch) {
        final int cmp;
        if (keyHeap == null) {
            cmp = compareTailKey(baseRecord, scratch);
            rows.put(baseRecord);
        } else {
            final Record row = rows.getRecordB();
            rows.recordAt(row, rows.put(baseRecord));
            cmp = compareTailKey(row, scratch);
        }
        if (cmp > 0 && rows.size() > 1) {
            return false;
        }
        saveTailKey(scratch);
        return true;
    }

    private void runBuild() {
        if (isEarlyStopEnabled) {
            runBuildTimestampEarlyStop();
            return;
        }
        if (records != null && isFirstN) {
            runTailBuild();
            return;
        }
        if (records != null) {
            runCopyingBuild();
            return;
        }
        while (baseCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            encoder.encodeTopK(baseRecord, baseRecord.getRowId(), entries);
        }
    }

    /**
     * The base is presorted by its designated timestamp and the selection is
     * FIRST N, so once a completed timestamp group pushes the cumulative row
     * count past the limit, every further row carries a strictly larger leading
     * key than every collected row and cannot enter the top-K slice.
     * <p>
     * The leading key word is a bijective transform of the designated timestamp,
     * so word equality detects group changes without re-reading the column; the
     * entry encoded for the stopping row sits beyond the buffer position, exactly
     * like a threshold-rejected entry, and is never observed.
     */
    private void runBuildTimestampEarlyStop() {
        if (!baseCursor.hasNext()) {
            return;
        }
        long addr = encodeCurrentRow();
        long groupKey = Unsafe.getLong(addr);
        endAppend();
        long rowsInGroup = 1;
        long rowsSoFar = 0;
        while (baseCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            addr = encodeCurrentRow();
            final long currentKey = Unsafe.getLong(addr);
            if (groupKey == currentKey) {
                rowsInGroup++;
            } else {
                rowsSoFar += rowsInGroup;
                if (rowsSoFar > limit) {
                    entries.discardLastAppend();
                    return;
                }
                groupKey = currentKey;
                rowsInGroup = 1;
            }
            endAppend();
        }
    }

    private void runCopyingBuild() {
        long batchRows = 0;
        long batchAppends = 0;
        boolean isFiltered = entries.hasThreshold();
        while (baseCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            final long count = entries.getCount();
            final long compactions = entries.getCompactionCount();
            final long rowId = records.size();
            encoder.encodeTopK(baseRecord, rowId, entries);
            batchRows++;
            final boolean isCompacted = entries.getCompactionCount() != compactions;
            if (isCompacted) {
                if (isFiltered && batchAppends + 1 > batchRows >> 1) {
                    entries.growCompactionTrigger();
                }
                isFiltered = true;
                batchRows = 0;
                batchAppends = 0;
            } else if (entries.getCount() != count) {
                batchAppends++;
            }
            if (isCompacted && rowId >> 2 >= limit) {
                compactRecords(rowId);
            } else if (isCompacted || entries.getCount() != count) {
                records.put(baseRecord);
            }
        }
    }

    private void runTailBuild() {
        final int shift = isScanAgainstOrder ? 2 : 4;
        final long trigger = limit > Long.MAX_VALUE >> shift ? Long.MAX_VALUE : Math.max(limit << shift, MIN_TAIL_ROWS);
        final long scratch = isScanAgainstOrder ? 0 : entries.beginAppend();
        RecordArray rows = records;
        RecordArray spare = spareRecords;
        boolean isGroupBound = true;
        boolean isOrdered = true;
        tailKeyLen = 0;
        while (isGroupBound && baseCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            if (isScanAgainstOrder) {
                rows.put(baseRecord);
            } else if (!putInOrder(rows, scratch)) {
                isOrdered = false;
                break;
            }
            if (rows.size() == trigger) {
                final long start = findTailStart(rows, scratch);
                if (start == 0) {
                    isGroupBound = false;
                } else {
                    copyRows(rows, start, spare);
                    rows.reset();
                    final RecordArray swap = rows;
                    rows = spare;
                    spare = swap;
                    if (!isScanAgainstOrder) {
                        final Record row = rows.getRecordB();
                        rows.recordAtRowIndex(row, rows.size() - 1);
                        compareTailKey(row, scratch);
                        saveTailKey(scratch);
                    }
                }
            }
        }
        long from = isOrdered && isGroupBound && rows.size() > limit ? findTailStart(rows, scratch) : 0;
        if (rows != records) {
            copyRows(rows, from, records);
            rows.reset();
            from = 0;
        }
        encodeCopies(from);
        if (!isOrdered || !isGroupBound) {
            runCopyingBuild();
        }
    }

    private void saveTailKey(long scratch) {
        if (keyHeap == null) {
            for (int i = 0; i < keyLongs; i++) {
                tailKeyWords[i] = Unsafe.getLong(scratch + 8L * i);
            }
            return;
        }
        // Keys are a few dozen bytes, so a Java copy beats a native call.
        final long len = Unsafe.getLong(scratch + EncodedTopKBuffer.VAR_ENTRY_LEN_OFFSET);
        final long src = keyHeap.addressOf(tailKeyLen);
        final long dst = keyHeap.addressOf(0);
        long i = 0;
        for (; i + Long.BYTES <= len; i += Long.BYTES) {
            Unsafe.putLong(dst + i, Unsafe.getLong(src + i));
        }
        for (; i < len; i++) {
            Unsafe.putByte(dst + i, Unsafe.getByte(src + i));
        }
        tailKeyLen = len;
    }
}
