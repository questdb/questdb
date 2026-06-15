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
import io.questdb.std.Misc;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;

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
    private final IntHashSet buildReadColumns;
    private final SortKeyEncoder encoder;
    private final EncodedTopKBuffer entries;
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
    private long limit;
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
        EncodedTopKBuffer entriesInit = null;
        try {
            encoderInit = new SortKeyEncoder(metadata, sortColumnFilter);
            entriesInit = new EncodedTopKBuffer(configuration);
        } catch (Throwable th) {
            Misc.free(encoderInit);
            Misc.free(entriesInit);
            throw th;
        }
        this.encoder = encoderInit;
        this.entries = entriesInit;
        this.timestampIndex = timestampIndex;
        this.buildReadColumns = SortKeyEncoder.extractSortKeyColumnIndexes(sortColumnFilter);
        this.isOpen = true;
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            Misc.free(entries);
            Misc.free(encoder);
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
        // Take ownership before reopen() can throw: on a reopen OOM, close()
        // must find baseCursor here to free it instead of leaking it.
        this.baseCursor = baseCursor;
        this.baseRecord = baseCursor.getRecord();
        if (!isOpen) {
            isOpen = true;
            entries.reopen();
        }
        baseCursor.setParquetDecodeHint(ParquetDecodeHint.SCATTERED);
        if (isEarlyStopEnabled) {
            baseCursor.expectLimitedIteration();
        }
        baseCursor.setParentUsedColumns(buildReadColumns);
        final SortKeyType keyType = encoder.init(baseCursor);
        assert keyType != SortKeyType.UNSUPPORTED;
        entrySize = keyType.entrySize();
        rowIdOffset = keyType.rowIdOffset();
        entries.of(keyType, isFirstN, limit);
        // Variable-length keys spill into the buffer's key heap; the encoder
        // writes the full key bytes there during the build pass.
        if (keyType.isVariable()) {
            encoder.setKeyHeap(entries.getKeyHeap());
        }
        circuitBreaker = executionContext.getCircuitBreaker();
        isBuilt = false;
        currentAddr = 0;
        emitStartAddr = 0;
        emitEndAddr = 0;
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
        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
        computeEmitWindow();
        toTop();
        if (emitStartAddr < emitEndAddr) {
            baseCursor.setRecordAtRows(this);
        }
        // Success-path free of the encoder's rank maps; a mid-build throw leaves them
        // for close(). The cursor is not retryable: buildAndSort resets state at entry.
        Misc.free(encoder);
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

    private long encodeCurrentRow() {
        final long addr = entries.beginAppend();
        encoder.encode(baseRecord, addr, baseRecord.getRowId());
        return addr;
    }

    private void runBuild() {
        if (isEarlyStopEnabled) {
            runBuildTimestampEarlyStop();
            return;
        }
        while (baseCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
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
        entries.endAppend();
        long rowsInGroup = 1;
        long rowsSoFar = 0;
        while (baseCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            addr = encodeCurrentRow();
            final long currentKey = Unsafe.getLong(addr);
            if (groupKey == currentKey) {
                rowsInGroup++;
            } else {
                rowsSoFar += rowsInGroup;
                if (rowsSoFar > limit) {
                    return;
                }
                groupKey = currentKey;
                rowsInGroup = 1;
            }
            entries.endAppend();
        }
    }
}
