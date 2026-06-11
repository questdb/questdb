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

class EncodedSortLightRecordCursor implements DelegatingRecordCursor, RecordCursor.RowIdSource {
    private final IntHashSet buildReadColumns;
    private final SortKeyEncoder encoder;
    private final DirectLongList entryMem;
    private final long keyCapBytes;
    private final long parallelThreshold;
    private final long valueCapBytes;
    private RecordCursor baseCursor;
    private Record baseRecord;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private long count;
    private long currentAddr;
    private long endAddr;
    private int entrySize;
    private boolean isOpen;
    private boolean isSorted;
    private SortKeyType keyType;
    private int longsPerEntry;
    private int rowIdOffset;
    private long startAddr;

    public EncodedSortLightRecordCursor(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            IntList sortColumnFilter
    ) {
        try {
            this.encoder = new SortKeyEncoder(metadata, sortColumnFilter);
            this.buildReadColumns = SortKeyEncoder.extractSortKeyColumnIndexes(sortColumnFilter);
            this.entryMem = new DirectLongList(16 * 1024, MemoryTag.NATIVE_DEFAULT, true); // 128KB
            this.keyCapBytes = configuration.getSqlSortKeyMaxBytes();
            this.valueCapBytes = configuration.getSqlSortLightValueMaxBytes();
            this.parallelThreshold = configuration.getSqlSortEncodedParallelThreshold();
            this.isOpen = true;
        } finally {
            if (!this.isOpen) {
                forceClose();
            }
        }
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            forceClose();
        }
    }

    @Override
    public void copyParquetRowIdsTo(DirectLongList target, PageFrameAddressCache addressCache) {
        long parquetRowCount = 0;
        for (long addr = startAddr; addr < endAddr; addr += entrySize) {
            if (addressCache.getFrameFormat(Rows.toPartitionIndex(Unsafe.getLong(addr))) == PartitionFormat.PARQUET) {
                parquetRowCount++;
            }
        }
        if (parquetRowCount == 0) {
            return;
        }
        target.ensureCapacity(parquetRowCount);
        for (long addr = startAddr; addr < endAddr; addr += entrySize) {
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
        if (!isSorted) {
            buildAndSort();
            isSorted = true;
        }
        if (currentAddr < endAddr) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            long rowId = Unsafe.getLong(currentAddr);
            currentAddr += entrySize;
            baseCursor.recordAt(baseRecord, rowId);
            return true;
        }
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return baseCursor.newSymbolTable(columnIndex);
    }

    @Override
    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        if (!isOpen) {
            isOpen = true;
            entryMem.reopen();
        }
        this.baseCursor = baseCursor;
        this.baseRecord = baseCursor.getRecord();
        baseCursor.setParquetDecodeHint(ParquetDecodeHint.SCATTERED);
        baseCursor.setParentUsedColumns(buildReadColumns);
        keyType = encoder.init(baseCursor);
        assert keyType != SortKeyType.UNSUPPORTED;
        entrySize = keyType.entrySize();
        rowIdOffset = keyType.rowIdOffset();
        longsPerEntry = entrySize / Long.BYTES;
        circuitBreaker = executionContext.getCircuitBreaker();
        isSorted = false;
        count = 0;
    }

    @Override
    public long preComputedStateSize() {
        return RecordCursor.fromBool(isSorted) + baseCursor.preComputedStateSize();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        baseCursor.recordAt(record, atRowId);
    }

    @Override
    public void setParquetDecodeHint(ParquetDecodeHint hint) {
        baseCursor.setParquetDecodeHint(hint);
    }

    @Override
    public long size() {
        return baseCursor.size();
    }

    @Override
    public void toTop() {
        currentAddr = startAddr;
        endAddr = startAddr + count * entrySize;
    }

    private void buildAndSort() {
        // Pre-allocate if size is known
        long estimatedSize = baseCursor.size();
        long maxEntries = SortKeyEncoder.maxEntries(keyCapBytes, valueCapBytes, keyType);
        long maxEntryMemBytes = maxEntries * entrySize;
        if (estimatedSize > 0) {
            if (estimatedSize > maxEntries) {
                SortKeyEncoder.throwSortHeapOverflow(maxEntryMemBytes);
            }
            entryMem.setCapacity(estimatedSize * longsPerEntry);
        }

        // Collect (key, rowId) entries
        entryMem.clear();
        count = 0;
        if (estimatedSize > 0) {
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                long addr = entryMem.getAppendAddress();
                encoder.encode(baseRecord, addr, baseRecord.getRowId());
                entryMem.skip(longsPerEntry);
                count++;
            }
        } else {
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                if (count >= maxEntries) {
                    SortKeyEncoder.throwSortHeapOverflow(maxEntryMemBytes);
                }
                entryMem.ensureCapacity(longsPerEntry);
                long addr = entryMem.getAppendAddress();
                encoder.encode(baseRecord, addr, baseRecord.getRowId());
                entryMem.skip(longsPerEntry);
                count++;
            }
        }
        if (count <= 1) {
            startAddr = entryMem.getAddress() + rowIdOffset;
            toTop();
            if (count > 0) {
                baseCursor.setRecordAtRows(this);
            }
            Misc.free(encoder);
            return;
        }

        Vect.sortEncodedEntries(entryMem.getAddress(), count, keyType.keyLength() / Long.BYTES, parallelThreshold);
        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
        startAddr = entryMem.getAddress() + rowIdOffset;
        toTop();
        baseCursor.setRecordAtRows(this);
        // No finally: a retry after a mid-build throw must not see freed rank maps.
        Misc.free(encoder);
    }

    private void forceClose() {
        Misc.free(entryMem);
        Misc.free(encoder);
        baseCursor = Misc.free(baseCursor);
        baseRecord = null;
    }
}
