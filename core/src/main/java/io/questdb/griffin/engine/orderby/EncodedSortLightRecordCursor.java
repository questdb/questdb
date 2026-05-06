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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

class EncodedSortLightRecordCursor implements DelegatingRecordCursor {
    private static final long MAX_HEAP_SIZE_LIMIT = (Integer.toUnsignedLong(-1) - 1) << 3;
    private final SortKeyEncoder encoder;
    private final DirectLongList entryMem;
    private final long maxEntryMemBytes;
    private final long parallelThreshold;
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
            this.entryMem = new DirectLongList(16 * 1024, MemoryTag.NATIVE_DEFAULT, true); // 128KB
            this.maxEntryMemBytes = Math.min(
                    configuration.getSqlSortKeyPageSize() * (long) configuration.getSqlSortKeyMaxPages()
                            + configuration.getSqlSortLightValuePageSize() * (long) configuration.getSqlSortLightValueMaxPages(),
                    MAX_HEAP_SIZE_LIMIT
            );
            this.parallelThreshold = configuration.getSqlSortEncodedParallelThreshold();
            this.isOpen = true;
        } catch (Throwable th) {
            close();
            throw th;
        }
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
        this.baseCursor = baseCursor;
        this.baseRecord = baseCursor.getRecord();
        if (!isOpen) {
            isOpen = true;
            entryMem.reopen();
        }
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
        try {
            long estimatedSize = baseCursor.size();
            long maxEntries = maxEntryMemBytes / entrySize;
            if (estimatedSize > 0) {
                if (estimatedSize > maxEntries) {
                    throw LimitOverflowException.instance().put("limit of ").put(maxEntryMemBytes).put(" memory exceeded in EncodedSort");
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
                        throw LimitOverflowException.instance().put("limit of ").put(maxEntryMemBytes).put(" memory exceeded in EncodedSort");
                    }
                    entryMem.ensureCapacity(longsPerEntry);
                    long addr = entryMem.getAppendAddress();
                    encoder.encode(baseRecord, addr, baseRecord.getRowId());
                    entryMem.skip(longsPerEntry);
                    count++;
                }
            }
        } finally {
            Misc.free(encoder);
        }

        if (count <= 1) {
            startAddr = entryMem.getAddress() + rowIdOffset;
            toTop();
            return;
        }

        Vect.sortEncodedEntries(entryMem.getAddress(), count, keyType.keyLength() / Long.BYTES, parallelThreshold);
        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
        startAddr = entryMem.getAddress() + rowIdOffset;
        toTop();
    }
}
