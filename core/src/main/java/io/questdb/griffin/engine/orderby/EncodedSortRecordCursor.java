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
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.DelegatingRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

class EncodedSortRecordCursor implements DelegatingRecordCursor {
    private final SortKeyEncoder encoder;
    private final DirectLongList entryMem;
    private final MemoryCARW keyHeap;
    private final long maxEntryMemBytes;
    private final long parallelThreshold;
    private final RecordChain recordChain;
    private RecordCursor baseCursor;
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

    public EncodedSortRecordCursor(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            IntList sortColumnFilter,
            RecordSink recordSink
    ) {
        try {
            this.encoder = new SortKeyEncoder(metadata, sortColumnFilter);
            this.entryMem = new DirectLongList(16 * 1024, MemoryTag.NATIVE_DEFAULT, true); // 128KB
            this.maxEntryMemBytes = Math.min(
                    configuration.getSqlSortKeyMaxBytes(),
                    SortKeyEncoder.MAX_ENTRY_HEAP_BYTES
            );
            this.parallelThreshold = configuration.getSqlSortEncodedParallelThreshold();
            final long keyHeapPageSize = configuration.getSqlSortKeyPageSize();
            this.keyHeap = new MemoryCARWImpl(
                    keyHeapPageSize,
                    (int) Math.min(Integer.MAX_VALUE, maxEntryMemBytes / Numbers.ceilPow2(keyHeapPageSize) + 1),
                    MemoryTag.NATIVE_DEFAULT,
                    PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath()
            );
            final long valuePageSize = configuration.getSqlSortValuePageSize();
            // RecordChain ceilPow2's the page before allocating; divide by the rounded unit to honor the cap.
            final long valueMaxPagesFromBytes = Math.max(1L, configuration.getSqlSortValueMaxBytes() / Numbers.ceilPow2(valuePageSize));
            this.recordChain = new RecordChain(
                    metadata,
                    recordSink,
                    valuePageSize,
                    (int) Math.min(valueMaxPagesFromBytes, Integer.MAX_VALUE),
                    PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_BYTES.getPropertyPath()
            );
            this.isOpen = true;
        } finally {
            if (!this.isOpen) forceClose();
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
    public Record getRecord() {
        return recordChain.getRecord();
    }

    @Override
    public Record getRecordB() {
        return recordChain.getRecordB();
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
            long chainOffset = Unsafe.getLong(currentAddr);
            currentAddr += entrySize;
            recordChain.recordAt(recordChain.getRecord(), chainOffset);
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
        // The tracker is rebound unconditionally below (outside the !isOpen guard) because
        // the ctor opens eagerly with isOpen=true, so an inside-guard bind would leave the
        // first query untracked. A second of() without an intervening close() would then
        // rebind onto still-charged backing and underflow the per-query counter on free.
        // close()/forceClose() null baseCursor, so a null field here means fresh-or-closed.
        assert this.baseCursor == null : "of() without intervening close(): rebinding the memory tracker would underflow the per-query counter";
        this.baseCursor = baseCursor;
        // Wire only the row-count-scaled allocators; keyHeap stays global-counter only as it is
        // bounded by maxEntryMemBytes (see the throwLimitOverflow check below).
        entryMem.setMemoryTracker(executionContext.getMemoryTracker());
        recordChain.setMemoryTracker(executionContext.getMemoryTracker());
        if (!isOpen) {
            isOpen = true;
            entryMem.reopen();
        } else {
            recordChain.clear();
        }
        recordChain.setSymbolTableResolver(baseCursor);
        keyType = encoder.init(baseCursor);
        assert keyType != SortKeyType.UNSUPPORTED;
        if (keyType.isVariable()) {
            encoder.setKeyHeap(keyHeap);
        }
        entrySize = keyType.entrySize();
        rowIdOffset = keyType.rowIdOffset();
        longsPerEntry = entrySize / Long.BYTES;
        circuitBreaker = executionContext.getCircuitBreaker();
        isSorted = false;
        count = 0;
    }

    @Override
    public long preComputedStateSize() {
        return RecordCursor.fromBool(isSorted);
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        recordChain.recordAt(record, atRowId);
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
        // Consult the breaker before consuming the base, so an empty base scan still observes cancellation.
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
        final boolean isVariable = keyType.isVariable();
        long estimatedSize = baseCursor.size();
        long maxEntries = maxEntryMemBytes / entrySize;
        if (estimatedSize > 0) {
            if (estimatedSize > maxEntries) {
                throwLimitOverflow();
            }
            entryMem.setCapacity(estimatedSize * longsPerEntry);
        }

        entryMem.clear();
        count = 0;
        Record record = baseCursor.getRecord();
        if (isVariable) {
            keyHeap.close();
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                long chainOffset = recordChain.put(record, -1L);
                entryMem.ensureCapacity(longsPerEntry);
                long addr = entryMem.getAppendAddress();
                encoder.encode(record, addr, chainOffset);
                entryMem.skip(longsPerEntry);
                count++;
                if (count * entrySize + keyHeap.getAppendOffset() > maxEntryMemBytes) {
                    throwLimitOverflow();
                }
            }
        } else if (estimatedSize > 0) {
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                long chainOffset = recordChain.put(record, -1L);
                long addr = entryMem.getAppendAddress();
                encoder.encode(record, addr, chainOffset);
                entryMem.skip(longsPerEntry);
                count++;
            }
        } else {
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                if (count >= maxEntries) {
                    throwLimitOverflow();
                }
                long chainOffset = recordChain.put(record, -1L);
                entryMem.ensureCapacity(longsPerEntry);
                long addr = entryMem.getAppendAddress();
                encoder.encode(record, addr, chainOffset);
                entryMem.skip(longsPerEntry);
                count++;
            }
        }
        // Success-path free of the encoder's rank maps; a mid-build throw leaves them
        // for close(). The cursor is not retryable: buildAndSort resets state at entry.
        Misc.free(encoder);

        if (count > 1) {
            if (isVariable) {
                long heapAddr = keyHeap.getAppendOffset() == 0 ? 0 : keyHeap.addressOf(0);
                Vect.sortEncodedVarEntries(entryMem.getAddress(), count, heapAddr, parallelThreshold);
            } else {
                Vect.sortEncodedEntries(entryMem.getAddress(), count, keyType.keyLength() / Long.BYTES, parallelThreshold);
            }
            circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
        }
        if (isVariable) {
            // emit reads only chain offsets; the key heap is not needed past the sort
            keyHeap.close();
        }
        startAddr = entryMem.getAddress() + rowIdOffset;
        toTop();
    }

    private void forceClose() {
        Misc.free(entryMem);
        Misc.free(keyHeap);
        Misc.free(encoder);
        Misc.free(recordChain);
        baseCursor = Misc.free(baseCursor);
    }

    private void throwLimitOverflow() {
        throw LimitOverflowException.instance()
                .put("limit of ").put(maxEntryMemBytes)
                .put(" memory exceeded in EncodedSort (raise ")
                .put(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath())
                .put(')');
    }
}
