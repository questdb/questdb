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

package io.questdb.griffin.engine.window;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.orderby.SortKeyEncoder;
import io.questdb.griffin.engine.orderby.SortKeyType;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

final class EncodedWindowSortBuffer implements WindowSortBuffer {
    private final SortKeyEncoder encoder;
    private final DirectLongList entryMem;
    private final MemoryCARW keyHeap;
    private final long maxEntryMemBytes;
    private final long parallelThreshold;
    private long count;
    private long currentAddr;
    private long endAddr;
    private int entrySize;
    private boolean isOpen;
    private boolean isVariable;
    private SortKeyType keyType;
    private int longsPerEntry;
    private long maxEntries;
    private int rowIdOffset;
    private long startAddr;

    EncodedWindowSortBuffer(
            CairoConfiguration configuration,
            RecordMetadata chainMetadata,
            IntList sortColumnFilter
    ) {
        this.encoder = new SortKeyEncoder(chainMetadata, sortColumnFilter);
        DirectLongList mem = null;
        MemoryCARW heap = null;
        try {
            mem = new DirectLongList(
                    Math.max(configuration.getSqlWindowTreeKeyPageSize() / Long.BYTES, 1),
                    MemoryTag.NATIVE_DEFAULT,
                    true
            );
            // Honor the window-specific sort memory caps - the same two keys the tree path uses -
            // so a user who lowers them to bound window sort memory keeps that bound on the encoded
            // path too. The encoded buffer interleaves sort keys and rowIds in one block, so its
            // budget is the sum of the key and rowid caps, mirroring the tree path's two heaps.
            this.maxEntryMemBytes = SortKeyType.maxHeapBytes(
                    configuration.getSqlWindowTreeKeyMaxBytes(),
                    configuration.getSqlWindowRowIdMaxBytes()
            );
            final int keyHeapPageSize = configuration.getSqlWindowTreeKeyPageSize();
            heap = new MemoryCARWImpl(
                    keyHeapPageSize,
                    (int) Math.min(Integer.MAX_VALUE, maxEntryMemBytes / keyHeapPageSize + 1),
                    MemoryTag.NATIVE_DEFAULT,
                    PropertyKey.CAIRO_SQL_WINDOW_TREE_MAX_BYTES.getPropertyPath()
            );
            this.parallelThreshold = configuration.getSqlSortEncodedParallelThreshold();
            this.entryMem = mem;
            this.keyHeap = heap;
            this.isOpen = true;
        } catch (Throwable th) {
            Misc.free(mem);
            Misc.free(heap);
            Misc.free(encoder);
            throw th;
        }
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            Misc.free(encoder);
            Misc.free(entryMem);
            Misc.free(keyHeap);
        }
    }

    @Override
    public void finishPut(SqlExecutionCircuitBreaker circuitBreaker) {
        if (count > 1) {
            if (isVariable) {
                long heapAddr = keyHeap.getAppendOffset() == 0 ? 0 : keyHeap.addressOf(0);
                Vect.sortEncodedVarEntries(entryMem.getAddress(), count, heapAddr, parallelThreshold);
            } else {
                Vect.sortEncodedEntries(entryMem.getAddress(), count, keyType.keyLength() / Long.BYTES, parallelThreshold);
            }
            circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
        }
        startAddr = entryMem.getAddress() + rowIdOffset;
        toTop();
    }

    @Override
    public boolean hasNext() {
        return currentAddr < endAddr;
    }

    @Override
    public long next() {
        long rowId = Unsafe.getLong(currentAddr);
        currentAddr += entrySize;
        return rowId;
    }

    @Override
    public void of(RecordCursor cursor, long expectedRows) {
        keyType = encoder.init(cursor);
        assert keyType != SortKeyType.UNSUPPORTED;
        isVariable = keyType.isVariable();
        if (isVariable) {
            encoder.setKeyHeap(keyHeap);
            keyHeap.jumpTo(0);
        }
        entrySize = keyType.entrySize();
        rowIdOffset = keyType.rowIdOffset();
        longsPerEntry = entrySize / Long.BYTES;
        maxEntries = maxEntryMemBytes / entrySize;
        count = 0;
        entryMem.clear();
        if (expectedRows > 0) {
            long requestedLongs = Math.min(expectedRows, maxEntries) * longsPerEntry;
            if (requestedLongs > entryMem.getCapacity()) {
                entryMem.setCapacity(requestedLongs);
            }
        }
        startAddr = entryMem.getAddress() + rowIdOffset;
        currentAddr = startAddr;
        endAddr = startAddr;
    }

    @Override
    public void put(Record record, long rowId) {
        // keyType is set by of(); a null here means put() ran before of(). A tiny configured cap
        // can legitimately leave maxEntries == 0, which falls through to the overflow below.
        assert keyType != null : "put() called before of()";
        if (count >= maxEntries) {
            throw windowSortOverflow();
        }
        entryMem.ensureCapacity(longsPerEntry);
        long entryAddr = entryMem.getAppendAddress();
        encoder.encode(record, entryAddr, rowId);
        entryMem.skip(longsPerEntry);
        count++;
        if (isVariable && count * entrySize + keyHeap.getAppendOffset() > maxEntryMemBytes) {
            throw windowSortOverflow();
        }
    }

    @Override
    public void reopen() {
        if (!isOpen) {
            isOpen = true;
            entryMem.reopen();
        }
    }

    @Override
    public void toTop() {
        currentAddr = startAddr;
        endAddr = startAddr + count * entrySize;
    }

    private CairoException windowSortOverflow() {
        return LimitOverflowException.instance().put("limit of ").put(maxEntryMemBytes)
                .put(" memory exceeded in window encoded sort (raise ")
                .put(PropertyKey.CAIRO_SQL_WINDOW_TREE_MAX_BYTES.getPropertyPath())
                .put(" / ")
                .put(PropertyKey.CAIRO_SQL_WINDOW_ROWID_MAX_BYTES.getPropertyPath())
                .put(')');
    }
}
