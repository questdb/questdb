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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
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
    private static final long MAX_HEAP_SIZE_LIMIT = (Integer.toUnsignedLong(-1) - 1) << 3;
    private final SortKeyEncoder encoder;
    private final DirectLongList entryMem;
    private final long maxEntryMemBytes;
    private final long parallelThreshold;
    private long count;
    private long currentAddr;
    private long endAddr;
    private int entrySize;
    private boolean isOpen;
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
        this.entryMem = new DirectLongList(
                Math.max(configuration.getSqlWindowTreeKeyPageSize() / Long.BYTES, 1),
                MemoryTag.NATIVE_DEFAULT,
                true
        );
        this.maxEntryMemBytes = Math.min(
                configuration.getSqlSortKeyPageSize() * (long) configuration.getSqlSortKeyMaxPages()
                        + configuration.getSqlSortLightValuePageSize() * (long) configuration.getSqlSortLightValueMaxPages(),
                MAX_HEAP_SIZE_LIMIT
        );
        this.parallelThreshold = configuration.getSqlSortEncodedParallelThreshold();
        this.isOpen = true;
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            Misc.free(encoder);
            Misc.free(entryMem);
        }
    }

    @Override
    public void finishPut(SqlExecutionCircuitBreaker circuitBreaker) {
        try {
            if (count > 1) {
                Vect.sortEncodedEntries(
                        entryMem.getAddress(),
                        count,
                        keyType.keyLength() / Long.BYTES,
                        parallelThreshold
                );
                circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
            }
            startAddr = entryMem.getAddress() + rowIdOffset;
            toTop();
        } finally {
            Misc.free(encoder);
        }
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
    public void of(RecordCursor cursor) {
        keyType = encoder.init(cursor);
        assert keyType != SortKeyType.UNSUPPORTED;
        entrySize = keyType.entrySize();
        rowIdOffset = keyType.rowIdOffset();
        longsPerEntry = entrySize / Long.BYTES;
        maxEntries = maxEntryMemBytes / entrySize;
        count = 0;
        entryMem.clear();
        startAddr = entryMem.getAddress() + rowIdOffset;
        currentAddr = startAddr;
        endAddr = startAddr;
    }

    @Override
    public void put(Record record, long rowId) {
        if (count >= maxEntries) {
            throw LimitOverflowException.instance().put("limit of ").put(maxEntryMemBytes)
                    .put(" memory exceeded in window encoded sort");
        }
        entryMem.ensureCapacity(longsPerEntry);
        long entryAddr = entryMem.getAppendAddress();
        encoder.encode(record, entryAddr, rowId);
        entryMem.skip(longsPerEntry);
        count++;
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
}
