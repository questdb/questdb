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

package io.questdb.cairo;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * RecordArray is similar to RecordChain, except that it stores the record's startOffset in a separate memory
 * location instead of at the record header.
 * This enhances its random access capability, making it behave more like an array,
 * while sacrificing the ability to access records like a linked list.
 * <p>
 * When all columns are fixed-width ({@code varOffset == 0}), row N sits at
 * offset {@code N * fixOffset} by construction, so the per-row aux index is
 * redundant and not allocated.
 */
public class RecordArray extends RecordChain {
    private final MemoryARW auxMem;
    private long nextRecordIndex = 0L;
    private long size = 0L;

    public RecordArray(@NotNull ColumnTypes columnTypes, RecordSink recordSink, long pageSize, int maxPages) {
        super(columnTypes, recordSink, pageSize, maxPages);
        try {
            this.auxMem = varOffset > 0
                    ? Vm.getCARWInstance(pageSize >> 4, maxPages, MemoryTag.NATIVE_RECORD_CHAIN)
                    : null;
        } catch (Throwable th) {
            super.close();
            throw th;
        }
    }

    public RecordArray(
            @NotNull ColumnTypes columnTypes,
            @Nullable RecordSink recordSink,
            long pageSize,
            int maxPages,
            String maxPagesConfigKey
    ) {
        super(columnTypes, recordSink, pageSize, maxPages, maxPagesConfigKey);
        try {
            this.auxMem = varOffset > 0
                    ? Vm.getCARWInstance(pageSize >> 4, maxPages, MemoryTag.NATIVE_RECORD_CHAIN, maxPagesConfigKey)
                    : null;
        } catch (Throwable th) {
            super.close();
            throw th;
        }
    }

    public long beginRecord() {
        recordOffset = varAppendOffset;
        if (auxMem != null) {
            auxMem.putLong(recordOffset);
        }
        size++;
        varAppendOffset = recordOffset + varOffset + fixOffset;
        // First jumpTo extends pages to the row end so random-access writes
        // via getAddressAtRowIndex land on allocated memory; second rewinds
        mem.jumpTo(varAppendOffset);
        mem.jumpTo(recordOffset + varOffset);
        return recordOffset;
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        counter.add(size - nextRecordIndex);
        nextRecordIndex = size;
    }

    @Override
    public void clear() {
        super.clear();
        size = 0L;
        nextRecordIndex = 0L;
        if (auxMem != null) {
            auxMem.close();
        }
    }

    /**
     * Returns the native address of {@code columnIndex} in the row stored
     * under the given dense rowIndex, without disturbing any positioned record.
     * Used by callers that need to write into a fixed column slot for a row
     * referenced only by its rowIndex.
     */
    public long getAddressAtRowIndex(long rowIndex, int columnIndex) {
        return getAddress(rowToOffset(rowIndex), columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (nextRecordIndex < size) {
            recordA.of(rowToOffset(nextRecordIndex));
            nextRecordIndex++;
            return true;
        }
        return false;
    }

    public boolean hasPrev() {
        if (nextRecordIndex >= 0 && nextRecordIndex < size) {
            recordA.of(rowToOffset(nextRecordIndex));
            nextRecordIndex--;
            return true;
        }
        return false;
    }

    public long put(Record record) {
        long offset = beginRecord();
        recordSink.copy(record, this);
        return offset;
    }

    public void recordAtRowIndex(Record record, long rowIndex) {
        recordAt(record, rowToOffset(rowIndex));
    }

    @Override
    public void setMemoryTracker(@Nullable MemoryTracker tracker) {
        // Bind auxMem too (the per-row startOffset region), else it escapes the
        // per-query limit. auxMem is absent for fixed-width-only chains.
        super.setMemoryTracker(tracker);
        if (auxMem != null) {
            auxMem.setMemoryTracker(tracker);
        }
    }

    @Override
    public long size() {
        return size;
    }

    public void toBottom() {
        nextRecordIndex = size - 1;
    }

    @Override
    public void toTop() {
        nextRecordIndex = 0;
    }

    private long rowToOffset(long rowIndex) {
        return auxMem != null ? auxMem.getLong(rowIndex * Long.BYTES) : rowIndex * fixOffset;
    }

    @Override
    protected RecordChainRecord newChainRecord() {
        return new RecordArrayRecord(columnCount);
    }

    @Override
    protected long rowToDataOffset(long row) {
        return row;
    }

    class RecordArrayRecord extends RecordChainRecord {
        public RecordArrayRecord(int columnCount) {
            super(columnCount);
        }

        @Override
        public long getRowId() {
            return baseOffset;
        }
    }
}
