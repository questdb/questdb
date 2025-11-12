/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import org.jetbrains.annotations.NotNull;

/**
 * RecordArray is similar to RecordChain, except that it stores the record's startOffset in a separate memory
 * location instead of at the record header.
 * This enhances its random access capability, making it behave more like an array,
 * while sacrificing the ability to access records like a linked list.
 */
public class RecordArray extends RecordChain {

    // auxMem is used to store records startOffset in dataMem
    private final MemoryARW auxMem;
    private long nextRecordIndex = 0L;
    private long size = 0L;

    public RecordArray(@NotNull ColumnTypes columnTypes, @NotNull RecordSink recordSink, long pageSize, int maxPages) {
        super(columnTypes, recordSink, pageSize, maxPages);
        this.auxMem = Vm.getCARWInstance(pageSize >> 4, maxPages, MemoryTag.NATIVE_RECORD_CHAIN);
    }

    public long beginRecord() {
        recordOffset = varAppendOffset;
        auxMem.putLong(recordOffset);
        size++;
        mem.jumpTo(recordOffset + varOffset);
        varAppendOffset = recordOffset + varOffset + fixOffset;
        return recordOffset;
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        counter.add(size - nextRecordIndex);
    }

    @Override
    public void clear() {
        super.clear();
        size = 0L;
        auxMem.close();
    }

    @Override
    public boolean hasNext() {
        if (nextRecordIndex < size) {
            final long offset = auxMem.getLong(nextRecordIndex * 8);
            recordA.of(offset);
            nextRecordIndex++;
            return true;
        }
        return false;
    }

    public boolean hasPrev() {
        if (nextRecordIndex >= 0) {
            final long offset = auxMem.getLong(nextRecordIndex * 8);
            recordA.of(offset);
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

    public void toBottom() {
        nextRecordIndex = size - 1;
    }

    @Override
    public void toTop() {
        nextRecordIndex = 0;
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
