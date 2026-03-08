/*******************************************************************************
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

package io.questdb.cairo.map;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.Unsafe;
import io.questdb.std.bytes.Bytes;

class OrderedMapVarSizeCursor implements OrderedMapCursor {
    private final OrderedMapVarSizeRecord recordA;
    private final OrderedMapVarSizeRecord recordB;
    private final long valueSize;
    private long heapAddr;
    private long heapStart;
    private int remaining;
    private int size;

    OrderedMapVarSizeCursor(OrderedMapVarSizeRecord record, OrderedMap map) {
        assert map.keySize() == -1;
        this.recordA = record;
        this.recordB = record.clone();
        this.valueSize = map.valueSize();
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        if (remaining > 0) {
            counter.add(remaining);
            remaining = 0;
        }
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public MapRecord getRecord() {
        return recordA;
    }

    @Override
    public MapRecord getRecordB() {
        return recordB;
    }

    @Override
    public boolean hasNext() {
        if (remaining > 0) {
            recordA.of(heapAddr);
            final int keySize = Unsafe.getUnsafe().getInt(heapAddr);
            heapAddr = Bytes.align8b(heapAddr + OrderedMap.VAR_KEY_HEADER_SIZE + keySize + valueSize);
            remaining--;
            return true;
        }
        return false;
    }

    @Override
    public OrderedMapVarSizeCursor init(long heapStart, long heapLimit, int size) {
        this.heapAddr = this.heapStart = heapStart;
        this.remaining = this.size = size;
        recordA.setLimit(heapLimit);
        recordB.setLimit(heapLimit);
        return this;
    }

    @Override
    public void longTopK(DirectLongLongSortedList list, Function recordFunction) {
        long addr = heapStart;
        for (int i = 0; i < size; i++) {
            recordA.of(addr);
            long v = recordFunction.getLong(recordA);
            list.add(addr, v);
            addr += Bytes.align8b(OrderedMap.VAR_KEY_HEADER_SIZE + recordA.keySize() + valueSize);
        }
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        ((OrderedMapVarSizeRecord) record).of(atRowId);
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public void toTop() {
        heapAddr = heapStart;
        remaining = size;
    }
}
