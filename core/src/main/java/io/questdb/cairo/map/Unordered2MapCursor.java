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

package io.questdb.cairo.map;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.DirectLongLongHeap;

public final class Unordered2MapCursor implements MapRecordCursor {
    private final long entrySize;
    private final Unordered2Map map;
    private final Unordered2MapRecord recordA;
    private final Unordered2MapRecord recordB;
    private long address;
    private int count;
    private boolean hasZero;
    private long memLimit;
    private long memStart;
    private int remaining;

    Unordered2MapCursor(Unordered2MapRecord record, Unordered2Map map) {
        this.recordA = record;
        this.recordB = record.clone();
        this.map = map;
        this.entrySize = map.entrySize();
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
            recordA.of(address);
            skipToNonZeroKey();
            remaining--;
            return true;
        }
        return false;
    }

    @Override
    public void longTopK(DirectLongLongHeap heap, Function recordFunction) {
        // First, we handle zero key.
        if (hasZero) {
            recordA.of(memStart);
            long v = recordFunction.getLong(recordA);
            heap.add(memStart, v);
        }

        // Then we handle all non-zero keys.
        for (long addr = memStart + entrySize; addr < memLimit; addr += entrySize) {
            if (!map.isZeroKey(addr)) {
                recordA.of(addr);
                long v = recordFunction.getLong(recordA);
                heap.add(addr, v);
            }
        }
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        ((Unordered2MapRecord) record).of(atRowId);
    }

    @Override
    public long size() {
        return map.size();
    }

    @Override
    public void toTop() {
        address = memStart;
        remaining = count;
        if (!hasZero) {
            skipToNonZeroKey();
        }
    }

    private void skipToNonZeroKey() {
        do {
            address += entrySize;
        } while (address < memLimit && map.isZeroKey(address));
    }

    Unordered2MapCursor init(long memStart, long memLimit, boolean hasZero, int count) {
        this.memStart = memStart;
        this.memLimit = memLimit;
        this.count = count;
        this.hasZero = hasZero;
        toTop();
        recordA.setLimit(memLimit);
        recordB.setLimit(memLimit);
        return this;
    }
}
