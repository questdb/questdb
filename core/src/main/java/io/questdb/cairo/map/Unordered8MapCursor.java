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

public final class Unordered8MapCursor implements MapRecordCursor {
    private final long entrySize;
    private final Unordered8Map map;
    private final Unordered8MapRecord recordA;
    private final Unordered8MapRecord recordB;
    private long address;
    private int count;
    private long memLimit;
    private long memStart;
    private int remaining;
    private long zeroKeyAddress; // set to 0 when there is no zero

    Unordered8MapCursor(Unordered8MapRecord record, Unordered8Map map) {
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
            if (remaining == 1 && zeroKeyAddress != 0) {
                recordA.of(zeroKeyAddress);
                remaining--;
                return true;
            }
            recordA.of(address);
            skipToNonZeroKey();
            remaining--;
            return true;
        }
        return false;
    }

    @Override
    public void longTopK(DirectLongLongSortedList list, Function recordFunction) {
        // First, we handle zero key.
        if (zeroKeyAddress != 0) {
            recordA.of(zeroKeyAddress);
            long v = recordFunction.getLong(recordA);
            list.add(zeroKeyAddress, v);
        }

        // Then we handle all non-zero keys.
        for (long addr = memStart; addr < memLimit; addr += entrySize) {
            if (!map.isZeroKey(addr)) {
                recordA.of(addr);
                long v = recordFunction.getLong(recordA);
                list.add(addr, v);
            }
        }
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        ((Unordered8MapRecord) record).of(atRowId);
    }

    @Override
    public long size() {
        return map.size();
    }

    @Override
    public void toTop() {
        address = memStart;
        remaining = count;
        if (count > 0 && (zeroKeyAddress == 0 || count > 1) && map.isZeroKey(address)) {
            skipToNonZeroKey();
        }
    }

    private void skipToNonZeroKey() {
        do {
            address += entrySize;
        } while (address < memLimit && map.isZeroKey(address));
    }

    Unordered8MapCursor init(long memStart, long memLimit, long zeroKeyAddress, int count) {
        this.memStart = memStart;
        this.memLimit = memLimit;
        this.count = count;
        this.zeroKeyAddress = zeroKeyAddress;
        toTop();
        recordA.setLimit(memLimit);
        recordB.setLimit(memLimit);
        return this;
    }
}
