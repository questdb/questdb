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

package io.questdb.cairo.map;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.Unsafe;

public final class Unordered8MapCursor implements MapRecordCursor {
    private static final int SCAN_SLOTS = 64;
    private static final int SPARSE_SLOTS_PER_ENTRY = 8;
    private final long entrySize;
    private final Unordered8Map map;
    private final Unordered8MapRecord recordA;
    private final Unordered8MapRecord recordB;
    private long address;
    private int count;
    private boolean isSparse;
    private int liveCount;
    private long[] liveEntries;
    private int livePos;
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
            if (livePos == liveCount) {
                scan();
            }
            recordA.of(liveEntries[livePos++]);
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
        liveCount = livePos = 0;
        isSparse = entrySize * count * SPARSE_SLOTS_PER_ENTRY < memLimit - memStart;
    }

    // Slot emptiness is random, so a branch per slot mispredicts about once per live entry at the usual load.
    // Only a sparse table, where the branch predicts well, takes it.
    private void scan() {
        // A sharded map has hundreds of these cursors, and most never iterate.
        if (liveEntries == null) {
            liveEntries = new long[SCAN_SLOTS];
        }
        long addr = address;
        int live = 0;
        if (isSparse) {
            while (addr < memLimit && map.isZeroKey(addr)) {
                addr += entrySize;
            }
            liveEntries[live++] = addr;
            addr += entrySize;
        } else {
            while (live == 0 && addr < memLimit) {
                final long limit = Math.min(memLimit, addr + SCAN_SLOTS * entrySize);
                for (; addr < limit; addr += entrySize) {
                    final long key = Unsafe.getLong(addr);
                    liveEntries[live] = addr;
                    live += (int) ((key | -key) >>> 63);
                }
            }
        }
        address = addr;
        liveCount = live;
        livePos = 0;
    }

    Unordered8MapCursor init(long memStart, long memLimit, long zeroKeyAddress, int count) {
        this.memStart = memStart;
        this.memLimit = memLimit;
        this.count = count;
        this.zeroKeyAddress = zeroKeyAddress;
        toTop();
        return this;
    }
}
