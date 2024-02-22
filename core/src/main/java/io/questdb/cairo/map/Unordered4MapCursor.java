/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;

public final class Unordered4MapCursor implements MapRecordCursor {
    private final long entrySize;
    private final Unordered4Map map;
    private final Unordered4MapRecord recordA;
    private final Unordered4MapRecord recordB;
    private long address;
    private int count;
    private long limit;
    private int remaining;
    private long topAddress;
    private long zeroKeyAddress; // set to 0 when there is no zero

    Unordered4MapCursor(Unordered4MapRecord record, Unordered4Map map) {
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
    public void recordAt(Record record, long atRowId) {
        ((Unordered4MapRecord) record).of(atRowId);
    }

    @Override
    public long size() {
        return map.size();
    }

    @Override
    public void toTop() {
        address = topAddress;
        remaining = count;
        if (count > 0 && (zeroKeyAddress == 0 || count > 1) && map.isZeroKey(address)) {
            skipToNonZeroKey();
        }
    }

    private void skipToNonZeroKey() {
        do {
            address += entrySize;
        } while (address < limit && map.isZeroKey(address));
    }

    Unordered4MapCursor init(long address, long limit, long zeroKeyAddress, int count) {
        this.topAddress = address;
        this.limit = limit;
        this.count = count;
        this.zeroKeyAddress = zeroKeyAddress;
        toTop();
        recordA.setLimit(limit);
        recordB.setLimit(limit);
        return this;
    }
}
