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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;

public final class Unordered8MapCursor implements MapRecordCursor {
    private final long entrySize;
    private final Unordered8Map map;
    private final Unordered8MapRecord recordA;
    private final Unordered8MapRecord recordB;
    private long address;
    private long capacity;
    private long count;
    private long index;
    private long remaining;

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
        // TODO iterate with SWAR
        if (remaining > 0) {
            recordA.of(address + index * entrySize);
            if (--remaining > 0) {
                skipToExistingKey();
            }
            return true;
        }
        return false;
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
        index = 0;
        remaining = count;
        if (count > 0 && map.isEmptyKey(index)) {
            skipToExistingKey();
        }
    }

    private void skipToExistingKey() {
        while (++index < capacity) {
            if (!map.isEmptyKey(index)) {
                break;
            }
        }
    }

    Unordered8MapCursor init(long address, long count, long capacity) {
        this.address = address;
        this.count = count;
        this.capacity = capacity;
        toTop();
        return this;
    }
}
