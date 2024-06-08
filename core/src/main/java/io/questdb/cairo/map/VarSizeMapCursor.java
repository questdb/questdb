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
import io.questdb.std.Unsafe;
import io.questdb.std.bytes.Bytes;

public final class VarSizeMapCursor implements MapRecordCursor {
    private final VarSizeMap map;
    private final VarSizeMapRecord recordA;
    private final VarSizeMapRecord recordB;
    private final long valueSize;
    private long address;
    private int count;
    private int remaining;
    private long topAddress;

    VarSizeMapCursor(VarSizeMapRecord record, VarSizeMap map) {
        this.recordA = record;
        this.recordB = record.clone();
        this.map = map;
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
            recordA.of(address);
            int keySize = Unsafe.getUnsafe().getInt(address);
            address = Bytes.align8b(address + VarSizeMap.VAR_KEY_HEADER_SIZE + keySize + valueSize);
            remaining--;
            return true;
        }
        return false;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        ((VarSizeMapRecord) record).of(atRowId);
    }

    @Override
    public long size() {
        return map.size();
    }

    @Override
    public void toTop() {
        address = topAddress;
        remaining = count;
    }

    VarSizeMapCursor init(long address, long limit, int count) {
        this.address = this.topAddress = address;
        this.remaining = this.count = count;
        recordA.setLimit(limit);
        recordB.setLimit(limit);
        return this;
    }
}
