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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.Unsafe;

public final class FastMapCursor implements RecordCursor {
    // Set to -1 when key-value pair is var-size.
    private final int keyValueSize;
    private final FastMap map;
    private final FastMapRecord recordA;
    private final MapRecord recordB;
    private long address;
    private int count;
    private long limit;
    private int remaining;
    private long topAddress;

    FastMapCursor(FastMapRecord record, FastMap map) {
        this.recordA = record;
        this.recordB = record.clone();
        this.map = map;
        if (map.keySize() != -1) {
            keyValueSize = map.keySize() + map.valueSize();
        } else {
            keyValueSize = -1;
        }
    }

    @Override
    public void close() {
        map.restoreInitialCapacity();
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
            long address = this.address;
            if (keyValueSize == -1) {
                int keySize = Unsafe.getUnsafe().getInt(address);
                this.address = address + Integer.BYTES + keySize + map.valueSize();
            } else {
                this.address = address + keyValueSize;
            }
            // Key-value pairs start at 8 byte aligned addresses, so we may need to align the next pointer.
            if ((this.address & 0x7) != 0) {
                this.address |= 0x7;
                this.address++;
            }

            remaining--;
            recordA.of(address, limit);
            return true;
        }
        return false;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        ((FastMapRecord) record).of(atRowId, limit);
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

    FastMapCursor init(long address, long limit, int count) {
        this.address = this.topAddress = address;
        this.limit = limit;
        this.remaining = this.count = count;
        return this;
    }
}
