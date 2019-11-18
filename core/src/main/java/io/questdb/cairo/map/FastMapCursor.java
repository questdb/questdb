/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
    private final FastMapRecord record;
    private final FastMap map;
    private int remaining;
    private long address;
    private long topAddress;
    private int count;

    FastMapCursor(FastMapRecord record, FastMap map) {
        this.record = record;
        this.map = map;
    }

    @Override
    public long size() {
        return map.size();
    }

    @Override
    public void close() {
    }

    @Override
    public MapRecord getRecord() {
        return record;
    }

    @Override
    public boolean hasNext() {
        if (remaining > 0) {
            long address = this.address;
            this.address = address + Unsafe.getUnsafe().getInt(address);
            remaining--;
            record.of(address);
            return true;
        }
        return false;
    }

    @Override
    public MapRecord newRecord() {
        return record.clone();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        assert record instanceof FastMapRecord;
        ((FastMapRecord) record).of(atRowId);
    }

    @Override
    public void recordAt(long rowId) {
        record.of(rowId);
    }

    @Override
    public void toTop() {
        this.address = topAddress;
        this.remaining = count;
    }

    FastMapCursor init(long address, int count) {
        this.address = this.topAddress = address;
        this.remaining = this.count = count;
        return this;
    }
}
