/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.ql.impl.map;

import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.std.AbstractImmutableIterator;
import com.questdb.std.ObjList;

public final class MapRecordCursor extends AbstractImmutableIterator<Record> {
    private final MapRecord record;
    private final MapValues values;
    private final ObjList<MapRecordValueInterceptor> interceptors;
    private final int interceptorsLen;
    private int count;
    private long address;

    MapRecordCursor(
            RecordMetadata metadata,
            int valueOffsets[],
            int keyDataOffset,
            int keyBlockOffset,
            MapValues values,
            ObjList<MapRecordValueInterceptor> interceptors
    ) {
        this.record = new MapRecord(metadata, valueOffsets, keyDataOffset, keyBlockOffset);
        this.values = values;
        this.interceptors = interceptors;
        this.interceptorsLen = interceptors != null ? interceptors.size() : 0;
    }

    @Override
    public boolean hasNext() {
        return count > 0;
    }

    @Override
    public Record next() {
        long address = this.address;
        this.address = address + Unsafe.getUnsafe().getInt(address);
        count--;
        if (interceptorsLen > 0) {
            notifyInterceptors(address);
        }
        return record.init(address);
    }

    MapRecordCursor init(long address, int count) {
        this.address = address;
        this.count = count;
        return this;
    }

    private void notifyInterceptors(long address) {
        for (int i = 0; i < interceptorsLen; i++) {
            interceptors.getQuick(i).beforeRecord(values.of(address, false));
        }
    }
}
