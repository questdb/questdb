/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.collections;

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.collections.ObjList;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.utils.Unsafe;

public final class MapRecordSource extends AbstractImmutableIterator<Record> implements RecordCursor<Record> {
    private final MapRecord record;
    private final MapValues values;
    private final ObjList<MapRecordValueInterceptor> interceptors;
    private final int interceptorsLen;
    private int count;
    private long address;

    MapRecordSource(MapRecord record, MapValues values, ObjList<MapRecordValueInterceptor> interceptors) {
        this.record = record;
        this.values = values;
        this.interceptors = interceptors;
        this.interceptorsLen = interceptors != null ? interceptors.size() : 0;
    }

    @Override
    public Record getByRowId(long rowId) {
        return null;
    }

    @Override
    public RecordMetadata getMetadata() {
        return record.getMetadata();
    }

    @Override
    public StorageFacade getStorageFacade() {
        return null;
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

    MapRecordSource init(long address, int count) {
        this.address = address;
        this.count = count;
        return this;
    }

    private void notifyInterceptors(long address) {
        for (int i = 0; i < interceptorsLen; i++) {
            interceptors.getQuick(i).beforeRecord(values.init(address, false));
        }
    }
}
