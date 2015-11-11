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

import com.nfsdb.collections.Mutable;
import com.nfsdb.collections.ObjHashSet;
import com.nfsdb.collections.ObjList;
import com.nfsdb.collections.Transient;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.storage.ColumnType;

import java.io.Closeable;
import java.io.IOException;

public class MultiRecordMap implements Closeable, Mutable {
    private static final ObjList<RecordColumnMetadata> valueCols = new ObjList<>(2);
    private final MultiMap map;
    private final RecordDequeue records;

    public MultiRecordMap(
            @Transient RecordMetadata keyMetadata,
            @Transient ObjHashSet<String> keyNames,
            RecordMetadata valueMetadata) {
        map = new MultiMap(keyMetadata, keyNames, valueCols, null);
        //todo: extract config
        records = new RecordDequeue(valueMetadata, 4 * 1024 * 1024);
    }

    public void add(MultiMap.KeyWriter key, Record record) {
        MapValues values = map.getOrCreateValues(key);
        if (values.isNew()) {
            long offset = records.append(record, -1);
            values.putLong(0, offset);
            values.putLong(1, offset);
        } else {
            values.putLong(1, records.append(record, values.getLong(1)));
        }
    }

    public MultiMap.KeyWriter claimKey() {
        return map.keyWriter();
    }

    public void clear() {
        map.clear();
        records.clear();
    }

    @Override
    public void close() throws IOException {
        map.free();
        records.close();
    }

    public RecordCursor<Record> get(MultiMap.KeyWriter key) {
        MapValues values = map.getValues(key);
        records.init(values == null ? -1 : values.getLong(0));
        return records;
    }

    public void setStorageFacade(StorageFacade storageFacade) {
        records.setStorageFacade(storageFacade);
    }

    static {
        ColumnMetadata top = new ColumnMetadata();
        top.setName("top");
        top.setType(ColumnType.LONG);

        ColumnMetadata curr = new ColumnMetadata();
        curr.setName("current");
        curr.setType(ColumnType.LONG);

        valueCols.add(top);
        valueCols.add(curr);
    }
}
