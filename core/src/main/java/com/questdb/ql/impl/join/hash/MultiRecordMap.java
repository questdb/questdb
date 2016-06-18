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

package com.questdb.ql.impl.join.hash;

import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.StorageFacade;
import com.questdb.ql.impl.RecordList;
import com.questdb.ql.impl.map.DirectMap;
import com.questdb.ql.impl.map.MapValues;
import com.questdb.std.Mutable;
import com.questdb.std.ObjList;
import com.questdb.store.ColumnType;

import java.io.Closeable;
import java.io.IOException;

public class MultiRecordMap implements Closeable, Mutable {
    private static final ObjList<ColumnType> valueCols = new ObjList<>(2);
    private final DirectMap map;
    private final RecordList records;

    public MultiRecordMap(int keyCount, RecordMetadata valueMetadata, int keyPageSize, int valuePageSize) {
        map = new DirectMap(keyPageSize, keyCount, valueCols);
        records = new RecordList(valueMetadata, valuePageSize);
    }

    public void add(DirectMap.KeyWriter key, Record record) {
        MapValues values = map.getOrCreateValues(key);
        if (values.isNew()) {
            long offset = records.append(record, -1);
            values.putLong(0, offset);
            values.putLong(1, offset);
        } else {
            values.putLong(1, records.append(record, values.getLong(1)));
        }
    }

    public DirectMap.KeyWriter claimKey() {
        return map.keyWriter();
    }

    public void clear() {
        map.clear();
        records.clear();
    }

    @Override
    public void close() throws IOException {
        map.close();
        records.close();
    }

    public RecordCursor get(DirectMap.KeyWriter key) {
        MapValues values = map.getValues(key);
        records.of(values == null ? -1 : values.getLong(0));
        return records;
    }

    public void setStorageFacade(StorageFacade storageFacade) {
        records.setStorageFacade(storageFacade);
    }

    static {
        valueCols.add(ColumnType.LONG);
        valueCols.add(ColumnType.LONG);
    }
}
