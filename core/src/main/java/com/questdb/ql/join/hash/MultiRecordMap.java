/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql.join.hash;

import com.questdb.ql.RecordList;
import com.questdb.ql.map.ColumnTypeResolver;
import com.questdb.ql.map.DirectMap;
import com.questdb.ql.map.DirectMapValues;
import com.questdb.ql.map.RecordKeyCopier;
import com.questdb.std.Mutable;
import com.questdb.store.*;

import java.io.Closeable;

public class MultiRecordMap implements Closeable, Mutable {
    private static final ColumnTypeResolver VALUE_RESOLVER = new ColumnTypeResolver() {
        @Override
        public int count() {
            return 2;
        }

        @Override
        public int getColumnType(int index) {
            assert index < 2;
            return ColumnType.LONG;
        }
    };

    private final DirectMap map;
    private final RecordList records;

    public MultiRecordMap(ColumnTypeResolver keyResolver, RecordMetadata valueMetadata, int keyPageSize, int valuePageSize) {
        map = new DirectMap(keyPageSize, keyResolver, VALUE_RESOLVER);
        records = new RecordList(valueMetadata, valuePageSize);
    }

    public void add(Record record) {
        DirectMapValues values = map.getOrCreateValues();
        if (values.isNew()) {
            long offset = records.append(record, -1);
            values.putLong(0, offset);
            values.putLong(1, offset);
        } else {
            values.putLong(1, records.append(record, values.getLong(1)));
        }
    }

    public void clear() {
        map.clear();
        records.clear();
    }

    @Override
    public void close() {
        map.close();
        records.close();
    }

    public RecordCursor get() {
        DirectMapValues values = map.getValues();
        records.of(values == null ? -1 : values.getLong(0));
        return records;
    }

    public Record getRecord() {
        return records.getRecord();
    }

    public void locate(RecordKeyCopier copier, Record record) {
        map.locate(copier, record);
    }

    public void setStorageFacade(StorageFacade storageFacade) {
        records.setStorageFacade(storageFacade);
    }
}
