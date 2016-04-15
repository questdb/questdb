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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.impl.join.hash;

import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.impl.map.MapValues;
import com.nfsdb.ql.impl.map.MultiMap;
import com.nfsdb.std.Mutable;
import com.nfsdb.std.ObjHashSet;
import com.nfsdb.std.ObjList;
import com.nfsdb.std.Transient;
import com.nfsdb.store.ColumnType;

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

    public RecordCursor get(MultiMap.KeyWriter key) {
        MapValues values = map.getValues(key);
        records.of(values == null ? -1 : values.getLong(0));
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
