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

package com.questdb.ql.impl.experimental;

import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.ql.Record;
import com.questdb.ql.RecordSource;
import com.questdb.ql.impl.join.LongMetadata;
import com.questdb.ql.impl.map.MapValues;
import com.questdb.ql.impl.map.MultiMap;
import com.questdb.std.IntList;
import com.questdb.std.ObjHashSet;
import com.questdb.std.ObjList;
import com.questdb.std.Transient;
import com.questdb.store.ColumnType;
import com.questdb.store.MemoryPages;

public class AnalyticFunction {

    private static ObjList<RecordColumnMetadata> valueColumn = new ObjList<>();
    private final MemoryPages pages;
    private final MultiMap map;
    private final String columnName;
    private final IntList indices;
    private final ObjList<ColumnType> types;

    public AnalyticFunction(int pageSize, RecordSource recordSource, @Transient ObjHashSet<String> partitionBy, String columnName) {
        this.pages = new MemoryPages(pageSize);
        this.map = new MultiMap(pageSize, recordSource.getMetadata(), partitionBy, valueColumn, null);
        this.columnName = columnName;
        this.indices = new IntList(partitionBy.size());
        this.types = new ObjList<>(partitionBy.size());

        RecordMetadata metadata = recordSource.getMetadata();
        for (int i = 0, n = partitionBy.size(); i < n; i++) {
            int index = metadata.getColumnIndexQuiet(partitionBy.get(i));
            indices.add(index);
            types.add(metadata.getColumn(index).getType());
        }
    }

    public void addRecord(Record record) {
        // allocate memory where we would eventually write "next" value
        long offset = pages.allocate(8);

        MultiMap.KeyWriter kw = map.keyWriter();
        for (int i = 0, n = indices.size(); i < n; i++) {
            kw.put(record, indices.getQuick(i), types.getQuick(i));
        }
        MapValues values = map.getOrCreateValues(kw);
        if (values.isNew()) {
            // if this is the first time we see partition values
            // put address of the current record in map
            // this would be address where we write value next time we see
            // partition values
            values.putLong(0, offset);
        } else {
            long theirOffset = values.getLong(0);
        }
    }

    static {
        valueColumn.add(LongMetadata.INSTANCE);
    }
}
