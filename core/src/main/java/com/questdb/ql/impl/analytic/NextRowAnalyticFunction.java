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

package com.questdb.ql.impl.analytic;

import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.impl.NullRecord;
import com.questdb.ql.impl.RecordColumnMetadataImpl;
import com.questdb.ql.impl.RecordList;
import com.questdb.ql.impl.RecordListRecord;
import com.questdb.ql.impl.join.LongMetadata;
import com.questdb.ql.impl.map.MapValues;
import com.questdb.ql.impl.map.MultiMap;
import com.questdb.std.*;
import com.questdb.store.ColumnType;
import com.questdb.store.MemoryPages;

public class NextRowAnalyticFunction extends AbstractAnalyticFunction {

    private static ObjList<RecordColumnMetadata> valueColumn = new ObjList<>();
    private final MemoryPages pages;
    private final MultiMap map;
    private final IntList indices;
    private final ObjList<ColumnType> types;
    private final RecordColumnMetadata metadata;
    private final int columnIndex;
    private long offset;
    private RecordListRecord record;
    private Record next;

    public NextRowAnalyticFunction(int pageSize, RecordMetadata parentMetadata, @Transient ObjHashSet<String> partitionBy, String columnName) {
        this.pages = new MemoryPages(pageSize);
        this.map = new MultiMap(pageSize, parentMetadata, partitionBy, valueColumn, null);
        this.indices = new IntList(partitionBy.size());
        this.types = new ObjList<>(partitionBy.size());

        for (int i = 0, n = partitionBy.size(); i < n; i++) {
            int index = parentMetadata.getColumnIndexQuiet(partitionBy.get(i));
            indices.add(index);
            types.add(parentMetadata.getColumn(index).getType());
        }
        this.columnIndex = parentMetadata.getColumnIndex(columnName);
        this.metadata = new RecordColumnMetadataImpl(columnName, parentMetadata.getColumnQuick(this.columnIndex).getType());
    }

    @Override
    public void addRecord(Record record, long rowid) {
        MultiMap.KeyWriter kw = map.keyWriter();
        for (int i = 0, n = indices.size(); i < n; i++) {
            kw.put(record, indices.getQuick(i), types.getQuick(i));
        }
        MapValues values = map.getOrCreateValues(kw);
        // allocateOffset memory where we would eventually write "next" value
        final long address = pages.allocate(8);
        if (!values.isNew()) {
            Unsafe.getUnsafe().putLong(values.getLong(0), rowid);
        }
        values.putLong(0, address);
        Unsafe.getUnsafe().putLong(address, -1);
    }

    @Override
    public RecordColumnMetadata getMetadata() {
        return metadata;
    }

    @Override
    public void prepare(RecordList base) {
        this.record = base.newRecord();
        this.offset = 0;
    }

    @Override
    public void scroll() {
        long rowid = Unsafe.getUnsafe().getLong(pages.addressOf(this.offset));
        if (rowid == -1) {
            next = NullRecord.INSTANCE;
        } else {
            record.of(rowid);
            next = record;
        }
        this.offset += 8;
    }

    @Override
    public byte get() {
        return next.get(columnIndex);
    }

    @Override
    public int getInt() {
        return next.getInt(columnIndex);
    }

    @Override
    public void getStr(CharSink sink) {
        next.getStr(columnIndex, sink);
    }

    static {
        valueColumn.add(LongMetadata.INSTANCE);
    }
}
