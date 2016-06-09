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

package com.questdb.ql.impl.analytic.prev;

import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Misc;
import com.questdb.misc.Numbers;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.impl.LongMetadata;
import com.questdb.ql.impl.map.MapValues;
import com.questdb.ql.impl.map.MultiMap;
import com.questdb.std.*;
import com.questdb.store.ColumnType;
import com.questdb.store.SymbolTable;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public class PrevRowIdValueAnalyticFunction extends AbstractPrevValueAnalyticFunction implements Closeable {
    private static final ObjList<RecordColumnMetadata> valueColumn = new ObjList<>();
    private final MultiMap map;
    private final IntList indices;
    private final ObjList<ColumnType> types;
    private RecordCursor parent;

    public PrevRowIdValueAnalyticFunction(int pageSize, RecordMetadata parentMetadata, @Transient ObjHashSet<String> partitionBy, String columnName, String alias) {

        super(parentMetadata, columnName, alias);
        this.map = new MultiMap(pageSize, parentMetadata, partitionBy, valueColumn, null);

        // key column particulars
        this.indices = new IntList(partitionBy.size());
        this.types = new ObjList<>(partitionBy.size());
        for (int i = 0, n = partitionBy.size(); i < n; i++) {
            int index = parentMetadata.getColumnIndexQuiet(partitionBy.get(i));
            indices.add(index);
            types.add(parentMetadata.getColumn(index).getType());
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        super.close();
        Misc.free(map);
    }

    @Override
    public byte get() {
        return nextNull ? 0 : getParentRecord().get(valueIndex);
    }

    @Override
    public void getBin(OutputStream s) {
        if (nextNull) {
            return;
        }
        getParentRecord().getBin(valueIndex, s);
    }

    @Override
    public DirectInputStream getBin() {
        return nextNull ? null : getParentRecord().getBin(valueIndex);
    }

    @Override
    public long getBinLen() {
        return nextNull ? 0 : getParentRecord().getBinLen(valueIndex);
    }

    @Override
    public boolean getBool() {
        return !nextNull && getParentRecord().getBool(valueIndex);
    }

    @Override
    public long getDate() {
        return nextNull ? Numbers.LONG_NaN : getParentRecord().getDate(valueIndex);
    }

    @Override
    public double getDouble() {
        return nextNull ? Double.NaN : getParentRecord().getDouble(valueIndex);
    }

    @Override
    public float getFloat() {
        return nextNull ? Float.NaN : getParentRecord().getFloat(valueIndex);
    }

    @Override
    public CharSequence getFlyweightStr() {
        return nextNull ? null : getParentRecord().getFlyweightStr(valueIndex);
    }

    @Override
    public CharSequence getFlyweightStrB() {
        return nextNull ? null : getParentRecord().getFlyweightStrB(valueIndex);
    }

    @Override
    public int getInt() {
        if (nextNull) {
            if (valueType == ColumnType.SYMBOL) {
                return SymbolTable.VALUE_IS_NULL;
            }
            return Numbers.INT_NaN;
        }
        return getParentRecord().getInt(valueIndex);
    }

    @Override
    public long getLong() {
        return nextNull ? Numbers.LONG_NaN : getParentRecord().getLong(valueIndex);
    }

    @Override
    public short getShort() {
        return nextNull ? 0 : getParentRecord().getShort(valueIndex);
    }

    @Override
    public void getStr(CharSink sink) {
        if (nextNull) {
            sink.put((CharSequence) null);
        } else {
            getParentRecord().getStr(valueIndex, sink);
        }
    }

    @Override
    public CharSequence getStr() {
        return nextNull ? null : getParentRecord().getStr(valueIndex);
    }

    @Override
    public int getStrLen() {
        return nextNull ? 0 : getParentRecord().getStrLen(valueIndex);
    }

    @Override
    public String getSym() {
        return nextNull ? null : getParentRecord().getSym(valueIndex);
    }

    @Override
    public void reset() {
        super.reset();
        map.clear();
    }

    @Override
    public void setParent(RecordCursor cursor) {
        parent = cursor;
    }

    @Override
    public void scroll(Record record) {
        MultiMap.KeyWriter kw = map.keyWriter();
        for (int i = 0, n = types.size(); i < n; i++) {
            kw.put(record, indices.getQuick(i), types.getQuick(i));
        }

        MapValues values = map.getOrCreateValues(kw);
        if (values.isNew()) {
            nextNull = true;
        } else {
            nextNull = false;
            Unsafe.getUnsafe().putLong(bufPtr, values.getLong(0));
        }
        values.putLong(0, record.getRowId());
    }

    private Record getParentRecord() {
        return parent.recordAt(Unsafe.getUnsafe().getLong(bufPtr));
    }

    static {
        valueColumn.add(LongMetadata.INSTANCE);
    }

}
