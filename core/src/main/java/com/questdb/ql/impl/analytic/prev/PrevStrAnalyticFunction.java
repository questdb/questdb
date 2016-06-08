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
import com.questdb.misc.Chars;
import com.questdb.misc.Misc;
import com.questdb.misc.Numbers;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.StorageFacade;
import com.questdb.ql.impl.IntMetadata;
import com.questdb.ql.impl.LongMetadata;
import com.questdb.ql.impl.RecordColumnMetadataImpl;
import com.questdb.ql.impl.analytic.AnalyticFunction;
import com.questdb.ql.impl.map.MapValues;
import com.questdb.ql.impl.map.MultiMap;
import com.questdb.std.*;
import com.questdb.store.ColumnType;
import com.questdb.store.SymbolTable;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public class PrevStrAnalyticFunction implements AnalyticFunction, Closeable {
    private static final ObjList<RecordColumnMetadata> valueColumns = new ObjList<>();
    private final MultiMap map;
    private final IntList indices;
    private final ObjList<ColumnType> types;
    private final int valueIndex;
    private final RecordColumnMetadata valueMetadata;
    private final DirectCharSequence cs = new DirectCharSequence();
    private final int addressIndex;
    private long bufPtr = 0;
    private int bufPtrLen = 0;
    private boolean nextNull = true;
    private boolean closed = false;

    public PrevStrAnalyticFunction(int pageSize, RecordMetadata parentMetadata, @Transient ObjHashSet<String> partitionBy, String columnName, String alias) {
        this.valueIndex = parentMetadata.getColumnIndex(columnName);
        // value column particulars
        this.map = new MultiMap(pageSize, parentMetadata, partitionBy, valueColumns, null);
        this.addressIndex = this.map.getMetadata().getColumnIndex(LongMetadata.INSTANCE.getName());

        // key column particulars
        this.indices = new IntList(partitionBy.size());
        this.types = new ObjList<>(partitionBy.size());
        for (int i = 0, n = partitionBy.size(); i < n; i++) {
            int index = parentMetadata.getColumnIndexQuiet(partitionBy.get(i));
            indices.add(index);
            types.add(parentMetadata.getColumn(index).getType());
        }
        this.valueMetadata = new RecordColumnMetadataImpl(alias == null ? columnName : alias, ColumnType.STRING);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        // free pointers in map values
        RecordCursor cursor = map.getCursor();
        while (cursor.hasNext()) {
            Unsafe.getUnsafe().freeMemory(cursor.next().getLong(addressIndex));
        }
        Misc.free(map);
        if (bufPtr != 0) {
            Unsafe.getUnsafe().freeMemory(bufPtr);
        }
        closed = true;
    }

    @Override
    public byte get() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getBin(OutputStream s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DirectInputStream getBin() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getBinLen() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBool() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getDate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble() {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getFlyweightStr() {
        return nextNull ? null : cs;
    }

    @Override
    public CharSequence getFlyweightStrB() {
        return nextNull ? null : cs;
    }

    @Override
    public int getInt() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordColumnMetadata getMetadata() {
        return valueMetadata;
    }

    @Override
    public short getShort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getStr(CharSink sink) {
        sink.put(nextNull ? null : cs);
    }

    @Override
    public CharSequence getStr() {
        return nextNull ? null : cs;
    }

    @Override
    public int getStrLen() {
        return nextNull ? 0 : cs.length();
    }

    @Override
    public String getSym() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SymbolTable getSymbolTable() {
        return null;
    }

    @Override
    public void reset() {
        map.clear();
    }

    @Override
    public void scroll(Record record) {
        MultiMap.KeyWriter kw = map.keyWriter();
        for (int i = 0, n = types.size(); i < n; i++) {
            kw.put(record, indices.getQuick(i), types.getQuick(i));
        }

        MapValues values = map.getOrCreateValues(kw);
        final CharSequence str = record.getFlyweightStr(valueIndex);

        if (values.isNew()) {
            nextNull = true;
            store(str, values);
        } else {
            nextNull = false;
            long ptr = values.getLong(0);
            int len = values.getInt(1);
            copyToBuffer(ptr);
            if (toByteLen(str.length()) > len) {
                Unsafe.getUnsafe().freeMemory(ptr);
                store(str, values);
            } else {
                Chars.put(ptr, str);
            }
        }
    }

    @Override
    public void setParent(RecordCursor cursor) {
    }

    @Override
    public void setStorageFacade(StorageFacade storageFacade) {
    }

    private static int toByteLen(int charLen) {
        return charLen * 2 + 4;
    }

    private void copyToBuffer(long ptr) {
        int l = toByteLen(Unsafe.getUnsafe().getInt(ptr));
        if (l >= bufPtrLen) {
            if (bufPtr != 0) {
                Unsafe.getUnsafe().freeMemory(bufPtr);
            }
            bufPtrLen = Numbers.ceilPow2(l);
            bufPtr = Unsafe.getUnsafe().allocateMemory(bufPtrLen);
            cs.of(bufPtr + 4, bufPtr + bufPtrLen);
        } else {
            cs.of(bufPtr + 4, bufPtr + l);
        }
        Unsafe.getUnsafe().copyMemory(ptr, bufPtr, l);
    }

    private void store(CharSequence str, MapValues values) {
        int l = Numbers.ceilPow2(toByteLen(str.length()));
        long ptr = Unsafe.getUnsafe().allocateMemory(l);
        values.putLong(0, ptr);
        values.putInt(1, l);
        Chars.put(ptr, str);
    }

    static {
        valueColumns.add(LongMetadata.INSTANCE);
        valueColumns.add(IntMetadata.INSTANCE);
    }
}
