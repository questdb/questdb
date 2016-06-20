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
import com.questdb.misc.Chars;
import com.questdb.misc.Misc;
import com.questdb.misc.Numbers;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.impl.analytic.AnalyticFunction;
import com.questdb.ql.impl.map.DirectMap;
import com.questdb.ql.impl.map.DirectMapEntry;
import com.questdb.ql.impl.map.DirectMapValues;
import com.questdb.ql.impl.map.MapUtils;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.CharSink;
import com.questdb.std.DirectCharSequence;
import com.questdb.std.DirectInputStream;
import com.questdb.std.ObjList;
import com.questdb.store.ColumnType;
import com.questdb.store.SymbolTable;
import com.questdb.store.VariableColumn;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public class PrevStrAnalyticFunction implements AnalyticFunction, Closeable {
    private final DirectMap map;
    private final DirectCharSequence cs = new DirectCharSequence();
    private final ObjList<VirtualColumn> partitionBy;
    private final VirtualColumn valueColumn;
    private long bufPtr = 0;
    private int bufPtrLen = 0;
    private boolean nextNull = true;
    private boolean closed = false;

    public PrevStrAnalyticFunction(int pageSize, ObjList<VirtualColumn> partitionBy, VirtualColumn valueColumn) {
        this.partitionBy = partitionBy;
        this.valueColumn = valueColumn;
        this.map = new DirectMap(pageSize, partitionBy.size(), MapUtils.toTypeList(ColumnType.LONG, ColumnType.BYTE));
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        // free pointers in map values
        for (DirectMapEntry e : map) {
            Unsafe.getUnsafe().freeMemory(e.getLong(0));
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
        return valueColumn;
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
        return nextNull ? VariableColumn.NULL_LEN : cs.length();
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
    public void prepare(RecordCursor cursor) {
    }

    @Override
    public void reset() {
        map.clear();
    }

    @Override
    public void scroll(Record record) {
        DirectMap.KeyWriter kw = map.keyWriter();
        for (int i = 0, n = partitionBy.size(); i < n; i++) {
            MapUtils.writeVirtualColumn(kw, record, partitionBy.getQuick(i));
        }

        DirectMapValues values = map.getOrCreateValues(kw);
        final CharSequence str = valueColumn.getFlyweightStr(record);

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

    private void store(CharSequence str, DirectMapValues values) {
        int l = Numbers.ceilPow2(toByteLen(str.length()));
        long ptr = Unsafe.getUnsafe().allocateMemory(l);
        values.putLong(0, ptr);
        values.putInt(1, l);
        Chars.put(ptr, str);
    }
}
