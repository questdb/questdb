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

package com.questdb.ql.impl.map;

import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Unsafe;
import com.questdb.ql.AbstractRecord;
import com.questdb.std.CharSink;
import com.questdb.std.DirectCharSequence;
import com.questdb.std.DirectInputStream;

import java.io.OutputStream;

final class MapRecord extends AbstractRecord {
    private final int split;
    private final int keyDataOffset;
    private final int keyBlockOffset;
    private final int valueOffsets[];
    private final DirectCharSequence csA = new DirectCharSequence();
    private final DirectCharSequence csB = new DirectCharSequence();
    private long address0;
    private long address1;
    private long address2;
    private char[] strBuf = null;

    MapRecord(RecordMetadata metadata, int valueOffsets[], int keyDataOffset, int keyBlockOffset) {
        super(metadata);
        this.split = valueOffsets.length;
        this.valueOffsets = valueOffsets;
        this.keyBlockOffset = keyBlockOffset;
        this.keyDataOffset = keyDataOffset;
    }

    @Override
    public byte get(int index) {
        return Unsafe.getUnsafe().getByte(address0(index));
    }

    @Override
    public void getBin(int col, OutputStream s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DirectInputStream getBin(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getBinLen(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBool(int index) {
        return Unsafe.getBool(address0(index));
    }

    @Override
    public long getDate(int index) {
        return Unsafe.getUnsafe().getLong(address0(index));
    }

    @Override
    public double getDouble(int index) {
        return Unsafe.getUnsafe().getDouble(address0(index));
    }

    @Override
    public float getFloat(int index) {
        return Unsafe.getUnsafe().getFloat(address0(index));
    }

    @Override
    public CharSequence getFlyweightStr(int index) {
        long address = address0(index);
        int len = Unsafe.getUnsafe().getInt(address);
        return csA.of(address + 4, address + 4 + len * 2);
    }

    @Override
    public CharSequence getFlyweightStrB(int index) {
        long address = address0(index);
        int len = Unsafe.getUnsafe().getInt(address);
        return csB.of(address + 4, address + 4 + len * 2);
    }

    @Override
    public int getInt(int index) {
        return Unsafe.getUnsafe().getInt(address0(index));
    }

    @Override
    public long getLong(int index) {
        return Unsafe.getUnsafe().getLong(address0(index));
    }

    @Override
    public long getRowId() {
        return address0;
    }

    @Override
    public short getShort(int index) {
        return Unsafe.getUnsafe().getShort(address0(index));
    }

    @Override
    public String getStr(int index) {
        long address = address0(index);
        int len = Unsafe.getUnsafe().getInt(address);
        if (strBuf == null || strBuf.length < len) {
            strBuf = new char[len];
        }
        Unsafe.getUnsafe().copyMemory(null, address + 4, strBuf, Unsafe.CHAR_OFFSET, ((long) len) << 1);
        return new String(strBuf, 0, len);
    }

    @Override
    public void getStr(int index, CharSink sink) {
        long address = address0(index);
        int len = Unsafe.getUnsafe().getInt(address);
        address += 4;
        for (int i = 0; i < len; i++) {
            sink.put(Unsafe.getUnsafe().getChar(address));
            address += 2;
        }
    }

    @Override
    public int getStrLen(int index) {
        return Unsafe.getUnsafe().getInt(address0(index));
    }

    @Override
    public String getSym(int index) {
        return metadata.getColumn(index).getSymbolTable().value(getInt(index));
    }

    private long address0(int index) {

        if (index < split) {
            return address0 + Unsafe.arrayGet(valueOffsets, index);
        }

        if (index == split) {
            return address1;
        }

        return Unsafe.getUnsafe().getInt(address2 + (index - split - 1) * 4) + address0;
    }

    MapRecord init(long address) {
        this.address0 = address;
        this.address1 = address + keyDataOffset;
        this.address2 = address + keyBlockOffset;
        return this;
    }
}