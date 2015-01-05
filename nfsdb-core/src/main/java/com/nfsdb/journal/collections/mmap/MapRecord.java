/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
 */

package com.nfsdb.journal.collections.mmap;

import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.lang.cst.impl.qry.AbstractRecord;
import com.nfsdb.journal.lang.cst.impl.qry.Record;
import com.nfsdb.journal.lang.cst.impl.qry.RecordMetadata;
import com.nfsdb.journal.utils.Unsafe;

import java.io.InputStream;
import java.io.OutputStream;

public class MapRecord extends AbstractRecord {
    private final int split;
    private final int keyDataOffset;
    private final int keyBlockOffset;
    private final int valueOffsets[];
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

    MapRecord init(long address) {
        this.address0 = address;
        this.address1 = address + keyDataOffset;
        this.address2 = address + keyBlockOffset;
        return this;
    }

    private long address0(int index) {

        if (index < split) {
            return address0 + valueOffsets[index];
        }

        if (index == split) {
            return address1;
        }

        return Unsafe.getUnsafe().getInt(address2 + (index - split - 1) * 4) + address0;
    }

    @Override
    public double getDouble(int index) {
        return Unsafe.getUnsafe().getDouble(address0(index));
    }

    @Override
    public long getLong(int index) {
        return Unsafe.getUnsafe().getLong(address0(index));
    }

    @Override
    public int getInt(int index) {
        return Unsafe.getUnsafe().getInt(address0(index));
    }

    @Override
    public String getStr(int index) {
        long address = address0(index);
        int len = (int) (address0(index + 1) - address) >> 1;
        if (strBuf == null || strBuf.length < len) {
            strBuf = new char[len];
        }
        Unsafe.getUnsafe().copyMemory(null, address, strBuf, sun.misc.Unsafe.ARRAY_CHAR_BASE_OFFSET, ((long) len) << 1);
        return new String(strBuf, 0, len);
    }

    @Override
    public long getDate(int index) {
        return Unsafe.getUnsafe().getLong(address0(index));
    }

    @Override
    public boolean getBool(int index) {
        return Unsafe.getUnsafe().getByte(address0(index)) == 1;
    }

    @Override
    public short getShort(int index) {
        return Unsafe.getUnsafe().getShort(address0(index));
    }

    @Override
    public Record getSlave() {
        return null;
    }

    @Override
    public byte get(int index) {
        return Unsafe.getUnsafe().getByte(address0(index));
    }

    @Override
    public void getBin(int col, OutputStream s) {
        throw new JournalRuntimeException("Not implemented");
    }

    @Override
    public InputStream getBin(int col) {
        throw new JournalRuntimeException("Not implemented");
    }

    @Override
    public String getSym(int index) {
        return getStr(index);
    }
}