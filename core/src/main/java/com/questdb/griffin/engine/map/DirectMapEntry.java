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

package com.questdb.griffin.engine.map;

import com.questdb.cairo.TableUtils;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.common.ColumnType;
import com.questdb.std.Transient;
import com.questdb.std.Unsafe;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.DirectCharSequence;

public final class DirectMapEntry {
    private final int split;
    private final int keyDataOffset;
    private final int keyBlockOffset;
    private final int valueOffsets[];
    private final DirectCharSequence csA[];
    private final DirectCharSequence csB[];
    private long address0;
    private long address1;
    private long address2;
    private char[] strBuf = null;

    DirectMapEntry(
            int valueOffsets[],
            int keyDataOffset,
            int keyBlockOffset,
            @Transient RecordMetadata keyMetadata
    ) {
        this.split = valueOffsets.length;
        this.valueOffsets = valueOffsets;
        this.keyBlockOffset = keyBlockOffset;
        this.keyDataOffset = keyDataOffset;

        int n = keyMetadata.getColumnCount();

        DirectCharSequence csA[] = null;
        DirectCharSequence csB[] = null;

        for (int i = 0; i < n; i++) {
            if (keyMetadata.getColumnType(i) == ColumnType.STRING) {
                if (csA == null) {
                    csA = new DirectCharSequence[n + split];
                    csB = new DirectCharSequence[n + split];
                }
                csA[i + split] = new DirectCharSequence();
                csB[i + split] = new DirectCharSequence();
            }
        }

        this.csA = csA;
        this.csB = csB;
    }

    public byte get(int index) {
        return Unsafe.getUnsafe().getByte(address0(index));
    }

    public boolean getBool(int index) {
        return Unsafe.getBool(address0(index));
    }

    public long getDate(int index) {
        return Unsafe.getUnsafe().getLong(address0(index));
    }

    public double getDouble(int index) {
        return Unsafe.getUnsafe().getDouble(address0(index));
    }

    public float getFloat(int index) {
        return Unsafe.getUnsafe().getFloat(address0(index));
    }

    public CharSequence getFlyweightStr(int index) {
        assert index < csA.length;
        return getFlyweightStr0(index, Unsafe.arrayGet(csA, index));
    }

    public CharSequence getFlyweightStrB(int index) {
        return getFlyweightStr0(index, Unsafe.arrayGet(csB, index));
    }

    public int getInt(int index) {
        return Unsafe.getUnsafe().getInt(address0(index));
    }

    public long getLong(int index) {
        return Unsafe.getUnsafe().getLong(address0(index));
    }

    public long getRowId() {
        return address0;
    }

    public short getShort(int index) {
        return Unsafe.getUnsafe().getShort(address0(index));
    }

    public String getStr(int index) {
        long address = address0(index);
        int len = Unsafe.getUnsafe().getInt(address);
        if (len == -1) {
            return null;
        }
        if (strBuf == null || strBuf.length < len) {
            strBuf = new char[len];
        }
        Unsafe.getUnsafe().copyMemory(null, address + 4, strBuf, Unsafe.CHAR_OFFSET, ((long) len) << 1);
        return new String(strBuf, 0, len);
    }

    public void getStr(int index, CharSink sink) {
        long address = address0(index);
        int len = Unsafe.getUnsafe().getInt(address);
        address += 4;
        for (int i = 0; i < len; i++) {
            sink.put(Unsafe.getUnsafe().getChar(address));
            address += 2;
        }
    }

    public int getStrLen(int index) {
        return Unsafe.getUnsafe().getInt(address0(index));
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

    private CharSequence getFlyweightStr0(int index, DirectCharSequence cs) {
        long address = address0(index);
        int len = Unsafe.getUnsafe().getInt(address);
        return len == TableUtils.NULL_LEN ? null : cs.of(address + 4, address + 4 + len * 2);
    }

    DirectMapEntry init(long address) {
        this.address0 = address;
        this.address1 = address + keyDataOffset;
        this.address2 = address + keyBlockOffset;
        return this;
    }
}