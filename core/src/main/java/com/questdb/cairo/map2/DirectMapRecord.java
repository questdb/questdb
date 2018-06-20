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

package com.questdb.cairo.map2;

import com.questdb.cairo.ColumnTypes;
import com.questdb.cairo.TableUtils;
import com.questdb.cairo.sql.Record;
import com.questdb.common.ColumnType;
import com.questdb.std.BinarySequence;
import com.questdb.std.Transient;
import com.questdb.std.Unsafe;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.DirectCharSequence;

public final class DirectMapRecord implements Record {
    private final int split;
    private final int keyDataOffset;
    private final int keyBlockOffset;
    private final int valueOffsets[];
    private final DirectCharSequence csA[];
    private final DirectCharSequence csB[];
    private final DirectBinarySequence bs[];
    private final DirectMapValues values;
    private long address0;
    private long address1;
    private long address2;

    DirectMapRecord(
            int valueOffsets[],
            int keyDataOffset,
            int keyBlockOffset,
            DirectMapValues values,
            @Transient ColumnTypes keyTypes) {
        this.split = valueOffsets.length;
        this.valueOffsets = valueOffsets;
        this.keyBlockOffset = keyBlockOffset;
        this.keyDataOffset = keyDataOffset;
        this.values = values;

        int n = keyTypes.getColumnCount();

        DirectCharSequence csA[] = null;
        DirectCharSequence csB[] = null;
        DirectBinarySequence bs[] = null;

        for (int i = 0; i < n; i++) {
            switch (keyTypes.getColumnType(i)) {
                case ColumnType.STRING:
                    if (csA == null) {
                        csA = new DirectCharSequence[n + split];
                        csB = new DirectCharSequence[n + split];
                    }
                    csA[i + split] = new DirectCharSequence();
                    csB[i + split] = new DirectCharSequence();
                    break;
                case ColumnType.BINARY:
                    if (bs == null) {
                        bs = new DirectBinarySequence[n + split];
                    }
                    bs[i + split] = new DirectBinarySequence();
                    break;
                default:
                    break;
            }
        }

        this.csA = csA;
        this.csB = csB;
        this.bs = bs;
    }

    @Override
    public byte getByte(int columnIndex) {
        return Unsafe.getUnsafe().getByte(addressOfColumn(columnIndex));
    }

    @Override
    public boolean getBool(int columnIndex) {
        return Unsafe.getBool(addressOfColumn(columnIndex));
    }

    @Override
    public double getDouble(int columnIndex) {
        return Unsafe.getUnsafe().getDouble(addressOfColumn(columnIndex));
    }

    @Override
    public float getFloat(int columnIndex) {
        return Unsafe.getUnsafe().getFloat(addressOfColumn(columnIndex));
    }

    @Override
    public CharSequence getStr(int columnIndex) {
        assert columnIndex < csA.length;
        return getStr0(columnIndex, Unsafe.arrayGet(csA, columnIndex));
    }

    @Override
    public CharSequence getStrB(int columnIndex) {
        return getStr0(columnIndex, Unsafe.arrayGet(csB, columnIndex));
    }

    @Override
    public int getInt(int columnIndex) {
        return Unsafe.getUnsafe().getInt(addressOfColumn(columnIndex));
    }

    @Override
    public long getLong(int columnIndex) {
        return Unsafe.getUnsafe().getLong(addressOfColumn(columnIndex));
    }

    @Override
    public long getRowId() {
        return address0;
    }

    @Override
    public short getShort(int columnIndex) {
        return Unsafe.getUnsafe().getShort(addressOfColumn(columnIndex));
    }

    @Override
    public void getStr(int columnIndex, CharSink sink) {
        long address = addressOfColumn(columnIndex);
        int len = Unsafe.getUnsafe().getInt(address);
        address += 4;
        for (int i = 0; i < len; i++) {
            sink.put(Unsafe.getUnsafe().getChar(address));
            address += 2;
        }
    }

    @Override
    public BinarySequence getBin(int columnIndex) {
        long address = addressOfColumn(columnIndex);
        int len = Unsafe.getUnsafe().getInt(address);
        if (len == TableUtils.NULL_LEN) {
            return null;
        }
        DirectBinarySequence bs = this.bs[columnIndex];
        bs.of(address + 4, len);
        return bs;
    }

    @Override
    public long getBinLen(int columnIndex) {
        return Unsafe.getUnsafe().getInt(addressOfColumn(columnIndex));
    }

    @Override
    public int getStrLen(int columnIndex) {
        return Unsafe.getUnsafe().getInt(addressOfColumn(columnIndex));
    }

    public DirectMapValues values() {
        return values.of(address0, false);
    }

    private long addressOfColumn(int index) {

        if (index < split) {
            return address0 + Unsafe.arrayGet(valueOffsets, index);
        }

        if (index == split) {
            return address1;
        }

        return Unsafe.getUnsafe().getInt(address2 + (index - split - 1) * 4) + address0;
    }

    private CharSequence getStr0(int index, DirectCharSequence cs) {
        long address = addressOfColumn(index);
        int len = Unsafe.getUnsafe().getInt(address);
        return len == TableUtils.NULL_LEN ? null : cs.of(address + 4, address + 4 + len * 2);
    }

    DirectMapRecord of(long address) {
        this.address0 = address;
        this.address1 = address + keyDataOffset;
        this.address2 = address + keyBlockOffset;
        return this;
    }

    private static class DirectBinarySequence implements BinarySequence {
        private long address;
        private long len;

        public void of(long address, long len) {
            this.address = address;
            this.len = len;
        }

        @Override
        public byte byteAt(long index) {
            return Unsafe.getUnsafe().getByte(address + index);
        }

        @Override
        public long length() {
            return len;
        }
    }
}