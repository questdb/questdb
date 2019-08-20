/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.cairo.map;

import com.questdb.std.Unsafe;

final class FastMapValue implements MapValue {
    private final int[] valueOffsets;
    private long address;
    private boolean _new;
    private FastMapRecord record; // double-linked

    public FastMapValue(int[] valueOffsets) {
        this.valueOffsets = valueOffsets;
    }

    @Override
    public long getAddress() {
        return address;
    }

    @Override
    public boolean getBool(int columnIndex) {
        return getByte(columnIndex) == 1;
    }

    @Override
    public byte getByte(int index) {
        return Unsafe.getUnsafe().getByte(address0(index));
    }

    @Override
    public long getDate(int columnIndex) {
        return getLong(columnIndex);
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
    public char getChar(int index) {
        return Unsafe.getUnsafe().getChar(address0(index));
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
    public short getShort(int index) {
        return Unsafe.getUnsafe().getShort(address0(index));
    }

    @Override
    public long getTimestamp(int columnIndex) {
        return getLong(columnIndex);
    }

    @Override
    public boolean isNew() {
        return _new;
    }

    @Override
    public void putBool(int columnIndex, boolean value) {
        putByte(columnIndex, (byte) (value ? 1 : 0));
    }

    @Override
    public void putByte(int index, byte value) {
        Unsafe.getUnsafe().putByte(address0(index), value);
    }

    @Override
    public void putDate(int index, long value) {
        putLong(index, value);
    }

    @Override
    public void putDouble(int index, double value) {
        Unsafe.getUnsafe().putDouble(address0(index), value);
    }

    @Override
    public void putFloat(int index, float value) {
        Unsafe.getUnsafe().putFloat(address0(index), value);
    }

    @Override
    public void putInt(int index, int value) {
        Unsafe.getUnsafe().putInt(address0(index), value);
    }

    @Override
    public void putLong(int index, long value) {
        Unsafe.getUnsafe().putLong(address0(index), value);
    }

    @Override
    public void putShort(int index, short value) {
        Unsafe.getUnsafe().putShort(address0(index), value);
    }

    @Override
    public void putChar(int index, char value) {
        Unsafe.getUnsafe().putChar(address0(index), value);
    }

    @Override
    public void putTimestamp(int columnIndex, long value) {
        putLong(columnIndex, value);
    }

    @Override
    public void setMapRecordHere() {
        this.record.of(address);
    }

    private long address0(int index) {
        return address + Unsafe.arrayGet(valueOffsets, index);
    }

    void linkRecord(FastMapRecord record) {
        this.record = record;
    }

    FastMapValue of(long address, boolean _new) {
        this.address = address;
        this._new = _new;
        return this;
    }
}
