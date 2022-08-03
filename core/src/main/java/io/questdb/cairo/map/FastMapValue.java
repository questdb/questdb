/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo.map;

import io.questdb.std.Long256;
import io.questdb.std.Unsafe;

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
    public boolean getBool(int index) {
        return getByte(index) == 1;
    }

    @Override
    public byte getByte(int index) {
        return Unsafe.getUnsafe().getByte(address0(index));
    }

    @Override
    public long getDate(int index) {
        return getLong(index);
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
    public CharSequence getStr(int index) {
        return null;
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
    public long getTimestamp(int index) {
        return getLong(index);
    }

    @Override
    public byte getGeoByte(int col) {
        return getByte(col);
    }

    @Override
    public short getGeoShort(int col) {
        return getShort(col);
    }

    @Override
    public int getGeoInt(int col) {
        return getInt(col);
    }

    @Override
    public long getGeoLong(int col) {
        return getLong(col);
    }

    @Override
    public boolean isNew() {
        return _new;
    }

    @Override
    public void putBool(int index, boolean value) {
        putByte(index, (byte) (value ? 1 : 0));
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
    public void addLong(int index, long value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putLong(p, Unsafe.getUnsafe().getLong(p) + value);
    }

    @Override
    public void addByte(int index, byte value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putByte(p, (byte) (Unsafe.getUnsafe().getByte(p) + value));
    }

    @Override
    public void addShort(int index, short value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putShort(p, (short) (Unsafe.getUnsafe().getShort(p) + value));
    }

    @Override
    public void addInt(int index, int value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putInt(p, Unsafe.getUnsafe().getInt(p) + value);
    }

    @Override
    public void addDouble(int index, double value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putDouble(p, Unsafe.getUnsafe().getDouble(p) + value);
    }

    @Override
    public void putStr(int index, CharSequence value) {

    }

    @Override
    public void addFloat(int index, float value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putFloat(p, Unsafe.getUnsafe().getFloat(p) + value);
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
    public void putTimestamp(int index, long value) {
        putLong(index, value);
    }

    @Override
    public void setMapRecordHere() {
        this.record.of(address);
    }

    @Override
    public void addLong256(int index, Long256 value) {

    }

    @Override
    public void putLong256(int index, Long256 value) {

    }

    private long address0(int index) {
        return address + valueOffsets[index];
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
