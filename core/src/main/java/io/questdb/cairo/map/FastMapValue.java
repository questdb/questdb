/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.std.Long256Impl;
import io.questdb.std.Long256Util;
import io.questdb.std.Unsafe;

final class FastMapValue implements MapValue {
    private final Long256Impl long256 = new Long256Impl();
    private final int[] valueOffsets;
    private long address;
    private long limit;
    private boolean newValue;
    private FastMapRecord record; // double-linked

    public FastMapValue(int[] valueOffsets) {
        this.valueOffsets = valueOffsets;
    }

    @Override
    public void addByte(int index, byte value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putByte(p, (byte) (Unsafe.getUnsafe().getByte(p) + value));
    }

    @Override
    public void addDouble(int index, double value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putDouble(p, Unsafe.getUnsafe().getDouble(p) + value);
    }

    @Override
    public void addFloat(int index, float value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putFloat(p, Unsafe.getUnsafe().getFloat(p) + value);
    }

    @Override
    public void addInt(int index, int value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putInt(p, Unsafe.getUnsafe().getInt(p) + value);
    }

    @Override
    public void addLong(int index, long value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putLong(p, Unsafe.getUnsafe().getLong(p) + value);
    }

    @Override
    public void addLong256(int index, Long256 value) {
        Long256 acc = getLong256A(index);
        Long256Util.add(acc, value);
        final long p = address0(index);
        Unsafe.getUnsafe().putLong(p, acc.getLong0());
        Unsafe.getUnsafe().putLong(p + Long.BYTES, acc.getLong1());
        Unsafe.getUnsafe().putLong(p + 2 * Long.BYTES, acc.getLong2());
        Unsafe.getUnsafe().putLong(p + 3 * Long.BYTES, acc.getLong3());
    }

    @Override
    public void addShort(int index, short value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putShort(p, (short) (Unsafe.getUnsafe().getShort(p) + value));
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
    public char getChar(int index) {
        return Unsafe.getUnsafe().getChar(address0(index));
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
    public byte getGeoByte(int col) {
        return getByte(col);
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
    public short getGeoShort(int col) {
        return getShort(col);
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
    public Long256 getLong256A(int index) {
        final long p = address0(index);
        long256.setAll(
                Unsafe.getUnsafe().getLong(p),
                Unsafe.getUnsafe().getLong(p + Long.BYTES),
                Unsafe.getUnsafe().getLong(p + 2 * Long.BYTES),
                Unsafe.getUnsafe().getLong(p + 3 * Long.BYTES)
        );
        return long256;
    }

    @Override
    public long getLong128Hi(int col) {
        return Unsafe.getUnsafe().getLong(address0(col) + Long.BYTES);
    }

    @Override
    public long getLong128Lo(int col) {
        return Unsafe.getUnsafe().getLong(address0(col));
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
    public boolean isNew() {
        return newValue;
    }

    @Override
    public void putBool(int index, boolean value) {
        putByte(index, (byte) (value ? 1 : 0));
    }

    @Override
    public void putByte(int index, byte value) {
        final long p = address0(index);
        assert p + Byte.BYTES < limit;
        Unsafe.getUnsafe().putByte(p, value);
    }

    @Override
    public void putChar(int index, char value) {
        final long p = address0(index);
        assert p + Character.BYTES < limit;
        Unsafe.getUnsafe().putChar(p, value);
    }

    @Override
    public void putDate(int index, long value) {
        putLong(index, value);
    }

    @Override
    public void putDouble(int index, double value) {
        final long p = address0(index);
        assert p + Double.BYTES < limit;
        Unsafe.getUnsafe().putDouble(p, value);
    }

    @Override
    public void putFloat(int index, float value) {
        final long p = address0(index);
        assert p + Float.BYTES < limit;
        Unsafe.getUnsafe().putFloat(p, value);
    }

    @Override
    public void putInt(int index, int value) {
        final long p = address0(index);
        assert p + Integer.BYTES < limit;
        Unsafe.getUnsafe().putInt(p, value);
    }

    @Override
    public void putLong(int index, long value) {
        final long p = address0(index);
        assert p + Long.BYTES < limit;
        Unsafe.getUnsafe().putLong(p, value);
    }

    @Override
    public void putLong128(int index, long lo, long hi) {
        long address = address0(index);
        Unsafe.getUnsafe().putLong(address, lo);
        Unsafe.getUnsafe().putLong(address + Long.BYTES, hi);
    }

    @Override
    public void putLong256(int index, Long256 value) {
        final long p = address0(index);
        assert p + Long256.BYTES < limit;
        Unsafe.getUnsafe().putLong(p, value.getLong0());
        Unsafe.getUnsafe().putLong(p + Long.BYTES, value.getLong1());
        Unsafe.getUnsafe().putLong(p + 2 * Long.BYTES, value.getLong2());
        Unsafe.getUnsafe().putLong(p + 3 * Long.BYTES, value.getLong3());
    }

    @Override
    public void putShort(int index, short value) {
        Unsafe.getUnsafe().putShort(address0(index), value);
    }

    @Override
    public void putTimestamp(int index, long value) {
        putLong(index, value);
    }

    @Override
    public void setMapRecordHere() {
        record.of(address, limit);
    }

    private long address0(int index) {
        return address + valueOffsets[index];
    }

    void linkRecord(FastMapRecord record) {
        this.record = record;
    }

    FastMapValue of(long address, long limit, boolean newValue) {
        this.address = address;
        this.limit = limit;
        this.newValue = newValue;
        return this;
    }
}
