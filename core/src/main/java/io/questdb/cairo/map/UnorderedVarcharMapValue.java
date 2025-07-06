/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.*;

final class UnorderedVarcharMapValue implements MapValue {
    private final Long256Impl long256 = new Long256Impl();
    private final long[] valueOffsets;
    private final long valueSize;
    private long limit;
    private boolean newValue;
    private UnorderedVarcharMapRecord record; // double-linked
    private long startAddress; // key-value pair start address
    private long valueAddress;

    public UnorderedVarcharMapValue(long valueSize, long[] valueOffsets) {
        this.valueSize = valueSize;
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
        Unsafe.getUnsafe().putLong(p + 8L, acc.getLong1());
        Unsafe.getUnsafe().putLong(p + 16L, acc.getLong2());
        Unsafe.getUnsafe().putLong(p + 24L, acc.getLong3());
    }

    @Override
    public void addShort(int index, short value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putShort(p, (short) (Unsafe.getUnsafe().getShort(p) + value));
    }

    @Override
    public void copyFrom(MapValue value) {
        UnorderedVarcharMapValue other = (UnorderedVarcharMapValue) value;
        Vect.memcpy(valueAddress, other.valueAddress, valueSize);
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
    public int getIPv4(int index) {
        return Unsafe.getUnsafe().getInt(address0(index));
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
    public long getLong128Hi(int col) {
        return Unsafe.getUnsafe().getLong(address0(col) + 8L);
    }

    @Override
    public long getLong128Lo(int col) {
        return Unsafe.getUnsafe().getLong(address0(col));
    }

    @Override
    public Long256 getLong256A(int index) {
        final long p = address0(index);
        long256.fromAddress(p);
        return long256;
    }

    @Override
    public short getShort(int index) {
        return Unsafe.getUnsafe().getShort(address0(index));
    }

    @Override
    public long getStartAddress() {
        return startAddress;
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
    public void maxInt(int index, int value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putInt(p, Math.max(value, Unsafe.getUnsafe().getInt(p)));
    }

    @Override
    public void maxLong(int index, long value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putLong(p, Math.max(value, Unsafe.getUnsafe().getLong(p)));
    }

    @Override
    public void minInt(int index, int value) {
        if (value != Numbers.INT_NULL) {
            final long p = address0(index);
            final int current = Unsafe.getUnsafe().getInt(p);
            Unsafe.getUnsafe().putInt(p, current != Numbers.INT_NULL ? Math.min(value, current) : value);
        }
    }

    @Override
    public void minLong(int index, long value) {
        if (value != Numbers.INT_NULL) {
            final long p = address0(index);
            final long current = Unsafe.getUnsafe().getLong(p);
            Unsafe.getUnsafe().putLong(p, current != Numbers.INT_NULL ? Math.min(value, current) : value);
        }
    }

    @Override
    public void putBool(int index, boolean value) {
        putByte(index, (byte) (value ? 1 : 0));
    }

    @Override
    public void putByte(int index, byte value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putByte(p, value);
    }

    @Override
    public void putChar(int index, char value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putChar(p, value);
    }

    @Override
    public void putDate(int index, long value) {
        putLong(index, value);
    }

    @Override
    public void putDouble(int index, double value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putDouble(p, value);
    }

    @Override
    public void putFloat(int index, float value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putFloat(p, value);
    }

    @Override
    public void putInt(int index, int value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putInt(p, value);
    }

    @Override
    public void putLong(int index, long value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putLong(p, value);
    }

    @Override
    public void putLong128(int index, long lo, long hi) {
        long address = address0(index);
        Unsafe.getUnsafe().putLong(address, lo);
        Unsafe.getUnsafe().putLong(address + 8L, hi);
    }

    @Override
    public void putLong256(int index, Long256 value) {
        final long p = address0(index);
        Unsafe.getUnsafe().putLong(p, value.getLong0());
        Unsafe.getUnsafe().putLong(p + 8L, value.getLong1());
        Unsafe.getUnsafe().putLong(p + 16L, value.getLong2());
        Unsafe.getUnsafe().putLong(p + 24L, value.getLong3());
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
        record.of(startAddress);
    }

    private long address0(int index) {
        return valueAddress + valueOffsets[index];
    }

    void copyRawValue(long ptr) {
        Vect.memcpy(valueAddress, ptr, valueSize);
    }

    void linkRecord(UnorderedVarcharMapRecord record) {
        this.record = record;
        record.setLimit(limit);
    }

    UnorderedVarcharMapValue of(long startAddress, long limit, boolean newValue) {
        this.startAddress = startAddress;
        this.valueAddress = startAddress + UnorderedVarcharMap.KEY_SIZE;
        this.limit = limit;
        this.newValue = newValue;
        return this;
    }
}
