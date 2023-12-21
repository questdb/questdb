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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.map.MapValue;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Long256Util;

public class SimpleMapValue implements MapValue {

    private final Long256Impl long256 = new Long256Impl();
    private final long[] values;
    private boolean isNew;

    public SimpleMapValue(int columnCount) {
        this.values = new long[4 * columnCount];
    }

    @Override
    public void addByte(int index, byte value) {
        values[4 * index] += value;
    }

    @Override
    public void addDouble(int index, double value) {
        final double d = Double.longBitsToDouble(values[4 * index]);
        values[4 * index] = Double.doubleToLongBits(value + d);
    }

    @Override
    public void addFloat(int index, float value) {
        final float d = Float.intBitsToFloat((int) values[4 * index]);
        values[4 * index] = Float.floatToIntBits(value + d);
    }

    @Override
    public void addInt(int index, int value) {
        values[4 * index] += value;
    }

    @Override
    public void addLong(int index, long value) {
        values[4 * index] += value;
    }

    @Override
    public void addLong256(int index, Long256 value) {
        Long256 acc = getLong256A(index);
        Long256Util.add(acc, value);
        final int idx = 4 * index;
        values[idx] = acc.getLong0();
        values[idx + 1] = acc.getLong1();
        values[idx + 2] = acc.getLong2();
        values[idx + 3] = acc.getLong3();
    }

    @Override
    public void addShort(int index, short value) {
        values[4 * index] += value;
    }

    public void copy(SimpleMapValue srcValue) {
        assert values.length >= srcValue.values.length;
        System.arraycopy(srcValue.values, 0, values, 0, srcValue.values.length);
    }

    @Override
    public void copyFrom(MapValue value) {
        copy((SimpleMapValue) value);
    }

    @Override
    public boolean getBool(int index) {
        return values[4 * index] == 0;
    }

    @Override
    public byte getByte(int index) {
        return (byte) values[4 * index];
    }

    @Override
    public char getChar(int index) {
        return (char) values[4 * index];
    }

    @Override
    public long getDate(int index) {
        return values[4 * index];
    }

    @Override
    public double getDouble(int index) {
        return Double.longBitsToDouble(values[4 * index]);
    }

    @Override
    public float getFloat(int index) {
        return Float.intBitsToFloat((int) values[4 * index]);
    }

    @Override
    public byte getGeoByte(int col) {
        return (byte) values[4 * col];
    }

    @Override
    public int getGeoInt(int col) {
        return (int) values[4 * col];
    }

    @Override
    public long getGeoLong(int col) {
        return values[4 * col];
    }

    @Override
    public short getGeoShort(int col) {
        return (short) values[4 * col];
    }

    @Override
    public int getIPv4(int index) {
        return (int) values[4 * index];
    }

    @Override
    public int getInt(int index) {
        return (int) values[4 * index];
    }

    @Override
    public long getLong(int index) {
        return values[4 * index];
    }

    @Override
    public long getLong128Hi(int col) {
        return values[4 * col + 1];
    }

    @Override
    public long getLong128Lo(int col) {
        return values[4 * col];
    }

    @Override
    public Long256 getLong256A(int index) {
        final int idx = 4 * index;
        long256.setAll(values[idx], values[idx + 1], values[idx + 2], values[idx + 3]);
        return long256;
    }

    @Override
    public short getShort(int index) {
        return (short) values[4 * index];
    }

    @Override
    public long getStartAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTimestamp(int index) {
        return values[4 * index];
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    @Override
    public void maxLong(int index, long value) {
        values[4 * index] = Math.max(value, values[4 * index]);
    }

    @Override
    public void putBool(int index, boolean value) {
        values[4 * index] = value ? 0 : 1;
    }

    @Override
    public void putByte(int index, byte value) {
        values[4 * index] = value;
    }

    @Override
    public void putChar(int index, char value) {
        values[4 * index] = value;
    }

    @Override
    public void putDate(int index, long value) {
        values[4 * index] = value;
    }

    @Override
    public void putDouble(int index, double value) {
        values[4 * index] = Double.doubleToLongBits(value);
    }

    @Override
    public void putFloat(int index, float value) {
        values[4 * index] = Float.floatToIntBits(value);
    }

    @Override
    public void putInt(int index, int value) {
        values[4 * index] = value;
    }

    @Override
    public void putLong(int index, long value) {
        values[4 * index] = value;
    }

    @Override
    public void putLong128(int index, long lo, long hi) {
        final int idx = 4 * index;
        values[idx] = lo;
        values[idx + 1] = hi;
    }

    @Override
    public void putLong256(int index, Long256 value) {
        final int idx = 4 * index;
        values[idx] = value.getLong0();
        values[idx + 1] = value.getLong1();
        values[idx + 2] = value.getLong2();
        values[idx + 3] = value.getLong3();
    }

    @Override
    public void putShort(int index, short value) {
        values[4 * index] = value;
    }

    @Override
    public void putTimestamp(int index, long value) {
        values[4 * index] = value;
    }

    @Override
    public void setMapRecordHere() {
        throw new UnsupportedOperationException();
    }

    public void setNew(boolean isNew) {
        this.isNew = isNew;
    }
}
