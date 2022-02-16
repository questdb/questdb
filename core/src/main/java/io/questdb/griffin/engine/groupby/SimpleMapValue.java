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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.map.MapValue;

public class SimpleMapValue implements MapValue {

    private final long[] values;

    public SimpleMapValue(int columnCount) {
        this.values = new long[columnCount];
    }

    public void copy(SimpleMapValue other) {
        assert values.length >= other.values.length;
        System.arraycopy(other.values, 0, values, 0, other.values.length);
    }

    @Override
    public long getAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBool(int index) {
        return values[index] == 0;
    }

    @Override
    public byte getByte(int index) {
        return (byte) values[index];
    }

    @Override
    public long getDate(int index) {
        return values[index];
    }

    @Override
    public double getDouble(int index) {
        return Double.longBitsToDouble(values[index]);
    }

    @Override
    public float getFloat(int index) {
        return Float.intBitsToFloat((int) values[index]);
    }

    @Override
    public char getChar(int index) {
        return (char) values[index];
    }

    @Override
    public int getInt(int index) {
        return (int) values[index];
    }

    @Override
    public long getLong(int index) {
        return values[index];
    }

    @Override
    public short getShort(int index) {
        return (short) values[index];
    }

    @Override
    public long getTimestamp(int index) {
        return values[index];
    }

    @Override
    public byte getGeoByte(int col) {
        return (byte)values[col];
    }

    @Override
    public short getGeoShort(int col) {
        return (short) values[col];
    }

    @Override
    public int getGeoInt(int col) {
        return (int) values[col];
    }

    @Override
    public long getGeoLong(int col) {
        return values[col];
    }

    @Override
    public boolean isNew() {
        return false;
    }

    @Override
    public void putBool(int index, boolean value) {
        values[index] = value ? 0 : 1;
    }

    @Override
    public void putByte(int index, byte value) {
        values[index] = value;
    }

    @Override
    public void addByte(int index, byte value) {
        values[index] += value;
    }

    @Override
    public void putDate(int index, long value) {
        values[index] = value;
    }

    @Override
    public void putDouble(int index, double value) {
        values[index] = Double.doubleToLongBits(value);
    }

    @Override
    public void addDouble(int index, double value) {
        final double d = Double.longBitsToDouble(values[index]);
        values[index] = Double.doubleToLongBits(value + d);
    }

    @Override
    public void putFloat(int index, float value) {
        values[index] = Float.floatToIntBits(value);
    }

    @Override
    public void addFloat(int index, float value) {
        final float d = Float.intBitsToFloat((int) values[index]);
        values[index] = Float.floatToIntBits(value + d);
    }

    @Override
    public void putInt(int index, int value) {
        values[index] = value;
    }

    @Override
    public void addInt(int index, int value) {
        values[index] += value;
    }

    @Override
    public void putLong(int index, long value) {
        values[index] = value;
    }

    @Override
    public void addLong(int index, long value) {
        values[index] += value;
    }

    @Override
    public void putShort(int index, short value) {
        values[index] = value;
    }

    @Override
    public void addShort(int index, short value) {
        values[index] += value;
    }

    @Override
    public void putChar(int index, char value) {
        values[index] = value;
    }

    @Override
    public void putTimestamp(int index, long value) {
        values[index] = value;
    }

    @Override
    public void setMapRecordHere() {
        throw new UnsupportedOperationException();
    }
}
