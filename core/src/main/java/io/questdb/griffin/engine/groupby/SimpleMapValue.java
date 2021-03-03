/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;

public class SimpleMapValue implements MapValue {

    private final long[] values;
    private final StringSink[] sinks;

    public SimpleMapValue(ArrayColumnTypes valueTypes) {
        int columnCount = valueTypes.getColumnCount();
        values = new long[columnCount];
        int stringColumns = 0;
        for (int i = 0; i < columnCount; i++) {
            if (valueTypes.getColumnType(i) == ColumnType.STRING) {
                stringColumns++;
            }
        }
        sinks = new StringSink[stringColumns];
        for (int i = 0; stringColumns > 0 && i <  columnCount; i++) {
            if (valueTypes.getColumnType(i) == ColumnType.STRING) {
                sinks[stringColumns - 1] = new StringSink();
                values[i] = -stringColumns;
                stringColumns--;
            }
        }
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
    public CharSequence getStr(int index) {
        long sinkIndex = values[index];
        if (sinkIndex > 0) {
            return Chars.toString(getSink(sinkIndex));
        }
        return null;
    }

    @Override
    public CharSequence getStrB(int index) {
        return getStr(index);
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
    public void putStr(int index, CharSequence value) {
        long sinkIndex = initAndGetSinkIndex(index);
        StringSink sink = getSink(sinkIndex);
        sink.clear();
        sink.put(value);
    }

    @Override
    public void putNullStr(int index) {
        long sinkIndex = values[index];
        if (sinkIndex > 0) {
            StringSink sink = getSink(sinkIndex);
            sink.clear();
            values[index] = -sinkIndex;
        }
    }

    @Override
    public void appendChar(int index, char value) {
        long sinkIndex = initAndGetSinkIndex(index);
        StringSink sink = getSink(sinkIndex);
        sink.put(value);
    }

    @Override
    public void appendStr(int index, CharSequence value) {
        long sinkIndex = initAndGetSinkIndex(index);
        StringSink sink = getSink(sinkIndex);
        sink.put(value);
    }

    @Override
    public void setMapRecordHere() {
        throw new UnsupportedOperationException();
    }

    private long initAndGetSinkIndex(int index) {
        long sinkIndex = values[index];
        if (sinkIndex < 0) {
            sinkIndex = -sinkIndex;
            values[index] = sinkIndex;
        }
        return sinkIndex;
    }

    private StringSink getSink(long sinkIndex) {
        return sinks[(int) sinkIndex - 1];
    }
}
