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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.InterpolationUtil;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;

public class InterpolationGroupByFunction implements GroupByFunction {
    private final GroupByFunction wrappedFunction;
    private long current;
    private long endTime;
    private boolean interpolating;
    private long interval;
    private long startTime;
    private Record target;

    private InterpolationGroupByFunction(GroupByFunction wrappedFunction) {
        this.wrappedFunction = wrappedFunction;
    }

    public static InterpolationGroupByFunction newInstance(GroupByFunction wrappedFunction) {
        return new InterpolationGroupByFunction(wrappedFunction);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        wrappedFunction.computeFirst(mapValue, record);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        wrappedFunction.computeNext(mapValue, record);
    }

    @Override
    public int getArrayLength() {
        return wrappedFunction.getArrayLength();
    }

    @Override
    public BinarySequence getBin(Record rec) {
        return wrappedFunction.getBin(rec);
    }

    @Override
    public long getBinLen(Record rec) {
        return wrappedFunction.getBinLen(rec);
    }

    @Override
    public boolean getBool(Record rec) {
        return wrappedFunction.getBool(rec);
    }

    @Override
    public byte getByte(Record rec) {
        byte value = wrappedFunction.getByte(rec);
        if (interpolating) {
            return (byte) InterpolationUtil.interpolate(startTime + current++ * interval, startTime, value, endTime, wrappedFunction.getByte(target));
        }
        return value;
    }

    @Override
    public char getChar(Record rec) {
        char value = wrappedFunction.getChar(rec);
        if (interpolating) {
            return (char) InterpolationUtil.interpolate(startTime + current++ * interval, startTime, value, endTime, wrappedFunction.getChar(target));
        }
        return value;
    }

    @Override
    public long getDate(Record rec) {
        return wrappedFunction.getDate(rec);
    }

    @Override
    public double getDouble(Record rec) {
        final double value = wrappedFunction.getDouble(rec);
        if (interpolating) {
            return InterpolationUtil.interpolate(startTime + current++ * interval, startTime, value, endTime, wrappedFunction.getDouble(target));
        }
        return value;
    }

    @Override
    public float getFloat(Record rec) {
        float value = wrappedFunction.getFloat(rec);
        if (interpolating) {
            return (float) InterpolationUtil.interpolate(startTime + current++ * interval, startTime, value, endTime, wrappedFunction.getFloat(target));
        }
        return value;
    }

    @Override
    public byte getGeoByte(Record rec) {
        return wrappedFunction.getGeoByte(rec);
    }

    @Override
    public int getGeoInt(Record rec) {
        return wrappedFunction.getGeoInt(rec);
    }

    @Override
    public long getGeoLong(Record rec) {
        return wrappedFunction.getGeoLong(rec);
    }

    @Override
    public short getGeoShort(Record rec) {
        return wrappedFunction.getGeoShort(rec);
    }

    @Override
    public final int getIPv4(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(Record rec) {
        int value = wrappedFunction.getInt(rec);
        if (interpolating) {
            return (int) InterpolationUtil.interpolate(startTime + current++ * interval, startTime, value, endTime, wrappedFunction.getInt(target));
        }
        return value;
    }

    @Override
    public long getLong(Record rec) {
        long value = wrappedFunction.getLong(rec);
        if (interpolating) {
            return (long) InterpolationUtil.interpolate(startTime + current++ * interval, startTime, value, endTime, wrappedFunction.getLong(target));
        }
        return value;
    }

    @Override
    public long getLong128Hi(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong128Lo(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getLong256(Record rec, CharSink sink) {
        wrappedFunction.getLong256(rec, sink);
    }

    @Override
    public Long256 getLong256A(Record rec) {
        return wrappedFunction.getLong256A(rec);
    }

    @Override
    public Long256 getLong256B(Record rec) {
        return wrappedFunction.getLong256B(rec);
    }

    @Override
    public Record getRecord(Record rec) {
        return wrappedFunction.getRecord(rec);
    }

    @Override
    public RecordCursorFactory getRecordCursorFactory() {
        return wrappedFunction.getRecordCursorFactory();
    }

    @Override
    public short getShort(Record rec) {
        short value = wrappedFunction.getShort(rec);
        if (interpolating) {
            return (short) InterpolationUtil.interpolate(startTime + current++ * interval, startTime, value, endTime, wrappedFunction.getShort(target));
        }
        return value;
    }

    @Override
    public CharSequence getStr(Record rec) {
        return wrappedFunction.getStr(rec);
    }

    @Override
    public CharSequence getStr(Record rec, int arrayIndex) {
        return wrappedFunction.getStr(rec, arrayIndex);
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        wrappedFunction.getStr(rec, sink);
    }

    @Override
    public void getStr(Record rec, CharSink sink, int arrayIndex) {
        wrappedFunction.getStr(rec, sink, arrayIndex);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return wrappedFunction.getStrB(rec);
    }

    @Override
    public CharSequence getStrB(Record rec, int arrayIndex) {
        return wrappedFunction.getStrB(rec, arrayIndex);
    }

    @Override
    public int getStrLen(Record rec) {
        return wrappedFunction.getStrLen(rec);
    }

    @Override
    public int getStrLen(Record rec, int arrayIndex) {
        return wrappedFunction.getStrLen(rec, arrayIndex);
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return wrappedFunction.getSymbol(rec);
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        return wrappedFunction.getSymbolB(rec);
    }

    @Override
    public long getTimestamp(Record rec) {
        return wrappedFunction.getTimestamp(rec);
    }

    @Override
    public int getType() {
        return wrappedFunction.getType();
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        wrappedFunction.pushValueTypes(columnTypes);
    }

    @Override
    public void setNull(MapValue mapValue) {
        wrappedFunction.setNull(mapValue);
    }

    public void setTarget(Record target) {
        this.target = target;
    }

    public void startInterpolating(long startTime, long currentTime, long endTime) {
        interpolating = true;
        this.startTime = startTime;
        this.endTime = endTime;
        this.interval = currentTime - startTime;
        this.current = 1;
    }

    public void stopInterpolating() {
        interpolating = false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("Interpolated(").val(wrappedFunction).val(")");
    }
}
