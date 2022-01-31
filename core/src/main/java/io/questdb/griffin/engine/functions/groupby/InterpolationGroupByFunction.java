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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.InterpolationUtil;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;

public class InterpolationGroupByFunction implements GroupByFunction {
    private final GroupByFunction wrappedFunction;

    private boolean interpolating;
    private long startTime;
    private long endTime;
    private long interval;
    private long current;
    private Record target;

    private InterpolationGroupByFunction(GroupByFunction wrappedFunction) {
        this.wrappedFunction = wrappedFunction;
    }

    public static InterpolationGroupByFunction newInstance(GroupByFunction wrappedFunction) {
        return new InterpolationGroupByFunction(wrappedFunction);
    }

    public void startInterpolating(long startTime, long currentTime, long endTime) {
        interpolating = true;
        this.startTime = startTime;
        this.endTime = endTime;
        this.interval = currentTime-startTime;
        this.current = 1;
    }

    public void stopInterpolating() {
        interpolating = false;
    }

    public void setTarget(Record target) {
        this.target = target;
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
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        wrappedFunction.pushValueTypes(columnTypes);
    }

    @Override
    public void setNull(MapValue mapValue) {
        wrappedFunction.setNull(mapValue);
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
        return wrappedFunction.getByte(rec);
    }

    @Override
    public char getChar(Record rec) {
        return wrappedFunction.getChar(rec);
    }

    @Override
    public long getDate(Record rec) {
        return wrappedFunction.getDate(rec);
    }

    @Override
    public double getDouble(Record rec) {
        // need to do this for float/int/long (or anything else groupby functions use)
        final double value = wrappedFunction.getDouble(rec);
        if (interpolating) {
            // could be done inside this class with an increment
            return InterpolationUtil.interpolate(startTime + current++ * interval, startTime, value, endTime, wrappedFunction.getDouble(target));
        }
        return value;
    }

    @Override
    public float getFloat(Record rec) {
        return wrappedFunction.getFloat(rec);
    }

    @Override
    public int getInt(Record rec) {
        return wrappedFunction.getInt(rec);
    }

    @Override
    public long getLong(Record rec) {
        return wrappedFunction.getLong(rec);
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
    public RecordCursorFactory getRecordCursorFactory() {
        return wrappedFunction.getRecordCursorFactory();
    }

    @Override
    public Record getRecord(Record rec) {
        return wrappedFunction.getRecord(rec);
    }

    @Override
    public short getShort(Record rec) {
        return wrappedFunction.getShort(rec);
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
    public byte getGeoByte(Record rec) {
        return wrappedFunction.getGeoByte(rec);
    }

    @Override
    public short getGeoShort(Record rec) {
        return wrappedFunction.getGeoShort(rec);
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
    public int getType() {
        return wrappedFunction.getType();
    }
}
