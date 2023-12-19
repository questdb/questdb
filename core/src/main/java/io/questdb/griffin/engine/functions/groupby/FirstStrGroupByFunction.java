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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectCharSink;
import org.jetbrains.annotations.NotNull;

public class FirstStrGroupByFunction extends StrFunction implements GroupByFunction, UnaryFunction {

    private static final int INITIAL_SINK_CAPACITY = 16;
    private static final int LIST_CLEAR_THRESHOLD = 64;

    protected final Function arg;
    protected final ObjList<DirectCharSink> sinks = new ObjList<>();

    protected int valueIndex;
    private int sinkIndex = 0;

    public FirstStrGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void clear() {
        // Free extra sinks.
        if (sinks.size() > LIST_CLEAR_THRESHOLD) {
            for (int i = LIST_CLEAR_THRESHOLD, n = sinks.size(); i < n; i++) {
                sinks.getAndSetQuick(i, null).close();
            }
            sinks.setPos(LIST_CLEAR_THRESHOLD);
        }
        // Reset capacity on the remaining ones.
        for (int i = 0, n = sinks.size(); i < n; i++) {
            DirectCharSink sink = sinks.getQuick(i);
            if (sink != null) {
                sink.resetCapacity();
            }
        }
        sinkIndex = 0;
    }

    @Override
    public void close() {
        Misc.freeObjListAndClear(sinks);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        final DirectCharSink sink;
        final CharSequence str = arg.getStr(record);
        final int strLen = str != null ? str.length() : 0;

        if (sinks.size() <= sinkIndex) {
            sinks.extendAndSet(sinkIndex, sink = new DirectCharSink(Math.max(INITIAL_SINK_CAPACITY, strLen)));
        } else {
            sink = sinks.getQuick(sinkIndex);
            sink.clear();
        }

        if (str != null) {
            sink.put(str);
            mapValue.putBool(valueIndex + 1, false);
        } else {
            mapValue.putBool(valueIndex + 1, true);
        }
        mapValue.putInt(valueIndex, sinkIndex++);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        // empty
    }

    @Override
    public Function getArg() {
        return this.arg;
    }

    @Override
    public String getName() {
        return "first";
    }

    @Override
    public CharSequence getStr(Record rec) {
        final boolean nullValue = rec.getBool(valueIndex + 1);
        if (nullValue) {
            return null;
        }
        return sinks.getQuick(rec.getInt(valueIndex));
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return getStr(rec);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isScalar() {
        return false;
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.INT); // sink index
        columnTypes.add(ColumnType.BOOLEAN); // null flag
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putBool(valueIndex + 1, true);
    }

    @Override
    public void toTop() {
        UnaryFunction.super.toTop();
        sinkIndex = 0;
    }
}
