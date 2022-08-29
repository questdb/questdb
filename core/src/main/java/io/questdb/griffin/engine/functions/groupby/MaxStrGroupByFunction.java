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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

public class MaxStrGroupByFunction extends StrFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private int valueIndex;
    private int sinkIndex;
    private final ObjList<StringSink> sinks = new ObjList<>();

    public MaxStrGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public boolean isScalar() {
        return false;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        final CharSequence val = arg.getStr(record);
        if (val == null) {
            mapValue.putInt(valueIndex, Numbers.INT_NaN);
        } else {
            final StringSink sink = nextSink();
            sink.put(val);
            mapValue.putInt(valueIndex, sinkIndex++);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        final CharSequence val = arg.getStr(record);
        if (val != null) {
            final int index = mapValue.getInt(valueIndex);
            if (index == Numbers.INT_NaN) {
                final StringSink sink = nextSink();
                sink.put(val);
                mapValue.putInt(valueIndex, sinkIndex++);
                return;
            }

            final StringSink maxSink = sinks.getQuick(index);
            if (Chars.compare(maxSink, val) < 0) {
                maxSink.clear();
                maxSink.put(val);
            }
        }
    }

    private StringSink nextSink() {
        final StringSink sink;
        if (sinks.size() <= sinkIndex) {
            sinks.extendAndSet(sinkIndex, sink = new StringSink());
        } else {
            sink = sinks.getQuick(sinkIndex);
        }
        sink.clear();
        return sink;
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.INT);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putInt(valueIndex, Numbers.INT_NaN);
    }

    @Override
    public CharSequence getStr(Record rec) {
        final int index = rec.getInt(valueIndex);
        return (index == Numbers.INT_NaN) ? null : sinks.getQuick(index);
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
    public boolean isReadThreadSafe() {
        return false;
    }

    @Override
    public void toTop() {
        UnaryFunction.super.toTop();
        sinkIndex = 0;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put("MaxStr(").put(arg).put(')');
    }
}
