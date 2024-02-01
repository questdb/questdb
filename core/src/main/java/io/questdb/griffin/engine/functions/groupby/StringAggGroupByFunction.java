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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf16Sink;

class StringAggGroupByFunction extends StrFunction implements UnaryFunction, GroupByFunction {
    // Cleared function retains up to INITIAL_SINK_CAPACITY * LIST_CLEAR_THRESHOLD bytes.
    private static final int INITIAL_SINK_CAPACITY = 512;
    private static final int LIST_CLEAR_THRESHOLD = 64;
    private final Function arg;
    private final char delimiter;
    private final ObjList<DirectUtf16Sink> sinks = new ObjList<>();
    private int sinkIndex = 0;
    private int valueIndex;

    public StringAggGroupByFunction(Function arg, char delimiter) {
        this.arg = arg;
        this.delimiter = delimiter;
    }

    @Override
    public void clear() {
        // Free extra sinks.
        if (sinks.size() > LIST_CLEAR_THRESHOLD) {
            for (int i = sinks.size() - 1; i > LIST_CLEAR_THRESHOLD - 1; i--) {
                Misc.free(sinks.getQuick(i));
                sinks.remove(i);
            }
        }
        // Reset capacity on the remaining ones.
        for (int i = 0, n = sinks.size(); i < n; i++) {
            DirectUtf16Sink sink = sinks.getQuick(i);
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
        final DirectUtf16Sink sink;
        if (sinks.size() <= sinkIndex) {
            sinks.extendAndSet(sinkIndex, sink = new DirectUtf16Sink(INITIAL_SINK_CAPACITY));
        } else {
            sink = sinks.getQuick(sinkIndex);
            sink.clear();
        }

        final CharSequence str = arg.getStr(record);
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
        final DirectUtf16Sink sink = sinks.getQuick(mapValue.getInt(valueIndex));
        final CharSequence str = arg.getStr(record);
        if (str != null) {
            final boolean nullValue = mapValue.getBool(valueIndex + 1);
            if (!nullValue) {
                sink.putAscii(delimiter);
            }
            sink.put(str);
            mapValue.putBool(valueIndex + 1, false);
        }
    }

    @Override
    public Function getArg() {
        return arg;
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
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isParallelismSupported() {
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
    public void setValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("string_agg(").val(arg).val(',').val(delimiter).val(')');
    }

    @Override
    public void toTop() {
        UnaryFunction.super.toTop();
        sinkIndex = 0;
    }
}
