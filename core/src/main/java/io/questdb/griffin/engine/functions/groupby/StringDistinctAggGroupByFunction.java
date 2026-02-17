/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf16Sink;

class StringDistinctAggGroupByFunction extends StrFunction implements UnaryFunction, GroupByFunction {
    private static final int INITIAL_SINK_CAPACITY = 128;
    private final Function arg;
    private final char delimiter;
    private final int setInitialCapacity;
    private final double setLoadFactor;
    private final ObjList<CharSequenceHashSet> sets = new ObjList<>();
    private final DirectUtf16Sink sinkA;
    private final DirectUtf16Sink sinkB;
    private int setIndex = 0;
    private int valueIndex;

    public StringDistinctAggGroupByFunction(Function arg, char delimiter, int setInitialCapacity, double setLoadFactor) {
        try {
            this.arg = arg;
            this.delimiter = delimiter;
            this.setInitialCapacity = setInitialCapacity;
            this.setLoadFactor = setLoadFactor;
            this.sinkA = new DirectUtf16Sink(INITIAL_SINK_CAPACITY);
            this.sinkB = new DirectUtf16Sink(INITIAL_SINK_CAPACITY);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        sets.clear();
        sinkA.resetCapacity();
        sinkB.resetCapacity();
        setIndex = 0;
    }

    @Override
    public void close() {
        Misc.free(sinkA);
        Misc.free(sinkB);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final CharSequenceHashSet set;
        if (sets.size() <= setIndex) {
            sets.extendAndSet(setIndex, set = new CharSequenceHashSet(setInitialCapacity, setLoadFactor));
        } else {
            set = sets.getQuick(setIndex);
            set.clear();
        }

        final CharSequence str = arg.getStrA(record);
        if (str != null) {
            set.add(str);
        }
        mapValue.putInt(valueIndex, setIndex++);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final CharSequenceHashSet set = sets.getQuick(mapValue.getInt(valueIndex));
        final CharSequence str = arg.getStrA(record);
        if (str != null) {
            set.add(str);
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public CharSequence getStrA(Record rec) {
        return getStr(rec, sinkA);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return getStr(rec, sinkB);
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.INT); // set index
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public boolean isScalar() {
        return false;
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putInt(valueIndex, -1);
    }

    @Override
    public boolean supportsParallelism() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("string_distinct_agg(").val(arg).val(',').val(delimiter).val(')');
    }

    @Override
    public void toTop() {
        UnaryFunction.super.toTop();
        setIndex = 0;
    }

    private CharSequence getStr(Record rec, DirectUtf16Sink sink) {
        final int setIndex = rec.getInt(valueIndex);
        if (setIndex == -1) {
            return null;
        }

        final CharSequenceHashSet set = sets.getQuick(setIndex);
        if (set.size() == 0) {
            return null;
        }

        sink.clear();
        ObjList<CharSequence> list = set.getList();
        for (int i = 0, n = list.size(); i < n; i++) {
            if (i > 0) {
                sink.putAscii(delimiter);
            }
            sink.put(set.get(i));
        }
        return sink;
    }
}
