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
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Utf8SequenceHashSet;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;

class StringDistinctAggVarcharGroupByFunction extends VarcharFunction implements UnaryFunction, GroupByFunction {
    private static final int INITIAL_SINK_CAPACITY = 128;
    private final Function arg;
    private final char delimiter;
    private final int setInitialCapacity;
    private final double setLoadFactor;
    private final ObjList<Utf8SequenceHashSet> sets = new ObjList<>();
    private final DirectUtf8Sink sinkA;
    private final DirectUtf8Sink sinkB;
    private int setIndex = 0;
    private int valueIndex;

    public StringDistinctAggVarcharGroupByFunction(Function arg, char delimiter, int setInitialCapacity, double setLoadFactor) {
        try {
            this.arg = arg;
            this.delimiter = delimiter;
            this.setInitialCapacity = setInitialCapacity;
            this.setLoadFactor = setLoadFactor;
            this.sinkA = new DirectUtf8Sink(INITIAL_SINK_CAPACITY);
            this.sinkB = new DirectUtf8Sink(INITIAL_SINK_CAPACITY);
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
        final Utf8SequenceHashSet set;
        if (sets.size() <= setIndex) {
            sets.extendAndSet(setIndex, set = new Utf8SequenceHashSet(setInitialCapacity, setLoadFactor));
        } else {
            set = sets.getQuick(setIndex);
            set.clear();
        }

        final Utf8Sequence str = arg.getVarcharA(record);
        if (str != null) {
            set.add(str);
        }
        mapValue.putInt(valueIndex, setIndex++);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final Utf8SequenceHashSet set = sets.getQuick(mapValue.getInt(valueIndex));
        final Utf8Sequence str = arg.getVarcharA(record);
        if (str != null) {
            set.add(str);
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public Utf8Sequence getVarcharA(Record rec) {
        return getVarchar(rec, sinkA);
    }

    @Override
    public Utf8Sequence getVarcharB(Record rec) {
        return getVarchar(rec, sinkB);
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

    private Utf8Sequence getVarchar(Record rec, DirectUtf8Sink sink) {
        final int setIndex = rec.getInt(valueIndex);
        if (setIndex == -1) {
            return null;
        }

        final Utf8SequenceHashSet set = sets.getQuick(setIndex);
        if (set.size() == 0) {
            return null;
        }

        sink.clear();
        ObjList<Utf8Sequence> list = set.getList();
        for (int i = 0, n = list.size(); i < n; i++) {
            if (i > 0) {
                sink.putAscii(delimiter);
            }
            sink.put(set.get(i));
        }
        return sink;
    }
}
