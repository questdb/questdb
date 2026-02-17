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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByIntHashSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf16Sink;

import static io.questdb.cairo.sql.SymbolTable.VALUE_IS_NULL;

class StringDistinctAggSymbolGroupByFunction extends StrFunction implements UnaryFunction, GroupByFunction {
    private static final int INITIAL_SINK_CAPACITY = 128;
    private final Function arg;
    private final char delimiter;
    private final GroupByIntHashSet set;
    private final ObjList<DirectUtf16Sink> sinks = new ObjList<>();
    private int sinkIndex = 0;
    private int valueIndex;

    public StringDistinctAggSymbolGroupByFunction(Function arg, char delimiter, int setInitialCapacity, double setLoadFactor) {
        this.arg = arg;
        this.delimiter = delimiter;
        this.set = new GroupByIntHashSet(setInitialCapacity, setLoadFactor, VALUE_IS_NULL);
    }

    @Override
    public void clear() {
        Misc.freeObjListAndClear(sinks);
        sinkIndex = 0;
    }

    @Override
    public void close() {
        Misc.freeObjListAndClear(sinks);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final DirectUtf16Sink sink;
        if (sinks.size() <= sinkIndex) {
            sinks.extendAndSet(sinkIndex, sink = new DirectUtf16Sink(INITIAL_SINK_CAPACITY));
        } else {
            sink = sinks.getQuick(sinkIndex);
            sink.clear();
        }

        final int key = arg.getInt(record);
        if (key != SymbolTable.VALUE_IS_NULL) {
            set.of(0).add(key);
            mapValue.putLong(valueIndex, set.ptr());
            sink.put(arg.getSymbol(record));
        } else {
            mapValue.putLong(valueIndex, 0);
        }
        mapValue.putInt(valueIndex + 1, sinkIndex++);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final DirectUtf16Sink sink = sinks.getQuick(mapValue.getInt(valueIndex + 1));
        final int key = arg.getInt(record);
        if (key != SymbolTable.VALUE_IS_NULL) {
            long ptr = mapValue.getLong(valueIndex);
            final long index = set.of(ptr).keyIndex(key);
            if (index >= 0) {
                if (ptr != 0) {
                    sink.putAscii(delimiter);
                }
                set.addAt(index, key);
                mapValue.putLong(valueIndex, set.ptr());
                sink.put(arg.getSymbol(record));
            }
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public CharSequence getStrA(Record rec) {
        final long ptr = rec.getLong(valueIndex);
        if (ptr == 0) {
            return null;
        }
        return sinks.getQuick(rec.getInt(valueIndex + 1));
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return getStrA(rec);
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
        columnTypes.add(ColumnType.LONG); // GroupByIntHashSet pointer
        columnTypes.add(ColumnType.INT); // sink index
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
    public void setAllocator(GroupByAllocator allocator) {
        set.setAllocator(allocator);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0);
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
        sinkIndex = 0;
    }
}
