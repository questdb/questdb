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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByUtf8SequenceLongHashMap;
import io.questdb.griffin.engine.groupby.GroupByUtf8Sink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

import static io.questdb.std.Numbers.LONG_NULL;

public class ModeVarcharGroupByFunction extends VarcharFunction implements UnaryFunction, GroupByFunction {
    private final Function arg;
    private final GroupByUtf8SequenceLongHashMap mapA;
    private final GroupByUtf8SequenceLongHashMap mapB;
    private final GroupByUtf8Sink sinkA = new GroupByUtf8Sink();
    private final GroupByUtf8Sink sinkB = new GroupByUtf8Sink();
    private int valueIndex;

    public ModeVarcharGroupByFunction(@NotNull CairoConfiguration configuration, @NotNull Function arg) {
        this.arg = arg;
        int initialCapacity = 4;
        double loadFactor = configuration.getSqlFastMapLoadFactor();
        mapA = new GroupByUtf8SequenceLongHashMap(initialCapacity, loadFactor, LONG_NULL, LONG_NULL);
        mapB = new GroupByUtf8SequenceLongHashMap(initialCapacity, loadFactor, LONG_NULL, LONG_NULL);
    }

    @Override
    public void clear() {
        mapA.resetPtr();
        mapB.resetPtr();
        sinkA.of(0);
        sinkB.of(0);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        Utf8Sequence val = arg.getVarcharA(record);
        if (val != null) {
            mapA.of(0).inc(val);
            mapValue.putLong(valueIndex, mapA.ptr());
        } else {
            mapValue.putLong(valueIndex, 0);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final Utf8Sequence value = arg.getVarcharA(record);
        if (value != null) {
            mapA.of(mapValue.getLong(valueIndex));
            mapA.inc(value);
            mapValue.putLong(valueIndex, mapA.ptr());
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public String getName() {
        return "mode";
    }

    @Override
    public int getSampleByFlags() {
        return GroupByFunction.SAMPLE_BY_FILL_ALL;
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public Utf8Sequence getVarcharA(Record record) {
        return getVarchar(record, sinkA);
    }

    @Override
    public Utf8Sequence getVarcharB(Record record) {
        return getVarchar(record, sinkB);
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG);
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
    public void merge(MapValue destValue, MapValue srcValue) {
        final long destPtr = destValue.getLong(valueIndex);
        mapA.of(destPtr);

        final long srcPtr = srcValue.getLong(valueIndex);
        mapB.of(srcPtr);

        final long outPtr = mapA.size() > mapB.size() ? mapA.mergeAdd(mapB) : mapB.mergeAdd(mapA);

        destValue.putLong(valueIndex, outPtr);
    }

    @Override
    public void setAllocator(GroupByAllocator allocator) {
        mapA.setAllocator(allocator);
        mapB.setAllocator(allocator);
        sinkA.setAllocator(allocator);
        sinkB.setAllocator(allocator);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, LONG_NULL);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("mode(").val(arg).val(')');
    }

    @Override
    public void toTop() {
        UnaryFunction.super.toTop();
    }

    Utf8Sequence getVarchar(Record record, GroupByUtf8Sink sink) {
        long mapPtr = record.getLong(valueIndex);
        if (mapPtr <= 0) {
            return null;
        }
        mapA.of(mapPtr);
        long modeKey = LONG_NULL;
        long modeCount = -1;

        for (int i = 0, n = mapA.capacity(); i < n; i++) {
            final long kPtr = mapA.keyAt(i);
            if (kPtr != LONG_NULL) {
                final long value = mapA.valueAt(i);
                if (value > modeCount) {
                    modeKey = kPtr;
                    modeCount = value;
                }
            }
        }
        sink.of(modeKey);
        return sink;
    }
}
