/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.arr.FlatArrayView;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByDoubleList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

import static io.questdb.std.Numbers.LONG_NULL;

public class MultiplePercentileDiscDoubleGroupByFunction extends ArrayFunction implements UnaryFunction, GroupByFunction {
    private final Function arg;
    private final GroupByDoubleList listA;
    private final GroupByDoubleList listB;
    private final Function percentileFunc;
    private final int percentilePos;
    private DirectArray out;
    private int valueIndex;

    public MultiplePercentileDiscDoubleGroupByFunction(@NotNull CairoConfiguration configuration, @NotNull Function arg, @NotNull Function percentileFunc, int percentilePos) {
        this.arg = arg;
        this.percentileFunc = percentileFunc;
        int initialCapacity = 4;
        listA = new GroupByDoubleList(initialCapacity, Double.NaN);
        listB = new GroupByDoubleList(initialCapacity, Double.NaN);
        this.percentilePos = percentilePos;
        this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
    }

    @Override
    public void clear() {
        listA.resetPtr();
        listB.resetPtr();
        if (out != null) {
            out.clear();
        }
    }

    @Override
    public void close() {
        Misc.free(arg);
        Misc.free(percentileFunc);
        Misc.free(out);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        double value = arg.getDouble(record);
        if (Numbers.isFinite(value)) {
            listA.of(0).add(value);
            mapValue.putLong(valueIndex, listA.ptr());
        } else {
            mapValue.putLong(valueIndex, 0);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final double value = arg.getDouble(record);
        if (Numbers.isFinite(value)) {
            listA.of(mapValue.getLong(valueIndex));
            listA.add(value);
            mapValue.putLong(valueIndex, listA.ptr());
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public ArrayView getArray(Record record) {
        long listPtr = record.getLong(valueIndex);
        if (listPtr <= 0) {
            if (out == null) {
                out = new DirectArray();
            }
            out.ofNull();
            return out;
        }
        listA.of(listPtr);
        int size = listA.size();
        if (size == 0) {
            if (out == null) {
                out = new DirectArray();
            }
            out.ofNull();
            return out;
        }
        listA.sort(0, size - 1);

        ArrayView percentiles = percentileFunc.getArray(record);
        FlatArrayView view = percentiles.flatView();
        int view_length = view.length();

        if (out == null) {
            out = new DirectArray();
            out.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
            out.setDimLen(0, view_length);
            out.applyShape();
        }

        for (int i = 0, len = view.length(); i < len; i++) {
            double percentile = view.getDoubleAtAbsIndex(i);
            if (percentile < 0.0d || percentile > 1.0d) {
                throw CairoException.nonCritical().position(percentilePos).put("invalid percentile [expected=range(0.0, 1.0), actual=").put(percentile).put(']');
            }
            int N = (int) Math.ceil(size * percentile) - 1;
            if (N < 0) {
                N = 0;
            }
            out.putDouble(i, listA.getQuick(N));
        }
        return out;
    }

    @Override
    public String getName() {
        return "percentile_disc";
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
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext sqlExecutionContext) throws SqlException {
        super.init(symbolTableSource, sqlExecutionContext);
        arg.init(symbolTableSource, sqlExecutionContext);
        percentileFunc.init(symbolTableSource, sqlExecutionContext);
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
        listA.of(destPtr);

        final long srcPtr = srcValue.getLong(valueIndex);
        listB.of(srcPtr);

        final long outPtr = listA.size() > listB.size() ? listA.add(listB) : listB.add(listA);
        destValue.putLong(valueIndex, outPtr);
    }

    @Override
    public void setAllocator(GroupByAllocator allocator) {
        listA.setAllocator(allocator);
        listB.setAllocator(allocator);
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
        sink.val("percentile_disc(").val(arg).val(')');
    }

    @Override
    public void toTop() {
        UnaryFunction.super.toTop();
    }
}
