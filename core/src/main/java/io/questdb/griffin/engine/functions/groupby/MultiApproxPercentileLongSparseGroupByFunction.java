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
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupBySparseHistogram;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;

/**
 * approx_percentile(LONG, DOUBLE[] percentiles [, precision]) over an off-heap {@link GroupBySparseHistogram}.
 * Returns the same arrays as {@link MultiApproxPercentileLongGroupByFunction} and
 * {@link MultiApproxPercentileLongPackedGroupByFunction}, and runs in parallel GROUP BY: the state is a
 * pointer in the group by map, and per-worker partials merge exactly. Used when
 * {@link ApproxPercentileLongSparseGroupByFunction#isEnabled(io.questdb.cairo.CairoConfiguration)}.
 */
public class MultiApproxPercentileLongSparseGroupByFunction extends ArrayFunction implements UnaryFunction, GroupByFunction {
    private final Function exprFunc;
    private final GroupBySparseHistogram histogramA;
    private final GroupBySparseHistogram histogramB;
    private final Function percentileFunc;
    private final int percentilesPos;
    private DirectArray out;
    private int valueIndex;

    public MultiApproxPercentileLongSparseGroupByFunction(Function exprFunc, Function percentileFunc, int precision, int percentilesPos) {
        assert precision >= 0 && precision <= 5;
        this.exprFunc = exprFunc;
        this.percentileFunc = percentileFunc;
        this.percentilesPos = percentilesPos;
        this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
        this.histogramA = new GroupBySparseHistogram(precision);
        this.histogramB = new GroupBySparseHistogram(precision);
    }

    @Override
    public void clear() {
        histogramA.clear();
        histogramB.clear();
        if (out != null) {
            out.clear();
        }
    }

    @Override
    public void close() {
        Misc.free(exprFunc);
        Misc.free(percentileFunc);
        out = Misc.free(out);
        histogramA.close();
        histogramB.close();
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final long val = exprFunc.getLong(record);
        if (val != Numbers.LONG_NULL) {
            histogramA.of(0).recordValue(val);
            mapValue.putLong(valueIndex, histogramA.ptr());
        } else {
            mapValue.putLong(valueIndex, 0);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final long val = exprFunc.getLong(record);
        if (val != Numbers.LONG_NULL) {
            final long ptr = mapValue.getLong(valueIndex);
            histogramA.of(ptr).recordValue(val);
            final long newPtr = histogramA.ptr();
            if (newPtr != ptr) {
                mapValue.putLong(valueIndex, newPtr);
            }
        }
    }

    @Override
    public Function getArg() {
        return exprFunc;
    }

    @Override
    public ArrayView getArray(Record rec) {
        if (out == null) {
            out = new DirectArray();
        }
        final GroupBySparseHistogram histogram = histogramA.of(rec.getLong(valueIndex));
        if (histogram.getTotalCount() == 0) {
            out.ofNull();
            return out;
        }

        ArrayView percentiles = percentileFunc.getArray(rec);
        FlatArrayView view = percentiles.flatView();
        int viewLength = view.length();

        out.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
        out.setDimLen(0, viewLength);
        out.applyShape();

        for (int i = 0; i < viewLength; i++) {
            double p = view.getDoubleAtAbsIndex(i);
            double multiplier = SqlUtil.getPercentileMultiplier(p, percentilesPos);
            out.putDouble(i, histogram.getValueAtPercentile(multiplier * 100));
        }
        return out;
    }

    @Override
    public String getName() {
        return "approx_percentile";
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
        exprFunc.init(symbolTableSource, sqlExecutionContext);
        percentileFunc.init(symbolTableSource, sqlExecutionContext);
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isOrderSensitive() {
        return false;
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        final long srcPtr = srcValue.getLong(valueIndex);
        if (srcPtr == 0) {
            return;
        }
        histogramA.of(destValue.getLong(valueIndex));
        histogramB.of(srcPtr);
        histogramA.merge(histogramB);
        destValue.putLong(valueIndex, histogramA.ptr());
    }

    @Override
    public void setAllocator(GroupByAllocator allocator) {
        histogramA.setAllocator(allocator);
        histogramB.setAllocator(allocator);
    }

    @Override
    public void setEmpty(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism() && (percentileFunc.isConstant() || percentileFunc.isRuntimeConstant());
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("approx_percentile(").val(exprFunc).val(')');
    }
}
