/*+*****************************************************************************
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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByHistogram;
import io.questdb.std.Numbers;

public class ApproxPercentileLongGroupByFunction extends DoubleFunction implements GroupByFunction, BinaryFunction {
    private final Function exprFunc;
    private final int funcPosition;
    private final GroupByHistogram histogramA;
    private final GroupByHistogram histogramB;
    private final Function percentileFunc;
    private double percentile;
    private int valueIndex;

    public ApproxPercentileLongGroupByFunction(Function exprFunc, Function percentileFunc, int precision, int funcPosition) {
        this.exprFunc = exprFunc;
        this.percentileFunc = percentileFunc;
        this.funcPosition = funcPosition;
        // We pre-size the histogram for [1, 1000] range to avoid resizes in some basic use cases
        // like CPU load percentile or latency in millis.
        this.histogramA = new GroupByHistogram(precision, 1000);
        this.histogramB = new GroupByHistogram(precision, 1000);
    }

    @Override
    public void clear() {
        histogramA.clear();
        histogramB.clear();
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final long val = exprFunc.getLong(record);
        if (val != Numbers.LONG_NULL) {
            histogramA.of(0);
            histogramA.recordValue(val);
            mapValue.putLong(valueIndex, histogramA.ptr());
        } else {
            mapValue.putLong(valueIndex, 0);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final long val = exprFunc.getLong(record);
        if (val != Numbers.LONG_NULL) {
            long ptr = mapValue.getLong(valueIndex);
            histogramA.of(ptr).recordValue(val);
            long newPtr = histogramA.ptr();
            if (newPtr != ptr) {
                mapValue.putLong(valueIndex, newPtr);
            }
        }
    }

    @Override
    public double getDouble(Record rec) {
        long ptr = rec.getLong(valueIndex);
        if (ptr == 0) {
            return Double.NaN;
        }
        GroupByHistogram histogram = histogramA.of(ptr);
        if (histogram.getTotalCount() == 0) {
            return Double.NaN;
        }
        return histogram.getValueAtPercentile(percentile * 100);
    }

    @Override
    public Function getLeft() {
        return exprFunc;
    }

    @Override
    public String getName() {
        return "approx_percentile";
    }

    @Override
    public Function getRight() {
        return percentileFunc;
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        BinaryFunction.super.init(symbolTableSource, executionContext);

        percentile = percentileFunc.getDouble(null);
        if (Numbers.isNull(percentile) || percentile < 0 || percentile > 1) {
            throw SqlException.$(funcPosition, "percentile must be between 0.0 and 1.0");
        }
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        initValueIndex(columnTypes.getColumnCount());
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
        long srcPtr = srcValue.getLong(valueIndex);
        if (srcPtr == 0) {
            return;
        }

        long destPtr = destValue.getLong(valueIndex);
        if (destPtr == 0) {
            destValue.putLong(valueIndex, srcPtr);
            return;
        }

        histogramA.of(destPtr);
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
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return true;
    }
}
