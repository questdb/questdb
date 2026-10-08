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
import io.questdb.cairo.CairoConfiguration;
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
import io.questdb.griffin.engine.groupby.GroupBySparseHistogram;
import io.questdb.std.Numbers;

/**
 * approx_percentile(LONG, percentile, precision) over an off-heap {@link GroupBySparseHistogram}. It returns
 * the same values as {@link ApproxPercentileLongPackedGroupByFunction} (the histogram records the same
 * per-index counts and reads the percentile the same way), but its state lives in the group by map as a
 * pointer, so per-worker partials merge exactly and the function runs in parallel GROUP BY. Used for
 * precision 3..5 when {@link #isEnabled(CairoConfiguration)}.
 */
public class ApproxPercentileLongSparseGroupByFunction extends DoubleFunction implements GroupByFunction, BinaryFunction {
    private final Function exprFunc;
    private final int funcPosition;
    private final GroupBySparseHistogram histogramA;
    private final GroupBySparseHistogram histogramB;
    private final Function percentileFunc;
    private int valueIndex;

    public ApproxPercentileLongSparseGroupByFunction(Function exprFunc, Function percentileFunc, int precision, int funcPosition) {
        this.exprFunc = exprFunc;
        this.percentileFunc = percentileFunc;
        this.funcPosition = funcPosition;
        this.histogramA = new GroupBySparseHistogram(precision);
        this.histogramB = new GroupBySparseHistogram(precision);
    }

    /**
     * True when approx_percentile over LONG uses the off-heap parallel functions: the feature key is on and
     * no per-query memory limit is configured. Under a limit the serial on-heap functions are kept, so the
     * per-worker partials of a parallel GROUP BY cannot make a query fail that ran within the limit before.
     */
    public static boolean isEnabled(CairoConfiguration configuration) {
        return configuration.isSqlParallelApproxPercentileEnabled() && configuration.getQueryMemoryLimitBytes() <= 0;
    }

    @Override
    public void clear() {
        histogramA.clear();
        histogramB.clear();
    }

    @Override
    public void close() {
        BinaryFunction.super.close();
        histogramA.close();
        histogramB.close();
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
        GroupBySparseHistogram histogram = histogramA.of(ptr);
        if (histogram.getTotalCount() == 0) {
            return Double.NaN;
        }
        // read at read time, not cached by init(): the copy of this function that reads a shared GROUP BY
        // cursor (the outer side of a JOIN LATERAL) is never initialised
        return histogram.getValueAtPercentile(percentileFunc.getDouble(null) * 100);
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

        final double percentile = percentileFunc.getDouble(null);
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
    public boolean isOrderSensitive() {
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
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0);
    }

    @Override
    public void setEmpty(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return exprFunc.supportsParallelism();
    }
}
