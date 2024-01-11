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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.histogram.org.HdrHistogram.Histogram;

public class ApproxPercentileLongGroupByFunction extends DoubleFunction implements GroupByFunction, BinaryFunction {
    private final Function exprFunc;
    private final int funcPosition;
    private final ObjList<Histogram> histograms = new ObjList<>();
    private final Function percentileFunc;
    private final int precision;
    private int histogramIndex;
    private int valueIndex;

    public ApproxPercentileLongGroupByFunction(Function exprFunc, Function percentileFunc, int precision, int funcPosition) {
        assert precision >= 0 && precision <= 5;
        this.exprFunc = exprFunc;
        this.percentileFunc = percentileFunc;
        this.precision = precision;
        this.funcPosition = funcPosition;
    }

    @Override
    public void clear() {
        histograms.clear();
        histogramIndex = 0;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        final Histogram histogram;
        if (histograms.size() <= histogramIndex) {
            // We pre-size the histogram for [1, 1000] range to avoid resizes in some basic use cases
            // like CPU load percentile or latency in millis.
            histograms.extendAndSet(histogramIndex, histogram = new Histogram(1, 1000, precision));
            histogram.setAutoResize(true);
        } else {
            histogram = histograms.getQuick(histogramIndex);
            histogram.reset();
        }

        final long val = exprFunc.getLong(record);
        if (val != Numbers.LONG_NaN) {
            histogram.recordValue(val);
        }
        mapValue.putLong(valueIndex, histogramIndex++);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        final Histogram histogram = histograms.getQuick(mapValue.getInt(valueIndex));
        final long val = exprFunc.getLong(record);
        if (val != Numbers.LONG_NaN) {
            histogram.recordValue(val);
        }
    }

    @Override
    public double getDouble(Record rec) {
        if (histograms.size() == 0) {
            return Double.NaN;
        }

        final Histogram histogram = histograms.getQuick(rec.getInt(valueIndex));
        if (histogram.getTotalCount() == 0) {
            return Double.NaN;
        }
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
        if (Double.isNaN(percentile) || percentile < 0 || percentile > 1) {
            throw SqlException.$(funcPosition, "percentile must be between 0.0 and 1.0");
        }
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
    public boolean isReadThreadSafe() {
        return false;
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG);
    }

    @Override
    public void setEmpty(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0L);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, Numbers.LONG_NaN);
    }

    @Override
    public void setValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }
}
