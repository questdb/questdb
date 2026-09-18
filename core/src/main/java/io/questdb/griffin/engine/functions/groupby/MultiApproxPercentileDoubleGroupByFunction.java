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
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.histogram.org.HdrHistogram.DoubleHistogram;

public class MultiApproxPercentileDoubleGroupByFunction extends ArrayFunction implements UnaryFunction, GroupByFunction {
    private final Function exprFunc;
    private final ObjList<DoubleHistogram> histograms = new ObjList<>();
    private final Function percentileFunc;
    private final int percentilesPos;
    private final int precision;
    private int histogramIndex;
    private DirectArray out;
    private int valueIndex;

    public MultiApproxPercentileDoubleGroupByFunction(Function exprFunc, Function percentileFunc, int precision, int percentilesPos) {
        assert precision >= 0 && precision <= 5;
        this.exprFunc = exprFunc;
        this.percentileFunc = percentileFunc;
        this.precision = precision;
        this.percentilesPos = percentilesPos;
        this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
    }

    @Override
    public void clear() {
        histograms.clear();
        histogramIndex = 0;
        if (out != null) {
            out.clear();
        }
    }

    @Override
    public void close() {
        Misc.free(exprFunc);
        Misc.free(percentileFunc);
        Misc.free(out);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final DoubleHistogram histogram;
        if (histograms.size() <= histogramIndex) {
            histograms.extendAndSet(histogramIndex, histogram = new DoubleHistogram(1000, precision));
            histogram.setAutoResize(true);
        } else {
            histogram = histograms.getQuick(histogramIndex);
            histogram.reset();
        }

        final double val = exprFunc.getDouble(record);
        if (Numbers.isFinite(val)) {
            histogram.recordValue(val);
        }
        mapValue.putLong(valueIndex, histogramIndex++);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final DoubleHistogram histogram = histograms.getQuick(mapValue.getInt(valueIndex));
        final double val = exprFunc.getDouble(record);
        if (Numbers.isFinite(val)) {
            histogram.recordValue(val);
        }
    }

    @Override
    public Function getArg() {
        return exprFunc;
    }

    @Override
    public ArrayView getArray(Record rec) {
        if (histograms.size() == 0) {
            if (out == null) {
                out = new DirectArray();
            }
            out.ofNull();
            return out;
        }

        final DoubleHistogram histogram = histograms.getQuick(rec.getInt(valueIndex));
        if (histogram.getTotalCount() == 0) {
            if (out == null) {
                out = new DirectArray();
            }
            out.ofNull();
            return out;
        }

        ArrayView percentiles = percentileFunc.getArray(rec);
        FlatArrayView view = percentiles.flatView();
        int viewLength = view.length();

        if (out == null) {
            out = new DirectArray();
        }
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
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void setEmpty(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0L);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, Numbers.LONG_NULL);
    }

    @Override
    public boolean supportsParallelism() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("approx_percentile(").val(exprFunc).val(')');
    }
}
