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
import io.questdb.griffin.engine.functions.*;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.histogram.org.HdrHistogram.DoubleHistogram;

public class ApproxPercentileDoubleGroupByFunction extends DoubleFunction implements GroupByFunction, BinaryFunction {
    // specifies the precision for the recorded values (between 0 and 5).
    // trade-off between memory usage and accuracy.
    private final int numberOfSignificantValueDigits = 3;
    private final Function left;
    private final Function right;
    private final ObjList<DoubleHistogram> histograms = new ObjList<>();

    private int histogramIndex;

    private int valueIndex;

    public ApproxPercentileDoubleGroupByFunction(Function left, Function right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public void clear() {
        histograms.clear();
        histogramIndex = 0;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        final DoubleHistogram histogram;
        if (histograms.size() <= histogramIndex) {
            histograms.extendAndSet(histogramIndex, histogram = new DoubleHistogram(numberOfSignificantValueDigits));
        } else {
            histogram = histograms.getQuick(histogramIndex);
        }
        histogram.reset();

        final double val = right.getDouble(record);
        if (Numbers.isFinite(val)) {
            histogram.recordValue(val);
        }
        mapValue.putLong(valueIndex, histogramIndex++);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        final DoubleHistogram histogram = histograms.getQuick(mapValue.getInt(valueIndex));
        final double val = right.getDouble(record);
        if (Numbers.isFinite(val)) {
            histogram.recordValue(val);
        }
    }

    @Override
    public Function getLeft() { return left; }

    @Override
    public Function getRight() { return right; }

    @Override
    public double getDouble(Record rec) {
        if (histograms.size() == 0) {
            return Double.NaN;
        }

        final DoubleHistogram histogram = histograms.getQuick(rec.getInt(valueIndex));
        if (histogram.empty()) {
            return Double.NaN;
        }
        return histogram.getValueAtPercentile(left.getDouble(rec) * 100);
    }

    @Override
    public String getName() { return "approx_percentile"; }

    @Override
    public boolean isConstant() {
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
}
