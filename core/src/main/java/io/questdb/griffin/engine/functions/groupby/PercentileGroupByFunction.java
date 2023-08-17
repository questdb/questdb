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
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.std.Numbers;
import io.questdb.std.histogram.org.HdrHistogram.Histogram;

public class PercentileGroupByFunction extends LongFunction implements GroupByFunction, TernaryFunction {
    private final Function left;
    private final Function center;
    private final Function right;
    private Histogram histogram;

    public PercentileGroupByFunction(Function left, Function center, Function right) {
        this.left = left;
        this.center = center;
        this.right = right;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        this.histogram = new Histogram(right.getInt(record));

        final long value = left.getLong(record);
        histogram.recordSingleValue(value);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        final long value = left.getLong(record);
        histogram.recordSingleValue(value);
    }

    @Override
    public Function getLeft() { return left; }

    @Override
    public Function getCenter() { return center; }

    @Override
    public Function getRight() { return right; }

    @Override
    public long getLong(Record rec) {
        if(histogram.empty()) {
            return Numbers.LONG_NaN;
        }
        return histogram.getValueAtPercentile(center.getDouble(rec));
    }

    @Override
    public String getName() { return "histogram"; }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {}

    @Override
    public void setNull(MapValue mapValue) {}
}
