/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;
import io.questdb.std.Sinkable;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

public class KSumDoubleGroupByFunction extends DoubleFunction implements GroupByFunction, UnaryFunction, Sinkable {
    private final Function arg;
    private int valueIndex;

    public KSumDoubleGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        final double value = arg.getDouble(record);
        if (Numbers.isFinite(value)) {
            mapValue.putDouble(valueIndex, value);
            mapValue.putLong(valueIndex + 2, 1);
        } else {
            mapValue.putDouble(valueIndex, 0); // sum = 0
            mapValue.putLong(valueIndex + 2, 0); // finite count = 0
        }
        mapValue.putDouble(valueIndex + 1, 0.0); // c = 0
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        final double value = arg.getDouble(record);
        if (Numbers.isFinite(value)) {
            double sum = mapValue.getDouble(valueIndex);
            double c = mapValue.getDouble(valueIndex + 1);
            double y = value - c;
            double t = sum + y;
            mapValue.putDouble(valueIndex, t);
            mapValue.putDouble(valueIndex + 1, t - sum - y);
            mapValue.addLong(valueIndex + 2, 1);
        }
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.DOUBLE); // sum
        columnTypes.add(ColumnType.DOUBLE); // c
        columnTypes.add(ColumnType.LONG); // finite value count
    }

    @Override
    public void setDouble(MapValue mapValue, double value) {
        mapValue.putDouble(valueIndex, value);
        mapValue.putLong(valueIndex + 2, 1);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDouble(valueIndex, Double.NaN);
        mapValue.putLong(valueIndex + 2, 0);
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public double getDouble(Record rec) {
        return rec.getLong(valueIndex + 2) > 0 ? rec.getDouble(valueIndex) : Double.NaN;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put("KSumDouble(").put(arg).put(')');
    }
}
