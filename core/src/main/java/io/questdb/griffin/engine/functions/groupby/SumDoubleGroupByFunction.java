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
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;

public class SumDoubleGroupByFunction extends DoubleFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private int valueIndex;

    public SumDoubleGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void computeBatch(MapValue mapValue, long ptr, int count) {
        if (count > 0) {
            final double value = Vect.sumDouble(ptr, count);
            mapValue.putDouble(valueIndex, value);
        }
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final double value = arg.getDouble(record);
        mapValue.putDouble(valueIndex, value);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final double value = arg.getDouble(record);
        if (Numbers.isFinite(value)) {
            final double sum = mapValue.getDouble(valueIndex);
            if (Numbers.isFinite(sum)) {
                mapValue.putDouble(valueIndex, sum + value);
            } else {
                mapValue.putDouble(valueIndex, value);
            }
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public double getDouble(Record rec) {
        return rec.getDouble(valueIndex);
    }

    @Override
    public String getName() {
        return "sum";
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
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.DOUBLE);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isThreadSafe() {
        return UnaryFunction.super.isThreadSafe();
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        final double srcSum = srcValue.getDouble(valueIndex);
        if (Numbers.isFinite(srcSum)) {
            final double destSum = destValue.getDouble(valueIndex);
            if (Numbers.isFinite(destSum)) {
                destValue.putDouble(valueIndex, destSum + srcSum);
            } else {
                destValue.putDouble(valueIndex, srcSum);
            }
        }
    }

    @Override
    public void setDouble(MapValue mapValue, double value) {
        mapValue.putDouble(valueIndex, value);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDouble(valueIndex, Double.NaN);
    }

    @Override
    public boolean supportsBatchComputation() {
        return true;
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }
}
