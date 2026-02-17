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

public class AvgShortGroupByFunction extends DoubleFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private int valueIndex;

    public AvgShortGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void computeBatch(MapValue mapValue, long ptr, int count) {
        if (count > 0) {
            mapValue.putLong(valueIndex, Vect.sumShort(ptr, count));
            mapValue.putLong(valueIndex + 1, count);
        }
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final short value = arg.getShort(record);
        mapValue.putLong(valueIndex, value);
        mapValue.putLong(valueIndex + 1, 1);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final short value = arg.getShort(record);
        mapValue.addLong(valueIndex, value);
        mapValue.addLong(valueIndex + 1, 1);
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public int getComputeBatchArgType() {
        return ColumnType.SHORT;
    }

    @Override
    public double getDouble(Record rec) {
        final long sum = rec.getLong(valueIndex);
        final long count = rec.getLong(valueIndex + 1);
        if (sum != Numbers.LONG_NULL && count > 0) {
            return (double) rec.getLong(valueIndex) / rec.getLong(valueIndex + 1);
        }
        return Double.NaN;
    }

    @Override
    public String getName() {
        return "avg";
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
        columnTypes.add(ColumnType.LONG);
        columnTypes.add(ColumnType.LONG);
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
        final long srcSum = srcValue.getLong(valueIndex);
        final long srcCount = srcValue.getLong(valueIndex + 1);
        if (srcCount > 0) {
            final long destSum = destValue.getLong(valueIndex);
            final long destCount = destValue.getLong(valueIndex + 1);
            if (destCount > 0) {
                destValue.putLong(valueIndex, destSum + srcSum);
                destValue.putLong(valueIndex + 1, destCount + srcCount);
            } else {
                destValue.putLong(valueIndex, srcSum);
                destValue.putLong(valueIndex + 1, srcCount);
            }
        }
    }

    @Override
    public void setDouble(MapValue mapValue, double value) {
        mapValue.putDouble(valueIndex, value);
        mapValue.putLong(valueIndex + 1, 1);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, Numbers.LONG_NULL);
        mapValue.putLong(valueIndex + 1, 0);
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
