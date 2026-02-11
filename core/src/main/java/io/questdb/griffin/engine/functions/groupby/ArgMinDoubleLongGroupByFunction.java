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
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

public class ArgMinDoubleLongGroupByFunction extends DoubleFunction implements GroupByFunction, BinaryFunction {
    private final Function keyArg;
    private final Function valueArg;
    private int valueIndex;

    public ArgMinDoubleLongGroupByFunction(@NotNull Function valueArg, @NotNull Function keyArg) {
        this.valueArg = valueArg;
        this.keyArg = keyArg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        long key = keyArg.getLong(record);
        if (key == Numbers.LONG_NULL) {
            mapValue.putDouble(valueIndex, Double.NaN);
            mapValue.putLong(valueIndex + 1, Numbers.LONG_NULL);
        } else {
            mapValue.putDouble(valueIndex, valueArg.getDouble(record));
            mapValue.putLong(valueIndex + 1, key);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        long nextKey = keyArg.getLong(record);
        if (nextKey == Numbers.LONG_NULL) {
            return;
        }
        long minKey = mapValue.getLong(valueIndex + 1);
        if (minKey == Numbers.LONG_NULL || nextKey < minKey) {
            mapValue.putDouble(valueIndex, valueArg.getDouble(record));
            mapValue.putLong(valueIndex + 1, nextKey);
        }
    }

    @Override
    public double getDouble(Record rec) {
        return rec.getDouble(valueIndex);
    }

    @Override
    public Function getLeft() {
        return valueArg;
    }

    @Override
    public String getName() {
        return "arg_min";
    }

    @Override
    public Function getRight() {
        return keyArg;
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
        columnTypes.add(ColumnType.LONG);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isThreadSafe() {
        return BinaryFunction.super.isThreadSafe();
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        long srcMinKey = srcValue.getLong(valueIndex + 1);
        if (srcMinKey == Numbers.LONG_NULL) {
            return;
        }
        long destMinKey = destValue.getLong(valueIndex + 1);
        if (destMinKey == Numbers.LONG_NULL || srcMinKey < destMinKey) {
            destValue.putDouble(valueIndex, srcValue.getDouble(valueIndex));
            destValue.putLong(valueIndex + 1, srcMinKey);
        }
    }

    @Override
    public void setDouble(MapValue mapValue, double value) {
        mapValue.putDouble(valueIndex, value);
        mapValue.putLong(valueIndex + 1, Numbers.LONG_NULL);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDouble(valueIndex, Double.NaN);
        mapValue.putLong(valueIndex + 1, Numbers.LONG_NULL);
    }

    @Override
    public boolean supportsParallelism() {
        return BinaryFunction.super.supportsParallelism();
    }
}
