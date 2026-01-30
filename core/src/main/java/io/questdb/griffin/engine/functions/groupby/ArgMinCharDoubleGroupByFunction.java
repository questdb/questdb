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
import io.questdb.griffin.engine.functions.CharFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

public class ArgMinCharDoubleGroupByFunction extends CharFunction implements GroupByFunction, BinaryFunction {
    private final Function keyArg;
    private final Function valueArg;
    private int valueIndex;

    public ArgMinCharDoubleGroupByFunction(@NotNull Function valueArg, @NotNull Function keyArg) {
        this.valueArg = valueArg;
        this.keyArg = keyArg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        double key = keyArg.getDouble(record);
        if (Numbers.isNull(key)) {
            mapValue.putChar(valueIndex, (char) 0);
            mapValue.putDouble(valueIndex + 1, Double.NaN);
        } else {
            mapValue.putChar(valueIndex, valueArg.getChar(record));
            mapValue.putDouble(valueIndex + 1, key);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        double nextKey = keyArg.getDouble(record);
        if (Numbers.isNull(nextKey)) {
            return;
        }
        double minKey = mapValue.getDouble(valueIndex + 1);
        if (nextKey < minKey || Numbers.isNull(minKey)) {
            mapValue.putChar(valueIndex, valueArg.getChar(record));
            mapValue.putDouble(valueIndex + 1, nextKey);
        }
    }

    @Override
    public char getChar(Record rec) {
        return rec.getChar(valueIndex);
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
        columnTypes.add(ColumnType.CHAR);
        columnTypes.add(ColumnType.DOUBLE);
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
        double srcMinKey = srcValue.getDouble(valueIndex + 1);
        if (Numbers.isNull(srcMinKey)) {
            return;
        }
        double destMinKey = destValue.getDouble(valueIndex + 1);
        if (srcMinKey < destMinKey || Numbers.isNull(destMinKey)) {
            destValue.putChar(valueIndex, srcValue.getChar(valueIndex));
            destValue.putDouble(valueIndex + 1, srcMinKey);
        }
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putChar(valueIndex, (char) 0);
        mapValue.putDouble(valueIndex + 1, Double.NaN);
    }

    @Override
    public boolean supportsParallelism() {
        return BinaryFunction.super.supportsParallelism();
    }
}
