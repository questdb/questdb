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
import io.questdb.griffin.engine.functions.CharFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public class MinCharGroupByFunction extends CharFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private int valueIndex;

    public MinCharGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void computeBatch(MapValue mapValue, long ptr, int count) {
        if (count > 0) {
            final long hi = ptr + count * (long) Character.BYTES;
            char min = Unsafe.getUnsafe().getChar(ptr);
            ptr += Character.BYTES;
            for (; ptr < hi; ptr += Character.BYTES) {
                char value = Unsafe.getUnsafe().getChar(ptr);
                if (value > 0 && value < min) {
                    min = value;
                }
            }
            mapValue.putChar(valueIndex, min);
        }
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        mapValue.putChar(valueIndex, arg.getChar(record));
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        char min = mapValue.getChar(valueIndex);
        char next = arg.getChar(record);
        if (next > 0 && next < min) {
            mapValue.putChar(valueIndex, next);
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public char getChar(Record rec) {
        return rec.getChar(valueIndex);
    }

    @Override
    public String getName() {
        return "min";
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
        char srcMin = srcValue.getChar(valueIndex);
        char destMin = destValue.getChar(valueIndex);
        if (srcMin != 0 && (srcMin < destMin || destMin == 0)) {
            destValue.putInt(valueIndex, srcMin);
        }
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putChar(valueIndex, (char) 0);
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
