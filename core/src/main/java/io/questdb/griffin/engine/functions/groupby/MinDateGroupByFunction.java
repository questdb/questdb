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
import io.questdb.griffin.engine.functions.DateFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;

public class MinDateGroupByFunction extends DateFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private int valueIndex;

    public MinDateGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void computeBatch(MapValue mapValue, long ptr, int count) {
        if (count > 0) {
            mapValue.putDate(valueIndex, Vect.minLong(ptr, count));
        }
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        mapValue.putDate(valueIndex, arg.getDate(record));
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        mapValue.minLong(valueIndex, arg.getDate(record));
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public long getDate(Record rec) {
        return rec.getDate(valueIndex);
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
        columnTypes.add(ColumnType.DATE);
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
        long srcMin = srcValue.getDate(valueIndex);
        long destMin = destValue.getDate(valueIndex);
        if (srcMin != Numbers.LONG_NULL && (srcMin < destMin || destMin == Numbers.LONG_NULL)) {
            destValue.putDate(valueIndex, srcMin);
        }
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDate(valueIndex, Numbers.LONG_NULL);
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
