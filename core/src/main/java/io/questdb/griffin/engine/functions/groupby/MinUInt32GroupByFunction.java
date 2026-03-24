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
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UInt32Function;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

public class MinUInt32GroupByFunction extends UInt32Function implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private int valueIndex;

    public MinUInt32GroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        if (arg.isNull(record)) {
            mapValue.putLong(valueIndex, Numbers.LONG_NULL);
        } else {
            mapValue.putLong(valueIndex, arg.getLong(record));
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        if (!arg.isNull(record)) {
            final long val = arg.getLong(record);
            final long cur = mapValue.getLong(valueIndex);
            if (cur == Numbers.LONG_NULL || val < cur) {
                mapValue.putLong(valueIndex, val);
            }
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public int getInt(Record rec) {
        final long v = rec.getLong(valueIndex);
        return v == Numbers.LONG_NULL ? Numbers.INT_NULL : (int) v;
    }

    @Override
    public String getName() {
        return "min";
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
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isNull(Record rec) {
        return rec.getLong(valueIndex) == Numbers.LONG_NULL;
    }

    @Override
    public boolean isThreadSafe() {
        return UnaryFunction.super.isThreadSafe();
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        final long srcMin = srcValue.getLong(valueIndex);
        final long destMin = destValue.getLong(valueIndex);
        if (srcMin != Numbers.LONG_NULL && (srcMin < destMin || destMin == Numbers.LONG_NULL)) {
            destValue.putLong(valueIndex, srcMin);
        }
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, Numbers.LONG_NULL);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }
}
