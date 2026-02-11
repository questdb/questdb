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
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UuidFunction;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

public class ArgMinUuidTimestampGroupByFunction extends UuidFunction implements GroupByFunction, BinaryFunction {
    private final Function keyArg;
    private final Function valueArg;
    private int valueIndex;

    public ArgMinUuidTimestampGroupByFunction(@NotNull Function valueArg, @NotNull Function keyArg) {
        this.valueArg = valueArg;
        this.keyArg = keyArg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        long key = keyArg.getTimestamp(record);
        if (key == Numbers.LONG_NULL) {
            mapValue.putLong128(valueIndex, Numbers.LONG_NULL, Numbers.LONG_NULL);
            mapValue.putLong(valueIndex + 1, Numbers.LONG_NULL);
        } else {
            mapValue.putLong128(valueIndex, valueArg.getLong128Lo(record), valueArg.getLong128Hi(record));
            mapValue.putLong(valueIndex + 1, key);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        long nextKey = keyArg.getTimestamp(record);
        if (nextKey == Numbers.LONG_NULL) {
            return;
        }
        long minKey = mapValue.getLong(valueIndex + 1);
        if (minKey == Numbers.LONG_NULL || nextKey < minKey) {
            mapValue.putLong128(valueIndex, valueArg.getLong128Lo(record), valueArg.getLong128Hi(record));
            mapValue.putLong(valueIndex + 1, nextKey);
        }
    }

    @Override
    public Function getLeft() {
        return valueArg;
    }

    @Override
    public long getLong128Hi(Record rec) {
        return rec.getLong128Hi(valueIndex);
    }

    @Override
    public long getLong128Lo(Record rec) {
        return rec.getLong128Lo(valueIndex);
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
        columnTypes.add(ColumnType.UUID);
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
            destValue.putLong128(valueIndex, srcValue.getLong128Lo(valueIndex), srcValue.getLong128Hi(valueIndex));
            destValue.putLong(valueIndex + 1, srcMinKey);
        }
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong128(valueIndex, Numbers.LONG_NULL, Numbers.LONG_NULL);
        mapValue.putLong(valueIndex + 1, Numbers.LONG_NULL);
    }

    @Override
    public boolean supportsParallelism() {
        return BinaryFunction.super.supportsParallelism();
    }
}
