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
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

public class ArgMaxTimestampUuidGroupByFunction extends TimestampFunction implements GroupByFunction, BinaryFunction {
    private final Function keyArg;
    private final Function valueArg;
    private int valueIndex;

    public ArgMaxTimestampUuidGroupByFunction(@NotNull Function valueArg, @NotNull Function keyArg) {
        super(ColumnType.TIMESTAMP);
        this.valueArg = valueArg;
        this.keyArg = keyArg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        long keyLo = keyArg.getLong128Lo(record);
        long keyHi = keyArg.getLong128Hi(record);
        if (isNullUuid(keyLo, keyHi)) {
            mapValue.putLong(valueIndex, Numbers.LONG_NULL);
            mapValue.putLong128(valueIndex + 1, Numbers.LONG_NULL, Numbers.LONG_NULL);
        } else {
            mapValue.putLong(valueIndex, valueArg.getTimestamp(record));
            mapValue.putLong128(valueIndex + 1, keyLo, keyHi);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        long nextKeyLo = keyArg.getLong128Lo(record);
        long nextKeyHi = keyArg.getLong128Hi(record);
        if (isNullUuid(nextKeyLo, nextKeyHi)) {
            return;
        }
        long maxKeyLo = mapValue.getLong128Lo(valueIndex + 1);
        long maxKeyHi = mapValue.getLong128Hi(valueIndex + 1);
        if (isNullUuid(maxKeyLo, maxKeyHi) || compareUuids(nextKeyLo, nextKeyHi, maxKeyLo, maxKeyHi) > 0) {
            mapValue.putLong(valueIndex, valueArg.getTimestamp(record));
            mapValue.putLong128(valueIndex + 1, nextKeyLo, nextKeyHi);
        }
    }

    @Override
    public Function getLeft() {
        return valueArg;
    }

    @Override
    public String getName() {
        return "arg_max";
    }

    @Override
    public Function getRight() {
        return keyArg;
    }

    @Override
    public long getTimestamp(Record rec) {
        return rec.getLong(valueIndex);
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
        columnTypes.add(ColumnType.UUID);
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
        long srcMaxKeyLo = srcValue.getLong128Lo(valueIndex + 1);
        long srcMaxKeyHi = srcValue.getLong128Hi(valueIndex + 1);
        if (isNullUuid(srcMaxKeyLo, srcMaxKeyHi)) {
            return;
        }
        long destMaxKeyLo = destValue.getLong128Lo(valueIndex + 1);
        long destMaxKeyHi = destValue.getLong128Hi(valueIndex + 1);
        if (isNullUuid(destMaxKeyLo, destMaxKeyHi) || compareUuids(srcMaxKeyLo, srcMaxKeyHi, destMaxKeyLo, destMaxKeyHi) > 0) {
            destValue.putLong(valueIndex, srcValue.getLong(valueIndex));
            destValue.putLong128(valueIndex + 1, srcMaxKeyLo, srcMaxKeyHi);
        }
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, Numbers.LONG_NULL);
        mapValue.putLong128(valueIndex + 1, Numbers.LONG_NULL, Numbers.LONG_NULL);
    }

    @Override
    public boolean supportsParallelism() {
        return BinaryFunction.super.supportsParallelism();
    }

    private static int compareUuids(long lo1, long hi1, long lo2, long hi2) {
        // Compare as unsigned longs: hi first, then lo
        int cmp = Long.compareUnsigned(hi1, hi2);
        if (cmp != 0) {
            return cmp;
        }
        return Long.compareUnsigned(lo1, lo2);
    }

    private static boolean isNullUuid(long lo, long hi) {
        return lo == Numbers.LONG_NULL && hi == Numbers.LONG_NULL;
    }
}
