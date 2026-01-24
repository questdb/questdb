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
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import org.jetbrains.annotations.NotNull;

/**
 * Boolean AND aggregate function - returns true if all values are true, false otherwise.
 * Returns false for empty groups (SQL standard would return null, but QuestDB booleans don't support null).
 */
public class BoolAndGroupByFunction extends BooleanFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private int valueIndex;

    public BoolAndGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        mapValue.putBool(valueIndex, arg.getBool(record));
        mapValue.putLong(valueIndex + 1, 1L);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        // Short-circuit: if already false, stay false
        if (mapValue.getBool(valueIndex)) {
            mapValue.putBool(valueIndex, arg.getBool(record));
        }
        mapValue.addLong(valueIndex + 1, 1L);
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public boolean getBool(Record rec) {
        // Empty group returns false (could be considered null in strict SQL)
        if (rec.getLong(valueIndex + 1) == 0) {
            return false;
        }
        return rec.getBool(valueIndex);
    }

    @Override
    public String getName() {
        return "bool_and";
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
        columnTypes.add(ColumnType.BOOLEAN); // aggregate result
        columnTypes.add(ColumnType.LONG);    // count (for empty group detection)
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
        long srcCount = srcValue.getLong(valueIndex + 1);
        if (srcCount == 0) {
            return; // Source has no data
        }
        long destCount = destValue.getLong(valueIndex + 1);
        if (destCount == 0) {
            // Destination empty: copy from source
            destValue.putBool(valueIndex, srcValue.getBool(valueIndex));
            destValue.putLong(valueIndex + 1, srcCount);
        } else {
            // Merge: AND the results
            destValue.putBool(valueIndex, destValue.getBool(valueIndex) && srcValue.getBool(valueIndex));
            destValue.putLong(valueIndex + 1, destCount + srcCount);
        }
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putBool(valueIndex, true); // Identity for AND
        mapValue.putLong(valueIndex + 1, 0L);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }
}
