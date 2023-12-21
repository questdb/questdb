/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

public class MaxTimestampGroupByFunction extends TimestampFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private int valueIndex;

    public MaxTimestampGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        mapValue.putTimestamp(valueIndex, arg.getTimestamp(record));
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        long max = mapValue.getTimestamp(valueIndex);
        long next = arg.getTimestamp(record);
        if (next > max) {
            mapValue.putTimestamp(valueIndex, next);
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public String getName() {
        return "max";
    }

    @Override
    public long getTimestamp(Record rec) {
        return rec.getTimestamp(valueIndex);
    }

    @Override
    public boolean isParallelismSupported() {
        return arg.isReadThreadSafe();
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        long srcMax = srcValue.getLong(valueIndex);
        long destMax = destValue.getLong(valueIndex);
        if (srcMax > destMax || destMax == Numbers.LONG_NaN) {
            destValue.putLong(valueIndex, srcMax);
        }
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.TIMESTAMP);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putTimestamp(valueIndex, Numbers.LONG_NaN);
    }
}
