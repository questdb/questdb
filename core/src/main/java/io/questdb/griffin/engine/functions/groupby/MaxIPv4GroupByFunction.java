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
import io.questdb.griffin.engine.functions.IPv4Function;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

public class MaxIPv4GroupByFunction extends IPv4Function implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private int valueIndex;

    public MaxIPv4GroupByFunction(@NotNull Function arg) {
        super();
        this.arg = arg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        mapValue.putInt(valueIndex, arg.getIPv4(record));
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        long max = Numbers.ipv4ToLong(mapValue.getIPv4(valueIndex));
        long next = Numbers.ipv4ToLong(arg.getIPv4(record));
        if (next > max) {
            mapValue.putInt(valueIndex, (int) next);
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public int getIPv4(Record rec) {
        return rec.getIPv4(valueIndex);
    }

    @Override
    public String getName() {
        return "max";
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isParallelismSupported() {
        return arg.isReadThreadSafe();
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        long srcMax = Numbers.ipv4ToLong(srcValue.getIPv4(valueIndex));
        long destMax = Numbers.ipv4ToLong(destValue.getIPv4(valueIndex));
        if (srcMax > destMax || destMax == Numbers.IPv4_NULL) {
            destValue.putInt(valueIndex, (int) srcMax);
        }
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.IPv4);
    }

    @Override
    public void setInt(MapValue mapValue, int value) {
        mapValue.putInt(valueIndex, value);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putInt(valueIndex, Numbers.IPv4_NULL);
    }

}
