/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;

public class CountLong256GroupByFunction extends LongFunction implements UnaryFunction, GroupByFunction {
    private static final ArrayColumnTypes groupedKeyTypes = new ArrayColumnTypes();

    private final Function arg;
    private int groupId;
    // Contains <group id, int value> pairs as keys; we use this map as a set.
    private Map groupedValues;
    private int valueIndex;

    public CountLong256GroupByFunction(Function arg) {
        this.arg = arg;
    }

    @Override
    public void clear() {
        groupId = 0;
        groupedValues.restoreInitialCapacity();
    }

    @Override
    public void close() {
        UnaryFunction.super.close();
        groupId = 0;
        Misc.free(groupedValues);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        final Long256 val = arg.getLong256A(record);
        if (isNotNull(val)) {
            MapKey groupedMapKey = groupedValues.withKey();
            groupedMapKey.putInt(groupId);
            groupedMapKey.putLong256(val);
            groupedMapKey.createValue();
            mapValue.putLong(valueIndex, 1);
        } else {
            mapValue.putLong(valueIndex, 0);
        }
        mapValue.putInt(valueIndex + 1, groupId++);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        final Long256 val = arg.getLong256A(record);
        if (isNotNull(val)) {
            MapKey groupedMapKey = groupedValues.withKey();
            groupedMapKey.putInt(mapValue.getInt(valueIndex + 1));
            groupedMapKey.putLong256(val);
            MapValue groupedMapValue = groupedMapKey.createValue();
            if (groupedMapValue.isNew()) {
                mapValue.addLong(valueIndex, 1);
            }
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public long getLong(Record rec) {
        return rec.getLong(valueIndex);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        UnaryFunction.super.init(symbolTableSource, executionContext);
        if (groupedValues == null) {
            groupedValues = MapFactory.createSmallMap(
                    executionContext.getCairoEngine().getConfiguration(),
                    groupedKeyTypes
            );
        }
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isReadThreadSafe() {
        return false;
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG);
        columnTypes.add(ColumnType.INT);
    }

    @Override
    public void setEmpty(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0L);
    }

    @Override
    public void setLong(MapValue mapValue, long value) {
        mapValue.putLong(valueIndex, value);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, Numbers.LONG_NaN);
    }

    @Override
    public void toTop() {
        UnaryFunction.super.toTop();
        groupId = 0;
        groupedValues.clear();
    }

    private static boolean isNotNull(Long256 value) {
        return value != null &&
                value != Long256Impl.NULL_LONG256 && (value.getLong0() != Numbers.LONG_NaN ||
                value.getLong1() != Numbers.LONG_NaN ||
                value.getLong2() != Numbers.LONG_NaN ||
                value.getLong3() != Numbers.LONG_NaN);
    }

    static {
        groupedKeyTypes.add(ColumnType.INT);
        groupedKeyTypes.add(ColumnType.LONG256);
    }
}
