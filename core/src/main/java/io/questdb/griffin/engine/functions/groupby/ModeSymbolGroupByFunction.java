/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.IntLongHashMap;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.Numbers.INT_NULL;

public class ModeSymbolGroupByFunction extends SymbolFunction implements UnaryFunction, GroupByFunction {
    final SymbolFunction arg;
    ObjList<IntList> keys = new ObjList<>();
    int mapIndex;// a pointer to the map that allows you to derive the mode
    ObjList<IntLongHashMap> maps = new ObjList<>();
    int valueIndex;

    public ModeSymbolGroupByFunction(@NotNull SymbolFunction arg) {
        this.arg = arg;
    }

    @Override
    public void clear() {
        maps.clear();
        mapIndex = 0;
        keys.clear();
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final IntLongHashMap map;
        final IntList mapKeys;
        if (maps.size() <= mapIndex) {
            maps.extendAndSet(mapIndex, map = new IntLongHashMap());
        } else {
            map = maps.getQuick(mapIndex);
            map.clear();
        }

        if (keys.size() <= mapIndex) {
            keys.extendAndSet(mapIndex, mapKeys = new IntList());
        } else {
            mapKeys = keys.getQuick(mapIndex);
            mapKeys.clear();
        }

        int value = arg.getInt(record);
        if (value != SymbolTable.VALUE_IS_NULL) {
            map.inc(value);
            mapKeys.add(value);
        }
        mapValue.putInt(valueIndex, mapIndex++);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final IntLongHashMap map = maps.getQuick(mapValue.getInt(valueIndex));
        final IntList mapKeys = keys.getQuick(mapValue.getInt(valueIndex));
        int value = arg.getInt(record);
        if (value != SymbolTable.VALUE_IS_NULL) {
            int index = map.keyIndex(value);
            map.inc(value);
            if (index > 0) {
                mapKeys.add(value);
            }
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public int getInt(Record record) {
        final IntLongHashMap map = maps.getQuick(record.getInt(valueIndex));
        final IntList mapKeys = keys.getQuick(record.getInt(valueIndex));
        int modeKey = SymbolTable.VALUE_IS_NULL;
        long modeCount = -1;
        for (int i = 0, n = mapKeys.size(); i < n; i++) {
            int key = mapKeys.getQuick(i);
            long count = map.get(key);
            if (count > modeCount) {
                modeKey = key;
                modeCount = count;
            }
        }
        return modeKey;
    }

    @Override
    public String getName() {
        return "mode";
    }

    @Override
    public int getSampleByFlags() {
        return GroupByFunction.SAMPLE_BY_FILL_ALL;
    }

    @Override
    public @Nullable StaticSymbolTable getStaticSymbolTable() {
        return arg.getStaticSymbolTable();
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return arg.valueOf(getInt(rec));
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        return arg.valueBOf(getInt(rec));
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
        columnTypes.add(ColumnType.INT);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isSymbolTableStatic() {
        return arg.isSymbolTableStatic();
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putInt(valueIndex, INT_NULL);
    }

    @SuppressWarnings("RedundantMethodOverride")
    @Override
    public boolean supportsParallelism() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("mode(").val(arg).val(')');
    }

    @Override
    public void toTop() {
        UnaryFunction.super.toTop();
        mapIndex = 0;
    }

    @Override
    public CharSequence valueBOf(int key) {
        return null;
    }

    @Override
    public CharSequence valueOf(int key) {
        return null;
    }
}
