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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.LongList;
import io.questdb.std.LongLongHashMap;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

import static io.questdb.std.Numbers.INT_NULL;
import static io.questdb.std.Numbers.LONG_NULL;

public class ModeLongGroupByFunction extends LongFunction implements UnaryFunction, GroupByFunction {
    final Function arg;
    ObjList<LongList> keys = new ObjList<>();
    int mapIndex;// a pointer to the map that allows you to derive the mode
    ObjList<LongLongHashMap> maps = new ObjList<>();
    int valueIndex;


    public ModeLongGroupByFunction(@NotNull Function arg) {
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
        final LongLongHashMap map;
        final LongList mapKeys;
        if (maps.size() <= mapIndex) {
            maps.extendAndSet(mapIndex, map = new LongLongHashMap());
        } else {
            map = maps.getQuick(mapIndex);
            map.clear();
        }

        if (keys.size() <= mapIndex) {
            keys.extendAndSet(mapIndex, mapKeys = new LongList());
        } else {
            mapKeys = keys.getQuick(mapIndex);
            mapKeys.clear();
        }

        long value = arg.getLong(record);
        if (value != LONG_NULL) {
            map.inc(value);
            mapKeys.add(value);
        }
        mapValue.putInt(valueIndex, mapIndex++);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final LongLongHashMap map = maps.getQuick(mapValue.getInt(valueIndex));
        final LongList mapKeys = keys.getQuick(mapValue.getInt(valueIndex));
        long value = arg.getLong(record);
        if (value != LONG_NULL) {
            int index = map.keyIndex(value);
            if (index > 0) {
                map.inc(value);
                mapKeys.add(value);
            } else {
                map.inc(index);
            }
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public long getLong(Record record) {
        final LongLongHashMap map = maps.getQuick(record.getInt(valueIndex));
        final LongList mapKeys = keys.getQuick(record.getInt(valueIndex));
        long modeKey = LONG_NULL;
        long modeCount = -1;
        for (int i = 0, n = mapKeys.size(); i < n; i++) {
            long key = mapKeys.getQuick(i);
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
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putInt(valueIndex, INT_NULL);
    }

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
}
