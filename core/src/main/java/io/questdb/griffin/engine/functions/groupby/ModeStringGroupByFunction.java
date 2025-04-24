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
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.CharSequenceLongHashMap;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.Numbers.INT_NULL;

public class ModeStringGroupByFunction extends StrFunction implements UnaryFunction, GroupByFunction {
    final Function arg;
    int mapIndex;// a pointer to the map that allows you to derive the mode
    ObjList<CharSequenceLongHashMap> maps = new ObjList<>();
    int valueIndex;

    public ModeStringGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void clear() {
        maps.clear();
        mapIndex = 0;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final CharSequenceLongHashMap map;
        if (maps.size() <= mapIndex) {
            maps.extendAndSet(mapIndex, map = new CharSequenceLongHashMap());
        } else {
            map = maps.getQuick(mapIndex);
            map.clear();
        }

        CharSequence value = arg.getStrA(record);
        if (value != null) {
            map.put(value, 1);
        }
        mapValue.putInt(valueIndex, mapIndex++);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final CharSequenceLongHashMap map = maps.getQuick(mapValue.getInt(valueIndex));
        CharSequence value = arg.getStrA(record);
        if (value != null) {
            map.inc(value);
        }
    }

    @Override
    public Function getArg() {
        return arg;
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
    public CharSequence getStrA(Record record) {
        final CharSequenceLongHashMap map = maps.getQuick(record.getInt(valueIndex));
        CharSequence modeKey = null;
        long modeCount = -1;
        for (int i = 0, n = map.keys().size(); i < n; i++) {
            CharSequence key = map.keys().getQuick(i);
            long count = map.get(key);
            if (count > modeCount) {
                modeKey = key;
                modeCount = count;
            }
        }
        return modeKey;
    }

    @Override
    public @Nullable CharSequence getStrB(Record rec) {
        return null;
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
