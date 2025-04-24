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
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.std.ObjList;
import io.questdb.std.Utf8SequenceLongHashMap;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.Numbers.INT_NULL;

public class ModeVarcharGroupByFunction extends VarcharFunction implements UnaryFunction, GroupByFunction {
    final Function arg;
    int mapIndex;// a pointer to the map that allows you to derive the mode
    ObjList<Utf8SequenceLongHashMap> maps = new ObjList<>();
    int valueIndex;


    public ModeVarcharGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void clear() {
        maps.clear();
        mapIndex = 0;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final Utf8SequenceLongHashMap map;
        if (maps.size() <= mapIndex) {
            maps.extendAndSet(mapIndex, map = new Utf8SequenceLongHashMap());
        } else {
            map = maps.getQuick(mapIndex);
            map.clear();
        }

        Utf8Sequence value = arg.getVarcharA(record);
        if (value != null) {
            map.put(value, 1);
        }
        mapValue.putInt(valueIndex, mapIndex++);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final Utf8SequenceLongHashMap map = maps.getQuick(mapValue.getInt(valueIndex));
        Utf8Sequence value = arg.getVarcharA(record);
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
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public Utf8Sequence getVarcharA(Record record) {
        final Utf8SequenceLongHashMap map = maps.getQuick(record.getInt(valueIndex));
        Utf8String modeKey = null;
        long modeCount = -1;
        for (int i = 0, n = map.keys().size(); i < n; i++) {
            Utf8String key = map.keys().getQuick(i);
            long count = map.get(key);
            if (count > modeCount) {
                modeKey = key;
                modeCount = count;
            }
        }
        return modeKey;
    }

    @Override
    public @Nullable Utf8Sequence getVarcharB(Record rec) {
        return null;
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
