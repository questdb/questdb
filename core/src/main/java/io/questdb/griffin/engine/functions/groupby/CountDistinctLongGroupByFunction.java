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
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.LongHashSet;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class CountDistinctLongGroupByFunction extends LongFunction implements UnaryFunction, GroupByFunction {
    private final Function arg;
    private final ObjList<LongHashSet> sets = new ObjList<>();
    private int valueIndex;
    private int setIndex;

    public CountDistinctLongGroupByFunction(Function arg) {
        this.arg = arg;
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public void clear() {
        sets.clear();
        setIndex = 0;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        final LongHashSet set;
        if (sets.size() <= setIndex) {
            sets.extendAndSet(setIndex, set = new LongHashSet());
        } else {
            set = sets.getQuick(setIndex);
        }

        set.clear();
        final long val = arg.getLong(record);
        if (val != Numbers.LONG_NaN) {
            set.add(val);
            mapValue.putLong(valueIndex, 1L);
        } else {
            mapValue.putLong(valueIndex, 0L);
        }
        mapValue.putInt(valueIndex + 1, setIndex++);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        final LongHashSet set = sets.getQuick(mapValue.getInt(valueIndex + 1));
        final long val = arg.getLong(record);
        if (val != Numbers.LONG_NaN) {
            final int index = set.keyIndex(val);
            if (index < 0) {
                return;
            }
            set.addAt(index, val);
            mapValue.addLong(valueIndex, 1);
        }
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
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
    public long getLong(Record rec) {
        return rec.getLong(valueIndex);
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
    public void toTop() {
        UnaryFunction.super.toTop();
        setIndex = 0;
    }
}
