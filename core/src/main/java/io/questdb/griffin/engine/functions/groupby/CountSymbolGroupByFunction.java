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
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

import static io.questdb.cairo.sql.SymbolTable.VALUE_IS_NULL;

public class CountSymbolGroupByFunction extends LongFunction implements GroupByFunction {
    private final Function arg;
    private final ObjList<IntList> lists = new ObjList<>();
    private int valueIndex;
    private int setIndex;

    public CountSymbolGroupByFunction(Function arg) {
        this.arg = arg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        final IntList list;
        if (lists.size() <= setIndex) {
            lists.extendAndSet(setIndex, list = new IntList());
        } else {
            list = lists.getQuick(setIndex);
        }
        list.clear(0);
        mapValue.putInt(valueIndex + 1, setIndex);
        setIndex++;

        int val = arg.getInt(record);
        if (val != VALUE_IS_NULL) {
            list.extendAndSet(val, 1);
            mapValue.putLong(valueIndex, 1L);
        } else {
            mapValue.putLong(valueIndex, 0L);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        final IntList set = lists.getQuick(mapValue.getInt(valueIndex + 1));
        final int val = arg.getInt(record);
        if (val != VALUE_IS_NULL) {
            if (val < set.size()) {
                if (set.getQuick(val) == 1) {
                    return;
                }
                set.setQuick(val, 1);
            } else {
                set.extendAndSet(val, 1);
            }
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
    public boolean isStateless() {
        return false;
    }

    @Override
    public void toTop() {
        setIndex = 0;
    }
}
