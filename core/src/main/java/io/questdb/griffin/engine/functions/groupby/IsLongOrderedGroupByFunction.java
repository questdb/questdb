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
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import org.jetbrains.annotations.NotNull;

public class IsLongOrderedGroupByFunction extends BooleanFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private int flagIndex;
    private int valueIndex;

    public IsLongOrderedGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        mapValue.putBool(flagIndex, true);
        mapValue.putLong(valueIndex, arg.getLong(record));
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        if (mapValue.getBool(flagIndex)) {
            long prev = mapValue.getLong(valueIndex);
            long curr = arg.getLong(record);
            if (curr < prev) {
                mapValue.putBool(flagIndex, false);
            } else {
                mapValue.putLong(valueIndex, curr);
            }
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public boolean getBool(Record rec) {
        return rec.getBool(flagIndex);
    }

    @Override
    public String getName() {
        return "isOrdered";
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.flagIndex = columnTypes.getColumnCount();
        this.valueIndex = flagIndex + 1;
        columnTypes.add(ColumnType.BOOLEAN);
        columnTypes.add(ColumnType.LONG);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putBool(flagIndex, true);
    }
}
