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
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;

public class LastStringGroupByFunction extends StrFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private int valueIndex;
    private final ArrayList<CharSequence> stringValues;

    public LastStringGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
        stringValues = new ArrayList<>(32);
        stringValues.add("N/A");
        // we initialize with one placeholder value
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        int index = stringValues.size();
        CharSequence cs = this.arg.getStr(record);
        // per default, int inside the map will be zero. we know zero points to the placeholder, indicating that
        // this is a new group key, and we need to overwrite it
        if(mapValue.getInt(this.valueIndex) == 0) {
            mapValue.putInt(this.valueIndex, index);
            // to string needed to create a copy, otherwise we will end up with all the same values
            stringValues.add(cs.toString());
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        stringValues.set(mapValue.getInt(this.valueIndex), this.arg.getStr(record).toString());
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.INT); // pointer to string
    }

    @Override
    public void setNull(MapValue mapValue) {
    }

    @Override
    public CharSequence getStr(Record record) {
        return this.stringValues.get(record.getInt(this.valueIndex));
    }

    @Override
    public CharSequence getStrB(Record record) {
        return getStr(record);
    }


    @Override
    public Function getArg() {
        return this.arg;
    }
}
