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
import io.questdb.griffin.engine.functions.*;
import io.questdb.std.str.DirectCharSink;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

public class FirstStringGroupByFunction extends StrFunction implements GroupByFunction, UnaryFunction {
    protected final Function arg;
    protected int valueIndex;
    protected List<DirectCharSink> stringValues;

    public FirstStringGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
        stringValues = new ArrayList<>(32);
    }

    @Override
    public void close() {
        for(DirectCharSink ds : stringValues) {
            ds.close();
        }
        stringValues.clear();
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        int index = stringValues.size();
        mapValue.putInt(this.valueIndex, index);
        CharSequence cs = this.arg.getStr(record);
        DirectCharSink copy = new DirectCharSink(cs.length());
        copy.put(cs);
        stringValues.add(copy);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.INT); // pointer to string
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putInt(this.valueIndex, -1);
    }

    @Override
    public CharSequence getStr(Record record) {
        if(record == null) {
            return this.arg.getStr(null);
        }
        int ix = record.getInt(this.valueIndex);
        if(ix == -1) {
            return null;
        }
        return this.stringValues.get(ix);
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
