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
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

public class FirstStrGroupByFunction extends StrFunction implements GroupByFunction, UnaryFunction {
    protected final Function arg;
    private final StringSink sink = new StringSink();
    protected int valueIndex;

    public FirstStrGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void clear() {
        sink.clear();
    }

    @Override
    public CharSequence getStr(Record rec) {
        boolean hasStr = rec.getBool(valueIndex);
        return hasStr ? sink : null;
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return getStr(rec);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isScalar() {
        return false;
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.BOOLEAN);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        CharSequence str = arg.getStr(record);
        sink.clear();
        if (null != str) {
            mapValue.putBool(this.valueIndex, true);
            sink.put(str);
        } else {
            mapValue.putBool(this.valueIndex, false);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        // empty
    }

    @Override
    public Function getArg() {
        return this.arg;
    }

    @Override
    public String getName() {
        return "first";
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putBool(valueIndex, false);
        sink.clear();
    }

}
