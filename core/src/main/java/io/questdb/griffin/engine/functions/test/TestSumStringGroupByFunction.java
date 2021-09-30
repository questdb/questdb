/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.test;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

/**
 * This function does not make sense. It is there to test how group-by algos deal
 * with unsupported function types.
 */
public class TestSumStringGroupByFunction extends StrFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    // allocate just to test that close() is correctly invoked
    private final long mem = Unsafe.malloc(1024, MemoryTag.NATIVE_DEFAULT);
    private int valueIndex;

    public TestSumStringGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void close() {
        Unsafe.free(mem, 1024, MemoryTag.NATIVE_DEFAULT);
    }

    @Override
    public CharSequence getStr(Record rec) {
        return null;
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return null;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        mapValue.putDouble(valueIndex, arg.getDouble(record));
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        mapValue.putDouble(valueIndex, mapValue.getDouble(valueIndex) + arg.getDouble(record));
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.DOUBLE);
    }

    @Override
    public void setDouble(MapValue mapValue, double value) {
        mapValue.putDouble(valueIndex, value);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDouble(valueIndex, Double.NaN);
    }

    @Override
    public Function getArg() {
        return arg;
    }
}
