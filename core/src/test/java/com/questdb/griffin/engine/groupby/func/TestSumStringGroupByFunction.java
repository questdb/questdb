/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.groupby.func;

import com.questdb.cairo.ArrayColumnTypes;
import com.questdb.cairo.ColumnType;
import com.questdb.cairo.map.MapValue;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.engine.functions.GroupByFunction;
import com.questdb.griffin.engine.functions.StrFunction;
import com.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

/**
 * This function does not make sense. It is there to test how group-by algos deal
 * with unsupported function types.
 */
public class TestSumStringGroupByFunction extends StrFunction implements GroupByFunction {
    private final Function value;
    private int valueIndex;
    // allocate just to test that close() is correctly invoked
    private long mem = Unsafe.malloc(1024);

    public TestSumStringGroupByFunction(int position, @NotNull Function value) {
        super(position);
        this.value = value;
    }

    @Override
    public void close() {
        Unsafe.free(mem, 1024);
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
        mapValue.putDouble(valueIndex, value.getDouble(record));
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        mapValue.putDouble(valueIndex, mapValue.getDouble(valueIndex) + value.getDouble(record));
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
}
