/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.griffin.engine.functions.groupby;

import com.questdb.cairo.ArrayColumnTypes;
import com.questdb.cairo.ColumnType;
import com.questdb.cairo.map.MapValue;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.engine.functions.GroupByFunction;
import com.questdb.griffin.engine.functions.LongFunction;
import com.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

public class SumLongGroupByFunction extends LongFunction implements GroupByFunction {
    private final Function value;
    private int valueIndex;

    public SumLongGroupByFunction(int position, @NotNull Function value) {
        super(position);
        this.value = value;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        mapValue.putLong(valueIndex, value.getLong(record));
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        mapValue.putLong(valueIndex, mapValue.getLong(valueIndex) + value.getLong(record));
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG);
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
}
