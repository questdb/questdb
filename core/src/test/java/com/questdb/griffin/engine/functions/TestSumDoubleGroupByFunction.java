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

package com.questdb.griffin.engine.functions;

import com.questdb.cairo.ArrayColumnTypes;
import com.questdb.cairo.ColumnType;
import com.questdb.cairo.map.MapValue;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import org.jetbrains.annotations.NotNull;

public class TestSumDoubleGroupByFunction extends DoubleFunction implements GroupByFunction {
    private final Function value1;
    private int valueIndex;

    public TestSumDoubleGroupByFunction(
            int position,
            @NotNull Function value1,
            @NotNull Function value2
    ) {
        super(position);
        // this is just random attempt to create a problem within a function
        value2.getDouble(null);
        this.value1 = value1;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        mapValue.putDouble(valueIndex, value1.getDouble(record));
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        mapValue.putDouble(valueIndex, mapValue.getDouble(valueIndex) + value1.getDouble(record));
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
    public double getDouble(Record rec) {
        return rec.getDouble(valueIndex);
    }
}
