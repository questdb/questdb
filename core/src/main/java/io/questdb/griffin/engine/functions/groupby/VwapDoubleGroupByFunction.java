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
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

public class VwapDoubleGroupByFunction extends DoubleFunction implements GroupByFunction, BinaryFunction {
    private final Function prcFunc;
    private final Function qtyFunc;
    private int valueIndex;

    public VwapDoubleGroupByFunction(@NotNull Function arg0, @NotNull Function arg1) {
        this.prcFunc = arg0;
        this.qtyFunc = arg1;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        final double prc = prcFunc.getDouble(record);
        final double qty = qtyFunc.getDouble(record);

        if (Numbers.isFinite(prc) && Numbers.isFinite(qty) && qty > 0.0d) {
            final double notional = prc * qty;
            final double vwap = notional / qty;

            mapValue.putDouble(valueIndex, vwap);
            mapValue.putDouble(valueIndex + 1, notional);
            mapValue.putDouble(valueIndex + 2, qty);
        } else {
            mapValue.putDouble(valueIndex, Double.NaN);
            mapValue.putDouble(valueIndex + 1, 0);
            mapValue.putDouble(valueIndex + 2, 0);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        final double prc = prcFunc.getDouble(record);
        final double qty = qtyFunc.getDouble(record);

        if (Numbers.isFinite(prc) && Numbers.isFinite(qty) && qty > 0.0d) {
            final double notional = prc * qty;

            mapValue.addDouble(valueIndex + 1, notional);
            mapValue.addDouble(valueIndex + 2, qty);

            mapValue.putDouble(valueIndex, mapValue.getDouble(valueIndex + 1) / mapValue.getDouble(valueIndex + 2));
        }
    }

    @Override
    public double getDouble(Record rec) {
        return rec.getDouble(valueIndex);
    }

    @Override
    public String getName() {
        return "vwap";
    }

    @Override
    public Function getLeft() {
        return prcFunc;
    }

    @Override
    public Function getRight() {
        return qtyFunc;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.DOUBLE);
    }

    @Override
    public void setDouble(MapValue mapValue, double value) {
        mapValue.putDouble(valueIndex, value);
        mapValue.putDouble(valueIndex + 1, valueIndex);
        mapValue.putDouble(valueIndex + 2, 1);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDouble(valueIndex, Double.NaN);
        mapValue.putDouble(valueIndex + 1, 0);
        mapValue.putDouble(valueIndex + 2, 0);
    }
}
