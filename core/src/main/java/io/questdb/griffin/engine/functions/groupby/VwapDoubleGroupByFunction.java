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
    private final Function priceFunction;
    private final Function volumeFunction;
    private int valueIndex;

    public VwapDoubleGroupByFunction(@NotNull Function arg0, @NotNull Function arg1) {
        this.priceFunction = arg0;
        this.volumeFunction = arg1;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        final double price = priceFunction.getDouble(record);
        final double volume = volumeFunction.getDouble(record);
        if (Numbers.isFinite(price) && Numbers.isFinite(volume) && volume > 0.0d) {
            final double notional = price * volume;
            final double vwap = notional / volume;
            mapValue.putDouble(valueIndex, vwap);
            mapValue.putDouble(valueIndex + 1, notional);
            mapValue.putDouble(valueIndex + 2, volume);
        } else {
            mapValue.putDouble(valueIndex, Double.NaN);
            mapValue.putDouble(valueIndex + 1, 0);
            mapValue.putDouble(valueIndex + 2, 0);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        final double price = priceFunction.getDouble(record);
        final double volume = volumeFunction.getDouble(record);
        if (Numbers.isFinite(price) && Numbers.isFinite(volume) && volume > 0.0d) {
            final double notional = price * volume;
            mapValue.addDouble(valueIndex + 1, notional);
            mapValue.addDouble(valueIndex + 2, volume);
            mapValue.putDouble(valueIndex, mapValue.getDouble(valueIndex + 1) / mapValue.getDouble(valueIndex + 2));
        }
    }

    @Override
    public double getDouble(Record rec) {
        return rec.getDouble(valueIndex);
    }

    @Override
    public Function getLeft() {
        return priceFunction;
    }

    @Override
    public String getName() {
        return "vwap";
    }

    @Override
    public Function getRight() {
        return volumeFunction;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isParallelismSupported() {
        return priceFunction.isReadThreadSafe() && volumeFunction.isReadThreadSafe();
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        double srcNotional = srcValue.getDouble(valueIndex + 1);
        double srcVolume = srcValue.getDouble(valueIndex + 2);
        if (Numbers.isFinite(srcNotional) && Numbers.isFinite(srcVolume) && srcVolume > 0.0d) {
            destValue.addDouble(valueIndex + 1, srcNotional);
            destValue.addDouble(valueIndex + 2, srcVolume);
            destValue.putDouble(valueIndex, destValue.getDouble(valueIndex + 1) / destValue.getDouble(valueIndex + 2));
        }
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
        mapValue.putDouble(valueIndex + 1, value);
        mapValue.putDouble(valueIndex + 2, 1);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDouble(valueIndex, Double.NaN);
        mapValue.putDouble(valueIndex + 1, 0);
        mapValue.putDouble(valueIndex + 2, 0);
    }
}
