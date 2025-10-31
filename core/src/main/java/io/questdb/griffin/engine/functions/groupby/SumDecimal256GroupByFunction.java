/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.decimal.Decimal256Function;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.NumericException;
import org.jetbrains.annotations.NotNull;

class SumDecimal256GroupByFunction extends Decimal256Function implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private final Decimal256 decimal256A = new Decimal256();
    private final Decimal256 decimal256B = new Decimal256();
    private final int position;
    private int valueIndex;

    public SumDecimal256GroupByFunction(@NotNull Function arg, int position) {
        super(ColumnType.getDecimalType(Decimals.MAX_PRECISION, ColumnType.getDecimalScale(arg.getType())));
        this.arg = arg;
        this.position = position;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        arg.getDecimal256(record, decimal256A);
        mapValue.putDecimal256(valueIndex, decimal256A);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        arg.getDecimal256(record, decimal256A);
        if (!decimal256A.isNull()) {
            mapValue.getDecimal256(valueIndex, decimal256B);
            if (decimal256B.isNull()) {
                mapValue.putDecimal256(valueIndex, decimal256A);
            } else {
                try {
                    decimal256A.uncheckedAdd(decimal256B);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                }
                mapValue.putDecimal256(valueIndex, decimal256A);
            }
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public void getDecimal256(Record rec, Decimal256 sink) {
        rec.getDecimal256(valueIndex, decimal256A);
        if (!decimal256A.isNull() && decimal256A.hasOverflowed()) {
            throw CairoException.nonCritical().position(position).put("sum aggregation failed: an overflow occurred");
        }
        sink.ofRaw(decimal256A.getHh(), decimal256A.getHl(), decimal256A.getLh(), decimal256A.getLl());
    }

    @Override
    public String getName() {
        return "sum";
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.DECIMAL256);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        srcValue.getDecimal256(valueIndex, decimal256A);
        if (!decimal256A.isNull()) {
            destValue.getDecimal256(valueIndex, decimal256B);
            if (decimal256B.isNull()) {
                destValue.putDecimal256(valueIndex, decimal256A);
            } else {
                try {
                    decimal256A.uncheckedAdd(decimal256B);
                } catch (NumericException e) {
                    throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                }
                destValue.putDecimal256(valueIndex, decimal256A);
            }
        }
    }

    @Override
    public void setDecimal256(MapValue mapValue, Decimal256 value) {
        mapValue.putDecimal256(valueIndex, value);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDecimal256Null(valueIndex);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }
}
