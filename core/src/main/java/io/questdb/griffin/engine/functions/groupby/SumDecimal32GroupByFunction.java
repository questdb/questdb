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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.decimal.Decimal128Function;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimals;
import org.jetbrains.annotations.NotNull;

class SumDecimal32GroupByFunction extends Decimal128Function implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private final Decimal128 decimal128A = new Decimal128();
    private final Decimal128 decimal128B = new Decimal128();
    private int valueIndex;

    public SumDecimal32GroupByFunction(@NotNull Function arg) {
        super(ColumnType.getDecimalType(Decimals.getDecimalTagPrecision(ColumnType.DECIMAL128), ColumnType.getDecimalScale(arg.getType())));
        this.arg = arg;
        this.decimal128A.setScale(0);
        this.decimal128B.setScale(0);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        int value = arg.getDecimal32(record);
        if (value == Decimals.DECIMAL32_NULL) {
            decimal128A.ofNullRaw();
        } else {
            decimal128A.ofRaw(value < 0 ? -1L : 0L, value);
        }
        mapValue.putDecimal128(valueIndex, decimal128A);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        int value = arg.getDecimal32(record);
        if (value != Decimals.DECIMAL32_NULL) {
            mapValue.getDecimal128(valueIndex, decimal128A);
            if (decimal128A.isNull()) {
                decimal128A.ofRaw(0, 0);
            }
            Decimal128.uncheckedAdd(decimal128A, value);
            mapValue.putDecimal128(valueIndex, decimal128A);
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public long getDecimal128Hi(Record rec) {
        return rec.getDecimal128Hi(valueIndex);
    }

    @Override
    public long getDecimal128Lo(Record rec) {
        return rec.getDecimal128Lo(valueIndex);
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
        columnTypes.add(ColumnType.DECIMAL128);
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
        srcValue.getDecimal128(valueIndex, decimal128A);
        if (!decimal128A.isNull()) {
            destValue.getDecimal128(valueIndex, decimal128B);
            if (decimal128B.isNull()) {
                destValue.putDecimal128(valueIndex, decimal128A);
            } else {
                decimal128B.uncheckedAdd(decimal128A);
                destValue.putDecimal128(valueIndex, decimal128B);
            }
        }
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDecimal128Null(valueIndex);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }
}
