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
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.NumericException;
import org.jetbrains.annotations.NotNull;

class SumDecimal128GroupByFunction extends Decimal256Function implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private final Decimal128 decimal128 = new Decimal128();
    private final Decimal256 decimal256A = new Decimal256();
    private final Decimal256 decimal256B = new Decimal256();
    private int valueIndex;

    public SumDecimal128GroupByFunction(@NotNull Function arg) {
        super(ColumnType.getDecimalType(Decimals.MAX_PRECISION, ColumnType.getDecimalScale(arg.getType())));
        this.arg = arg;
        this.decimal256A.setScale(0);
        this.decimal256B.setScale(0);
        this.decimal128.setScale(0);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        arg.getDecimal128(record, decimal128);
        if (!decimal128.isNull()) {
            long s = decimal128.isNegative() ? -1L: 0L;
            mapValue.putDecimal256(valueIndex, s, s, decimal128.getHigh(), decimal128.getLow());
        } else {
            mapValue.putDecimal256Null(valueIndex);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        arg.getDecimal128(record, decimal128);
        if (!decimal128.isNull()) {
            mapValue.getDecimal256(valueIndex, decimal256A);
            if (decimal256A.isNull()) {
                decimal256A.ofRaw(0, 0, 0, 0);
            }
            Decimal256.uncheckedAdd(decimal256A, decimal128);
            mapValue.putDecimal256(valueIndex, decimal256A);
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public long getDecimal256HH(Record rec) {
        return rec.getDecimal256HH(valueIndex);
    }

    @Override
    public long getDecimal256HL(Record rec) {
        return rec.getDecimal256HL(valueIndex);
    }

    @Override
    public long getDecimal256LH(Record rec) {
        return rec.getDecimal256LH(valueIndex);
    }

    @Override
    public long getDecimal256LL(Record rec) {
        return rec.getDecimal256LL(valueIndex);
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
                Decimal256.uncheckedAdd(decimal256A, decimal256B);
                destValue.putDecimal256(valueIndex, decimal256A);
            }
        }
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
