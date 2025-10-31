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
import io.questdb.griffin.engine.functions.decimal.Decimal128Function;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.NumericException;
import org.jetbrains.annotations.NotNull;

class SumDecimal64GroupByFunction extends Decimal128Function implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private final Decimal128 decimal128A = new Decimal128();
    private final Decimal128 decimal128B = new Decimal128();
    private final int position;
    private boolean overflow;
    private long value = 0;
    private int valueIndex;

    public SumDecimal64GroupByFunction(@NotNull Function arg, int position) {
        super(ColumnType.getDecimalType(Decimals.getDecimalTagPrecision(ColumnType.DECIMAL128), ColumnType.getDecimalScale(arg.getType())));
        this.arg = arg;
        this.position = position;
        this.decimal128A.setScale(0);
        this.decimal128B.setScale(0);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        mapValue.putLong(valueIndex + 1, arg.getDecimal64(record));
        mapValue.putBool(valueIndex + 2, false);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        long decimal64A = arg.getDecimal64(record);
        if (decimal64A != Decimals.DECIMAL64_NULL) {
            try {
                if (!mapValue.getBool(valueIndex + 2)) {
                    final long decimal64B = mapValue.getDecimal64(valueIndex + 1);
                    if (decimal64B == Decimals.DECIMAL64_NULL) {
                        mapValue.putLong(valueIndex + 1, decimal64A);
                    } else {
                        add(mapValue, decimal64A, decimal64B);
                    }
                } else {
                    // decimal128 cannot be null, because it catches overflow, NULL would not overflow
                    mapValue.getDecimal128(valueIndex, decimal128A);
                    Decimal128.uncheckedAdd(decimal128A, decimal64A);
                    mapValue.putDecimal128(valueIndex, decimal128A);
                }
            } catch (NumericException e) {
                throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
            }
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public void getDecimal128(Record rec, Decimal128 sink) {
        overflow = rec.getBool(valueIndex + 2);
        if (overflow) {
            rec.getDecimal128(valueIndex, sink);
        } else {
            value = rec.getDecimal64(valueIndex + 1);
            if (value == Decimals.DECIMAL64_NULL) {
                sink.ofRawNull();
            } else {
                sink.ofRaw(value < 0 ? -1 : 0, value);
            }
        }
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
        columnTypes.add(ColumnType.DECIMAL64);
        columnTypes.add(ColumnType.BOOLEAN);
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
        boolean srcOverflow = srcValue.getBool(valueIndex + 2);
        boolean destOverflow = destValue.getBool(valueIndex + 2);

        if (!srcOverflow && !destOverflow) {
            long decimal64B = srcValue.getDecimal64(valueIndex + 1);
            long decimal64A = destValue.getDecimal64(valueIndex + 1);

            boolean srcNull = decimal64B == Decimals.DECIMAL64_NULL;
            boolean destNull = decimal64A == Decimals.DECIMAL64_NULL;

            if (!destNull && !srcNull) {
                // both not null
                add(destValue, decimal64A, decimal64B);
            } else if (destNull) {
                // put src value in
                destValue.putLong(valueIndex + 1, decimal64B);
            }
        } else if (srcOverflow && !destOverflow) {
            // src value cannot be null, null does not overflow
            srcValue.getDecimal128(valueIndex, decimal128A);
            long decimal64A = destValue.getDecimal64(valueIndex + 1);
            boolean destNull = decimal64A == Decimals.DECIMAL64_NULL;

            if (!destNull) {
                Decimal128.uncheckedAdd(decimal128A, decimal64A);
            }
            destValue.putDecimal128(valueIndex, decimal128A);
            destValue.putBool(valueIndex + 2, true);
        } else if (!srcOverflow) {
            // dest overflown, it cannot be null
            long decimal64A = srcValue.getDecimal64(valueIndex + 1);
            destValue.getDecimal128(valueIndex, decimal128A);
            if (decimal64A != Decimals.DECIMAL64_NULL) {
                // both not null
                Decimal128.uncheckedAdd(decimal128A, decimal64A);
                destValue.putDecimal128(valueIndex, decimal128A);
            }
        } else {
            // all overflown - they cannot be null
            srcValue.getDecimal128(valueIndex, decimal128A);
            destValue.getDecimal128(valueIndex, decimal128B);
            Decimal128.uncheckedAdd(decimal128B, decimal128A);
            destValue.putDecimal128(valueIndex, decimal128B);
        }
    }

    @Override
    public void setDecimal128(MapValue mapValue, Decimal128 value) {
        mapValue.putDecimal128(valueIndex, value);
        mapValue.putBool(valueIndex + 2, true);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDecimal128Null(valueIndex);
        mapValue.putLong(valueIndex + 1, Decimals.DECIMAL64_NULL);
        mapValue.putBool(valueIndex + 2, false);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }

    private void add(MapValue mapValue, long decimal64A, long decimal64B) {
        try {
            mapValue.putLong(valueIndex + 1, Decimal64.uncheckedAdd(decimal64A, decimal64B));
        } catch (NumericException e) {
            decimal128A.ofRaw(decimal64B);
            Decimal128.uncheckedAdd(decimal128A, decimal64A);
            mapValue.putDecimal128(valueIndex, decimal128A);
            mapValue.putBool(valueIndex + 2, true);
        }
    }
}
