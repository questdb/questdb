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
    private final Decimal128 decimal128 = new Decimal128();
    private final int position;
    private boolean overflow;
    private long value = 0;
    private int valueIndex;

    public SumDecimal64GroupByFunction(@NotNull Function arg, int position) {
        super(ColumnType.getDecimalType(Decimals.getDecimalTagPrecision(ColumnType.DECIMAL128), ColumnType.getDecimalScale(arg.getType())));
        this.arg = arg;
        this.position = position;
        final int scale = ColumnType.getDecimalScale(type);
        this.decimal128.setScale(scale);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        long decimal64B = arg.getDecimal64(record);
        if (decimal64B != Decimals.DECIMAL64_NULL) {
            mapValue.putLong(valueIndex + 1, decimal64B);
        } else {
            mapValue.putLong(valueIndex + 1, Decimals.DECIMAL64_NULL);
        }
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
                        mapValue.putLong(valueIndex + 1, Decimal64.addSameScaleNoMaxValueCheck(decimal64A, decimal64A));
                    } else {
                        add(mapValue, decimal64A, decimal64B);
                    }
                } else {
                    // decimal128 cannot be null, because it catches overflow, NULL would not overflow
                    mapValue.getDecimal128(valueIndex, decimal128);
                    Decimal128.uncheckedAdd(decimal128, decimal64A);
                    mapValue.putDecimal128(valueIndex, decimal128);
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
    public long getDecimal128Hi(Record rec) {
        // by convention, this is called directly before "lo" call
        overflow = rec.getBool(valueIndex + 2);
        if (overflow) {
            return rec.getDecimal128Hi(valueIndex);
        }
        value = rec.getDecimal64(valueIndex + 1);
        if (value == Decimals.DECIMAL64_NULL) {
            return Decimals.DECIMAL128_HI_NULL;
        }
        return 0;
    }

    @Override
    public long getDecimal128Lo(Record rec) {
        if (overflow) {
            return rec.getDecimal128Lo(valueIndex);
        }
        return value == Decimals.DECIMAL64_NULL ? Decimals.DECIMAL128_LO_NULL : value;
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
            srcValue.getDecimal128(valueIndex, decimal128);
            long decimal64A = destValue.getDecimal64(valueIndex + 1);
            boolean srcNull = decimal128.isNull();
            boolean destNull = decimal64A == Decimals.DECIMAL64_NULL;

            if (!destNull && !srcNull) {
                Decimal128.uncheckedAdd(decimal128, decimal64A);
            }
            destValue.putDecimal128(valueIndex, decimal128);
            destValue.putBool(valueIndex + 2, false);
        } else if (!srcOverflow) {
            // dest overflown
            long decimal64A = srcValue.getDecimal64(valueIndex + 1);
            destValue.getDecimal128(valueIndex, decimal128);

            boolean srcNull = decimal64A == Decimals.DECIMAL64_NULL;
            boolean destNull = decimal128.isNull();

            if (!destNull && !srcNull) {
                // both not null
                Decimal128.uncheckedAdd(decimal128, decimal64A);
                destValue.putDecimal128(valueIndex, decimal128);
            } else if (destNull) {
                // put src value in, dest was null, so we get it back into 128bit range
                destValue.putLong(valueIndex + 1, decimal64A);
            }
        }
    }

    @Override
    public void setDecimal256(MapValue mapValue, long hh, long hl, long lh, long ll) {
        mapValue.putDecimal256(valueIndex, hh, hl, lh, ll);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDecimal256Null(valueIndex);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }

    private void add(MapValue mapValue, long decimal64A, long decimal64B) {
        try {
            mapValue.putLong(valueIndex + 1, Decimal64.addSameScaleNoMaxValueCheck(decimal64A, decimal64B));
        } catch (NumericException e) {
            decimal128.ofRaw(0, 0);
            Decimal128.uncheckedAdd(decimal128, decimal64B);
            Decimal128.uncheckedAdd(decimal128, decimal64A);
            mapValue.putDecimal128(valueIndex, decimal128);
            mapValue.putBool(valueIndex + 2, true);
        }
    }
}
