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
import io.questdb.griffin.engine.functions.decimal.Decimal32Function;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.NumericException;
import org.jetbrains.annotations.NotNull;

import java.math.RoundingMode;

class AvgDecimal32GroupByFunction extends Decimal32Function implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private final Decimal128 decimal128A = new Decimal128();
    private final Decimal128 decimal128B = new Decimal128();
    private final int position;
    private int valueIndex;

    public AvgDecimal32GroupByFunction(@NotNull Function arg, int position) {
        super(arg.getType());
        this.arg = arg;
        this.position = position;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        int value = arg.getDecimal32(record);
        if (value == Decimals.DECIMAL32_NULL) {
            setNull(mapValue);
        } else {
            mapValue.putLong(valueIndex + 1, value);
            mapValue.putLong(valueIndex + 2, 1);
            mapValue.putBool(valueIndex + 3, false);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        int decimal32A = arg.getDecimal32(record);
        if (decimal32A != Decimals.DECIMAL32_NULL) {
            try {
                if (!mapValue.getBool(valueIndex + 3)) {
                    long decimal64B = mapValue.getLong(valueIndex + 1);
                    add(mapValue, decimal32A, decimal64B);
                } else {
                    mapValue.getDecimal128(valueIndex, decimal128B);
                    Decimal128.uncheckedAdd(decimal128B, decimal32A);
                    mapValue.putDecimal128(valueIndex, decimal128B);
                }
                mapValue.addLong(valueIndex + 2, 1);
            } catch (NumericException e) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
            }
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public int getDecimal32(Record rec) {
        return calc(rec);
    }

    @Override
    public String getName() {
        return "avg";
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
        columnTypes.add(ColumnType.LONG);
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
        boolean srcOverflow = srcValue.getBool(valueIndex + 3);
        boolean destOverflow = destValue.getBool(valueIndex + 3);

        if (!srcOverflow && !destOverflow) {
            long srcCount = srcValue.getLong(valueIndex + 2);
            final boolean srcNull = srcCount == 0;
            if (!srcNull) {
                long destCount = destValue.getLong(valueIndex + 2);
                final boolean destNull = destCount == 0;
                long src = srcValue.getDecimal64(valueIndex + 1);
                if (!destNull) {
                    long dest = destValue.getDecimal64(valueIndex + 1);
                    // both not null
                    add(destValue, src, dest);
                    destValue.addLong(valueIndex + 2, srcCount);
                } else {
                    // put src value in
                    destValue.putLong(valueIndex + 1, src);
                    destValue.putLong(valueIndex + 2, srcCount);
                }
            }
        } else if (srcOverflow && !destOverflow) {
            // src overflown, therefore it could not be null (null does not overflow)
            long srcCount = srcValue.getLong(valueIndex + 2);
            long destCount = destValue.getLong(valueIndex + 2);
            final boolean destNull = destCount == 0;

            srcValue.getDecimal128(valueIndex, decimal128B);
            if (!destNull) {
                long dest = destValue.getDecimal64(valueIndex + 1);
                Decimal128.uncheckedAdd(decimal128B, dest);
            }
            destValue.putDecimal128(valueIndex, decimal128B);
            destValue.addLong(valueIndex + 2, srcCount);
            destValue.putBool(valueIndex + 3, true);
        } else if (!srcOverflow) {
            // dest overflown, it cannot be null
            long srcCount = srcValue.getLong(valueIndex + 2);
            final boolean srcNull = srcCount == 0;

            if (!srcNull) {
                // both not null
                long src = srcValue.getDecimal64(valueIndex + 1);
                destValue.getDecimal128(valueIndex, decimal128B);
                Decimal128.uncheckedAdd(decimal128B, src);
                destValue.putDecimal128(valueIndex, decimal128B);
                destValue.addLong(valueIndex + 2, srcCount);
            }
        } else {
            // both overflown, neither could be null
            srcValue.getDecimal128(valueIndex, decimal128A);
            destValue.getDecimal128(valueIndex, decimal128B);
            decimal128B.add(decimal128A);
            destValue.putDecimal128(valueIndex, decimal128B);
            destValue.addLong(valueIndex + 2, srcValue.getLong(valueIndex + 2));
        }
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex + 1, 0);
        mapValue.putLong(valueIndex + 2, 0);
        mapValue.putBool(valueIndex + 3, false);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }

    private void add(MapValue mapValue, int decimal32A, long decimal64B) {
        try {
            mapValue.putLong(valueIndex + 1, Decimal64.uncheckedAdd(decimal32A, decimal64B));
        } catch (NumericException e) {
            decimal128A.ofRaw(decimal64B);
            Decimal128.uncheckedAdd(decimal128A, decimal32A);
            mapValue.putDecimal128(valueIndex, decimal128A);
            mapValue.putBool(valueIndex + 3, true);
        }
    }

    private void add(MapValue mapValue, long decimal64A, long decimal64B) {
        try {
            mapValue.putLong(valueIndex + 1, Decimal64.uncheckedAdd(decimal64A, decimal64B));
        } catch (NumericException e) {
            decimal128A.ofRaw(decimal64B);
            Decimal128.uncheckedAdd(decimal128A, decimal64A);
            mapValue.putDecimal128(valueIndex, decimal128A);
            mapValue.putBool(valueIndex + 3, true);
        }
    }

    private int calc(Record rec) {
        long count = rec.getLong(valueIndex + 2);
        if (count > 0) {
            if (rec.getBool(valueIndex + 3)) {
                rec.getDecimal128(valueIndex, decimal128A);
            } else {
                long v = rec.getDecimal64(valueIndex + 1);
                decimal128A.ofRaw(v);
            }
            final int scale = ColumnType.getDecimalScale(type);
            decimal128A.setScale(scale);
            try {
                decimal128A.divide(0, count, 0, scale, RoundingMode.HALF_EVEN);
            } catch (NumericException e) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
            }
            return (int) decimal128A.getLow();
        } else {
            return Decimals.DECIMAL32_NULL;
        }
    }
}
