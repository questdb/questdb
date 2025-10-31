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
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.NumericException;
import org.jetbrains.annotations.NotNull;

import java.math.RoundingMode;

class AvgDecimal64Rescale256GroupByFunction extends Decimal256Function implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private final Decimal128 decimal128A = new Decimal128();
    private final Decimal128 decimal128B = new Decimal128();
    private final Decimal256 decimal256A = new Decimal256();
    private final int position;
    private int valueIndex;

    public AvgDecimal64Rescale256GroupByFunction(@NotNull Function arg, int position, int targetType) {
        super(targetType);
        this.arg = arg;
        this.position = position;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        long value = arg.getDecimal64(record);
        if (value == Decimals.DECIMAL64_NULL) {
            setNull(mapValue);
        } else {
            mapValue.putLong(valueIndex + 1, value);
            mapValue.putLong(valueIndex + 2, 1);
            mapValue.putBool(valueIndex + 3, false);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        long decimal64A = arg.getDecimal64(record);
        if (decimal64A != Decimals.DECIMAL64_NULL) {
            try {
                if (!mapValue.getBool(valueIndex + 3)) {
                    long decimal64B = mapValue.getLong(valueIndex + 1);
                    add(mapValue, decimal64A, decimal64B);
                } else {
                    mapValue.getDecimal128(valueIndex, decimal128B);
                    Decimal128.uncheckedAdd(decimal128B, decimal64A);
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
    public void getDecimal128(Record rec, Decimal128 sink) {
        if (calc(rec)) {
            sink.ofRaw(
                    decimal256A.getLh(),
                    decimal256A.getLl()
            );
        } else {
            sink.ofRawNull();
        }
    }

    @Override
    public short getDecimal16(Record rec) {
        if (calc(rec)) {
            return (short) decimal256A.getLl();
        } else {
            return Decimals.DECIMAL16_NULL;
        }
    }

    @Override
    public void getDecimal256(Record rec, Decimal256 sink) {
        if (calc(rec)) {
            sink.copyRaw(decimal256A);
        } else {
            sink.ofRawNull();
        }
    }

    @Override
    public int getDecimal32(Record rec) {
        if (calc(rec)) {
            return (int) decimal256A.getLl();
        } else {
            return Decimals.DECIMAL32_NULL;
        }
    }

    @Override
    public long getDecimal64(Record rec) {
        if (calc(rec)) {
            return decimal256A.getLl();
        } else {
            return Decimals.DECIMAL64_NULL;
        }
    }

    @Override
    public byte getDecimal8(Record rec) {
        if (calc(rec)) {
            return (byte) decimal256A.getLl();
        } else {
            return Decimals.DECIMAL8_NULL;
        }
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
            long destCount = destValue.getLong(valueIndex + 2);
            final boolean srcNull = srcCount == 0;
            final boolean destNull = destCount == 0;
            if (!destNull && !srcNull) {
                long src = srcValue.getDecimal64(valueIndex + 1);
                long dest = destValue.getDecimal64(valueIndex + 1);
                // both not null
                add(destValue, src, dest);
                destValue.addLong(valueIndex + 2, srcCount);
            } else if (destNull) {
                // put src value in
                long src = srcValue.getDecimal64(valueIndex + 1);
                destValue.putLong(valueIndex + 1, src);
                destValue.putLong(valueIndex + 2, srcCount);
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

    private boolean calc(Record rec) {
        long count = rec.getLong(valueIndex + 2);
        if (count > 0) {
            try {
                final int argScale = ColumnType.getDecimalScale(arg.getType());
                if (rec.getBool(valueIndex + 3)) {
                    rec.getDecimal128(valueIndex, decimal128A);
                    long s = decimal128A.getHigh() < 0 ? -1L : 0L;
                    decimal256A.of(s, s, decimal128A.getHigh(), decimal128A.getLow(), argScale);
                } else {
                    long v = rec.getDecimal64(valueIndex + 1);
                    long s = v < 0 ? -1L : 0L;
                    decimal256A.of(s, s, s, v, argScale);
                }
                decimal256A.divide(0, 0, 0, count, 0, ColumnType.getDecimalScale(type), RoundingMode.HALF_EVEN);
            } catch (NumericException e) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
            }
            return true;
        } else {
            return false;
        }
    }
}
