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

import java.math.RoundingMode;

class AvgDecimal128Rescale256GroupByFunction extends Decimal256Function implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private final Decimal128 decimal128A = new Decimal128();
    private final Decimal128 decimal128B = new Decimal128();
    private final Decimal256 decimal256A = new Decimal256();
    private final Decimal256 decimal256B = new Decimal256();
    private final int position;
    private int valueIndex;

    public AvgDecimal128Rescale256GroupByFunction(@NotNull Function arg, int position, int targetType) {
        super(targetType);
        this.arg = arg;
        this.position = position;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        arg.getDecimal128(record, decimal128A);
        if (decimal128A.isNull()) {
            mapValue.putDecimal128(valueIndex + 1, Decimal128.ZERO);
            mapValue.putLong(valueIndex + 2, 0);
        } else {
            mapValue.putDecimal128(valueIndex + 1, decimal128A);
            mapValue.putLong(valueIndex + 2, 1);
        }
        mapValue.putBool(valueIndex + 3, false);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        arg.getDecimal128(record, decimal128A);
        if (!decimal128A.isNull()) {
            try {
                if (!mapValue.getBool(valueIndex + 3)) {
                    mapValue.getDecimal128(valueIndex + 1, decimal128B);
                    try {
                        decimal128B.uncheckedAdd(decimal128A);
                        mapValue.putDecimal128(valueIndex + 1, decimal128B);
                    } catch (NumericException e) {
                        decimal256B.ofRaw(0, 0, 0, 0);
                        Decimal256.uncheckedAdd(decimal256B, decimal128B);
                        Decimal256.uncheckedAdd(decimal256B, decimal128A);
                        mapValue.putDecimal256(valueIndex, decimal256B);
                        mapValue.putBool(valueIndex + 3, true);
                    }
                } else {
                    mapValue.getDecimal256(valueIndex, decimal256B);
                    Decimal256.uncheckedAdd(decimal256B, decimal128A);
                    mapValue.putDecimal256(valueIndex, decimal256B);
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
            sink.ofRaw((short) decimal256A.getLh(), decimal256A.getLl());
        } else {
            sink.ofRawNull();
        }
    }

    @Override
    public short getDecimal16(Record rec) {
        if (calc(rec)) {
            return (short) decimal256A.getLl();
        }
        return Decimals.DECIMAL16_NULL;
    }

    @Override
    public void getDecimal256(Record rec, Decimal256 sink) {
        if (!calc(rec)) {
            decimal256A.ofRawNull();
        }
        sink.ofRaw(decimal256A.getHh(), decimal256A.getHl(), decimal256A.getLh(), decimal256A.getLl());
    }

    @Override
    public int getDecimal32(Record rec) {
        if (calc(rec)) {
            return (int) decimal256A.getLl();
        }
        return Decimals.DECIMAL32_NULL;
    }

    @Override
    public long getDecimal64(Record rec) {
        if (calc(rec)) {
            return decimal256A.getLl();
        }
        return Decimals.DECIMAL64_NULL;
    }

    @Override
    public byte getDecimal8(Record rec) {
        if (calc(rec)) {
            return (byte) decimal256A.getLl();
        }
        return Decimals.DECIMAL8_NULL;
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
        columnTypes.add(ColumnType.DECIMAL256);
        columnTypes.add(ColumnType.DECIMAL128);
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
                srcValue.getDecimal128(valueIndex + 1, decimal128B);
                destValue.getDecimal128(valueIndex + 1, decimal128A);
                // both not null
                try {
                    Decimal128.uncheckedAdd(decimal128A, decimal128B);
                    destValue.putDecimal128(valueIndex + 1, decimal128A);
                } catch (NumericException e) {
                    decimal256A.ofRaw(0, 0, 0, 0);
                    Decimal256.uncheckedAdd(decimal256A, decimal128A);
                    Decimal256.uncheckedAdd(decimal256A, decimal128B);
                    destValue.putDecimal256(valueIndex, decimal256A);
                    destValue.putBool(valueIndex + 3, true);
                }
                destValue.addLong(valueIndex + 2, srcCount);
            } else if (destNull) {
                // put src value in
                srcValue.getDecimal128(valueIndex + 1, decimal128B);
                destValue.putDecimal128(valueIndex + 1, decimal128B);
                destValue.putLong(valueIndex + 2, srcCount);
            }
        } else if (srcOverflow && !destOverflow) {
            // src overflown, therefore it could not be null (null does not overflow)
            long srcCount = srcValue.getLong(valueIndex + 2);
            long destCount = destValue.getLong(valueIndex + 2);
            final boolean destNull = destCount == 0;

            srcValue.getDecimal256(valueIndex, decimal256B);
            if (!destNull) {
                destValue.getDecimal128(valueIndex + 1, decimal128A);
                Decimal256.uncheckedAdd(decimal256B, decimal128A);
            }
            destValue.putDecimal256(valueIndex, decimal256B);
            destValue.addLong(valueIndex + 2, srcCount);
            destValue.putBool(valueIndex + 3, true);
        } else if (!srcOverflow) {
            // dest overflown, it cannot be null
            long srcCount = srcValue.getLong(valueIndex + 2);
            final boolean srcNull = srcCount == 0;

            if (!srcNull) {
                // both not null
                srcValue.getDecimal128(valueIndex + 1, decimal128A);
                destValue.getDecimal256(valueIndex, decimal256B);
                Decimal256.uncheckedAdd(decimal256B, decimal128A);
                destValue.putDecimal256(valueIndex, decimal256B);
                destValue.addLong(valueIndex + 2, srcCount);
            }
        } else {
            // both overflown, neither could be null
            srcValue.getDecimal256(valueIndex, decimal256A);
            destValue.getDecimal256(valueIndex, decimal256B);
            decimal256B.add(decimal256A);
            destValue.putDecimal256(valueIndex, decimal256B);
            destValue.addLong(valueIndex + 2, srcValue.getLong(valueIndex + 2));
        }
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDecimal128(valueIndex + 1, Decimal128.ZERO);
        mapValue.putLong(valueIndex + 2, 0);
        mapValue.putBool(valueIndex + 3, false);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }

    private boolean calc(Record rec) {
        long count = rec.getLong(valueIndex + 2);
        if (count > 0) {
            if (rec.getBool(valueIndex + 3)) {
                rec.getDecimal256(valueIndex, decimal256A);
            } else {
                rec.getDecimal128(valueIndex + 1, decimal128A);
                decimal256A.ofRaw(decimal128A.getHigh(), decimal128A.getLow());
            }
            try {
                decimal256A.setScale(ColumnType.getDecimalScale(arg.getType()));
                decimal256A.divide(0, 0, 0, count, 0, ColumnType.getDecimalScale(type), RoundingMode.HALF_EVEN);
            } catch (NumericException e) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
            }
            return true;
        }
        return false;
    }
}
