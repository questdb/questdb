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
import io.questdb.std.Decimal256;
import io.questdb.std.NumericException;
import org.jetbrains.annotations.NotNull;

import java.math.RoundingMode;

class AvgDecimal128GroupByFunction extends Decimal128Function implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private final Decimal128 decimal128A = new Decimal128();
    private final Decimal128 decimal128B = new Decimal128();
    private final Decimal256 decimal256A = new Decimal256();
    private final Decimal256 decimal256B = new Decimal256();
    private final int position;
    private int valueIndex;

    public AvgDecimal128GroupByFunction(@NotNull Function arg, int position) {
        super(arg.getType());
        this.arg = arg;
        this.position = position;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        arg.getDecimal128(record, decimal128A);
        if (decimal128A.isNull()) {
            setNull(mapValue);
        } else {
            mapValue.putDecimal128(valueIndex + 1, decimal128A);
            mapValue.putLong(valueIndex + 2, 1);
            mapValue.putBool(valueIndex + 3, false);
        }
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
        calc(rec);
        sink.ofRaw(decimal128A.getHigh(), decimal128A.getLow());
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
            final boolean srcNull = srcCount == 0;
            if (!srcNull) {
                long destCount = destValue.getLong(valueIndex + 2);
                final boolean destNull = destCount == 0;
                srcValue.getDecimal128(valueIndex + 1, decimal128B);
                if (!destNull) {
                    // both not null
                    destValue.getDecimal128(valueIndex + 1, decimal128A);
                    try {
                        decimal128A.uncheckedAdd(decimal128B);
                        destValue.putDecimal128(valueIndex + 1, decimal128A);
                    } catch (NumericException e) {
                        decimal256A.ofRaw(0, 0, 0, 0);
                        Decimal256.uncheckedAdd(decimal256A, decimal128A);
                        Decimal256.uncheckedAdd(decimal256A, decimal128B);
                        destValue.putDecimal256(valueIndex, decimal256A);
                        destValue.putBool(valueIndex + 3, true);
                    }
                    destValue.addLong(valueIndex + 2, srcCount);
                } else {
                    // put src value in
                    destValue.putDecimal128(valueIndex + 1, decimal128B);
                    destValue.putLong(valueIndex + 2, srcCount);
                }
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

    private void calc(Record rec) {
        long count = rec.getLong(valueIndex + 2);
        if (count > 0) {
            if (rec.getBool(valueIndex + 3)) {
                rec.getDecimal256(valueIndex, decimal256A);
            } else {
                rec.getDecimal128(valueIndex + 1, decimal128A);
                decimal256A.ofRaw(decimal128A.getHigh(), decimal128A.getLow());
            }
            final int scale = ColumnType.getDecimalScale(type);
            decimal256A.setScale(scale);
            try {
                decimal256A.divide(0, 0, 0, count, 0, scale, RoundingMode.HALF_EVEN);
            } catch (NumericException e) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
            }
            decimal128A.ofRaw(decimal256A.getLh(), decimal256A.getLl());
        } else {
            decimal128A.ofRawNull();
        }
    }
}
