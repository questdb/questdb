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
    private final Decimal128 decimal128A = new Decimal128();
    private final Decimal128 decimal128B = new Decimal128();
    private final Decimal256 decimal256A = new Decimal256();
    private final Decimal256 decimal256B = new Decimal256();
    private final int position;
    private boolean overflow;
    private int valueIndex;

    public SumDecimal128GroupByFunction(@NotNull Function arg, int position) {
        super(ColumnType.getDecimalType(Decimals.MAX_PRECISION, ColumnType.getDecimalScale(arg.getType())));
        this.arg = arg;
        this.position = position;
        final int scale = ColumnType.getDecimalScale(type);
        this.decimal256A.setScale(scale);
        this.decimal256B.setScale(scale);
        this.decimal128A.setScale(scale);
        this.decimal128B.setScale(scale);
    }

    @Override
    public void computeFirst(MapValue mapValue, io.questdb.cairo.sql.Record record, long rowId) {
        arg.getDecimal128(record, decimal128B);
        if (!decimal128B.isNull()) {
            mapValue.putDecimal128(valueIndex + 1, decimal128B);
        } else {
            mapValue.putDecimal128Null(valueIndex + 1);
        }
        mapValue.putBool(valueIndex + 2, false);
    }

    @Override
    public void computeNext(MapValue mapValue, io.questdb.cairo.sql.Record record, long rowId) {
        arg.getDecimal128(record, decimal128A);
        if (!decimal128A.isNull()) {
            try {
                if (!mapValue.getBool(valueIndex + 2)) {
                    mapValue.getDecimal128(valueIndex + 1, decimal128B);
                    if (decimal128B.isNull()) {
                        mapValue.putDecimal128(valueIndex + 1, decimal128A);
                    } else {
                        try {
                            decimal128B.uncheckedAdd(decimal128A);
                            mapValue.putDecimal128(valueIndex + 1, decimal128B);
                        } catch (NumericException e) {
                            decimal256B.ofRaw(0, 0, 0, 0);
                            Decimal256.uncheckedAdd(decimal256B, decimal128B);
                            Decimal256.uncheckedAdd(decimal256B, decimal128A);
                            mapValue.putDecimal256(valueIndex, decimal256B);
                            mapValue.putBool(valueIndex + 2, true);
                        }
                    }
                } else {
                    mapValue.getDecimal256(valueIndex, decimal256B);
                    Decimal256.uncheckedAdd(decimal256B, decimal128A);
                    mapValue.putDecimal256(valueIndex, decimal256B);
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
    public long getDecimal256HH(Record rec) {
        this.overflow = rec.getBool(valueIndex + 2);
        if (overflow) {
            return rec.getDecimal256HH(valueIndex);
        }
        rec.getDecimal128(valueIndex + 1, decimal128A);
        return decimal128A.isNull() ? Decimals.DECIMAL256_HH_NULL : 0;
    }

    @Override
    public long getDecimal256HL(Record rec) {
        if (overflow) {
            return rec.getDecimal256HL(valueIndex);
        }
        return decimal128A.isNull() ? Decimals.DECIMAL256_HL_NULL : 0;
    }

    @Override
    public long getDecimal256LH(io.questdb.cairo.sql.Record rec) {
        if (overflow) {
            return rec.getDecimal256LH(valueIndex);
        }
        return decimal128A.isNull() ? Decimals.DECIMAL256_LH_NULL : decimal128A.getHigh();
    }

    @Override
    public long getDecimal256LL(Record rec) {
        if (overflow) {
            return rec.getDecimal256LL(valueIndex);
        }
        return decimal128A.isNull() ? Decimals.DECIMAL256_LL_NULL : decimal128A.getLow();
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
        columnTypes.add(ColumnType.DECIMAL128);
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
            srcValue.getDecimal128(valueIndex + 1, decimal128B);
            destValue.getDecimal128(valueIndex + 1, decimal128A);

            boolean srcNull = decimal128B.isNull();
            boolean destNull = decimal128A.isNull();

            if (!destNull && !srcNull) {
                // both not null
                try {
                    decimal128A.add(decimal128B);
                    destValue.putDecimal128(valueIndex + 1, decimal128A);
                } catch (NumericException e) {
                    // overflow
                    decimal256A.ofRaw(0, 0, 0, 0);
                    Decimal256.uncheckedAdd(decimal256A, decimal128A);
                    Decimal256.uncheckedAdd(decimal256A, decimal128B);
                    destValue.putDecimal256(valueIndex, decimal256A);
                    destValue.putBool(valueIndex + 2, true);
                }
            } else if (destNull) {
                // put src value in
                destValue.putDecimal128(valueIndex + 1, decimal128B);
            }
        } else if (srcOverflow && !destOverflow) {
            srcValue.getDecimal256(valueIndex, decimal256B);
            destValue.getDecimal128(valueIndex + 1, decimal128A);
            boolean srcNull = decimal128B.isNull();
            boolean destNull = decimal128A.isNull();

            if (!destNull && !srcNull) {
                decimal256B.add(0, 0, decimal128A.getHigh(), decimal128A.getLow(), decimal128A.getScale());
            }
            destValue.putDecimal256(valueIndex, decimal256B);
            destValue.putBool(valueIndex + 2, false);
        } else if (!srcOverflow) {
            // dest overflown
            srcValue.getDecimal128(valueIndex + 1, decimal128A);
            destValue.getDecimal256(valueIndex, decimal256B);
            boolean srcNull = decimal128A.isNull();
            boolean destNull = decimal256B.isNull();

            if (!destNull && !srcNull) {
                // both not null
                decimal256B.add(0, 0, decimal128A.getHigh(), decimal128A.getLow(), decimal128A.getScale());
                destValue.putDecimal256(valueIndex, decimal256B);
            } else if (destNull) {
                // put src value in, dest was null, so we get it back into 128bit range
                destValue.putDecimal128(valueIndex + 1, decimal128B);
            }
        } else {
            // both overflown
            srcValue.getDecimal256(valueIndex, decimal256A);
            if (decimal128A.isNull()) {
                return;
            }
            destValue.getDecimal256(valueIndex, decimal256B);
            if (decimal256B.isNull()) {
                destValue.putDecimal256(valueIndex, decimal256A);
            } else {
                decimal256B.add(decimal256A);
                destValue.putDecimal256(valueIndex, decimal256B);
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
}
