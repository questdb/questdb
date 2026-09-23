/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
    private final boolean isArgNotNull;
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
        this.isArgNotNull = arg != null && arg.isNotNull();
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
        final boolean hasValue = isArgNotNull || !decimal128B.isNull();
        if (hasValue) {
            mapValue.putDecimal128(valueIndex + 1, decimal128B);
        } else {
            mapValue.putDecimal128Null(valueIndex + 1);
        }
        mapValue.putBool(valueIndex + 2, false);
        mapValue.putBool(valueIndex + 3, hasValue);
    }

    @Override
    public void computeNext(MapValue mapValue, io.questdb.cairo.sql.Record record, long rowId) {
        arg.getDecimal128(record, decimal128A);
        if (isArgNotNull || !decimal128A.isNull()) {
            try {
                if (!mapValue.getBool(valueIndex + 2)) {
                    if (!mapValue.getBool(valueIndex + 3)) {
                        mapValue.putDecimal128(valueIndex + 1, decimal128A);
                        mapValue.putBool(valueIndex + 3, true);
                    } else {
                        mapValue.getDecimal128(valueIndex + 1, decimal128B);
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
    public void getDecimal256(Record rec, Decimal256 sink) {
        this.overflow = rec.getBool(valueIndex + 2);
        if (overflow) {
            rec.getDecimal256(valueIndex, sink);
        } else if (!rec.getBool(valueIndex + 3)) {
            // no input value has been aggregated: SUM over zero rows is NULL
            // regardless of the input column's nullability. The accumulator's
            // bit pattern cannot decide this, since a NOT NULL column's
            // reclassified sentinel is a legal value that may equal it.
            sink.ofRawNull();
        } else {
            rec.getDecimal128(valueIndex + 1, decimal128A);
            long hh = decimal128A.getHigh() < 0 ? -1 : 0;
            long hl = decimal128A.getHigh() < 0 ? -1 : 0;
            long lh = decimal128A.getHigh();
            long ll = decimal128A.getLow();
            sink.ofRaw(hh, hl, lh, ll);
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
        columnTypes.add(ColumnType.DECIMAL256);
        columnTypes.add(ColumnType.DECIMAL128);
        // overflow: the accumulator has been promoted to decimal256
        columnTypes.add(ColumnType.BOOLEAN);
        // has-value: at least one input value has been aggregated; tracks
        // the empty-aggregate state independently of accumulator bit patterns
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

        // an aggregate that has absorbed no value cannot have overflown, so
        // the has-value flag fully decides emptiness in every branch below
        boolean srcHasValue = srcValue.getBool(valueIndex + 3);
        boolean destHasValue = destValue.getBool(valueIndex + 3);
        destValue.putBool(valueIndex + 3, srcHasValue || destHasValue);

        if (!srcOverflow && !destOverflow) {
            srcValue.getDecimal128(valueIndex + 1, decimal128B);
            destValue.getDecimal128(valueIndex + 1, decimal128A);
            if (destHasValue && srcHasValue) {
                try {
                    decimal128A.uncheckedAdd(decimal128B);
                    destValue.putDecimal128(valueIndex + 1, decimal128A);
                } catch (NumericException e) {
                    decimal256A.ofRaw(0, 0, 0, 0);
                    Decimal256.uncheckedAdd(decimal256A, decimal128A);
                    Decimal256.uncheckedAdd(decimal256A, decimal128B);
                    destValue.putDecimal256(valueIndex, decimal256A);
                    destValue.putBool(valueIndex + 2, true);
                }
            } else if (srcHasValue) {
                // put src value in
                destValue.putDecimal128(valueIndex + 1, decimal128B);
            }
        } else if (srcOverflow && !destOverflow) {
            // src has overflown, therefore it has a value
            srcValue.getDecimal256(valueIndex, decimal256B);
            destValue.getDecimal128(valueIndex + 1, decimal128A);

            if (destHasValue) {
                Decimal256.uncheckedAdd(decimal256B, decimal128A);
            }
            destValue.putDecimal256(valueIndex, decimal256B);
            destValue.putBool(valueIndex + 2, true);
        } else if (!srcOverflow) {
            // dest has overflown, therefore it has a value
            srcValue.getDecimal128(valueIndex + 1, decimal128A);
            destValue.getDecimal256(valueIndex, decimal256B);

            if (srcHasValue) {
                Decimal256.uncheckedAdd(decimal256B, decimal128A);
                destValue.putDecimal256(valueIndex, decimal256B);
            }
        } else {
            // both overflown, neither could be null
            srcValue.getDecimal256(valueIndex, decimal256A);
            destValue.getDecimal256(valueIndex, decimal256B);
            decimal256B.add(decimal256A);
            destValue.putDecimal256(valueIndex, decimal256B);
        }
    }

    @Override
    public void setDecimal256(MapValue mapValue, Decimal256 value) {
        mapValue.putDecimal256(valueIndex, value);
        mapValue.putBool(valueIndex + 2, true);
        mapValue.putBool(valueIndex + 3, true);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDecimal256Null(valueIndex);
        mapValue.putDecimal128Null(valueIndex + 1);
        mapValue.putBool(valueIndex + 2, false);
        mapValue.putBool(valueIndex + 3, false);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }
}
