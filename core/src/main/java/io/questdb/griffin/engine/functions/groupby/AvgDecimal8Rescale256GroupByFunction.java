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

class AvgDecimal8Rescale256GroupByFunction extends Decimal256Function implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private final Decimal256 decimal256A = new Decimal256();
    private final int position;
    private int valueIndex;

    public AvgDecimal8Rescale256GroupByFunction(@NotNull Function arg, int position, int targetType) {
        super(targetType);
        this.arg = arg;
        this.position = position;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        byte value = arg.getDecimal8(record);
        if (value == Decimals.DECIMAL8_NULL) {
            setNull(mapValue);
        } else {
            mapValue.putLong(valueIndex, value);
            mapValue.putLong(valueIndex + 1, 1);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        byte value = arg.getDecimal8(record);
        if (value != Decimals.DECIMAL8_NULL) {
            mapValue.addLong(valueIndex, value);
            mapValue.addLong(valueIndex + 1, 1);
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
        columnTypes.add(ColumnType.DECIMAL64);
        columnTypes.add(ColumnType.LONG);
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
        long srcCount = srcValue.getLong(valueIndex + 1);
        if (srcCount > 0) {
            long src = srcValue.getLong(valueIndex);
            destValue.addLong(valueIndex, src);
            destValue.addLong(valueIndex + 1, srcCount);
        }
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0);
        mapValue.putLong(valueIndex + 1, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }

    private boolean calc(Record rec) {
        long count = rec.getLong(valueIndex + 1);
        if (count > 0) {
            try {
                long value = rec.getLong(valueIndex);
                decimal256A.ofRaw(value);
                decimal256A.setScale(ColumnType.getDecimalScale(arg.getType()));
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
