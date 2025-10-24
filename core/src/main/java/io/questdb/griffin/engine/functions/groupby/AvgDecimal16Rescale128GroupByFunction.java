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
import io.questdb.std.Decimals;
import io.questdb.std.NumericException;
import org.jetbrains.annotations.NotNull;

import java.math.RoundingMode;

class AvgDecimal16Rescale128GroupByFunction extends Decimal128Function implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private final Decimal128 decimal128A = new Decimal128();
    private final int position;
    private int valueIndex;

    public AvgDecimal16Rescale128GroupByFunction(@NotNull Function arg, int position, int targetType) {
        super(targetType);
        this.arg = arg;
        this.position = position;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        short value = arg.getDecimal16(record);
        if (value == Decimals.DECIMAL16_NULL) {
            mapValue.putLong(valueIndex, 0);
            mapValue.putLong(valueIndex + 1, 0);
        } else {
            mapValue.putLong(valueIndex, value);
            mapValue.putLong(valueIndex + 1, 1);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        short value = arg.getDecimal16(record);
        if (value != Decimals.DECIMAL16_NULL) {
            long curr = mapValue.getLong(valueIndex);
            if (curr == Decimals.DECIMAL64_NULL) {
                curr = 0;
            }
            mapValue.putLong(valueIndex, curr + value);
            mapValue.addLong(valueIndex + 1, 1);
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }


    @Override
    public long getDecimal128Hi(Record rec) {
        long count = rec.getLong(valueIndex + 1);
        if (count > 0) {
            try {
                long value = rec.getLong(valueIndex);
                decimal128A.of(value < 0 ? -1L : 0L, value, ColumnType.getDecimalScale(arg.getType()));
                decimal128A.divide(0, count, 0, ColumnType.getDecimalScale(type), RoundingMode.HALF_EVEN);
            } catch (NumericException e) {
                throw CairoException.nonCritical().position(position).put("avg aggregation failed: ").put(e.getFlyweightMessage());
            }
        } else {
            decimal128A.ofRawNull();
        }
        return decimal128A.getHigh();
    }

    @Override
    public long getDecimal128Lo(Record rec) {
        return decimal128A.getLow();
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
            long dest = destValue.getLong(valueIndex);
            if (dest == Decimals.DECIMAL64_NULL) {
                destValue.putLong(valueIndex, src);
                destValue.putLong(valueIndex + 1, srcCount);
            } else {
                destValue.putLong(valueIndex, src + dest);
                destValue.addLong(valueIndex + 1, srcCount);
            }
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
}
