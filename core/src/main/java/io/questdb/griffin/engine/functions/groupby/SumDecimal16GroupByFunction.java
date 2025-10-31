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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.decimal.Decimal64Function;
import io.questdb.std.Decimals;
import org.jetbrains.annotations.NotNull;

class SumDecimal16GroupByFunction extends Decimal64Function implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private int valueIndex;

    public SumDecimal16GroupByFunction(@NotNull Function arg) {
        super(ColumnType.getDecimalType(Decimals.getDecimalTagPrecision(ColumnType.DECIMAL64), ColumnType.getDecimalScale(arg.getType())));
        this.arg = arg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        short value = arg.getDecimal16(record);
        if (value == Decimals.DECIMAL16_NULL) {
            mapValue.putLong(valueIndex, Decimals.DECIMAL64_NULL);
        } else {
            mapValue.putLong(valueIndex, value);
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
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public long getDecimal64(Record rec) {
        return rec.getDecimal64(valueIndex);
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
        columnTypes.add(ColumnType.DECIMAL64);
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
        long src = srcValue.getLong(valueIndex);
        if (src != Decimals.DECIMAL64_NULL) {
            long dest = destValue.getLong(valueIndex);
            if (dest == Decimals.DECIMAL64_NULL) {
                destValue.putLong(valueIndex, src);
            } else {
                destValue.putLong(valueIndex, src + dest);
            }
        }
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, Decimals.DECIMAL64_NULL);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }
}
