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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class SumDecimalGroupByFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "sum(Ξ)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new Func(args.getQuick(0), position);
    }

    private static class Func extends DecimalFunction implements GroupByFunction, UnaryFunction {
        private final Function arg;
        private final Decimal256 decimal = new Decimal256();
        private final int position;
        private final int scale;
        private int valueIndex;

        public Func(@NotNull Function arg, int position) {
            super(ColumnType.getDecimalType(Decimals.MAX_PRECISION, ColumnType.getDecimalScale(arg.getType())));
            this.arg = arg;
            this.position = position;
            this.scale = ColumnType.getDecimalScale(type);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            DecimalUtil.load(decimal, arg, record);
            if (!decimal.isNull()) {
                mapValue.putDecimal256(valueIndex, decimal);
            } else {
                mapValue.putDecimal256Null(valueIndex);
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            DecimalUtil.load(decimal, arg, record);
            if (!decimal.isNull()) {
                final long hh = mapValue.getDecimal256HH(valueIndex);
                final long hl = mapValue.getDecimal256HL(valueIndex);
                final long lh = mapValue.getDecimal256LH(valueIndex);
                final long ll = mapValue.getDecimal256LL(valueIndex);
                if (!Decimal256.isNull(hh, hl, lh, ll)) {
                    try {
                        decimal.add(hh, hl, lh, ll, scale);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                    }
                }
                mapValue.putDecimal256(valueIndex, decimal);
            }
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getDecimal256HH(Record rec) {
            return rec.getDecimal256HH(valueIndex);
        }

        @Override
        public long getDecimal256HL(Record rec) {
            return rec.getDecimal256HL(valueIndex);
        }

        @Override
        public long getDecimal256LH(Record rec) {
            return rec.getDecimal256LH(valueIndex);
        }

        @Override
        public long getDecimal256LL(Record rec) {
            return rec.getDecimal256LL(valueIndex);
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
            final long srcHH = srcValue.getDecimal256HH(valueIndex);
            final long srcHL = srcValue.getDecimal256HL(valueIndex);
            final long srcLH = srcValue.getDecimal256LH(valueIndex);
            final long srcLL = srcValue.getDecimal256LL(valueIndex);
            if (!Decimal256.isNull(srcHH, srcHL, srcLH, srcLL)) {
                final long destHH = destValue.getDecimal256HH(valueIndex);
                final long destHL = destValue.getDecimal256HL(valueIndex);
                final long destLH = destValue.getDecimal256LH(valueIndex);
                final long destLL = destValue.getDecimal256LL(valueIndex);
                if (!Decimal256.isNull(srcHH, srcHL, srcLH, srcLL)) {
                    decimal.of(destHH, destHL, destLH, destLL, scale);
                    try {
                        decimal.add(srcHH, srcHL, srcLH, srcLL, scale);
                    } catch (NumericException e) {
                        throw CairoException.nonCritical().position(position)
                                .put('\'').put(getName()).put("sum aggregation failed: ").put(e.getFlyweightMessage());
                    }
                    destValue.putDecimal256(valueIndex, decimal);
                } else {
                    destValue.putDecimal256(valueIndex, srcHH, srcHL, srcLH, srcLL);
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
}
