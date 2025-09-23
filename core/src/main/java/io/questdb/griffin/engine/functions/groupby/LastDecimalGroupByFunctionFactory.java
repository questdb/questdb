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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class LastDecimalGroupByFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "last(Ξ)";
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
        final Function func = args.getQuick(0);
        switch (Decimals.getStorageSizePow2(ColumnType.getDecimalPrecision(func.getType()))) {
            case 0:
                return new Decimal8Func(func);
            case 1:
                return new Decimal16Func(func);
            case 2:
                return new Decimal32Func(func);
            case 3:
                return new Decimal64Func(func);
            case 4:
                return new Decimal128Func(func);
            default:
                return new Decimal256Func(func);
        }
    }

    private static class Decimal128Func extends FirstDecimalGroupByFunctionFactory.FirstLastDecimal128Func {

        public Decimal128Func(Function arg) {
            super(arg);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            computeFirst(mapValue, record, rowId);
        }

        @Override
        public String getName() {
            return "last";
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            final long srcRowId = srcValue.getLong(valueIndex);
            final long destRowId = destValue.getLong(valueIndex);
            if (srcRowId > destRowId) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putDecimal128(
                        valueIndex + 1,
                        srcValue.getDecimal128Hi(valueIndex + 1),
                        srcValue.getDecimal128Lo(valueIndex + 1)
                );
            }
        }
    }

    private static class Decimal16Func extends FirstDecimalGroupByFunctionFactory.FirstLastDecimal16Func {

        public Decimal16Func(Function arg) {
            super(arg);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            computeFirst(mapValue, record, rowId);
        }

        @Override
        public String getName() {
            return "last";
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            final long srcRowId = srcValue.getLong(valueIndex);
            final long destRowId = destValue.getLong(valueIndex);
            if (srcRowId > destRowId) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putShort(valueIndex + 1, srcValue.getShort(valueIndex + 1));
            }
        }
    }

    private static class Decimal256Func extends FirstDecimalGroupByFunctionFactory.FirstLastDecimal256Func {

        public Decimal256Func(Function arg) {
            super(arg);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            computeFirst(mapValue, record, rowId);
        }

        @Override
        public String getName() {
            return "last";
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            final long srcRowId = srcValue.getLong(valueIndex);
            final long destRowId = destValue.getLong(valueIndex);
            if (srcRowId > destRowId) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putDecimal256(
                        valueIndex + 1,
                        srcValue.getDecimal256HH(valueIndex + 1),
                        srcValue.getDecimal256HL(valueIndex + 1),
                        srcValue.getDecimal256LH(valueIndex + 1),
                        srcValue.getDecimal256LL(valueIndex + 1)
                );
            }
        }
    }

    private static class Decimal32Func extends FirstDecimalGroupByFunctionFactory.FirstLastDecimal32Func {

        public Decimal32Func(Function arg) {
            super(arg);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            computeFirst(mapValue, record, rowId);
        }

        @Override
        public String getName() {
            return "last";
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            final long srcRowId = srcValue.getLong(valueIndex);
            final long destRowId = destValue.getLong(valueIndex);
            if (srcRowId > destRowId) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putInt(valueIndex + 1, srcValue.getInt(valueIndex + 1));
            }
        }
    }

    private static class Decimal64Func extends FirstDecimalGroupByFunctionFactory.FirstLastDecimal64Func {

        public Decimal64Func(Function arg) {
            super(arg);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            computeFirst(mapValue, record, rowId);
        }

        @Override
        public String getName() {
            return "last";
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            final long srcRowId = srcValue.getLong(valueIndex);
            final long destRowId = destValue.getLong(valueIndex);
            if (srcRowId > destRowId) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putLong(valueIndex + 1, srcValue.getLong(valueIndex + 1));
            }
        }
    }

    private static class Decimal8Func extends FirstDecimalGroupByFunctionFactory.FirstLastDecimal8Func {

        public Decimal8Func(Function arg) {
            super(arg);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            computeFirst(mapValue, record, rowId);
        }

        @Override
        public String getName() {
            return "last";
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            final long srcRowId = srcValue.getLong(valueIndex);
            final long destRowId = destValue.getLong(valueIndex);
            if (srcRowId > destRowId) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putByte(valueIndex + 1, srcValue.getByte(valueIndex + 1));
            }
        }
    }
}
