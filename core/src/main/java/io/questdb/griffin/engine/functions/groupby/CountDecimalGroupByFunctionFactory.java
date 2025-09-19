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
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class CountDecimalGroupByFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "count(Ξ)";
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
        final Function arg = args.getQuick(0);
        final int precision = ColumnType.getDecimalPrecision(arg.getType());
        switch (Decimals.getStorageSizePow2(precision)) {
            case 0:
                return new Decimal8Func(arg);
            case 1:
                return new Decimal16Func(arg);
            case 2:
                return new Decimal32Func(arg);
            case 3:
                return new Decimal64Func(arg);
            case 4:
                return new Decimal128Func(arg);
            default:
                return new Decimal256Func(arg);
        }
    }

    private static class Decimal128Func extends AbstractCountGroupByFunction {

        public Decimal128Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final long high = arg.getDecimal128Hi(record);
            final long low = arg.getDecimal128Lo(record);
            if (!Decimal128.isNull(high, low)) {
                mapValue.putLong(valueIndex, 1);
            } else {
                mapValue.putLong(valueIndex, 0);
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            final long high = arg.getDecimal128Hi(record);
            final long low = arg.getDecimal128Lo(record);
            if (!Decimal128.isNull(high, low)) {
                mapValue.addLong(valueIndex, 1);
            }
        }
    }

    private static class Decimal16Func extends AbstractCountGroupByFunction {

        public Decimal16Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final short value = arg.getDecimal16(record);
            if (value != Decimals.DECIMAL16_NULL) {
                mapValue.putLong(valueIndex, 1);
            } else {
                mapValue.putLong(valueIndex, 0);
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            final short value = arg.getDecimal16(record);
            if (value != Decimals.DECIMAL16_NULL) {
                mapValue.addLong(valueIndex, 1);
            }
        }
    }

    private static class Decimal256Func extends AbstractCountGroupByFunction {

        public Decimal256Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final long hh = arg.getDecimal256HH(record);
            final long hl = arg.getDecimal256HL(record);
            final long lh = arg.getDecimal256LH(record);
            final long ll = arg.getDecimal256LL(record);
            if (!Decimal256.isNull(hh, hl, lh, ll)) {
                mapValue.putLong(valueIndex, 1);
            } else {
                mapValue.putLong(valueIndex, 0);
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            final long hh = arg.getDecimal256HH(record);
            final long hl = arg.getDecimal256HL(record);
            final long lh = arg.getDecimal256LH(record);
            final long ll = arg.getDecimal256LL(record);
            if (!Decimal256.isNull(hh, hl, lh, ll)) {
                mapValue.addLong(valueIndex, 1);
            }
        }
    }

    private static class Decimal32Func extends AbstractCountGroupByFunction {

        public Decimal32Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final int value = arg.getDecimal32(record);
            if (value != Decimals.DECIMAL32_NULL) {
                mapValue.putLong(valueIndex, 1);
            } else {
                mapValue.putLong(valueIndex, 0);
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            final int value = arg.getDecimal32(record);
            if (value != Decimals.DECIMAL32_NULL) {
                mapValue.addLong(valueIndex, 1);
            }
        }
    }

    private static class Decimal64Func extends AbstractCountGroupByFunction {

        public Decimal64Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final long value = arg.getDecimal64(record);
            if (!Decimal64.isNull(value)) {
                mapValue.putLong(valueIndex, 1);
            } else {
                mapValue.putLong(valueIndex, 0);
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            final long value = arg.getDecimal64(record);
            if (!Decimal64.isNull(value)) {
                mapValue.addLong(valueIndex, 1);
            }
        }
    }

    private static class Decimal8Func extends AbstractCountGroupByFunction {

        public Decimal8Func(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            final byte value = arg.getDecimal8(record);
            if (value != Decimals.DECIMAL8_NULL) {
                mapValue.putLong(valueIndex, 1);
            } else {
                mapValue.putLong(valueIndex, 0);
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            final byte value = arg.getDecimal8(record);
            if (value != Decimals.DECIMAL8_NULL) {
                mapValue.addLong(valueIndex, 1);
            }
        }
    }
}
