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
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class MaxDecimalGroupByFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "max(Ξ)";
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
        return switch (ColumnType.tagOf(func.getType())) {
            case ColumnType.DECIMAL8 -> new Decimal8Func(func);
            case ColumnType.DECIMAL16 -> new Decimal16Func(func);
            case ColumnType.DECIMAL32 -> new Decimal32Func(func);
            case ColumnType.DECIMAL64 -> new Decimal64Func(func);
            case ColumnType.DECIMAL128 -> new Decimal128Func(func);
            default -> new Decimal256Func(func);
        };
    }

    private static class Decimal128Func extends MinDecimalGroupByFunctionFactory.MinMaxDecimal128Func {

        public Decimal128Func(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return "max";
        }

        @Override
        protected boolean shouldStoreA(Decimal128 decimal128A, Decimal128 decimal128B) {
            return Decimal128.compare(decimal128A, decimal128B) > 0;
        }
    }

    private static class Decimal16Func extends MinDecimalGroupByFunctionFactory.MinMaxDecimal16Func {

        public Decimal16Func(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return "max";
        }

        @Override
        protected boolean shouldStoreA(short aValue, short bValue) {
            return aValue > bValue;
        }
    }

    private static class Decimal256Func extends MinDecimalGroupByFunctionFactory.MinMaxDecimal256Func {

        public Decimal256Func(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return "max";
        }

        @Override
        protected boolean shouldStoreA(Decimal256 decimal256A, Decimal256 decimal256B) {
            return Decimal256.compare(decimal256A, decimal256B) > 0;
        }
    }

    private static class Decimal32Func extends MinDecimalGroupByFunctionFactory.MinMaxDecimal32Func {

        public Decimal32Func(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return "max";
        }

        @Override
        protected boolean shouldStoreA(int aValue, int bValue) {
            return aValue > bValue;
        }
    }

    private static class Decimal64Func extends MinDecimalGroupByFunctionFactory.MinMaxDecimal64Func {

        public Decimal64Func(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return "max";
        }

        @Override
        protected boolean shouldStoreA(long aValue, long bValue) {
            return aValue > bValue;
        }
    }

    private static class Decimal8Func extends MinDecimalGroupByFunctionFactory.MinMaxDecimal8Func {

        public Decimal8Func(Function arg) {
            super(arg);
        }

        @Override
        public String getName() {
            return "max";
        }

        @Override
        protected boolean shouldStoreA(byte aValue, byte bValue) {
            return aValue > bValue;
        }
    }
}
