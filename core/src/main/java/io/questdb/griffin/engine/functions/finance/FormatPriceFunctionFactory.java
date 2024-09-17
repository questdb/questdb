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

package io.questdb.griffin.engine.functions.finance;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class FormatPriceFunctionFactory implements FunctionFactory  {
    @Override
    public String getSignature() {
        return "format_price(DI)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        return new FormatPrice(args.getQuick(0), args.getQuick(1));
    }

    private static class FormatPrice extends StrFunction implements BinaryFunction {
        private final Function decimal_form;
        private final Function ticket_size;

        public FormatPrice(Function decimal_form, Function ticket_size) {
            this.decimal_form = decimal_form;
            this.ticket_size = ticket_size;
        }

        @Override
        public Function getLeft() {
            return ticket_size;
        }

        @Override
        public String getName() {
            return "format_price";
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return getStr(rec);
        }

        public CharSequence getStr(Record rec) {
            final double decimalFormDouble = decimal_form.getDouble(rec);
            final int ticketSizeInt = ticket_size.getInt(rec);
            return formatPrice(decimalFormDouble, ticketSizeInt);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return getStr(rec);
        }

        @Override
        public Function getRight() {
            return decimal_form;
        }

        public static String formatPrice(double decimalPrice, int tickSize) {
            if (Numbers.isNull(decimalPrice) || tickSize <= 0) {
                return "NULL";
            }
            int wholePart = (int) decimalPrice;

            double fractionalPart = decimalPrice - wholePart;
            int fractionalTicks = (int) Math.round(fractionalPart * tickSize);

            return String.format("%d-%02d", wholePart, fractionalTicks);
        }
    }
}
