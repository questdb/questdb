/*******************************************************************************
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

package io.questdb.griffin.engine.functions.finance;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;

public class FormatPriceFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "format_price(DI)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new FormatPriceFunction(
                args.getQuick(0),
                args.getQuick(1)
        );
    }

    private static class FormatPriceFunction extends VarcharFunction implements BinaryFunction {

        private final Function decimalForm;
        private final Function tickSize;

        private final Utf8StringSink sinkA = new Utf8StringSink();
        private final Utf8StringSink sinkB = new Utf8StringSink();

        private boolean constantTick;
        private int constantWidth;

        public FormatPriceFunction(Function decimalForm, Function tickSize) {
            this.decimalForm = decimalForm;
            this.tickSize = tickSize;
        }

        @Override
        public Function getLeft() {
            return decimalForm;
        }

        @Override
        public String getName() {
            return "format_price";
        }

        @Override
        public Function getRight() {
            return tickSize;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);

            constantTick = tickSize.isConstant();

            if (constantTick) {
                constantWidth = calculateWidth(tickSize.getInt(null));
            }
        }

        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            return format(rec, sinkA);
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            return format(rec, sinkB);
        }

        private int calculateWidth(int tick) {
            int width = 0;
            int temp = tick - 1;

            do {
                width++;
                temp /= 10;
            } while (temp > 0);

            return width;
        }

        private int countDigits(int value) {
            int digits = 0;

            do {
                digits++;
                value /= 10;
            } while (value > 0);

            return digits;
        }

        private Utf8Sequence format(Record rec, Utf8StringSink sink) {
            sink.clear();

            final double decimalPrice = decimalForm.getDouble(rec);
            final int tick = tickSize.getInt(rec);

            if (Numbers.isNull(decimalPrice) || tick <= 0) {
                return sink;
            }

            final boolean negative = decimalPrice < 0;
            final double absolutePrice = Math.abs(decimalPrice);

            long wholePart = (long) absolutePrice;

            final double fractionalPart = absolutePrice - wholePart;

            int fractionalTicks =
                    (int) Math.round(fractionalPart * tick);

            // floating point normalization safety
            if (fractionalTicks == tick) {
                wholePart++;
                fractionalTicks = 0;
            }

            final int width = constantTick
                    ? constantWidth
                    : calculateWidth(tick);

            final int digits = countDigits(fractionalTicks);

            if (negative) {
                sink.putAscii('-');
            }

            Numbers.append(sink, wholePart);
            sink.putAscii('-');

            for (int i = digits; i < width; i++) {
                sink.putAscii('0');
            }

            Numbers.append(sink, fractionalTicks);

            return sink;
        }
    }
}