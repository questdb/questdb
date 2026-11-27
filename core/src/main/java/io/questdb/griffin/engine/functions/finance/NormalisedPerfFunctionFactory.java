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
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class NormalisedPerfFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "normalised_perf(DD)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new NormalisedPerfFunction(args.getQuick(0), args.getQuick(1));
    }

    private static class NormalisedPerfFunction extends DoubleFunction implements BinaryFunction {
        private final Function baseValue;
        private final Function column;
        private double firstValue;
        private boolean isFirstRow;

        public NormalisedPerfFunction(Function column, Function baseValue) {
            this.baseValue = baseValue;
            this.column = column;
            this.firstValue = Double.NaN;
            this.isFirstRow = true;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            column.init(symbolTableSource, executionContext);
            baseValue.init(symbolTableSource, executionContext);
            firstValue = Double.NaN;
            isFirstRow = true;
        }

        @Override
        public void toTop() {
            column.toTop();
            baseValue.toTop();
        }

        @Override
        public double getDouble(Record rec) {
            if (isFirstRow) {
                firstValue = column.getDouble(rec);
                isFirstRow = false;
            }

            if (Numbers.isNull(firstValue) || firstValue == 0.0) {
                return Double.NaN;
            }

            final double colVal = column.getDouble(rec);
            if (Numbers.isNull(colVal)) {
                return Double.NaN;
            }

            final double bv = baseValue.getDouble(rec);
            if (Numbers.isNull(bv)) {
                return Double.NaN;
            }

            return bv * colVal / firstValue;
        }

        @Override
        public Function getLeft() {
            return column;
        }

        @Override
        public String getName() {
            return "normalised_perf";
        }

        @Override
        public Function getRight() {
            return baseValue;
        }
    }
}