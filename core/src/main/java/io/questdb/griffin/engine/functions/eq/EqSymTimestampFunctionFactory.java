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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

/**
 * Equality operator between symbol and timestamp. Typically, it does not make sense
 * to compare these types, but rare cases when symbol acts as a string, for example:
 * <p>
 * where timestamp = '2021-09-01'::symbol
 * <p>
 * in fact, this is the only comparison that is supported
 */
public class EqSymTimestampFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(KN)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(
            int position, ObjList<Function> args,
            IntList argPositions, CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {

        Function fn0 = args.getQuick(0);
        if (!fn0.isConstant()) {
            return new VariableSymbolAndTimestampFunction(args.getQuick(0), args.getQuick(1));
        }

        long symbolTimestampEpoch;

        CharSequence value = fn0.getSymbol(null);
        symbolTimestampEpoch = value != null ? SqlUtil.implicitCastSymbolAsTimestamp(value, ColumnType.SYMBOL) : Numbers.LONG_NULL;

        Function timestampFn = args.getQuick(1);

        if (timestampFn.isConstant()) {
            return symbolTimestampEpoch == timestampFn.getLong(null) ? BooleanConstant.TRUE : BooleanConstant.FALSE;
        }

        return new VariableTimestampFunction(fn0, timestampFn, symbolTimestampEpoch);
    }

    private static class VariableSymbolAndTimestampFunction extends AbstractEqBinaryFunction {

        public VariableSymbolAndTimestampFunction(Function symFn, Function timestampFn) {
            super(symFn, timestampFn);
        }

        @Override
        public boolean getBool(Record rec) {
            long symbol = left.getTimestamp(rec);
            long timestamp = right.getTimestamp(rec);
            return negated == (symbol != timestamp);
        }
    }

    private static class VariableTimestampFunction extends AbstractEqBinaryFunction {
        private final long symbolTimestampEpoch;

        public VariableTimestampFunction(Function symFn, Function timestampFn, long symbolTimestampEpoch) {
            super(symFn, timestampFn);
            this.symbolTimestampEpoch = symbolTimestampEpoch;
        }

        @Override
        public boolean getBool(Record rec) {
            long value = right.getTimestamp(rec);
            return negated == (value != symbolTimestampEpoch);
        }
    }
}
