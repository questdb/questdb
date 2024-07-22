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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
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
            throw SqlException.$(argPositions.getQuick(0), "constant symbol expression is expected");
        }

        long symbolTimestampEpoch;
        try {
            CharSequence value = fn0.getSymbol(null);
            symbolTimestampEpoch = value != null ? IntervalUtils.parseFloorPartialTimestamp(value) : Numbers.LONG_NULL;
        } catch (NumericException e) {
            throw SqlException.$(argPositions.getQuick(0), "invalid timestamp: ").put(fn0.getSymbol(null));
        }

        Function timestampFn = args.getQuick(1);

        if (timestampFn.isConstant()) {
            return symbolTimestampEpoch == timestampFn.getLong(null) ? BooleanConstant.TRUE : BooleanConstant.FALSE;
        }

        return new VariableTimestampFunction(fn0, timestampFn, symbolTimestampEpoch);
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
