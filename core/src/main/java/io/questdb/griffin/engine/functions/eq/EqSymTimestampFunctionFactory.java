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
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.BitSet;
import io.questdb.std.IntList;
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

    public static final int BITSET_OPTIMISATION_THRESHOLD = 1048576;

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
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {

        Function symbolFunc = args.getQuick(0);
        Function timestampFunc = args.getQuick(1);
        TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(timestampFunc.getType()));

        if (symbolFunc.isConstant()) {
            CharSequence value = symbolFunc.getSymbol(null);
            long symbolConstant = driver.implicitCast(value, ColumnType.SYMBOL);

            if (timestampFunc.isConstant()) {
                return symbolConstant == timestampFunc.getLong(null) ? BooleanConstant.TRUE : BooleanConstant.FALSE;
            }

            return new ConstSymbolVarTimestampFunction(symbolFunc, timestampFunc, symbolConstant);
        }


        if (timestampFunc.isRuntimeConstant() && !symbolFunc.isNonDeterministic()) {
            return new VarSymbolRuntimeConstTimestampFunction(symbolFunc, timestampFunc, driver);
        }

        if (timestampFunc.isConstant() && !symbolFunc.isNonDeterministic()) {
            return new VarSymbolConstTimestampFunction(symbolFunc, timestampFunc, timestampFunc.getTimestamp(null), driver);
        }

        return new VarSymbolVarTimestampFunction(symbolFunc, timestampFunc, driver);
    }

    private static class ConstSymbolVarTimestampFunction extends AbstractEqBinaryFunction {
        private final long symbolConstant;

        public ConstSymbolVarTimestampFunction(Function symbolFunc, Function timestampFunc, long symbolConstant) {
            super(symbolFunc, timestampFunc);
            this.symbolConstant = symbolConstant;
        }

        @Override
        public boolean getBool(Record rec) {
            long timestamp = right.getTimestamp(rec);
            return negated == (timestamp != symbolConstant);
        }
    }

    private static class VarSymbolConstTimestampFunction extends AbstractEqBinaryFunction {
        private final TimestampDriver driver;
        private final BitSet hits;
        private final BitSet misses;
        private final long timestampConstant;


        public VarSymbolConstTimestampFunction(Function symbolFunc, Function timestampFunc, long timestampConstant, TimestampDriver driver) {
            super(symbolFunc, timestampFunc);
            this.timestampConstant = timestampConstant;
            this.hits = new BitSet();
            this.misses = new BitSet();
            this.driver = driver;
        }

        @Override
        public void clear() {
            super.clear();
            hits.clear();
            misses.clear();
        }

        @Override
        public boolean getBool(Record rec) {
            int id = left.getInt(rec);
            if (id >= 0 && id < BITSET_OPTIMISATION_THRESHOLD) {
                if (hits.get(id)) {
                    return true;
                }

                if (misses.get(id)) {
                    return false;
                }
            }

            final CharSequence value = left.getSymbol(rec);
            long symbol = driver.implicitCast(value, ColumnType.SYMBOL);
            boolean result = negated == (symbol != timestampConstant);
            if (id >= 0 && id < BITSET_OPTIMISATION_THRESHOLD) {
                if (result) {
                    hits.set(id);
                } else {
                    misses.set(id);
                }
            }

            return result;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class VarSymbolRuntimeConstTimestampFunction extends AbstractEqBinaryFunction {
        private final TimestampDriver driver;
        private VarSymbolConstTimestampFunction innerFunc;

        public VarSymbolRuntimeConstTimestampFunction(Function symbolFunc, Function timestampFunc, TimestampDriver driver) {
            super(symbolFunc, timestampFunc);
            this.driver = driver;
        }

        @Override
        public boolean getBool(Record rec) {
            return this.innerFunc.getBool(rec);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);

            long timestampConstant = right.getTimestamp(null);
            this.innerFunc = new VarSymbolConstTimestampFunction(left, right, timestampConstant, driver);
        }
    }

    private static class VarSymbolVarTimestampFunction extends AbstractEqBinaryFunction {
        private final TimestampDriver driver;

        public VarSymbolVarTimestampFunction(Function symbolFunc, Function timestampFunc, TimestampDriver driver) {
            super(symbolFunc, timestampFunc);
            this.driver = driver;
        }

        @Override
        public boolean getBool(Record rec) {
            final CharSequence value = left.getSymbol(rec);
            long symbol = driver.implicitCast(value, ColumnType.SYMBOL);
            long timestamp = right.getTimestamp(rec);
            return negated == (symbol != timestamp);
        }
    }
}
