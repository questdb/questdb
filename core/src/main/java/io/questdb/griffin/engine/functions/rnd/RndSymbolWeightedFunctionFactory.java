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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.std.Chars;
import io.questdb.std.DoubleList;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Transient;

import static io.questdb.std.Vect.BIN_SEARCH_SCAN_UP;

public class RndSymbolWeightedFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "rnd_symbol_weighted(V)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (args == null || args.size() < 2) {
            throw SqlException.$(position, "expected at least one symbol-weight pair");
        }

        if (args.size() % 2 != 0) {
            throw SqlException.$(position, "expected even number of arguments (symbol-weight pairs)");
        }

        final int pairCount = args.size() / 2;
        final ObjList<String> symbols = new ObjList<>(pairCount);
        final DoubleList weights = new DoubleList(pairCount);

        // Extract symbol-weight pairs
        for (int i = 0; i < pairCount; i++) {
            int symbolIdx = i * 2;
            int weightIdx = i * 2 + 1;

            // Extract symbol
            Function symbolFunc = args.getQuick(symbolIdx);
            if (!symbolFunc.isConstant()) {
                throw SqlException.$(argPositions.getQuick(symbolIdx), "constant expected");
            }
            CharSequence symbolValue = symbolFunc.getStrA(null);
            if (symbolValue == null) {
                throw SqlException.$(argPositions.getQuick(symbolIdx), "STRING constant expected");
            }
            symbols.add(Chars.toString(symbolValue));

            // Extract weight
            Function weightFunc = args.getQuick(weightIdx);
            if (!weightFunc.isConstant()) {
                throw SqlException.$(argPositions.getQuick(weightIdx), "constant weight expected");
            }
            double weight = weightFunc.getDouble(null);

            if (weight < 0 || Double.isNaN(weight)) {
                throw SqlException.$(argPositions.getQuick(weightIdx), "weight must be non-negative");
            }
            weights.add(weight);
        }

        return new Func(symbols, weights);
    }

    private static final class Func extends SymbolFunction implements Function {
        private final int count;
        private final DoubleList cumulativeProbabilities;
        private final ObjList<String> symbols;
        private final DoubleList weights;
        private Rnd rnd;

        public Func(ObjList<String> symbols, DoubleList weights) {
            this.symbols = symbols;
            this.weights = weights;
            this.count = symbols.size();
            this.cumulativeProbabilities = new DoubleList(count);
            this.cumulativeProbabilities.setPos(count);

            // Calculate total weight
            double totalWeight = 0.0;
            for (int i = 0; i < count; i++) {
                totalWeight += weights.getQuick(i);
            }

            if (totalWeight == 0) {
                throw new IllegalArgumentException("total weight must be positive");
            }

            // Build cumulative probability distribution
            double cumulative = 0.0;
            for (int i = 0; i < count; i++) {
                double probability = weights.getQuick(i) / totalWeight;
                cumulative += probability;
                cumulativeProbabilities.setQuick(i, cumulative);
            }
        }

        @Override
        public int getInt(Record rec) {
            return next();
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            return symbols.getQuick(next());
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            return getSymbol(rec);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();
        }

        @Override
        public boolean isNonDeterministic() {
            return true;
        }

        @Override
        public boolean isRandom() {
            return true;
        }

        @Override
        public boolean isSymbolTableStatic() {
            return false;
        }

        @Override
        public SymbolTable newSymbolTable() {
            Func func = new Func(symbols, weights);
            func.rnd = new Rnd(this.rnd.getSeed0(), this.rnd.getSeed1());
            return func;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("rnd_symbol_weighted(");
            for (int i = 0; i < count; i++) {
                if (i > 0) {
                    sink.val(',');
                }
                sink.val(symbols.getQuick(i)).val(',').val(weights.getQuick(i));
            }
            sink.val(')');
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int symbolKey) {
            return symbolKey != -1 ? symbols.getQuick(symbolKey) : null;
        }

        private int next() {
            // Generate random value between 0 and 1
            double u = rnd.nextDouble();
            int idx = cumulativeProbabilities.binarySearch(u, BIN_SEARCH_SCAN_UP);
            if (idx >= 0) {
                return idx;
            }
            idx = -idx - 1;
            return idx < count ? idx : count - 1;
        }
    }
}
