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
import io.questdb.std.DoubleList;
import io.questdb.std.IntList;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Transient;
import io.questdb.std.str.StringSink;

import static io.questdb.std.Vect.BIN_SEARCH_SCAN_UP;

public class RndSymbolZipfNFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "rnd_symbol_zipf(ID)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (args == null || args.size() != 2) {
            throw SqlException.$(position, "expected 2 arguments: symbol count (int) and alpha (double)");
        }

        // First argument: number of symbols
        Function countFunc = args.getQuick(0);
        if (!countFunc.isConstant()) {
            throw SqlException.$(argPositions.getQuick(0), "constant symbol count expected");
        }
        int symbolCount;
        try {
            symbolCount = countFunc.getInt(null);
        } catch (NumericException e) {
            throw SqlException.$(argPositions.getQuick(0), "integer constant expected");
        }

        if (symbolCount <= 0) {
            throw SqlException.$(argPositions.getQuick(0), "symbol count must be positive");
        }

        // Second argument: alpha parameter
        Function alphaFunc = args.getQuick(1);
        if (!alphaFunc.isConstant()) {
            throw SqlException.$(argPositions.getQuick(1), "constant alpha expected");
        }
        double alpha = alphaFunc.getDouble(null);

        if (alpha <= 0 || Double.isNaN(alpha)) {
            throw SqlException.$(argPositions.getQuick(1), "alpha must be positive");
        }

        return new Func(symbolCount, alpha);
    }

    private static final class Func extends SymbolFunction implements Function {
        private final double alpha;
        private final int count;
        private final DoubleList cumulativeProbabilities;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();
        private Rnd rnd;

        public Func(int count, double alpha) {
            this.count = count;
            this.alpha = alpha;
            this.cumulativeProbabilities = new DoubleList(count);
            this.cumulativeProbabilities.setPos(count);

            // Calculate Zipf distribution
            // p(k) = (1/k^alpha) / sum(1/i^alpha for i=1..n)
            double sum = 0.0;
            for (int i = 1; i <= count; i++) {
                sum += 1.0 / Math.pow(i, alpha);
            }

            // Build cumulative probability distribution
            double cumulative = 0.0;
            for (int i = 0; i < count; i++) {
                double probability = (1.0 / Math.pow(i + 1, alpha)) / sum;
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
            int key = next();
            sinkA.clear();
            sinkA.put("sym").put(key);
            return sinkA;
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            int key = next();
            sinkB.clear();
            sinkB.put("sym").put(key);
            return sinkB;
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
            Func func = new Func(count, alpha);
            func.rnd = new Rnd(this.rnd.getSeed0(), this.rnd.getSeed1());
            return func;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("rnd_symbol_zipf(").val(count).val(',').val(alpha).val(')');
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int symbolKey) {
            if (symbolKey == -1) {
                return null;
            }
            sinkA.clear();
            sinkA.put("sym").put(symbolKey);
            return sinkA;
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
