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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class RndStrRndListFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "rnd_str(iiii)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final int count = args.getQuick(0).getInt(null);
        final int lo = args.getQuick(1).getInt(null);
        final int hi = args.getQuick(2).getInt(null);
        final int nullRate = args.getQuick(3).getInt(null);

        if (count < 1) {
            throw SqlException.$(argPositions.getQuick(0), "invalid string count");
        }

        if (lo > hi || lo < 1) {
            throw SqlException.$(position, "invalid range");
        }

        if (nullRate < 0) {
            throw SqlException.position(argPositions.getQuick(3)).put("null rate must be positive");
        }

        final RndStringMemory strMem = new RndStringMemory(getSignature(), count, lo, hi, argPositions.getQuick(0), configuration);
        return new Func(strMem, count, nullRate);
    }

    private static final class Func extends StrFunction implements Function {
        private final int count;
        private final int nullRate;
        private final RndStringMemory strMem;
        private Rnd rnd;

        public Func(RndStringMemory strMem, int count, int nullRate) {
            this.count = count;
            this.strMem = strMem;
            this.nullRate = nullRate;
        }

        @Override
        public void close() {
            strMem.close();
        }

        @Override
        public CharSequence getStrA(Record rec) {
            if (nullRate > 0 && (rnd.nextPositiveInt() % nullRate) == 0) {
                return null;
            }
            return strMem.getStr(rnd.nextPositiveInt() % count);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            if (nullRate > 0 && (rnd.nextPositiveInt() % nullRate) == 0) {
                return null;
            }
            return strMem.getStr2(rnd.nextPositiveInt() % count);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            rnd = executionContext.getRandom();
            strMem.init(rnd);
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
        public void toPlan(PlanSink sink) {
            sink.val("rnd_str(").val(count).val(',').val(strMem.getLo()).val(',').val(strMem.getHi()).val(',').val(nullRate).val(')');
        }
    }
}
