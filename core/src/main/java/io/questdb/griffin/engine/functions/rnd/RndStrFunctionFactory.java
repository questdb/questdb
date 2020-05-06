/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.NoArgFunction;
import io.questdb.griffin.engine.functions.StatelessFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class RndStrFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "rnd_str(iii)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {

        int lo = args.getQuick(0).getInt(null);
        int hi = args.getQuick(1).getInt(null);
        int nullRate = args.getQuick(2).getInt(null);

        if (nullRate < 0) {
            throw SqlException.position(args.getQuick(2).getPosition()).put("rate must be positive");
        }

        if (lo < hi && lo > 0) {
            return new RndStrFunction(position, lo, hi, nullRate + 1);
        } else if (lo == hi) {
            return new FixedFunction(position, lo, nullRate + 1);
        }

        throw SqlException.position(position).put("invalid range");
    }

    private static class FixedFunction extends StrFunction implements StatelessFunction, NoArgFunction {
        private final int len;
        private final int nullRate;
        private Rnd rnd;

        public FixedFunction(int position, int len, int nullRate) {
            super(position);
            this.len = len;
            this.nullRate = nullRate;
        }

        @Override
        public CharSequence getStr(Record rec) {
            if ((rnd.nextInt() % nullRate) == 1) {
                return null;
            }
            return rnd.nextChars(len);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return getStr(rec);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();
        }
    }
}
