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
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import io.questdb.std.str.Utf8StringSink;

public class RndVarcharFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "rnd_varchar(iii)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        int lo = args.getQuick(0).getInt(null);
        int hi = args.getQuick(1).getInt(null);
        int nullRate = args.getQuick(2).getInt(null);

        if (nullRate < 0) {
            throw SqlException.position(argPositions.getQuick(2)).put("null rate must be positive");
        }

        if (lo < hi && lo > 0) {
            return new RndVarcharFunction(lo, hi, nullRate + 1);
        } else if (lo == hi) {
            return new RndFixedVarcharFunction(lo, nullRate + 1);
        }

        throw SqlException.position(position).put("invalid range");
    }

    private static class RndFixedVarcharFunction extends VarcharFunction implements Function {
        private final int len;
        private final int nullRate;

        private final Utf8StringSink utf8SinkA = new Utf8StringSink();
        private final Utf8StringSink utf8SinkB = new Utf8StringSink();

        private Rnd rnd;

        public RndFixedVarcharFunction(int len, int nullRate) {
            this.len = len;
            this.nullRate = nullRate;
        }

        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            if ((rnd.nextInt() % nullRate) == 1) {
                return null;
            }
            utf8SinkA.clear();
            sinkRnd(utf8SinkA);
            return utf8SinkA;
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            if ((rnd.nextInt() % nullRate) == 1) {
                return null;
            }
            utf8SinkB.clear();
            sinkRnd(utf8SinkB);
            return utf8SinkB;
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
        public void toPlan(PlanSink sink) {
            sink.val("rnd_str(").val(len).val(',').val(len).val(',').val(nullRate - 1).val(')');
        }

        private void sinkRnd(Utf8Sink utf8Sink) {
            rnd.nextUtf8Str(len, utf8Sink);
        }
    }
}
