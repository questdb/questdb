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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

public class RndLong256NFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "rnd_long256(i)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new RndFunction(args.getQuick(0).getInt(null));
    }

    private static class RndFunction extends Long256Function implements Function {

        private final Long256Impl long256A = new Long256Impl();
        private final Long256Impl long256B = new Long256Impl();
        private final long[] values;
        private Rnd rnd;

        public RndFunction(int count) {
            this.values = new long[count * 4];
        }

        @Override
        public void getLong256(Record rec, CharSink<?> sink) {
            Numbers.appendLong256(rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), sink);
        }

        @Override
        public Long256 getLong256A(Record rec) {
            return rndLong(long256A);
        }

        @Override
        public Long256 getLong256B(Record rec) {
            return rndLong(long256B);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();
            for (int i = 0, n = values.length; i < n; i++) {
                values[i] = rnd.nextLong();
            }
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
            sink.val("rnd_long256(").val(values.length / 4).val(')');
        }

        @NotNull
        private Long256 rndLong(Long256Impl long256) {
            int index = rnd.nextPositiveInt() % (values.length / 4);
            long256.setAll(
                    values[index * 4],
                    values[index * 4 + 1],
                    values[index * 4 + 2],
                    values[index * 4 + 3]
            );
            return long256;
        }
    }
}
