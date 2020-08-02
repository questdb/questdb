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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

public class RndLong256FunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "rnd_long256()";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        return new RndFunction(position);
    }

    private static class RndFunction extends Long256Function implements Function {

        private final Long256Impl long256A = new Long256Impl();
        private final Long256Impl long256B = new Long256Impl();
        private Rnd rnd;

        public RndFunction(int position) {
            super(position);
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
        public void getLong256(Record rec, CharSink sink) {
            Numbers.appendLong256(rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), sink);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();
        }

        @NotNull
        private Long256 rndLong(Long256Impl long256) {
            long256.setLong0(rnd.nextLong());
            long256.setLong1(rnd.nextLong());
            long256.setLong2(rnd.nextLong());
            long256.setLong3(rnd.nextLong());
            return long256;
        }
    }
}
