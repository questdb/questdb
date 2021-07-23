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
import io.questdb.cairo.vm.CARWMemoryImpl;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class RndStringRndListFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "rnd_str(iiii)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {

        // todo: limit pages
        CARWMemoryImpl strMem = new CARWMemoryImpl(1024 * 1024, Integer.MAX_VALUE);
        CARWMemoryImpl idxMem = new CARWMemoryImpl(1024 * 1024, Integer.MAX_VALUE);

        final int lo = args.getQuick(1).getInt(null);
        final int hi = args.getQuick(2).getInt(null);

        if (lo == hi) {
            return new FixedFunc(
                    strMem,
                    idxMem,
                    lo,
                    args.getQuick(0).getInt(null),
                    args.getQuick(3).getInt(null)
            );
        }
        return new Func(
                strMem,
                idxMem,
                args.getQuick(1).getInt(null),
                args.getQuick(2).getInt(null),
                args.getQuick(0).getInt(null),
                args.getQuick(3).getInt(null)
        );
    }

    private static final class Func extends StrFunction implements Function {
        private final int count;
        private final CARWMemoryImpl strMem;
        private final CARWMemoryImpl idxMem;
        private final int strLo;
        private final int strHi;
        private Rnd rnd;

        public Func(CARWMemoryImpl strMem, CARWMemoryImpl idxMem, int strLo, int strHi, int strCount, int nullRate) {
            this.count = strCount;
            this.strMem = strMem;
            this.idxMem = idxMem;
            this.strLo = strLo;
            this.strHi = strHi;
        }

        @Override
        public CharSequence getStr(Record rec) {
            long o = idxMem.getLong((rnd.nextPositiveInt() % count) * 8L);
            return strMem.getStr(o);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            long o = idxMem.getLong((rnd.nextPositiveInt() % count) * 8L);
            return strMem.getStr2(o);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();

            strMem.jumpTo(0);
            idxMem.jumpTo(0);
            for (int i = 0; i < count; i++) {
                final long o = strMem.putStr(rnd.nextChars(strLo + rnd.nextPositiveInt() % (strHi - strLo)));
                idxMem.putLong(o);
            }
        }

        @Override
        public void close() {
            Misc.free(strMem);
            Misc.free(idxMem);
        }
    }

    private static final class FixedFunc extends StrFunction implements Function {
        private final int count;
        private final CARWMemoryImpl strMem;
        private final CARWMemoryImpl idxMem;
        private final int strLen;
        private Rnd rnd;

        public FixedFunc(CARWMemoryImpl strMem, CARWMemoryImpl idxMem, int strLen, int strCount, int nullRate) {
            this.count = strCount;
            this.strMem = strMem;
            this.idxMem = idxMem;
            this.strLen = strLen;
        }

        @Override
        public CharSequence getStr(Record rec) {
            long o = idxMem.getLong((rnd.nextPositiveInt() % count) * 8L);
            return strMem.getStr(o);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            long o = idxMem.getLong((rnd.nextPositiveInt() % count) * 8L);
            return strMem.getStr2(o);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();

            strMem.jumpTo(0);
            idxMem.jumpTo(0);
            for (int i = 0; i < count; i++) {
                final long o = strMem.putStr(rnd.nextChars(strLen));
                idxMem.putLong(o);
            }
        }

        @Override
        public void close() {
            Misc.free(strMem);
            Misc.free(idxMem);
        }
    }
}
