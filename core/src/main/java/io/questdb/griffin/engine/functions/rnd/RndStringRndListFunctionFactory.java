/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryAR;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.*;

public class RndStringRndListFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "rnd_str(iiii)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {

        // todo: limit pages
        MemoryAR strMem = Vm.getARInstance(1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
        MemoryAR idxMem = Vm.getARInstance(1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);

        final int count = args.getQuick(0).getInt(null);
        final int lo = args.getQuick(1).getInt(null);
        final int hi = args.getQuick(2).getInt(null);
        final int nullRate = args.getQuick(3).getInt(null);

        if (lo == hi) {
            return new FixedFunc(strMem, idxMem, lo, count, nullRate);
        }
        return new Func(strMem, idxMem, lo, hi, count, nullRate);
    }

    private static final class Func extends StrFunction implements Function {
        private final int count;
        private final MemoryAR strMem;
        private final MemoryAR idxMem;
        private final int lo;
        private final int hi;
        private final int nullRate;
        private Rnd rnd;

        public Func(MemoryAR strMem, MemoryAR idxMem, int lo, int hi, int count, int nullRate) {
            this.count = count;
            this.strMem = strMem;
            this.idxMem = idxMem;
            this.lo = lo;
            this.hi = hi;
            this.nullRate = nullRate;
        }

        @Override
        public void close() {
            Misc.free(strMem);
            Misc.free(idxMem);
        }

        @Override
        public CharSequence getStr(Record rec) {
            if (nullRate > 0 && (rnd.nextInt() % nullRate) == 1) {
                return null;
            }
            long o = idxMem.getLong((rnd.nextPositiveInt() % count) * 8L);
            return strMem.getStr(o);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            if (nullRate > 0 && (rnd.nextInt() % nullRate) == 1) {
                return null;
            }
            long o = idxMem.getLong((rnd.nextPositiveInt() % count) * 8L);
            return strMem.getStr2(o);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();

            strMem.jumpTo(0);
            idxMem.jumpTo(0);
            for (int i = 0; i < count; i++) {
                final int len = lo + rnd.nextPositiveInt() % (hi - lo);
                final long o = strMem.putStr(rnd.nextChars(len));
                idxMem.putLong(o - Vm.getStorageLength(len));
            }
        }
    }

    private static final class FixedFunc extends StrFunction implements Function {
        private final int count;
        private final MemoryAR strMem;
        private final MemoryAR idxMem;
        private final int len;
        private final int nullRate;
        private Rnd rnd;

        public FixedFunc(MemoryAR strMem, MemoryAR idxMem, int len, int strCount, int nullRate) {
            this.count = strCount;
            this.strMem = strMem;
            this.idxMem = idxMem;
            this.len = len;
            this.nullRate = nullRate;
        }

        @Override
        public void close() {
            Misc.free(strMem);
            Misc.free(idxMem);
        }

        @Override
        public CharSequence getStr(Record rec) {
            if (nullRate > 0 && (rnd.nextInt() % nullRate) == 1) {
                return null;
            }
            long o = idxMem.getLong((rnd.nextPositiveInt() % count) * 8L);
            return strMem.getStr(o);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            if (nullRate > 0 && (rnd.nextInt() % nullRate) == 1) {
                return null;
            }
            long o = idxMem.getLong((rnd.nextPositiveInt() % count) * 8L);
            return strMem.getStr2(o);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();

            strMem.jumpTo(0);
            idxMem.jumpTo(0);
            for (int i = 0; i < count; i++) {
                final long o = strMem.putStr(rnd.nextChars(len));
                idxMem.putLong(o - Vm.getStorageLength(len));
            }
        }
    }
}
