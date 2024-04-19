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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

public class StrPosVarcharFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "strpos(ØØ)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final Function substrFunc = args.getQuick(1);
        if (substrFunc.isConstant()) {
            Utf8Sequence substr = substrFunc.getVarcharA(null);
            if (substr == null) {
                return IntConstant.NULL;
            }
            return new ConstFunc(args.getQuick(0), substr);
        }
        return new Func(args.getQuick(0), substrFunc);
    }

    private static int strpos(@NotNull Utf8Sequence str, @NotNull Utf8Sequence substr) {
        final int substrSize = substr.size();
        if (substrSize < 1) {
            return 1;
        }
        final int strSize = str.size();
        if (strSize < 1) {
            return 0;
        }

        OUTER:
        for (int i = 0, strPos = 0, n = strSize - substrSize + 1; i < n; i++) {
            final byte c = str.byteAt(i);
            // Only advance strPos if c is not a continuation byte
            if ((c & 0b1100_0000) != 0b1000_0000) {
                strPos++;
            }
            if (c == substr.byteAt(0)) {
                for (int k = 1; k < substrSize; k++) {
                    if (str.byteAt(i + k) != substr.byteAt(k)) {
                        continue OUTER;
                    }
                }
                return strPos;
            }
        }
        return 0;
    }

    public static class ConstFunc extends IntFunction implements UnaryFunction {

        private final Utf8Sequence substr;
        private final Function varcharFunc;

        public ConstFunc(Function varcharFunc, Utf8Sequence substr) {
            this.varcharFunc = varcharFunc;
            this.substr = substr;
        }

        @Override
        public Function getArg() {
            return varcharFunc;
        }

        @Override
        public int getInt(Record rec) {
            final Utf8Sequence str = this.varcharFunc.getVarcharA(rec);
            if (str == null) {
                return Numbers.INT_NaN;
            }
            return strpos(str, substr);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("strpos(").val(varcharFunc).val(",'").val(substr).val("')");
        }
    }

    public static class Func extends IntFunction implements BinaryFunction {

        private final Function strFunc;
        private final Function substrFunc;

        public Func(Function strFunc, Function substrFunc) {
            this.strFunc = strFunc;
            this.substrFunc = substrFunc;
        }

        @Override
        public int getInt(Record rec) {
            final Utf8Sequence str = this.strFunc.getVarcharA(rec);
            if (str == null) {
                return Numbers.INT_NaN;
            }
            final Utf8Sequence substr = this.substrFunc.getVarcharA(rec);
            if (substr == null) {
                return Numbers.INT_NaN;
            }
            return strpos(str, substr);
        }

        @Override
        public Function getLeft() {
            return strFunc;
        }

        @Override
        public String getName() {
            return "strpos";
        }

        @Override
        public Function getRight() {
            return substrFunc;
        }
    }
}
