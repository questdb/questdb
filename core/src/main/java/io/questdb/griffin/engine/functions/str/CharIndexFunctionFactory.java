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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public class CharIndexFunctionFactory implements FunctionFactory {

    private static final IntConstant NOT_FOUND = new IntConstant(0);

    @Override
    public String getSignature() {
        return "charindex(SSI)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function substrFunc = args.getQuick(0);
        if (substrFunc.isConstant()) {
            if (substrFunc.getStrLen(null) < 1) {
                return NOT_FOUND;
            }
        }

        final Function strFunc = args.getQuick(1);
        if (strFunc.isConstant()) {
            if (strFunc.getStrLen(null) < 1) {
                return NOT_FOUND;
            }
        }

        final Function startFunc = args.getQuick(2);

        return new Func(substrFunc, strFunc, startFunc);
    }

    public static class Func extends IntFunction {

        private final Function substrFunc;
        private final Function strFunc;
        private final Function startFunc; // positions start with 1

        public Func(Function substrFunc, Function strFunc, Function startFunc) {
            this.substrFunc = substrFunc;
            this.strFunc = strFunc;
            this.startFunc = startFunc;
        }

        @Override
        public int getInt(Record rec) {
            final CharSequence substr = this.substrFunc.getStr(rec);
            final CharSequence str = this.strFunc.getStr(rec);
            if (substr != null && str != null) {
                final int start = this.startFunc.getInt(rec);
                if (str.length() < start) {
                    return 0;
                }
                return charIndex(substr, str, start);
            }
            return 0;
        }

        private int charIndex(@NotNull CharSequence substr, @NotNull CharSequence str, int start) {
            final int substrLen = substr.length();
            if (substrLen < 1) {
                return 0;
            }
            final int strLen = str.length();
            if (strLen < 1) {
                return 0;
            }
            if (start < 1) {
                start = 1;
            }

            OUTER:
            for (int i = start - 1; i < strLen; i++) {
                final char c = str.charAt(i);
                if (c == substr.charAt(0)) {
                    if (strLen - i < substrLen) {
                        return 0;
                    }

                    for (int k = 1; k < substrLen && k + i < strLen; k++) {
                        if (str.charAt(i + k) != substr.charAt(k)) {
                            continue OUTER;
                        }
                    }

                    return i + 1;
                }
            }
            return 0;
        }
    }
}
