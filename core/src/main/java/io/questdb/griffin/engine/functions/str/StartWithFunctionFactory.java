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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class StartWithFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "starts_with(SS)";
    }

    @Override
    public Function newInstance(final int position, final ObjList<Function> args, IntList argPositions, final CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new StartWithFunc(args.getQuick(0), args.getQuick(1));
    }

    public static class StartWithFunc extends BooleanFunction implements BinaryFunction {

        private final Function strFunc;
        private final Function prefixFunc;

        public StartWithFunc(Function strFunc, Function prefixFunc) {
            this.strFunc = strFunc;
            this.prefixFunc = prefixFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            final var str = strFunc.getStr(rec);
            if (str == null) {
                return false;
            }
            final var prefix = prefixFunc.getStr(rec);
            if (prefix == null) {
                return false;
            }
            return isStartWith(str, prefix);
        }

        @Override
        public Function getLeft() {
            return strFunc;
        }

        @Override
        public Function getRight() {
            return prefixFunc;
        }

        private static boolean isStartWith(CharSequence str, CharSequence prefix) {
            if (prefix.length() > str.length()) {
                return false;
            }
            return str.subSequence(0, prefix.length()).equals(prefix);
        }
    }
}
