/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class StartsWithStrFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "starts_with(SS)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        Function strFunc = args.get(0);
        Function prefixFunc = args.get(1);

        return new StartsWithStrFunction(strFunc, prefixFunc);
    }

    private static class StartsWithStrFunction extends BooleanFunction implements BinaryFunction {
        private final Function prefixFunc;
        private final Function strFunc;

        public StartsWithStrFunction(Function strFunc, Function prefixFunc) {
            this.strFunc = strFunc;
            this.prefixFunc = prefixFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            CharSequence str = strFunc.getStr(rec);
            CharSequence prefix = prefixFunc.getStr(rec);
            if (str == null || prefix == null) {
                return false;
            }

            return Chars.startsWith(str, prefix);
        }

        @Override
        public Function getLeft() {
            return strFunc;
        }

        @Override
        public String getName() {
            return "starts_with";
        }

        @Override
        public Function getRight() {
            return prefixFunc;
        }
    }
}
