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
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        Function strFunc1 = args.get(0);
        Function strFunc2 = args.get(1);

        return new StartsWithStrFunction(strFunc1, strFunc2);
    }

    private static class StartsWithStrFunction extends BooleanFunction implements BinaryFunction {
        private final Function strFunc1;
        private final Function strFunc2;

        public StartsWithStrFunction(Function strFunc1, Function strFunc2) {
            this.strFunc1 = strFunc1;
            this.strFunc2 = strFunc2;
        }

        @Override
        public boolean getBool(Record rec) {
            CharSequence str1 = strFunc1.getStr(rec);
            CharSequence str2 = strFunc2.getStr(rec);
            if (str1 == null || str2 == null)
                return false;

            if (Chars.equals(str2, ""))
                return true;

            return Chars.startsWith(str1, str2);
        }

        @Override
        public Function getLeft() {
            return strFunc1;
        }

        @Override
        public Function getRight() {
            return strFunc2;
        }
    }
}
