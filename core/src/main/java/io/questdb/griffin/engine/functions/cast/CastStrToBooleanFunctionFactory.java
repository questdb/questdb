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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class CastStrToBooleanFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(St)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        Function arg0 = args.getQuick(0);
        if (arg0.isConstant()) {
            return resolveBoolean(arg0.getStrA(null)) ? BooleanConstant.TRUE : BooleanConstant.FALSE;
        }
        return new Func(arg0);
    }

    private static boolean resolveBoolean(CharSequence str) {
        return str != null && SqlKeywords.isTrueKeyword(str);
    }

    private static class Func extends AbstractCastToBooleanFunction {
        public Func(Function arg) {
            super(arg);
        }

        @Override
        public boolean getBool(Record rec) {
            return resolveBoolean(arg.getStrA(rec));
        }
    }
}
