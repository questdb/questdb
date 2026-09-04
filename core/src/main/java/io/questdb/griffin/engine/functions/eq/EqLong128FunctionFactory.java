/*+*****************************************************************************
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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class EqLong128FunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(JJ)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function a = args.getQuick(0);
        final Function b = args.getQuick(1);
        // Fold `x = NULL` / `x IS NULL` to FALSE when the variable side is NOT NULL.
        // LONG128 null is represented as (LONG_NULL, LONG_NULL) in the hi/lo halves.
        // NegatingFunctionFactory handles the IS NOT NULL path by flipping the constant.
        if (a.isConstant() && isLong128Null(a) && b.isNotNull()) {
            return BooleanConstant.FALSE;
        }
        if (b.isConstant() && isLong128Null(b) && a.isNotNull()) {
            return BooleanConstant.FALSE;
        }
        return new Func(a, b);
    }

    private static boolean isLong128Null(Function constFunc) {
        return constFunc.getLong128Hi(null) == Numbers.LONG_NULL
                && constFunc.getLong128Lo(null) == Numbers.LONG_NULL;
    }

    private static class Func extends AbstractEqBinaryFunction {
        public Func(Function left, Function right) {
            super(left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (
                    left.getLong128Lo(rec) == right.getLong128Lo(rec)
                            && left.getLong128Hi(rec) == right.getLong128Hi(rec)
            );
        }
    }
}
