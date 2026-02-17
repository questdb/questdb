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

package io.questdb.griffin.engine.functions.bool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.CharConstant;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Utf8Sequence;

public class InCharFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "in(Av)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        IntHashSet set = new IntHashSet();
        int n = args.size();

        if (n == 1) {
            return BooleanConstant.FALSE;
        }

        for (int i = 1; i < n; i++) {
            Function func = args.getQuick(i);
            if (ColumnType.isChar(func.getType())) {
                set.add(func.getChar(null));
            } else if (ColumnType.isString(func.getType())) {
                // Implicitly cast empty string literal ('') to zero char
                if (func.getStrLen(null) != 0) {
                    throw SqlException.$(argPositions.getQuick(i), "CHAR constant expected");
                }
                set.add(CharConstant.ZERO.getChar(null));
            } else if (ColumnType.isVarchar(func.getType())) {
                Utf8Sequence seq = func.getVarcharA(null);
                if (seq != null && seq.size() != 0) {
                    throw SqlException.$(argPositions.getQuick(i), "CHAR constant expected");
                }
                set.add(CharConstant.ZERO.getChar(null));
            } else if (ColumnType.isNull(func.getType())) {
                set.add(CharConstant.ZERO.getChar(null));
            } else {
                throw SqlException.$(argPositions.getQuick(i), "CHAR constant expected");
            }
        }
        Function var = args.getQuick(0);
        if (var.isConstant()) {
            return BooleanConstant.of(set.contains(var.getChar(null)));
        }
        return new InCharConstFunction(var, set);
    }

    private static class InCharConstFunction extends BooleanFunction implements UnaryFunction {
        private final Function arg;
        private final IntHashSet set;

        public InCharConstFunction(Function arg, IntHashSet set) {
            this.arg = arg;
            this.set = set;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return set.contains(arg.getChar(rec));
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val(" in ").val(set);
        }
    }
}
