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
import io.questdb.std.*;

public final class InUuidFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "in(Zv)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        int n = args.size();
        LongLongHashSet set = new LongLongHashSet(n - 1, 0.6, Numbers.LONG_NaN, LongLongHashSet.UUID_STRATEGY);

        for (int i = 1; i < n; i++) {
            Function func = args.getQuick(i);
            switch (ColumnType.tagOf(func.getType())) {
                case ColumnType.NULL:
                case ColumnType.STRING:
                case ColumnType.SYMBOL:
                    CharSequence value = func.getStr(null);
                    if (value == null) {
                        throw SqlException.$(argPositions.getQuick(i), "NULL is not allowed in IN list");
                    }
                    try {
                        Uuid.checkDashesAndLength(value);
                        set.add(Uuid.parseLo(value), Uuid.parseHi(value));
                    } catch (NumericException e) {
                        // the given string is not a valid UUID -> no UUID value can match it -> we can ignore it
                    }
                    break;
                case ColumnType.UUID:
                    long lo = func.getLong128Lo(null);
                    long hi = func.getLong128Hi(null);
                    if (hi == Numbers.LONG_NaN && lo == Numbers.LONG_NaN) {
                        throw SqlException.$(argPositions.getQuick(i), "NULL is not allowed in IN list");
                    }
                    set.add(lo, hi);
                    break;
                default:
                    throw SqlException.$(argPositions.getQuick(i), "STRING or UUID constant expected in IN list");
            }
        }
        Function var = args.getQuick(0);
        if (var.isConstant()) {
            long lo = var.getLong128Lo(null);
            long hi = var.getLong128Hi(null);
            if (Uuid.isNull(lo, hi)) {
                return BooleanConstant.FALSE;
            }
            return BooleanConstant.of(set.contains(lo, hi));
        }
        return new Func(var, set);
    }

    private static class Func extends BooleanFunction implements UnaryFunction {
        private final Function arg;
        private final LongLongHashSet set;

        public Func(Function arg, LongLongHashSet set) {
            this.arg = arg;
            this.set = set;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            long lo = arg.getLong128Lo(rec);
            long hi = arg.getLong128Hi(rec);
            if (Uuid.isNull(lo, hi)) {
                return false;
            }
            return set.contains(lo, hi);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val(" in ").val(set);
        }
    }
}
