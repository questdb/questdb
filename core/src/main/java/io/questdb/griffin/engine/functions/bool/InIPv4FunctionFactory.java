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

package io.questdb.griffin.engine.functions.bool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.LongHashSet;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public final class InIPv4FunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "in(Xv)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final int n = args.size();
        if (n == 1) {
            return BooleanConstant.FALSE;
        }

        final int argCount = n - 1;
        int constCount = 0;
        for (int i = 1; i < n; i++) {
            Function func = args.getQuick(i);
            switch (ColumnType.tagOf(func.getType())) {
                case ColumnType.NULL:
                case ColumnType.STRING:
                case ColumnType.VARCHAR:
                case ColumnType.SYMBOL:
                case ColumnType.IPv4:
                case ColumnType.UNDEFINED: // bind variable
                    break;
                default:
                    throw SqlException.position(argPositions.getQuick(i))
                            .put("cannot compare IPv4 with type ")
                            .put(ColumnType.nameOf(func.getType()));
            }
            if (func.isConstant()) {
                constCount++;
            }
            // FunctionParser already rejects args that are neither constant nor
            // runtime-constant under the variadic 'v' tail, so no extra check needed.
        }

        if (constCount == argCount) {
            LongHashSet set = makeIPv4HashSet(argCount);
            for (int i = 1; i < n; i++) {
                addIPv4ToSet(args.getQuick(i), argPositions.getQuick(i), set);
            }

            Function keyFunc = args.getQuick(0);
            if (keyFunc.isConstant()) {
                return BooleanConstant.of(set.contains(keyFunc.getIPv4(null)));
            }
            return new InIPv4ConstFunction(keyFunc, set);
        }

        final IntList positions = new IntList();
        positions.addAll(argPositions);
        return new InIPv4RuntimeConstFunction(args.getQuick(0), new ObjList<>(args), positions, makeIPv4HashSet(argCount));
    }

    private static void addIPv4ToSet(Function func, int argPosition, LongHashSet set) throws SqlException {
        try {
            switch (ColumnType.tagOf(func.getType())) {
                case ColumnType.NULL:
                    set.add(Numbers.IPv4_NULL);
                    break;
                case ColumnType.IPv4:
                    set.add(func.getIPv4(null));
                    break;
                case ColumnType.STRING:
                case ColumnType.SYMBOL:
                    // SymbolFunction.getIPv4 is final and throws UnsupportedOperationException,
                    // so SYMBOL elements must go through the type-agnostic string getter.
                    set.add(SqlUtil.implicitCastStrAsIPv4(func.getStrA(null)));
                    break;
                case ColumnType.VARCHAR:
                    set.add(SqlUtil.implicitCastStrAsIPv4(func.getVarcharA(null)));
                    break;
                default:
                    throw SqlException.position(argPosition)
                            .put("cannot compare IPv4 with type ")
                            .put(ColumnType.nameOf(func.getType()));
            }
        } catch (ImplicitCastException e) {
            throw SqlException.$(argPosition, e.getFlyweightMessage());
        }
    }

    // IPv4 is stored internally as a 32-bit int, but every value in that range
    // (including IPv4_NULL = 0 and the broadcast address whose int repr is -1)
    // is a valid IPv4, so IntHashSet has no spare sentinel to reserve for its
    // empty-slot marker. Widening to long gives the hash set headroom: any
    // sign-extended int lands in [Integer.MIN_VALUE, Integer.MAX_VALUE], which
    // never collides with LONG_NULL (= Long.MIN_VALUE).
    private static LongHashSet makeIPv4HashSet(int capacity) {
        return new LongHashSet(capacity, 0.4, Numbers.LONG_NULL);
    }

    private static class InIPv4ConstFunction extends BooleanFunction implements UnaryFunction {
        private final Function arg;
        private final LongHashSet set;

        public InIPv4ConstFunction(Function arg, LongHashSet set) {
            this.arg = arg;
            this.set = set;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return set.contains(arg.getIPv4(rec));
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val(" in [");
            for (int i = 0, n = set.size(); i < n; i++) {
                if (i > 0) {
                    sink.val(',');
                }
                sink.valIPv4((int) set.get(i));
            }
            sink.val(']');
        }
    }

    private static class InIPv4RuntimeConstFunction extends BooleanFunction implements MultiArgFunction {
        private final Function keyFunc;
        private final LongHashSet set;
        private final IntList valueFunctionPositions;
        private final ObjList<Function> valueFunctions;

        public InIPv4RuntimeConstFunction(
                Function keyFunc,
                ObjList<Function> valueFunctions,
                IntList valueFunctionPositions,
                LongHashSet set
        ) {
            this.keyFunc = keyFunc;
            this.valueFunctions = valueFunctions;
            this.valueFunctionPositions = valueFunctionPositions;
            this.set = set;
        }

        @Override
        public ObjList<Function> args() {
            return valueFunctions;
        }

        @Override
        public boolean getBool(Record rec) {
            return set.contains(keyFunc.getIPv4(rec));
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            MultiArgFunction.super.init(symbolTableSource, executionContext);
            set.clear();
            for (int i = 1, n = valueFunctions.size(); i < n; i++) {
                addIPv4ToSet(valueFunctions.getQuick(i), valueFunctionPositions.getQuick(i), set);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(keyFunc).val(" in ").val(valueFunctions, 1);
        }
    }
}
