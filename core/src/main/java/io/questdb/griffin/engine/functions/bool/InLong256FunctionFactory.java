/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\__,_|\___||___/\__|____/|____/
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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.cast.CastByteToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToLong256FunctionFactory;
import io.questdb.std.IntList;
import io.questdb.std.Long256HashSet;
import io.questdb.std.Long256Impl;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public final class InLong256FunctionFactory implements FunctionFactory {

    @Override
    public int getResultType(IntList argTypes) {
        return ColumnType.BOOLEAN;
    }

    @Override
    public String getSignature() {
        return "in(Hv)";
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
        int constCount = 0;
        for (int i = 1; i < n; i++) {
            final Function func = args.getQuick(i);
            switch (ColumnType.tagOf(func.getType())) {
                case ColumnType.NULL, ColumnType.LONG256, ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL,
                     ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG, ColumnType.UNDEFINED -> {
                }
                default -> throw SqlException.position(argPositions.getQuick(i))
                        .put("cannot compare LONG256 with type ").put(ColumnType.nameOf(func.getType()));
            }
            if (func.isConstant()) {
                constCount++;
            }
        }

        final Long256HashSet set = new Long256HashSet(n - 1, 0.6);
        final Long256Impl parsed = new Long256Impl();
        if (constCount == n - 1) {
            for (int i = 1; i < n; i++) {
                addToSet(widen(args.getQuick(i)), argPositions.getQuick(i), set, parsed);
            }
            for (int i = 1; i < n; i++) {
                args.setQuick(i, Misc.free(args.getQuick(i)));
            }
            return new InLong256ConstFunction(args.getQuick(0), set);
        }

        final ObjList<Function> functions = new ObjList<>(n);
        functions.add(args.getQuick(0));
        for (int i = 1; i < n; i++) {
            functions.add(widen(args.getQuick(i)));
        }
        final IntList positions = new IntList();
        positions.addAll(argPositions);
        return new InLong256RuntimeConstFunction(functions, positions, set, parsed);
    }

    private static void addToSet(Function func, int position, Long256HashSet set, Long256Impl parsed) throws SqlException {
        switch (ColumnType.tagOf(func.getType())) {
            case ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL -> {
                final CharSequence text = func.getStrA(null);
                if (text == null) {
                    set.add(Long256Impl.NULL_LONG256);
                } else if (Numbers.extractLong256(text, parsed)) {
                    set.add(parsed);
                } else {
                    throw SqlException.$(position, "invalid LONG256 value [").put(text).put(']');
                }
            }
            default -> set.add(func.getLong256A(null));
        }
    }

    // An integer element widens exactly as FunctionResolver widens an integer operand of a LONG256 call.
    private static Function widen(Function func) {
        return switch (ColumnType.tagOf(func.getType())) {
            case ColumnType.BYTE -> new CastByteToLong256FunctionFactory.Func(func);
            case ColumnType.SHORT -> new CastShortToLong256FunctionFactory.Func(func);
            case ColumnType.INT -> new CastIntToLong256FunctionFactory.Func(func);
            case ColumnType.LONG -> new CastLongToLong256FunctionFactory.Func(func);
            default -> func;
        };
    }

    private static class InLong256ConstFunction extends BooleanFunction implements UnaryFunction {
        private final Function arg;
        private final Long256HashSet set;

        public InLong256ConstFunction(Function arg, Long256HashSet set) {
            this.arg = arg;
            this.set = set;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return set.contains(arg.getLong256A(rec));
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val(" in ").val(set);
        }
    }

    private static class InLong256RuntimeConstFunction extends BooleanFunction implements MultiArgFunction {
        private final ObjList<Function> functions;
        private final Long256Impl parsed;
        private final IntList positions;
        private final Long256HashSet set;

        public InLong256RuntimeConstFunction(ObjList<Function> functions, IntList positions, Long256HashSet set, Long256Impl parsed) {
            this.functions = functions;
            this.positions = positions;
            this.set = set;
            this.parsed = parsed;
        }

        @Override
        public ObjList<Function> args() {
            return functions;
        }

        @Override
        public boolean getBool(Record rec) {
            return set.contains(functions.getQuick(0).getLong256A(rec));
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            MultiArgFunction.super.init(symbolTableSource, executionContext);
            set.clear();
            for (int i = 1, n = functions.size(); i < n; i++) {
                addToSet(functions.getQuick(i), positions.getQuick(i), set, parsed);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(functions.getQuick(0)).val(" in ").val(set);
        }
    }
}
