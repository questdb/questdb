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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.StringSink;

public class InSymbolFunctionFactory implements FunctionFactory {

    @Override
    public int getResultType(IntList argTypes) {
        return ColumnType.BOOLEAN;
    }

    @Override
    public String getSignature() {
        return "in(Kv)";
    }

    @Override
    public boolean isConstructionDeferrable(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration) throws SqlException {
        validateValueTypes(args, argPositions);
        return args.size() > 1;
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        int n = args.size();
        if (n == 1) {
            CairoException.rethrowCleanupFailure(Misc.freeObjListBestEffort(null, args));
            return BooleanConstant.FALSE;
        }

        validateValueTypes(args, argPositions);
        final CharSequenceHashSet set = new CharSequenceHashSet();
        ObjList<Function> deferredValues = null;
        IntList deferredValuePositions = null;
        for (int i = 1; i < n; i++) {
            Function func = args.getQuick(i);
            // Defer runtime-constants (bind variables and runtime-constant
            // function chains over them) to init() regardless of whether the
            // type is STRING/VARCHAR or SYMBOL/CHAR. Reading the value here
            // hits NamedParameterLinkFunction.getBase() before the variable
            // is bound and trips its assertion.
            if (!func.isConstant() && func.isRuntimeConstant()) {
                if (deferredValues == null) {
                    deferredValues = new ObjList<>();
                    deferredValuePositions = new IntList();
                }
                deferredValues.add(func);
                deferredValuePositions.add(argPositions.getQuick(i));
                continue;
            }
            set.add(valueText(func, Misc.getThreadLocalSink()));
        }

        SymbolFunction var = (SymbolFunction) args.getQuick(0);
        if (var.isConstant() && deferredValues == null) {
            // Fast path for all constants case.
            return BooleanConstant.of(set.contains(var.getSymbol(null)));
        }
        return new Func(var, set, deferredValues, deferredValuePositions);
    }

    @Override
    public boolean variadicTypeSupportUndefinedBindVariables(int argCount) {
        return argCount > 2;
    }

    @FunctionalInterface
    interface TestFunc {
        boolean test(Record rec);
    }

    private static void validateValueTypes(ObjList<Function> args, IntList argPositions) throws SqlException {
        for (int i = 1, n = args.size(); i < n; i++) {
            final Function arg = args.getQuick(i);
            switch (ColumnType.tagOf(arg.getType())) {
                case ColumnType.STRING:
                case ColumnType.VARCHAR:
                case ColumnType.UNDEFINED:
                case ColumnType.SYMBOL:
                case ColumnType.NULL:
                case ColumnType.CHAR:
                    break;
                case ColumnType.BYTE:
                case ColumnType.SHORT:
                case ColumnType.INT:
                case ColumnType.LONG:
                    if (!arg.isConstant()) {
                        throw SqlException.$(argPositions.getQuick(i), "STRING constant expected");
                    }
                    break;
                default:
                    throw SqlException.$(argPositions.getQuick(i), "STRING constant expected");
            }
        }
    }

    /**
     * The text a value matches a SYMBOL by, the decimal text of an integer as {@code =} formats it.
     */
    private static CharSequence valueText(Function func, StringSink sink) {
        switch (ColumnType.tagOf(func.getType())) {
            case ColumnType.CHAR -> {
                final char c = func.getChar(null);
                if (c == 0) {
                    return null;
                }
                sink.clear();
                sink.put(c);
                return sink;
            }
            case ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG -> {
                final long value = func.getLong(null);
                if (value == Numbers.LONG_NULL) {
                    return null;
                }
                sink.clear();
                sink.put(value);
                return sink;
            }
            default -> {
                return func.getStrA(null);
            }
        }
    }

    private static class Func extends BooleanFunction implements UnaryFunction {
        private final SymbolFunction arg;
        private final CharSequenceHashSet deferredSet;
        private final IntList deferredValuePositions;
        private final ObjList<Function> deferredValues;
        private final IntHashSet intSet = new IntHashSet();
        private final TestFunc intTest = this::testAsInt;
        private final CharSequenceHashSet set;
        private final StringSink sink = new StringSink();
        private final TestFunc strTest = this::testAsString;
        private TestFunc testFunc;

        public Func(SymbolFunction arg, CharSequenceHashSet set, ObjList<Function> deferredValues, IntList deferredValuePositions) {
            this.arg = arg;
            this.set = set;
            this.deferredValues = deferredValues;
            this.deferredSet = deferredValues != null ? new CharSequenceHashSet() : null;
            this.deferredValuePositions = deferredValuePositions;
        }

        @Override
        public void close() {
            UnaryFunction.super.close();
            Misc.freeObjList(deferredValues);
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return testFunc.test(rec);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            if (deferredValues != null) {
                for (int i = 0, n = deferredValues.size(); i < n; i++) {
                    deferredValues.getQuick(i).init(symbolTableSource, executionContext);

                    // validate types of bind variables we support
                    final Function func = deferredValues.getQuick(i);
                    switch (ColumnType.tagOf(func.getType())) {
                        case ColumnType.VARCHAR:
                        case ColumnType.STRING:
                        case ColumnType.SYMBOL:
                        case ColumnType.CHAR:
                        case ColumnType.NULL:
                            continue;
                        default:
                            throw SqlException.inconvertibleTypes(
                                    deferredValuePositions.getQuick(i),
                                    func.getType(),
                                    ColumnType.nameOf(func.getType()),
                                    ColumnType.SYMBOL,
                                    ColumnType.nameOf(ColumnType.SYMBOL)
                            );
                    }
                }
            }

            final StaticSymbolTable symbolTable = arg.getStaticSymbolTable();
            if (symbolTable != null) {
                intSet.clear();
                for (int i = 0, n = set.size(); i < n; i++) {
                    intSet.add(symbolTable.keyOf(set.get(i)));
                }
                if (deferredValues != null) {
                    for (int i = 0, n = deferredValues.size(); i < n; i++) {
                        final Function func = deferredValues.getQuick(i);
                        intSet.add(symbolTable.keyOf(valueText(func, sink)));
                    }
                }
                testFunc = intTest;
            } else {
                if (deferredValues != null) {
                    deferredSet.clear();
                    for (int i = 0, n = deferredValues.size(); i < n; i++) {
                        final Function func = deferredValues.getQuick(i);
                        deferredSet.add(valueText(func, sink));
                    }
                }
                testFunc = strTest;
            }
        }

        // Override required: UnaryFunction's default delegates to
        // arg.isConstant(), which is true when the LHS is a literal (e.g.
        // "'A'::SYMBOL IN (:b0)"). FunctionParser would then fold the
        // function via getBool(null) before init() set testFunc, tripping
        // an NPE.
        @Override
        public boolean isConstant() {
            if (!arg.isConstant()) {
                return false;
            }
            if (deferredValues != null) {
                for (int i = 0, n = deferredValues.size(); i < n; i++) {
                    if (!deferredValues.getQuick(i).isConstant()) {
                        return false;
                    }
                }
            }
            return true;
        }

        @Override
        public void offerStateTo(Function that) {
            UnaryFunction.super.offerStateTo(that);
            // deferred values sit outside the argument tree the default walks
            if (deferredValues != null && that instanceof Func other
                    && other.deferredValues != null && other.deferredValues.size() == deferredValues.size()) {
                for (int i = 0, n = deferredValues.size(); i < n; i++) {
                    deferredValues.getQuick(i).offerStateTo(other.deferredValues.getQuick(i));
                }
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
            boolean hasLiterals = set != null && set.size() > 0;
            if (hasLiterals) {
                sink.val(" in ").val(set);
            }
            if (deferredValues != null && deferredValues.size() > 0) {
                if (hasLiterals) {
                    sink.val(" or ").val(arg);
                }
                sink.val(" in ").val(deferredValues);
            }
        }

        private boolean testAsInt(Record rec) {
            return intSet.contains(arg.getInt(rec));
        }

        private boolean testAsString(Record rec) {
            final CharSequence symbol = arg.getSymbol(rec);
            if (set.contains(symbol)) {
                return true;
            }
            if (deferredSet != null) {
                return deferredSet.contains(symbol);
            }
            return false;
        }
    }
}
