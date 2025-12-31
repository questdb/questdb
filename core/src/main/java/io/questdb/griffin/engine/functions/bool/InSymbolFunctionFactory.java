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
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class InSymbolFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "in(Kv)";
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
            return BooleanConstant.FALSE;
        }

        final CharSequenceHashSet set = new CharSequenceHashSet();
        ObjList<Function> deferredValues = null;
        IntList deferredValuePositions = null;
        for (int i = 1; i < n; i++) {
            Function func = args.getQuick(i);
            switch (ColumnType.tagOf(func.getType())) {
                case ColumnType.STRING:
                case ColumnType.VARCHAR:
                case ColumnType.UNDEFINED:
                    if (func.isRuntimeConstant()) {
                        // string bind variable case
                        if (deferredValues == null) {
                            deferredValues = new ObjList<>();
                            deferredValuePositions = new IntList();
                        }
                        deferredValues.add(func);
                        deferredValuePositions.add(argPositions.getQuick(i));
                        continue;
                    }
                    // fall through
                case ColumnType.SYMBOL:
                case ColumnType.NULL:
                    CharSequence value = func.getStrA(null);
                    if (value == null) {
                        set.add((CharSequence) null);
                    } else {
                        set.add(Chars.toString(value));
                    }
                    break;
                case ColumnType.CHAR:
                    set.add(String.valueOf(func.getChar(null)));
                    break;
                default:
                    throw SqlException.$(argPositions.getQuick(i), "STRING constant expected");
            }
        }

        SymbolFunction var = (SymbolFunction) args.getQuick(0);
        if (var.isConstant() && deferredValues == null) {
            // Fast path for all constants case.
            return BooleanConstant.of(set.contains(var.getSymbol(null)));
        }
        return new Func(var, set, deferredValues, deferredValuePositions);
    }

    @Override
    public boolean variadicTypeSupportUndefinedBindVariables(ObjList<Function> args) {
        return args.size() > 2;
    }

    @FunctionalInterface
    interface TestFunc {
        boolean test(Record rec);
    }

    private static class Func extends BooleanFunction implements UnaryFunction {
        private final SymbolFunction arg;
        private final CharSequenceHashSet deferredSet;
        private final IntList deferredValuePositions;
        private final ObjList<Function> deferredValues;
        private final IntHashSet intSet = new IntHashSet();
        private final TestFunc intTest = this::testAsInt;
        private final CharSequenceHashSet set;
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
                        intSet.add(symbolTable.keyOf(func.getStrA(null)));
                    }
                }
                testFunc = intTest;
            } else {
                if (deferredValues != null) {
                    deferredSet.clear();
                    for (int i = 0, n = deferredValues.size(); i < n; i++) {
                        final Function func = deferredValues.getQuick(i);
                        deferredSet.add(func.getStrA(null));
                    }
                }
                testFunc = strTest;
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
