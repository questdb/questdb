/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class InStrFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "in(Sv)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        int n = args.size();
        if (n == 1) {
            return BooleanConstant.FALSE;
        }
        ObjList<Function> deferredValueFunctions = null;
        IntList deferredValuePositions = null;

        final CharSequenceHashSet valueSet = new CharSequenceHashSet();
        for (int i = 1; i < n; i++) {
            Function func = args.getQuick(i);
            switch (ColumnType.tagOf(func.getType())) {
                case ColumnType.NULL:
                case ColumnType.STRING:
                case ColumnType.VARCHAR:
                case ColumnType.SYMBOL:
                    // the "undefined" case hinges on the fact it is "runtime constant" and that
                    // all undefined bind variable would end up being deferred.
                case ColumnType.UNDEFINED:
                    if (func.isRuntimeConstant()) { // bind variables
                        if (deferredValueFunctions == null) {
                            deferredValueFunctions = new ObjList<>();
                            deferredValuePositions = new IntList();
                        }
                        deferredValueFunctions.add(func);
                        deferredValuePositions.add(argPositions.getQuick(i));
                        continue;
                    }
                    CharSequence value = func.getStrA(null);
                    if (value == null) {
                        valueSet.addNull();
                    }
                    valueSet.add(Chars.toString(value));
                    break;
                case ColumnType.CHAR:
                    valueSet.add(String.valueOf(func.getChar(null)));
                    break;
                default:
                    throw SqlException.$(argPositions.getQuick(i), "STRING constant expected");
            }
        }
        final Function var = args.getQuick(0);
        if (var.isConstant() && deferredValueFunctions == null) {
            return BooleanConstant.of(valueSet.contains(var.getStrA(null)));
        }
        return new InStrFunction(var, valueSet, deferredValueFunctions, deferredValuePositions);
    }

    private static class InStrFunction extends BooleanFunction implements UnaryFunction {
        private final Function arg;
        private final CharSequenceHashSet deferredSet;
        private final ObjList<Function> deferredValueFunctions;
        private final IntList deferredValuePositions;
        private final CharSequenceHashSet valueSet;

        public InStrFunction(Function arg, CharSequenceHashSet valueSet, ObjList<Function> deferredValueFunctions, IntList deferredValuePositions) {
            this.arg = arg;
            this.valueSet = valueSet;
            this.deferredValueFunctions = deferredValueFunctions;
            this.deferredSet = deferredValueFunctions != null ? new CharSequenceHashSet() : null;
            this.deferredValuePositions = deferredValuePositions;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            CharSequence val = arg.getStrA(rec);
            return valueSet.contains(val)
                    || (deferredSet != null && deferredSet.contains(val));
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            if (deferredValueFunctions != null) {
                deferredSet.clear();
                for (int i = 0, n = deferredValueFunctions.size(); i < n; i++) {
                    Function func = deferredValueFunctions.getQuick(i);
                    func.init(symbolTableSource, executionContext);
                    // check the supported types, these types may mutate as bind variables get redefined
                    switch (func.getType()) {
                        case ColumnType.STRING:
                        case ColumnType.CHAR:
                        case ColumnType.VARCHAR:
                            deferredSet.add(func.getStrA(null));
                            break;
                        default:
                            throw SqlException.inconvertibleTypes(
                                    deferredValuePositions.getQuick(i),
                                    func.getType(),
                                    ColumnType.nameOf(func.getType()),
                                    ColumnType.TIMESTAMP,
                                    ColumnType.nameOf(ColumnType.TIMESTAMP)
                            );
                    }
                }
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            if (deferredValueFunctions != null) {
                sink.val('(');
            }
            sink.val(arg).val(" in ").val(valueSet);
            if (deferredValueFunctions != null) {
                sink.val(" or ").val(arg).val(" in ").val(deferredValueFunctions).val(')');
            }
        }
    }
}
