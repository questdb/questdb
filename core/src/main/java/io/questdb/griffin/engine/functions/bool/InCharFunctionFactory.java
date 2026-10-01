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
        int n = args.size();
        if (n == 1) {
            return BooleanConstant.FALSE;
        }

        // Validate each list element. Compile-time constants are folded into
        // the set immediately so a bad-shaped literal (e.g. multi-char string)
        // is reported at its own position. Runtime-constants (bind variables
        // wrapped in ::CHAR / ::STRING / ::VARCHAR) are deferred to init() so
        // we don't read their value before they are bound.
        IntHashSet set = new IntHashSet();
        ObjList<Function> deferredValues = null;
        IntList deferredValuePositions = null;
        for (int i = 1; i < n; i++) {
            Function func = args.getQuick(i);
            if (!ColumnType.isChar(func.getType())
                    && !ColumnType.isString(func.getType())
                    && !ColumnType.isVarchar(func.getType())
                    && !ColumnType.isNull(func.getType())) {
                throw SqlException.$(argPositions.getQuick(i), "CHAR constant expected");
            }
            if (!func.isConstant() && func.isRuntimeConstant()) {
                if (deferredValues == null) {
                    deferredValues = new ObjList<>();
                    deferredValuePositions = new IntList();
                }
                deferredValues.add(func);
                deferredValuePositions.add(argPositions.getQuick(i));
                continue;
            }
            addCharToSet(set, func, argPositions.getQuick(i));
        }

        if (deferredValues == null) {
            Function var = args.getQuick(0);
            if (var.isConstant()) {
                return BooleanConstant.of(set.contains(var.getChar(null)));
            }
            return new InCharConstFunction(var, set);
        }

        return new InCharRuntimeConstFunction(args.getQuick(0), set, deferredValues, deferredValuePositions);
    }

    private static void addCharToSet(IntHashSet set, Function func, int position) throws SqlException {
        if (ColumnType.isChar(func.getType())) {
            set.add(func.getChar(null));
        } else if (ColumnType.isString(func.getType())) {
            CharSequence cs = func.getStrA(null);
            if (cs == null || cs.isEmpty()) {
                set.add(CharConstant.ZERO.getChar(null));
            } else if (cs.length() == 1) {
                set.add(cs.charAt(0));
            } else {
                throw SqlException.$(position, "CHAR constant expected");
            }
        } else if (ColumnType.isVarchar(func.getType())) {
            Utf8Sequence seq = func.getVarcharA(null);
            if (seq == null || seq.size() == 0) {
                set.add(CharConstant.ZERO.getChar(null));
            } else if (seq.size() == 1) {
                set.add((char) (seq.byteAt(0) & 0xFF));
            } else {
                throw SqlException.$(position, "CHAR constant expected");
            }
        } else if (ColumnType.isNull(func.getType())) {
            // CHAR has no NULL bit pattern; CHAR-zero is the agreed sentinel
            // for "absent", matching the eager STRING-empty / VARCHAR-empty
            // handling above and the CHAR null marker used elsewhere.
            set.add(CharConstant.ZERO.getChar(null));
        } else {
            throw SqlException.$(position, "CHAR constant expected");
        }
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

    private static class InCharRuntimeConstFunction extends BooleanFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final IntList deferredPositions;
        private final ObjList<Function> deferredValues;
        private final IntHashSet literalSet;
        private final IntHashSet runtimeSet = new IntHashSet();

        public InCharRuntimeConstFunction(
                Function arg,
                IntHashSet literalSet,
                ObjList<Function> deferredValues,
                IntList deferredPositions
        ) {
            this.literalSet = literalSet;
            this.deferredValues = deferredValues;
            this.deferredPositions = deferredPositions;
            this.args = new ObjList<>(deferredValues.size() + 1);
            this.args.add(arg);
            this.args.addAll(deferredValues);
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public boolean getBool(Record rec) {
            char value = args.getQuick(0).getChar(rec);
            return literalSet.contains(value) || runtimeSet.contains(value);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            MultiArgFunction.super.init(symbolTableSource, executionContext);
            runtimeSet.clear();
            for (int i = 0, n = deferredValues.size(); i < n; i++) {
                addCharToSet(runtimeSet, deferredValues.getQuick(i), deferredPositions.getQuick(i));
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(args.getQuick(0)).val(" in ").val(literalSet);
            if (deferredValues.size() > 0) {
                sink.val(" or ").val(args.getQuick(0)).val(" in ").val(deferredValues);
            }
        }
    }
}
