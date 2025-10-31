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
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Utf8SequenceHashSet;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;

public class InVarcharFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "in(Øv)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final int n = args.size();
        if (n == 1) {
            return BooleanConstant.FALSE;
        }

        boolean allConst = true;
        for (int i = 1; i < n; i++) {
            Function func = args.getQuick(i);
            switch (ColumnType.tagOf(func.getType())) {
                case ColumnType.NULL:
                case ColumnType.STRING:
                case ColumnType.VARCHAR:
                case ColumnType.SYMBOL:
                case ColumnType.CHAR:
                case ColumnType.UNDEFINED:
                    break;
                default:
                    throw SqlException.position(argPositions.getQuick(i)).put("cannot compare VARCHAR with type ").put(ColumnType.nameOf(func.getType()));
            }

            if (!func.isConstant()) {
                allConst = false;

                if (!func.isRuntimeConstant()) {
                    // we should never get here because
                    // FunctionParser rejects the SQL if the expression is not constant or runtime constant
                    throw SqlException.position(argPositions.getQuick(i)).put("unsupported expression");
                }
            }
        }

        if (allConst) {
            final Utf8SequenceHashSet set = new Utf8SequenceHashSet();
            parseToVarchar(args, argPositions, set);

            final Function arg = args.getQuick(0);
            if (arg.isConstant()) {
                return BooleanConstant.of(set.contains(arg.getVarcharA(null)));
            }
            return new ConstFunc(arg, set);
        }
        final IntList positions = new IntList();
        positions.addAll(argPositions);
        return new RuntimeConstFunc(new ObjList<>(args), positions);
    }

    private static void parseToVarchar(ObjList<Function> args, IntList argPositions, Utf8SequenceHashSet set) throws SqlException {
        set.clear();
        final int n = args.size();
        for (int i = 1; i < n; i++) {
            Function func = args.getQuick(i);
            switch (ColumnType.tagOf(func.getType())) {
                case ColumnType.NULL:
                case ColumnType.STRING:
                case ColumnType.VARCHAR:
                case ColumnType.SYMBOL:
                    set.add(Utf8s.toUtf8String(func.getVarcharA(null)));
                    break;
                case ColumnType.CHAR:
                    set.add(new Utf8String(func.getChar(null)));
                    break;
                default:
                    throw SqlException.position(argPositions.getQuick(i)).put("cannot compare VARCHAR with type ").put(ColumnType.nameOf(func.getType()));
            }
        }
    }

    private static class ConstFunc extends BooleanFunction implements UnaryFunction {
        private final Function arg;
        private final Utf8SequenceHashSet set;

        public ConstFunc(Function arg, Utf8SequenceHashSet set) {
            this.arg = arg;
            this.set = set;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            final Utf8Sequence val = arg.getVarcharA(rec);
            return set.contains(val);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val(" in ").val(set);
        }
    }

    private static class RuntimeConstFunc extends BooleanFunction implements MultiArgFunction {
        private final IntList argPositions;
        private final ObjList<Function> args;
        private final Utf8SequenceHashSet set = new Utf8SequenceHashSet();

        public RuntimeConstFunc(ObjList<Function> args, IntList argPositions) {
            this.args = args;
            this.argPositions = argPositions;
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public boolean getBool(Record rec) {
            final Utf8Sequence val = args.getQuick(0).getVarcharA(rec);
            return set.contains(val);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            MultiArgFunction.super.init(symbolTableSource, executionContext);
            parseToVarchar(args, argPositions, set);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(args.getQuick(0)).val(" in ").val(args, 1);
        }
    }
}
