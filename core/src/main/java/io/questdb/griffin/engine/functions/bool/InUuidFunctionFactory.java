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
import io.questdb.std.LongLongHashSet;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.Uuid;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

public final class InUuidFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "in(Zv)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final int argCount = args.size() - 1;
        int n = args.size();

        int constCount = 0;
        for (int i = 1; i < n; i++) {
            Function func = args.getQuick(i);
            switch (func.getType()) {
                case ColumnType.NULL:
                case ColumnType.VARCHAR:
                case ColumnType.STRING:
                case ColumnType.SYMBOL:
                case ColumnType.UUID:
                case ColumnType.UNDEFINED: // bind variable
                    break;
                default:
                    throw SqlException.position(argPositions.getQuick(i)).put("cannot compare UUID with type ").put(ColumnType.nameOf(func.getType()));
            }

            if (func.isConstant()) {
                constCount++;
            }

            if (!func.isRuntimeConstant() && !func.isConstant()) {
                throw SqlException.$(argPositions.getQuick(i), "constant or bind variable expected");
            }
        }

        if (constCount == argCount) {
            LongLongHashSet set = makeUUIDHashSet(argCount);
            for (int i = 1; i < n; i++) {
                addUUIDToSet(args.getQuick(i), argPositions.getQuick(i), set);
            }

            // eliminate outright constant expressions
            Function keyFunc = args.getQuick(0);
            if (keyFunc.isConstant()) {
                long lo = keyFunc.getLong128Lo(null);
                long hi = keyFunc.getLong128Hi(null);
                if (Uuid.isNull(lo, hi)) {
                    return BooleanConstant.FALSE;
                }
                return BooleanConstant.of(set.contains(lo, hi));
            }
            return new InUUIDConstFunction(keyFunc, set);
        }

        final IntList positions = new IntList();
        positions.addAll(argPositions);
        return new InUUIDRuntimeConstFunction(args.getQuick(0), new ObjList<>(args), positions, makeUUIDHashSet(argCount));
    }

    private static void addUUIDToSet(Function func, int argPosition, LongLongHashSet set) throws SqlException {
        switch (ColumnType.tagOf(func.getType())) {
            case ColumnType.VARCHAR:
                Utf8Sequence value2 = func.getVarcharA(null);
                if (value2 == null) {
                    set.addNull();
                } else {
                    try {
                        Uuid.checkDashesAndLength(value2);
                        set.add(Uuid.parseLo(value2, 0), Uuid.parseHi(value2, 0));
                    } catch (NumericException e) {
                        throw SqlException.$(argPosition, "invalid UUID value [").put(value2).put(']');
                    }
                }
                break;
            case ColumnType.NULL:
            case ColumnType.STRING:
            case ColumnType.SYMBOL:
                CharSequence value = func.getStrA(null);
                if (value == null) {
                    set.addNull();
                } else {
                    try {
                        Uuid.checkDashesAndLength(value);
                        set.add(Uuid.parseLo(value), Uuid.parseHi(value));
                    } catch (NumericException e) {
                        throw SqlException.$(argPosition, "invalid UUID value [").put(value).put(']');
                    }
                }
                break;
            case ColumnType.UUID:
                long lo = func.getLong128Lo(null);
                long hi = func.getLong128Hi(null);
                if (hi == Numbers.LONG_NULL && lo == Numbers.LONG_NULL) {
                    throw SqlException.$(argPosition, "NULL is not allowed in IN list");
                }
                set.add(lo, hi);
                break;
            default:
                throw SqlException.inconvertibleTypes(
                        argPosition,
                        func.getType(),
                        ColumnType.nameOf(func.getType()),
                        ColumnType.UUID,
                        ColumnType.nameOf(ColumnType.UUID)
                );
        }
    }

    private static @NotNull LongLongHashSet makeUUIDHashSet(int argCount) {
        return new LongLongHashSet(argCount, 0.6, Numbers.LONG_NULL, LongLongHashSet.UUID_STRATEGY);
    }

    private static class InUUIDConstFunction extends BooleanFunction implements UnaryFunction {
        private final Function arg;
        private final LongLongHashSet set;

        public InUUIDConstFunction(Function arg, LongLongHashSet set) {
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
                return set.hasNull();
            }
            return set.contains(lo, hi);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val(" in ").val(set);
        }
    }

    private static class InUUIDRuntimeConstFunction extends BooleanFunction implements MultiArgFunction {
        private final Function keyFunc;
        private final LongLongHashSet set;
        private final IntList valueFunctionPositions;
        private final ObjList<Function> valueFunctions;

        public InUUIDRuntimeConstFunction(
                Function keyFunc,
                ObjList<Function> valueFunctions,
                IntList valueFunctionPositions,
                LongLongHashSet set
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
            long lo = keyFunc.getLong128Lo(rec);
            long hi = keyFunc.getLong128Hi(rec);
            if (Uuid.isNull(lo, hi)) {
                return set.hasNull();
            }
            return set.contains(lo, hi);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            MultiArgFunction.super.init(symbolTableSource, executionContext);
            set.clear();
            for (int i = 1, n = valueFunctions.size(); i < n; i++) {
                addUUIDToSet(valueFunctions.getQuick(i), valueFunctionPositions.getQuick(i), set);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(keyFunc).val(" in ").val(set);
        }
    }
}
