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

package io.questdb.griffin.engine.functions.eq;

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
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.*;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

/**
 * Equality operator between symbol and long value. Long value is cast to string representation before being
 * compared to the symbol.
 */
public class EqSymLongFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(KL)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(
            int position, ObjList<Function> args,
            IntList argPositions, CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {

        Function fn0 = args.getQuick(0);
        Function longFn = args.getQuick(1);

        // we get this because of overload. We may end up handling NULL type instead of symbol
        if (ColumnType.isNull(fn0.getType())) {
            if (longFn.isConstant()) {
                return longFn.getLong(null) == Numbers.LONG_NULL ? BooleanConstant.TRUE : BooleanConstant.FALSE;
            }
            return new EqLongNullFunction(longFn);
        }

        final SymbolFunction symFn = (SymbolFunction) fn0;
        if (longFn.isConstant()) {
            final long val = longFn.getLong(null);
            final String constValue = val != Numbers.LONG_NULL ? Misc.getThreadLocalSink().put(val).toString() : null;
            if (symFn.getStaticSymbolTable() != null) {
                return new ConstValueStaticSymbolTableFunction(symFn, constValue);
            } else {
                return new ConstValueDynamicSymbolTableFunction(symFn, constValue);
            }
        }

        return new VariableValueFunction(symFn, longFn);
    }

    private static class ConstValueDynamicSymbolTableFunction extends NegatableBooleanFunction implements UnaryFunction {
        private final Function arg;
        private final String constant;

        public ConstValueDynamicSymbolTableFunction(Function arg, @Nullable String constant) {
            this.arg = arg;
            this.constant = constant;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != Chars.equalsNullable(arg.getSymbol(rec), constant);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
            if (negated) {
                sink.val('!');
            }
            sink.val("='").val(constant).val('\'');
        }
    }

    private static class ConstValueStaticSymbolTableFunction extends NegatableBooleanFunction implements UnaryFunction {
        private final SymbolFunction arg;
        private final String constant;
        private int valueIndex;

        public ConstValueStaticSymbolTableFunction(SymbolFunction arg, @Nullable String constant) {
            this.arg = arg;
            this.constant = constant;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (arg.getInt(rec) == valueIndex);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            final StaticSymbolTable symbolTable = arg.getStaticSymbolTable();
            assert symbolTable != null;
            valueIndex = symbolTable.keyOf(constant);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
            if (negated) {
                sink.val('!');
            }
            sink.val("='").val(constant).val('\'');
        }
    }

    private static class EqLongNullFunction extends NegatableBooleanFunction implements UnaryFunction {
        private final Function longFn;

        public EqLongNullFunction(Function longFn) {
            this.longFn = longFn;
        }

        @Override
        public Function getArg() {
            return longFn;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated == (longFn.getLong(rec) != Numbers.LONG_NULL);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(longFn);
            if (negated) {
                sink.val('!');
            }
            sink.val("=").val("null");
        }
    }

    private static class VariableValueFunction extends AbstractEqBinaryFunction {
        private final StringSink sink = new StringSink();

        public VariableValueFunction(Function symFn, Function longFn) {
            super(symFn, longFn);
        }

        @Override
        public boolean getBool(Record rec) {
            long value = right.getLong(rec);
            if (value != Numbers.LONG_NULL) {
                sink.clear();
                sink.put(value);
                return negated != Chars.equalsNc(sink, left.getSymbol(rec));
            }
            return negated == (left.getSymbol(rec) != null);
        }
    }
}
