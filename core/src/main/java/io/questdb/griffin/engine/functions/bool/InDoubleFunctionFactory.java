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

import io.questdb.cairo.BinarySearch;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.*;

public class InDoubleFunctionFactory implements FunctionFactory {
    public static double tryParseDouble(CharSequence seq, int position) throws SqlException {
        try {
            return Numbers.parseDouble(seq);
        } catch (NumericException e) {
            throw SqlException.position(position).put("invalid DOUBLE value [").put(seq).put(']');
        }
    }

    @Override
    public String getSignature() {
        return "in(DV)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        int constCount = 0;
        int runtimeConstCount = 0;
        final int argCount = args.size() - 1;
        for (int i = 1, n = args.size(); i < n; i++) {
            Function func = args.getQuick(i);
            switch (ColumnType.tagOf(func.getType())) {
                case ColumnType.NULL:
                case ColumnType.BYTE:
                case ColumnType.SHORT:
                case ColumnType.INT:
                case ColumnType.LONG:
                case ColumnType.FLOAT:
                case ColumnType.DOUBLE:
                case ColumnType.STRING:
                case ColumnType.SYMBOL:
                case ColumnType.VARCHAR:
                    // allow undefined bind variables to be defined at the cursor creation time
                case ColumnType.UNDEFINED:
                    break;
                default:
                    throw SqlException.position(argPositions.getQuick(i)).put("cannot compare DOUBLE with type ").put(ColumnType.nameOf(func.getType()));
            }

            if (func.isConstant()) {
                constCount++;
            }

            if (func.isRuntimeConstant()) {
                runtimeConstCount++;
            }
        }

        if (constCount == argCount) {
            // bind variable will not be constant
            DoubleList values = new DoubleList(args.size() - 1);
            parseToDouble(args, argPositions, values);
            return new InDoubleConstFunction(args.getQuick(0), values);
        }

        if (runtimeConstCount == argCount || runtimeConstCount + constCount == argCount) {
            return new InDoubleRuntimeConstFunction(args.getQuick(0), args, argPositions);
        }

        // have to copy, args is mutable
        return new InDoubleVarFunction(new ObjList<>(args));
    }

    private static void parseToDouble(
            ObjList<Function> args,
            IntList argPositions,
            DoubleList outDoubleList
    ) throws SqlException {
        outDoubleList.extendAndSet(args.size() - 2, 0);
        for (int i = 1, n = args.size(); i < n; i++) {
            outDoubleList.setQuick(i - 1, parseValue(argPositions, args.getQuick(i), i));
        }
        outDoubleList.sort();
    }

    private static double parseValue(IntList argPositions, Function func, int i) throws SqlException {
        double val;
        switch (ColumnType.tagOf(func.getType())) {
            case ColumnType.BYTE:
            case ColumnType.SHORT:
            case ColumnType.INT:
            case ColumnType.LONG:
            case ColumnType.FLOAT:
            case ColumnType.DOUBLE:
                val = func.getDouble(null);
                break;
            case ColumnType.STRING:
            case ColumnType.SYMBOL:
            case ColumnType.VARCHAR:
            case ColumnType.NULL:
                CharSequence tsValue = func.getStrA(null);
                val = (tsValue != null) ? tryParseDouble(tsValue, argPositions.getQuick(i)) : Double.NaN;
                break;
            default:
                throw SqlException.inconvertibleTypes(
                        argPositions.getQuick(i),
                        func.getType(),
                        ColumnType.nameOf(func.getType()),
                        ColumnType.DOUBLE,
                        ColumnType.nameOf(ColumnType.DOUBLE)
                );
        }
        return val;
    }

    private static class InDoubleConstFunction extends NegatableBooleanFunction implements UnaryFunction {
        private final Function func;
        private final DoubleList inList;

        public InDoubleConstFunction(Function func, DoubleList doubleList) {
            this.func = func;
            this.inList = doubleList;
        }

        @Override
        public Function getArg() {
            return func;
        }

        @Override
        public boolean getBool(Record rec) {
            double val = func.getDouble(rec);
            return negated != inList.binarySearch(val, BinarySearch.SCAN_UP) >= 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(func);
            if (negated) {
                sink.val(" not");
            }
            sink.val(" in ").val(inList);
        }
    }

    private static class InDoubleRuntimeConstFunction extends NegatableBooleanFunction implements UnaryFunction {
        private final Function keyFunction;
        private final IntList valueFunctionPositions;
        private final ObjList<Function> valueFunctions;
        private final DoubleList values;

        public InDoubleRuntimeConstFunction(Function keyFunction, ObjList<Function> valueFunctions, IntList valueFunctionPositions) {
            this.keyFunction = keyFunction;
            // value functions also contain key function at 0 index.
            this.valueFunctions = valueFunctions;
            this.valueFunctionPositions = valueFunctionPositions;
            this.values = new DoubleList(valueFunctions.size());

        }

        @Override
        public Function getArg() {
            return keyFunction;
        }

        @Override
        public boolean getBool(Record rec) {
            double val = keyFunction.getDouble(rec);
            return negated != values.binarySearch(val, BinarySearch.SCAN_UP) >= 0;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            for (int i = 1, n = valueFunctions.size(); i < n; i++) {
                Function valueFunction = valueFunctions.getQuick(i);
                valueFunction.init(symbolTableSource, executionContext);
            }
            values.clear();
            parseToDouble(valueFunctions, valueFunctionPositions, values);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(keyFunction);
            if (negated) {
                sink.val(" not");
            }
            sink.val(" in ").val(values);
        }
    }

    private static class InDoubleVarFunction extends NegatableBooleanFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        public InDoubleVarFunction(ObjList<Function> args) {
            this.args = args;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public boolean getBool(Record rec) {
            double argVal = args.getQuick(0).getDouble(rec);

            for (int i = 1, n = args.size(); i < n; i++) {
                Function func = args.getQuick(i);
                double val = Double.NaN;
                switch (ColumnType.tagOf(func.getType())) {
                    case ColumnType.BYTE:
                    case ColumnType.SHORT:
                    case ColumnType.INT:
                    case ColumnType.LONG:
                    case ColumnType.FLOAT:
                    case ColumnType.DOUBLE:
                        val = func.getDouble(rec);
                        break;
                    case ColumnType.STRING:
                    case ColumnType.SYMBOL:
                    case ColumnType.VARCHAR:
                        val = Numbers.parseDoubleQuiet(func.getStrA(rec));
                        break;
                }
                if (Numbers.equals(val, argVal)) {
                    return !negated;
                }
            }
            return negated;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(args.getQuick(0));
            if (negated) {
                sink.val(" not");
            }
            sink.val(" in ");
            sink.val(args, 1);
        }
    }
}
