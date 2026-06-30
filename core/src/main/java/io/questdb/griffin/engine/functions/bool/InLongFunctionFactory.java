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
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.DirectLongHashSet;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Utf8Sequence;

public class InLongFunctionFactory implements FunctionFactory {

    public static long tryParseLong(CharSequence seq, int position) throws SqlException {
        try {
            return Numbers.parseLong(seq, 0, seq.length());
        } catch (NumericException e) {
            throw SqlException.position(position).put("invalid LONG value [").put(seq).put(']');
        }
    }

    @Override
    public String getSignature() {
        return "in(LV)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        int constCount = 0;
        int runtimeConstCount = 0;
        final int argCount = args.size() - 1;
        // When the key column (arg 0) is a narrow integer (INT/SHORT/BYTE), the value
        // list is read at INT width so an overflowing INT arithmetic fold wraps exactly
        // as '=' (EqInt) does, instead of widening to its full-width product via
        // getLong(). For a LONG/TIMESTAMP key the elements still widen to long.
        final boolean keyIsNarrowInt = isNarrowInt(ColumnType.tagOf(args.getQuick(0).getType()));
        for (int i = 1, n = args.size(); i < n; i++) {
            Function func = args.getQuick(i);
            switch (ColumnType.tagOf(func.getType())) {
                case ColumnType.NULL:
                case ColumnType.TIMESTAMP:
                case ColumnType.LONG:
                case ColumnType.INT:
                case ColumnType.SHORT:
                case ColumnType.BYTE:
                case ColumnType.STRING:
                case ColumnType.SYMBOL:
                case ColumnType.VARCHAR:
                case ColumnType.UNDEFINED:
                    break;
                default:
                    throw SqlException.position(argPositions.get(i)).put("cannot compare LONG with type ").put(ColumnType.nameOf(func.getType()));
            }
            if (func.isConstant()) {
                constCount++;
            }

            if (func.isRuntimeConstant()) {
                runtimeConstCount++;
            }
        }

        if (constCount == argCount) {
            switch (argCount) {
                case 1:
                    return new InLongSingleConstFunction(
                            args.getQuick(0),
                            parseValue(argPositions, args.getQuick(1),
                                    1, keyIsNarrowInt),
                            keyIsNarrowInt);
                case 2:
                    return new InLongTwoConstFunction(
                            args.getQuick(0),
                            parseValue(
                                    argPositions,
                                    args.getQuick(1),
                                    1, keyIsNarrowInt),
                            parseValue(
                                    argPositions,
                                    args.getQuick(2),
                                    2, keyIsNarrowInt),
                            keyIsNarrowInt
                    );
                default:
                    DirectLongHashSet inVals = new DirectLongHashSet(argCount, MemoryTag.NATIVE_FUNC_RSS);
                    try {
                        parseToLong(args, argPositions, inVals, keyIsNarrowInt);
                        return new InLongConstFunction(args.getQuick(0), inVals, keyIsNarrowInt);
                    } catch (Throwable e) {
                        Misc.free(inVals);
                        throw e;
                    }
            }
        }

        if (runtimeConstCount + constCount == argCount) {
            final IntList positions = new IntList();
            positions.addAll(argPositions);
            return new InLongRuntimeConstFunction(args.getQuick(0), new ObjList<>(args), positions, keyIsNarrowInt);
        }

        // have to copy, args is mutable
        return new InLongVarFunction(new ObjList<>(args), keyIsNarrowInt);
    }

    private static boolean isNarrowInt(int typeTag) {
        return typeTag == ColumnType.BYTE || typeTag == ColumnType.SHORT || typeTag == ColumnType.INT;
    }

    private static void parseToLong(
            ObjList<Function> args,
            IntList argPositions,
            DirectLongHashSet outLongSet,
            boolean keyIsNarrowInt
    ) throws SqlException {
        for (int i = 1, n = args.size(); i < n; i++) {
            outLongSet.add(parseValue(argPositions, args.getQuick(i), i, keyIsNarrowInt));
        }
    }

    private static long parseValue(IntList argPositions, Function func, int i, boolean keyIsNarrowInt) throws SqlException {
        long val;
        switch (ColumnType.tagOf(func.getType())) {
            case ColumnType.INT:
            case ColumnType.SHORT:
            case ColumnType.BYTE:
                // Match '=' on a narrow-integer key: read the element at INT width so an
                // overflowing INT arithmetic wraps (getInt) instead of widening (getLong).
                // intToLong preserves a genuine INT_NULL element as LONG_NULL.
                val = keyIsNarrowInt ? Numbers.intToLong(func.getInt(null)) : func.getLong(null);
                break;
            case ColumnType.TIMESTAMP:
            case ColumnType.LONG:
                val = func.getLong(null);
                break;
            case ColumnType.STRING:
            case ColumnType.SYMBOL:
            case ColumnType.NULL:
                CharSequence tsValue = func.getStrA(null);
                val = (tsValue != null) ? tryParseLong(tsValue, argPositions.getQuick(i)) : Numbers.LONG_NULL;
                break;
            case ColumnType.VARCHAR:
                Utf8Sequence seq = func.getVarcharA(null);
                val = (seq != null) ? tryParseLong(seq.asAsciiCharSequence(), argPositions.getQuick(i)) : Numbers.LONG_NULL;
                break;
            default:
                throw SqlException.inconvertibleTypes(
                        argPositions.getQuick(i),
                        func.getType(),
                        ColumnType.nameOf(func.getType()),
                        ColumnType.LONG,
                        ColumnType.nameOf(ColumnType.LONG)
                );
        }
        return val;
    }

    private static class InLongConstFunction extends NegatableBooleanFunction implements UnaryFunction {
        private final DirectLongHashSet inSet;
        private final boolean keyIsNarrowInt;
        private final Function tsFunc;

        public InLongConstFunction(Function tsFunc, DirectLongHashSet inSet, boolean keyIsNarrowInt) {
            this.tsFunc = tsFunc;
            this.inSet = inSet;
            this.keyIsNarrowInt = keyIsNarrowInt;
        }

        @Override
        public void close() {
            UnaryFunction.super.close();
            Misc.free(inSet);
        }

        @Override
        public Function getArg() {
            return tsFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            // A narrow-int key reads at INT width so an overflowing INT arithmetic wraps (getInt),
            // symmetric with the IN-list elements and matching '=' (EqInt) and the JIT filter.
            long val = keyIsNarrowInt ? Numbers.intToLong(tsFunc.getInt(rec)) : tsFunc.getLong(rec);
            return negated != inSet.contains(val);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(tsFunc);
            if (negated) {
                sink.val(" not");
            }
            sink.val(" in ").val(inSet);
        }
    }

    private static class InLongRuntimeConstFunction extends NegatableBooleanFunction implements MultiArgFunction {
        private final DirectLongHashSet inSet;
        private final boolean keyIsNarrowInt;
        private final Function keyFunc;
        private final IntList valueFunctionPositions;
        private final ObjList<Function> valueFunctions;

        public InLongRuntimeConstFunction(Function keyFunc, ObjList<Function> valueFunctions, IntList valueFunctionPositions, boolean keyIsNarrowInt) {
            this.keyFunc = keyFunc;
            // value functions also contain key function at 0 index.
            this.valueFunctions = valueFunctions;
            this.valueFunctionPositions = valueFunctionPositions;
            this.keyIsNarrowInt = keyIsNarrowInt;
            this.inSet = new DirectLongHashSet(valueFunctions.size() - 1, MemoryTag.NATIVE_FUNC_RSS);
        }

        @Override
        public ObjList<Function> args() {
            return valueFunctions;
        }

        @Override
        public void close() {
            MultiArgFunction.super.close();
            Misc.free(inSet);
        }

        @Override
        public boolean getBool(Record rec) {
            // A narrow-int key reads at INT width so an overflowing INT arithmetic wraps (getInt),
            // symmetric with the IN-list elements and matching '=' (EqInt) and the JIT filter.
            long val = keyIsNarrowInt ? Numbers.intToLong(keyFunc.getInt(rec)) : keyFunc.getLong(rec);
            return negated != inSet.contains(val);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            MultiArgFunction.super.init(symbolTableSource, executionContext);
            inSet.clear();
            parseToLong(valueFunctions, valueFunctionPositions, inSet, keyIsNarrowInt);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(keyFunc);
            if (negated) {
                sink.val(" not");
            }
            sink.val(" in ").val(inSet);
        }
    }

    private static class InLongSingleConstFunction extends NegatableBooleanFunction implements UnaryFunction {
        private final long inVal;
        private final boolean keyIsNarrowInt;
        private final Function longFunc;

        public InLongSingleConstFunction(Function longFunc, long inVal, boolean keyIsNarrowInt) {
            this.longFunc = longFunc;
            this.inVal = inVal;
            this.keyIsNarrowInt = keyIsNarrowInt;
        }

        @Override
        public Function getArg() {
            return longFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            // A narrow-int key reads at INT width so an overflowing INT arithmetic wraps (getInt),
            // symmetric with the IN-list element and matching '=' (EqInt) and the JIT filter.
            long val = keyIsNarrowInt ? Numbers.intToLong(longFunc.getInt(rec)) : longFunc.getLong(rec);
            return negated != (val == inVal);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(longFunc);
            if (negated) {
                sink.val(" not");
            }
            sink.val(" in [").val(inVal).val(']');
        }
    }

    private static class InLongTwoConstFunction extends NegatableBooleanFunction implements UnaryFunction {
        private final long inVal0;
        private final long inVal1;
        private final boolean keyIsNarrowInt;
        private final Function longFunc;

        public InLongTwoConstFunction(Function longFunc, long inVal0, long inVal1, boolean keyIsNarrowInt) {
            this.longFunc = longFunc;
            this.inVal0 = inVal0;
            this.inVal1 = inVal1;
            this.keyIsNarrowInt = keyIsNarrowInt;
        }

        @Override
        public Function getArg() {
            return longFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            // A narrow-int key reads at INT width so an overflowing INT arithmetic wraps (getInt),
            // symmetric with the IN-list elements and matching '=' (EqInt) and the JIT filter.
            long val = keyIsNarrowInt ? Numbers.intToLong(longFunc.getInt(rec)) : longFunc.getLong(rec);
            return negated != (val == inVal0 || val == inVal1);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(longFunc);
            if (negated) {
                sink.val(" not");
            }
            sink.val(" in [").val(inVal0).val(',').val(inVal1).val(']');
        }
    }

    private static class InLongVarFunction extends NegatableBooleanFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final boolean keyIsNarrowInt;

        public InLongVarFunction(ObjList<Function> args, boolean keyIsNarrowInt) {
            this.args = args;
            this.keyIsNarrowInt = keyIsNarrowInt;
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public boolean getBool(Record rec) {
            // A narrow-int key reads at INT width so an overflowing INT arithmetic wraps (getInt),
            // symmetric with the IN-list elements and matching '=' (EqInt) and the JIT filter.
            long val = keyIsNarrowInt ? Numbers.intToLong(args.getQuick(0).getInt(rec)) : args.getQuick(0).getLong(rec);

            for (int i = 1, n = args.size(); i < n; i++) {
                Function func = args.getQuick(i);
                long inVal = Numbers.LONG_NULL;
                switch (ColumnType.tagOf(func.getType())) {
                    case ColumnType.BYTE:
                    case ColumnType.SHORT:
                    case ColumnType.INT:
                        // Match '=' on a narrow-integer key: read the element at INT width
                        // (wrap) rather than widening an overflowing INT arithmetic via getLong().
                        inVal = keyIsNarrowInt ? Numbers.intToLong(func.getInt(rec)) : func.getLong(rec);
                        break;
                    case ColumnType.LONG:
                    case ColumnType.TIMESTAMP:
                        inVal = func.getLong(rec);
                        break;
                    case ColumnType.STRING:
                    case ColumnType.SYMBOL:
                        CharSequence str = func.getStrA(rec);
                        inVal = Numbers.parseLongQuiet(str);
                        break;
                    case ColumnType.VARCHAR:
                        Utf8Sequence seq = func.getVarcharA(rec);
                        CharSequence cs = seq == null ? null : seq.asAsciiCharSequence();
                        inVal = Numbers.parseLongQuiet(cs);
                        break;
                }
                if (inVal == val) {
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

