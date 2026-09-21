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
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Utf8Sequence;

public class InLongFunctionFactory implements FunctionFactory {

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
                case 1: {
                    final long v = parseValue(argPositions, args.getQuick(1), 1);
                    final Function fn = new InLongSingleConstFunction(args.getQuick(0), v);
                    freeElements(args);
                    return fn;
                }
                case 2: {
                    final long v0 = parseValue(argPositions, args.getQuick(1), 1);
                    final long v1 = parseValue(argPositions, args.getQuick(2), 2);
                    final Function fn = new InLongTwoConstFunction(args.getQuick(0), v0, v1);
                    freeElements(args);
                    return fn;
                }
                default:
                    DirectLongHashSet vals = null;
                    try {
                        vals = new DirectLongHashSet(argCount, MemoryTag.NATIVE_FUNC_RSS);
                        parseToSet(args, argPositions, vals);
                        final Function fn = new InLongConstFunction(args.getQuick(0), vals);
                        freeElements(args);
                        return fn;
                    } catch (Throwable e) {
                        Misc.free(vals);
                        throw e;
                    }
            }
        }

        if (runtimeConstCount + constCount == argCount) {
            final IntList positions = new IntList();
            positions.addAll(argPositions);
            return new InLongRuntimeConstFunction(args.getQuick(0), new ObjList<>(args), positions);
        }

        // have to copy, args is mutable
        return new InLongVarFunction(new ObjList<>(args));
    }

    /**
     * Frees the IN-list element functions (args past index 0). The all-constant forms read every
     * element into a primitive value or a set and keep only the key function, so nothing else ever
     * closes the elements: {@link io.questdb.griffin.FunctionParser} frees args on the error path
     * only, and on the success path the returned function owns what it retains. The elements used to
     * be leaf constants, but a constant IN element is now an unfolded overflowing arithmetic subtree
     * (see FunctionParser#functionToConstant0), i.e. a whole function tree to close. The runtime-const
     * and var forms retain the full arg list and close it themselves.
     */
    private static void freeElements(ObjList<Function> args) {
        for (int i = 1, n = args.size(); i < n; i++) {
            // Null each slot after closing it: a constant IN element can now be a whole arithmetic
            // function tree (see FunctionParser#functionToConstant0) holding native memory, so
            // nulling keeps any later pass over args from double-freeing it. The all-constant forms
            // keep only the key (args[0]), so the elements are dead here.
            args.setQuick(i, Misc.free(args.getQuick(i)));
        }
    }

    // Every element that reaches here is constant or runtime-constant, so an unparseable one is a
    // query error reported at the element's own position rather than a silent LONG_NULL that would
    // match the NULL rows. Only the per-row path (InLongVarFunction) parses quietly, because it
    // re-reads its elements for every row and has no position to report - which is exactly where
    // master drew the line.
    private static long parseLongElement(CharSequence seq, IntList argPositions, int i) throws SqlException {
        try {
            return Numbers.parseLong(seq, 0, seq.length());
        } catch (NumericException e) {
            throw SqlException.position(argPositions.getQuick(i)).put("invalid LONG value [").put(seq).put(']');
        }
    }

    private static void parseToSet(
            ObjList<Function> args,
            IntList argPositions,
            DirectLongHashSet outSet
    ) throws SqlException {
        for (int i = 1, n = args.size(); i < n; i++) {
            outSet.add(parseValue(argPositions, args.getQuick(i), i));
        }
    }

    private static long parseValue(IntList argPositions, Function func, int i) throws SqlException {
        long val;
        switch (ColumnType.tagOf(func.getType())) {
            case ColumnType.INT:
            case ColumnType.SHORT:
            case ColumnType.BYTE:
            case ColumnType.TIMESTAMP:
            case ColumnType.LONG:
                // A narrow-integer element carries one value: func.getLong() is
                // Numbers.intToLong(getInt()) for every INT/SHORT/BYTE function, so it wraps
                // overflowing INT arithmetic and maps INT_NULL to LONG_NULL exactly as '=' does.
                val = func.getLong(null);
                break;
            case ColumnType.STRING:
            case ColumnType.SYMBOL:
            case ColumnType.NULL:
                // A non-numeric element is a query error here, exactly as 'a = <not a long>' raises
                // ImplicitCastException. Reading it as LONG_NULL instead silently matched every NULL
                // row. This covers the all-constant list and the runtime-constant one (a bind
                // variable resolved at cursor open); both threw on master.
                CharSequence tsValue = func.getStrA(null);
                val = (tsValue != null) ? parseLongElement(tsValue, argPositions, i) : Numbers.LONG_NULL;
                break;
            case ColumnType.VARCHAR:
                Utf8Sequence seq = func.getVarcharA(null);
                val = (seq != null) ? parseLongElement(seq.asAsciiCharSequence(), argPositions, i) : Numbers.LONG_NULL;
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
        private final DirectLongHashSet set;
        private final Function tsFunc;

        public InLongConstFunction(Function tsFunc, DirectLongHashSet set) {
            this.tsFunc = tsFunc;
            this.set = set;
        }

        @Override
        public void close() {
            UnaryFunction.super.close();
            Misc.free(set);
        }

        @Override
        public Function getArg() {
            return tsFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != set.contains(tsFunc.getLong(rec));
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(tsFunc);
            if (negated) {
                sink.val(" not");
            }
            sink.val(" in ").val(set);
        }
    }

    private static class InLongRuntimeConstFunction extends NegatableBooleanFunction implements MultiArgFunction {
        private final Function keyFunc;
        private final DirectLongHashSet set;
        private final IntList valueFunctionPositions;
        private final ObjList<Function> valueFunctions;

        public InLongRuntimeConstFunction(
                Function keyFunc,
                ObjList<Function> valueFunctions,
                IntList valueFunctionPositions
        ) {
            this.keyFunc = keyFunc;
            // value functions also contain key function at 0 index.
            this.valueFunctions = valueFunctions;
            this.valueFunctionPositions = valueFunctionPositions;
            this.set = new DirectLongHashSet(valueFunctions.size() - 1, MemoryTag.NATIVE_FUNC_RSS);
        }

        @Override
        public ObjList<Function> args() {
            return valueFunctions;
        }

        @Override
        public void close() {
            MultiArgFunction.super.close();
            Misc.free(set);
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != set.contains(keyFunc.getLong(rec));
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            MultiArgFunction.super.init(symbolTableSource, executionContext);
            set.clear();
            parseToSet(valueFunctions, valueFunctionPositions, set);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(keyFunc);
            if (negated) {
                sink.val(" not");
            }
            sink.val(" in ").val(set);
        }
    }

    private static class InLongSingleConstFunction extends NegatableBooleanFunction implements UnaryFunction {
        private final long inVal;
        private final Function longFunc;

        public InLongSingleConstFunction(Function longFunc, long inVal) {
            this.longFunc = longFunc;
            this.inVal = inVal;
        }

        @Override
        public Function getArg() {
            return longFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated != (longFunc.getLong(rec) == inVal);
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
        private final Function longFunc;

        public InLongTwoConstFunction(Function longFunc, long inVal0, long inVal1) {
            this.longFunc = longFunc;
            this.inVal0 = inVal0;
            this.inVal1 = inVal1;
        }

        @Override
        public Function getArg() {
            return longFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            // Read the key once: both elements compare against it at the same width.
            final long val = longFunc.getLong(rec);
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

    /**
     * The mixed path: at least one element is neither constant nor runtime constant, so its value is
     * only known per row. The constructor hashes contiguous, same-width constant runs and records
     * each run at its source position. The per-row loop probes each run in expected O(1) and reads
     * dynamic elements using precomputed kind codes. Keeping runs in source order preserves the
     * short-circuit and error behavior of the original expression.
     */
    private static class InLongVarFunction extends NegatableBooleanFunction implements MultiArgFunction {
        private static final int KIND_CONST = 5;
        private static final int KIND_LONG = 1;
        private static final int KIND_NONE = 4;
        private static final int KIND_STR = 2;
        private static final int KIND_VARCHAR = 3;
        private final ObjList<Function> args;
        private final ObjList<DirectLongHashSet> constSets;
        private final LongList constValues;
        // Dynamic entries hold their args index. Constant entries hold -(run index + 1).
        private final IntList elementIndexes;
        private final IntList elementKinds;

        public InLongVarFunction(ObjList<Function> args) {
            this.args = args;
            final int n = args.size();
            // At most one run starts per IN element. Reserve every owner slot before allocating
            // native sets so constSets.add() cannot grow and strand a just-allocated set on OOM.
            this.constSets = new ObjList<>(n - 1);
            this.constValues = new LongList(n - 1);
            this.elementIndexes = new IntList(n - 1);
            this.elementKinds = new IntList(n - 1);
            DirectLongHashSet currentConstSet = null;
            int currentConstKind = -1;
            try {
                for (int i = 1; i < n; i++) {
                    final Function func = args.getQuick(i);
                    final int tag = ColumnType.tagOf(func.getType());
                    if (func.isConstant()) {
                        final long value = constElementValue(func, tag);
                        if (currentConstKind != KIND_CONST) {
                            currentConstSet = null;
                            constSets.add(null);
                            constValues.add(value);
                            elementIndexes.add(-constSets.size());
                            elementKinds.add(KIND_CONST);
                            currentConstKind = KIND_CONST;
                        } else {
                            if (currentConstSet == null) {
                                currentConstSet = new DirectLongHashSet(4, MemoryTag.NATIVE_FUNC_RSS);
                                currentConstSet.add(constValues.getLast());
                                constSets.setQuick(constSets.size() - 1, currentConstSet);
                            }
                            currentConstSet.add(value);
                        }
                        continue;
                    }

                    currentConstSet = null;
                    currentConstKind = -1;
                    elementIndexes.add(i);
                    elementKinds.add(dynamicElementKind(tag));
                }
            } catch (Throwable e) {
                Misc.freeObjList(constSets);
                throw e;
            }
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public void close() {
            MultiArgFunction.super.close();
            Misc.freeObjList(constSets);
        }

        @Override
        public boolean getBool(Record rec) {
            final Function keyFunc = args.getQuick(0);
            long key = 0;
            boolean hasKey = false;

            for (int i = 0, n = elementIndexes.size(); i < n; i++) {
                final int elementIndex = elementIndexes.getQuick(i);
                final int kind = elementKinds.getQuick(i);
                if (!hasKey) {
                    key = keyFunc.getLong(rec);
                    hasKey = true;
                }
                if (kind == KIND_CONST) {
                    final int runIndex = -elementIndex - 1;
                    final DirectLongHashSet set = constSets.getQuick(runIndex);
                    if (set != null ? set.contains(key) : constValues.getQuick(runIndex) == key) {
                        return !negated;
                    }
                    continue;
                }

                final Function func = args.getQuick(elementIndex);
                final long inVal;
                switch (kind) {
                    case KIND_LONG:
                        // A narrow-integer element reads through getLong() too: it is
                        // Numbers.intToLong(getInt()) for every INT/SHORT/BYTE function, so the
                        // element wraps overflowing INT arithmetic and maps INT_NULL to LONG_NULL.
                        inVal = func.getLong(rec);
                        break;
                    case KIND_VARCHAR:
                        Utf8Sequence seq = func.getVarcharA(rec);
                        inVal = Numbers.parseLongQuiet(seq == null ? null : seq.asAsciiCharSequence());
                        break;
                    case KIND_STR:
                        inVal = Numbers.parseLongQuiet(func.getStrA(rec));
                        break;
                    default:
                        inVal = Numbers.LONG_NULL;
                        break;
                }

                if (inVal == key) {
                    return !negated;
                }
            }
            return negated;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            MultiArgFunction.super.init(symbolTableSource, executionContext);
            // A non-constant element's type is only final once the call above has refreshed the
            // link functions: an indexed bind variable is legally UNDEFINED at compile time, and the
            // same factory is re-executed after a re-bind to a different type. Freezing the kinds in
            // the ctor sent a re-bound element down the wrong accessor - getStrA() on a LONG bind
            // throws, and a stale KIND_NONE silently compared every row against NULL instead.
            // Constant entries keep their ctor kinds: their types ARE final, and their values are
            // already partitioned into constSets by width.
            for (int i = 0, n = elementIndexes.size(); i < n; i++) {
                final int elementIndex = elementIndexes.getQuick(i);
                if (elementIndex >= 0) {
                    elementKinds.setQuick(i, dynamicElementKind(ColumnType.tagOf(args.getQuick(elementIndex).getType())));
                }
            }
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

        private static long constElementValue(Function func, int tag) {
            switch (tag) {
                case ColumnType.BYTE:
                case ColumnType.SHORT:
                case ColumnType.INT:
                case ColumnType.LONG:
                case ColumnType.TIMESTAMP:
                    return func.getLong(null);
                case ColumnType.VARCHAR:
                    Utf8Sequence seq = func.getVarcharA(null);
                    return Numbers.parseLongQuiet(seq == null ? null : seq.asAsciiCharSequence());
                default:
                    return Numbers.parseLongQuiet(func.getStrA(null));
            }
        }

        /**
         * Maps a non-constant element's type tag to the KIND_* the per-row dispatch reads. Shared by
         * the constructor and {@link #init}, which has to recompute it once the element types are
         * final.
         */
        private static int dynamicElementKind(int tag) {
            return switch (tag) {
                case ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG, ColumnType.TIMESTAMP ->
                        KIND_LONG;
                case ColumnType.VARCHAR -> KIND_VARCHAR;
                case ColumnType.STRING, ColumnType.SYMBOL -> KIND_STR;
                default -> KIND_NONE;
            };
        }
    }
}
