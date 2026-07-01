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
        // When the key column (arg 0) is a narrow integer (INT/SHORT/BYTE) the IN
        // list is compared per element at the width of '=': an INT-typed element
        // (including an overflowing INT arithmetic fold) is read at INT width so
        // both key and element wrap mod 2^32, exactly as EqInt and the JIT do,
        // while a LONG/TIMESTAMP element is read at long width so the key widens
        // (getLong) to its full value. A single flag cannot express this because
        // an overflowing INT arithmetic key wraps under getInt() but widens under
        // getLong(); the per-element width picks the correct key read for each
        // element. For a LONG/TIMESTAMP key every element widens to long anyway.
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
                            parseValue(argPositions, args.getQuick(1), 1, keyIsNarrowInt),
                            isIntWidthElement(args.getQuick(1), keyIsNarrowInt));
                case 2:
                    return new InLongTwoConstFunction(
                            args.getQuick(0),
                            parseValue(argPositions, args.getQuick(1), 1, keyIsNarrowInt),
                            parseValue(argPositions, args.getQuick(2), 2, keyIsNarrowInt),
                            isIntWidthElement(args.getQuick(1), keyIsNarrowInt),
                            isIntWidthElement(args.getQuick(2), keyIsNarrowInt)
                    );
                default:
                    // A narrow-int key needs an INT-width set for the elements the key wraps
                    // against; a LONG/TIMESTAMP key only ever widens, so the int set stays null.
                    // Allocate both inside the try so a native OOM on the second set cannot
                    // leak the first, then drop whichever set stayed empty so getBool probes
                    // once per row on the common single-width list.
                    DirectLongHashSet intVals = null;
                    DirectLongHashSet longVals = null;
                    try {
                        if (keyIsNarrowInt) {
                            intVals = new DirectLongHashSet(argCount, MemoryTag.NATIVE_FUNC_RSS);
                        }
                        longVals = new DirectLongHashSet(argCount, MemoryTag.NATIVE_FUNC_RSS);
                        parseToSets(args, argPositions, intVals, longVals, keyIsNarrowInt);
                        if (intVals != null && intVals.size() == 0) {
                            intVals = Misc.free(intVals);
                        }
                        if (longVals.size() == 0) {
                            longVals = Misc.free(longVals);
                        }
                        return new InLongConstFunction(args.getQuick(0), intVals, longVals);
                    } catch (Throwable e) {
                        Misc.free(intVals);
                        Misc.free(longVals);
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

    /**
     * Reports whether any IN-list element (args past index 0) is LONG-width
     * typed, i.e. not an INT/SHORT/BYTE literal, so a long-width set is needed.
     */
    private static boolean hasLongWidthElement(ObjList<Function> args) {
        for (int i = 1, n = args.size(); i < n; i++) {
            if (!isNarrowInt(ColumnType.tagOf(args.getQuick(i).getType()))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reports whether any IN-list element (args past index 0) is INT/SHORT/BYTE
     * typed, so an INT-width set is needed for a narrow-integer key.
     */
    private static boolean hasNarrowIntElement(ObjList<Function> args) {
        for (int i = 1, n = args.size(); i < n; i++) {
            if (isNarrowInt(ColumnType.tagOf(args.getQuick(i).getType()))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reports whether {@code func}, as an IN-list element, is compared at INT
     * width against the key: true only when the key is a narrow integer and the
     * element is itself INT/SHORT/BYTE-typed, in which case both key and element
     * wrap mod 2^32 (matching EqInt and the JIT). Otherwise the element is
     * compared at long width and the key widens via getLong().
     */
    private static boolean isIntWidthElement(Function func, boolean keyIsNarrowInt) {
        return keyIsNarrowInt && isNarrowInt(ColumnType.tagOf(func.getType()));
    }

    private static boolean isNarrowInt(int typeTag) {
        return typeTag == ColumnType.BYTE || typeTag == ColumnType.SHORT || typeTag == ColumnType.INT;
    }

    private static void parseToSets(
            ObjList<Function> args,
            IntList argPositions,
            DirectLongHashSet outIntSet,
            DirectLongHashSet outLongSet,
            boolean keyIsNarrowInt
    ) throws SqlException {
        for (int i = 1, n = args.size(); i < n; i++) {
            Function func = args.getQuick(i);
            long val = parseValue(argPositions, func, i, keyIsNarrowInt);
            if (isIntWidthElement(func, keyIsNarrowInt)) {
                outIntSet.add(val);
            } else {
                outLongSet.add(val);
            }
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

    /**
     * Renders the IN value list for EXPLAIN. Merges the INT-width and long-width
     * sets into one sorted, de-duplicated list so the output is a single
     * {@code [...]} block, byte-for-byte identical to the original single-set
     * plan regardless of how the elements were partitioned by width. A value
     * present at BOTH widths (e.g. {@code i IN (5, 5::long, 7)}) lands in both
     * sets, so the merge drops the adjacent duplicate to render it once, exactly
     * as a single hash set would.
     */
    private static void plan(PlanSink sink, DirectLongHashSet intSet, DirectLongHashSet longSet) {
        boolean hasInt = intSet != null && intSet.size() > 0;
        boolean hasLong = longSet != null && longSet.size() > 0;
        if (hasInt && hasLong) {
            LongList merged = new LongList(intSet.size() + longSet.size());
            intSet.copyTo(merged);
            longSet.copyTo(merged);
            merged.sort();
            // Drop adjacent duplicates so a value present at both widths renders once.
            int w = 0;
            for (int r = 0, n = merged.size(); r < n; r++) {
                long v = merged.getQuick(r);
                if (w == 0 || v != merged.getQuick(w - 1)) {
                    merged.setQuick(w++, v);
                }
            }
            merged.setPos(w);
            sink.val(merged);
        } else if (hasInt) {
            sink.val(intSet);
        } else if (longSet != null) {
            // A present long set, or an empty long set that renders as [].
            sink.val(longSet);
        } else {
            // No long set: render the (possibly empty) int set.
            sink.val(intSet);
        }
    }

    private static class InLongConstFunction extends NegatableBooleanFunction implements UnaryFunction {
        // Elements compared at INT width against a wrapped (getInt) narrow key;
        // null when no element feeds it (the key is not a narrow integer, or every
        // element is LONG/TIMESTAMP-typed).
        private final DirectLongHashSet intSet;
        // Elements compared at long width against the widened (getLong) key; null
        // when every element is INT-width against a narrow-integer key.
        private final DirectLongHashSet longSet;
        private final Function tsFunc;

        public InLongConstFunction(Function tsFunc, DirectLongHashSet intSet, DirectLongHashSet longSet) {
            this.tsFunc = tsFunc;
            this.intSet = intSet;
            this.longSet = longSet;
        }

        @Override
        public void close() {
            UnaryFunction.super.close();
            Misc.free(intSet);
            Misc.free(longSet);
        }

        @Override
        public Function getArg() {
            return tsFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            // The key widens (getLong) against long-width elements and wraps (getInt)
            // against INT-width elements. Each set is null when no element feeds its
            // width, so the common single-width list probes exactly once per row.
            boolean found = false;
            if (longSet != null) {
                found = longSet.contains(tsFunc.getLong(rec));
            }
            if (!found && intSet != null) {
                found = intSet.contains(Numbers.intToLong(tsFunc.getInt(rec)));
            }
            return negated != found;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(tsFunc);
            if (negated) {
                sink.val(" not");
            }
            sink.val(" in ");
            plan(sink, intSet, longSet);
        }
    }

    private static class InLongRuntimeConstFunction extends NegatableBooleanFunction implements MultiArgFunction {
        private final DirectLongHashSet intSet;
        private final Function keyFunc;
        private final boolean keyIsNarrowInt;
        private final DirectLongHashSet longSet;
        private final IntList valueFunctionPositions;
        private final ObjList<Function> valueFunctions;

        public InLongRuntimeConstFunction(Function keyFunc, ObjList<Function> valueFunctions, IntList valueFunctionPositions, boolean keyIsNarrowInt) {
            this.keyFunc = keyFunc;
            // value functions also contain key function at 0 index.
            this.valueFunctions = valueFunctions;
            this.valueFunctionPositions = valueFunctionPositions;
            this.keyIsNarrowInt = keyIsNarrowInt;
            // The int/long split is by element TYPE, so which sets are ever used is
            // fixed here (init() only refreshes their runtime-constant values).
            // Allocate only the sets an element feeds so getBool probes once on a
            // single-width list, and guard both allocations so a native OOM on the
            // second cannot leak the first.
            final boolean needIntSet = keyIsNarrowInt && hasNarrowIntElement(valueFunctions);
            final boolean needLongSet = !keyIsNarrowInt || hasLongWidthElement(valueFunctions);
            DirectLongHashSet intSet = null;
            DirectLongHashSet longSet = null;
            try {
                if (needIntSet) {
                    intSet = new DirectLongHashSet(valueFunctions.size() - 1, MemoryTag.NATIVE_FUNC_RSS);
                }
                if (needLongSet) {
                    longSet = new DirectLongHashSet(valueFunctions.size() - 1, MemoryTag.NATIVE_FUNC_RSS);
                }
            } catch (Throwable e) {
                Misc.free(intSet);
                Misc.free(longSet);
                throw e;
            }
            this.intSet = intSet;
            this.longSet = longSet;
        }

        @Override
        public ObjList<Function> args() {
            return valueFunctions;
        }

        @Override
        public void close() {
            MultiArgFunction.super.close();
            Misc.free(intSet);
            Misc.free(longSet);
        }

        @Override
        public boolean getBool(Record rec) {
            boolean found = false;
            if (longSet != null) {
                found = longSet.contains(keyFunc.getLong(rec));
            }
            if (!found && intSet != null) {
                found = intSet.contains(Numbers.intToLong(keyFunc.getInt(rec)));
            }
            return negated != found;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            MultiArgFunction.super.init(symbolTableSource, executionContext);
            if (longSet != null) {
                longSet.clear();
            }
            if (intSet != null) {
                intSet.clear();
            }
            parseToSets(valueFunctions, valueFunctionPositions, intSet, longSet, keyIsNarrowInt);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(keyFunc);
            if (negated) {
                sink.val(" not");
            }
            sink.val(" in ");
            plan(sink, intSet, longSet);
        }
    }

    private static class InLongSingleConstFunction extends NegatableBooleanFunction implements UnaryFunction {
        private final long inVal;
        // Read the key at INT width (wrap) against an INT-width element, else at
        // long width (widen). Int width is only ever set for a narrow-integer key.
        private final boolean keyReadInt;
        private final Function longFunc;

        public InLongSingleConstFunction(Function longFunc, long inVal, boolean keyReadInt) {
            this.longFunc = longFunc;
            this.inVal = inVal;
            this.keyReadInt = keyReadInt;
        }

        @Override
        public Function getArg() {
            return longFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            long val = keyReadInt ? Numbers.intToLong(longFunc.getInt(rec)) : longFunc.getLong(rec);
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
        // Per-element key read width; see InLongSingleConstFunction#keyReadInt.
        private final boolean keyRead0Int;
        private final boolean keyRead1Int;
        private final Function longFunc;

        public InLongTwoConstFunction(Function longFunc, long inVal0, long inVal1, boolean keyRead0Int, boolean keyRead1Int) {
            this.longFunc = longFunc;
            this.inVal0 = inVal0;
            this.inVal1 = inVal1;
            this.keyRead0Int = keyRead0Int;
            this.keyRead1Int = keyRead1Int;
        }

        @Override
        public Function getArg() {
            return longFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            final long val0 = keyRead0Int ? Numbers.intToLong(longFunc.getInt(rec)) : longFunc.getLong(rec);
            // Both elements read the key at the same width in the common case, so
            // reuse val0 rather than reading the key a second time.
            final long val1 = keyRead1Int == keyRead0Int
                    ? val0
                    : (keyRead1Int ? Numbers.intToLong(longFunc.getInt(rec)) : longFunc.getLong(rec));
            return negated != (val0 == inVal0 || val1 == inVal1);
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
            final Function keyFunc = args.getQuick(0);
            // The key is compared at long width against long-width elements and at
            // INT width (wrap) against INT-width elements; a narrow key wraps under
            // getInt while widening under getLong, so read it at both widths once.
            final long keyLong = keyFunc.getLong(rec);
            final long keyInt = keyIsNarrowInt ? Numbers.intToLong(keyFunc.getInt(rec)) : keyLong;

            for (int i = 1, n = args.size(); i < n; i++) {
                Function func = args.getQuick(i);
                long inVal = Numbers.LONG_NULL;
                long keyVal = keyLong;
                switch (ColumnType.tagOf(func.getType())) {
                    case ColumnType.BYTE:
                    case ColumnType.SHORT:
                    case ColumnType.INT:
                        // Match '=' on a narrow-integer key: read the element at INT width
                        // (wrap) rather than widening an overflowing INT arithmetic via getLong().
                        if (keyIsNarrowInt) {
                            inVal = Numbers.intToLong(func.getInt(rec));
                            keyVal = keyInt;
                        } else {
                            inVal = func.getLong(rec);
                        }
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
                if (inVal == keyVal) {
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
