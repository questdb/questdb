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
        // When the key column (arg 0) is a narrow integer (INT/SHORT/BYTE) the IN
        // list is compared per element at the width of '=': an INT-typed element
        // (including an overflowing INT arithmetic fold), an untyped null, or a numeric
        // STRING whose value fits INT, is read at INT width so both key and element wrap
        // mod 2^32, exactly as EqInt and the JIT do, while a LONG/TIMESTAMP element (a
        // LONG-typed null among them) or a wider numeric string is read at long width so
        // the key widens (getLong) to its full value. For a LONG/TIMESTAMP key every
        // element widens to long anyway.
        final boolean isNarrowIntKey = isNarrowInt(ColumnType.tagOf(args.getQuick(0).getType()));
        // The two key widths only differ for the INT functions that override getLong() to compute
        // at long width - the arithmetic and bitwise operators, the conditionals that forward a
        // branch of one (CASE, COALESCE, NULLIF), and a runtime-const wrapper over one: those wrap
        // mod 2^32 under getInt() but keep the full value under getLong(), so the key has to be
        // read once per element width. Every other narrow key - a column, a cast, a constant, a
        // bind variable - reports isIntWidthStable(), and its two reads are the same number; then
        // one set holds every element and getBool probes it once per row.
        //
        // Splitting reads the key at both widths, which is only safe when the two reads carry
        // consistent values. A row-unstable key (e.g. rnd_int() + 0) breaks that: getInt() and
        // getLong() would draw two different random values for one row, and the row would be probed
        // against the two width sets with two unrelated keys. Treat such a key as non-split so
        // getBool reads it exactly once per row (at long width); every element then lands in the
        // long-width set. This is correct because a row-unstable INT key has no single stable value
        // to wrap anyway, and it matches how a width-stable key behaves.
        //
        // isRowStable, not !isNonDeterministic: the guard only cares whether the two reads land on
        // the same value within ONE row. Non-determinism asks whether two separate EXECUTIONS agree,
        // and it is true for a bind variable, whose value is fixed for the whole cursor. Reading that
        // signal sent every bind-variable key down the long-width-only path and reinstated the very
        // bug the split key exists to fix: (i*$1) IN (null) disagreed with (i*2) IN (null) and with
        // (i*$1) = null.
        final boolean isSplitKey = isNarrowIntKey
                && !args.getQuick(0).isIntWidthStable()
                && args.getQuick(0).isRowStable();
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
                    final long v = parseValue(argPositions, args.getQuick(1), 1, isNarrowIntKey);
                    final Function fn = new InLongSingleConstFunction(
                            args.getQuick(0),
                            v,
                            isIntWidthElement(args.getQuick(1), v, isSplitKey));
                    freeElements(args);
                    return fn;
                }
                case 2: {
                    final long v0 = parseValue(argPositions, args.getQuick(1), 1, isNarrowIntKey);
                    final long v1 = parseValue(argPositions, args.getQuick(2), 2, isNarrowIntKey);
                    final Function fn = new InLongTwoConstFunction(
                            args.getQuick(0),
                            v0,
                            v1,
                            isIntWidthElement(args.getQuick(1), v0, isSplitKey),
                            isIntWidthElement(args.getQuick(2), v1, isSplitKey)
                    );
                    freeElements(args);
                    return fn;
                }
                default:
                    // A split key needs an INT-width set for the elements it wraps against; every
                    // other key reads the same number at both widths, so the int set stays null and
                    // the long set holds the lot. Allocate both inside the try so a native OOM on
                    // the second set cannot leak the first, then drop whichever set stayed empty so
                    // getBool probes once per row on the common single-width list.
                    DirectLongHashSet intVals = null;
                    DirectLongHashSet longVals = null;
                    try {
                        if (isSplitKey) {
                            intVals = new DirectLongHashSet(argCount, MemoryTag.NATIVE_FUNC_RSS);
                        }
                        longVals = new DirectLongHashSet(argCount, MemoryTag.NATIVE_FUNC_RSS);
                        parseToSets(args, argPositions, intVals, longVals, isNarrowIntKey, isSplitKey);
                        if (intVals != null && intVals.size() == 0) {
                            intVals = Misc.free(intVals);
                        }
                        if (longVals.size() == 0) {
                            longVals = Misc.free(longVals);
                        }
                        final Function fn = new InLongConstFunction(args.getQuick(0), intVals, longVals);
                        freeElements(args);
                        return fn;
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
            return new InLongRuntimeConstFunction(args.getQuick(0), new ObjList<>(args), positions, isNarrowIntKey, isSplitKey);
        }

        // have to copy, args is mutable
        return new InLongVarFunction(new ObjList<>(args), isNarrowIntKey, isSplitKey);
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

    /**
     * Reports whether {@code val} fits the INT range excluding the INT_NULL
     * sentinel (Integer.MIN_VALUE), i.e. it would type as an INT literal and so
     * wrap a narrow-integer key mod 2^32 rather than widen it.
     */
    private static boolean isIntRangeValue(long val) {
        return val > Integer.MIN_VALUE && val <= Integer.MAX_VALUE;
    }

    /**
     * Reports whether the key must be read at INT width (wrapped) to compare against
     * {@code func}, an IN-list element with parsed value {@code parsedVal}: true when the key is
     * a split one - a narrow-integer key whose getInt() and getLong() can disagree - and the
     * element is INT-width (see {@link #isIntWidthTag}) or a numeric STRING/VARCHAR/SYMBOL whose
     * value fits INT. Both key and element then wrap mod 2^32, matching EqInt, IN of a numeric
     * literal, and the JIT.
     * <p>
     * False otherwise, and the key is read once at long width: either the element compares at
     * long width (a LONG/TIMESTAMP element, or a LONG-typed null, widens the key via getLong()),
     * or the key is not a split one and its two reads carry the same value anyway. The element
     * itself is still read at the width {@link #parseValue} picked for it, so an INT element
     * keeps wrapping.
     */
    private static boolean isIntWidthElement(Function func, long parsedVal, boolean isSplitKey) {
        if (!isSplitKey) {
            return false;
        }
        final int tag = ColumnType.tagOf(func.getType());
        if (isIntWidthTag(tag)) {
            return true;
        }
        // A numeric string has no declared integer width, so compare it at the width
        // its value would carry as a literal: an INT-range value wraps (matching
        // IN (intLiteral) and '='), a wider value or NULL widens (matching IN (longLiteral)).
        // A string that does not parse carries LONG_NULL, i.e. it IS null - so it has to compare
        // at the same width an untyped null does (INT), not at long width. Numbers.LONG_NULL is
        // Long.MIN_VALUE, which isIntRangeValue rejects, so it needs its own arm.
        return isNumericStringLike(tag) && (parsedVal == Numbers.LONG_NULL || isIntRangeValue(parsedVal));
    }

    /**
     * Reports whether an element of this type compares against a split narrow-integer key at INT
     * width. A BYTE/SHORT/INT element does, and so does an untyped {@code null}: '=' resolves it
     * to EqInt on a narrow key, so the key is NULL there exactly when its getInt() carries
     * INT_NULL - which is also when the projection of the key prints null. Probing the key at long
     * width instead would disagree with both, in both directions, for the one key whose two widths
     * can disagree about the sentinel (INT arithmetic): it would miss a key that wraps onto
     * INT_NULL (long width: +/-2^31, not LONG_NULL), and match a key whose long-width product
     * overflows exactly onto LONG_NULL while its value is not null. Since
     * {@code Numbers.intToLong(INT_NULL) == LONG_NULL}, the element's parsed LONG_NULL matches the
     * INT-width key read as-is, and a genuinely-null key still matches at either width.
     * <p>
     * A LONG-typed null ({@code null::long}, or a null LONG element) is NOT int-width: '=' resolves
     * it to EqLong, which reads the key with getLong(), so it keeps long width and both agree.
     * UNDEFINED covers an element whose type is not resolved; it carries no numeric value, so it
     * follows the untyped null.
     */
    private static boolean isIntWidthTag(int typeTag) {
        return isNarrowInt(typeTag) || typeTag == ColumnType.NULL || typeTag == ColumnType.UNDEFINED;
    }

    private static boolean isNarrowInt(int typeTag) {
        return typeTag == ColumnType.BYTE || typeTag == ColumnType.SHORT || typeTag == ColumnType.INT;
    }

    private static boolean isNumericStringLike(int typeTag) {
        return typeTag == ColumnType.STRING || typeTag == ColumnType.SYMBOL || typeTag == ColumnType.VARCHAR;
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

    private static void parseToSets(
            ObjList<Function> args,
            IntList argPositions,
            DirectLongHashSet outIntSet,
            DirectLongHashSet outLongSet,
            boolean isNarrowIntKey,
            boolean isSplitKey
    ) throws SqlException {
        for (int i = 1, n = args.size(); i < n; i++) {
            Function func = args.getQuick(i);
            long val = parseValue(argPositions, func, i, isNarrowIntKey);
            if (isIntWidthElement(func, val, isSplitKey)) {
                outIntSet.add(val);
            } else {
                outLongSet.add(val);
            }
        }
    }

    private static long parseValue(IntList argPositions, Function func, int i, boolean isNarrowIntKey) throws SqlException {
        long val;
        switch (ColumnType.tagOf(func.getType())) {
            case ColumnType.INT:
            case ColumnType.SHORT:
            case ColumnType.BYTE:
                // Match '=' on a narrow-integer key: read the element at INT width so an
                // overflowing INT arithmetic wraps (getInt) instead of widening (getLong).
                // intToLong preserves a genuine INT_NULL element as LONG_NULL.
                val = isNarrowIntKey ? Numbers.intToLong(func.getInt(null)) : func.getLong(null);
                break;
            case ColumnType.TIMESTAMP:
            case ColumnType.LONG:
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

    /**
     * Renders the IN value list for EXPLAIN. Merges the INT-width and long-width
     * sets into one sorted, de-duplicated list so the mixed-width case still
     * renders as a single {@code [...]} block regardless of how the elements were
     * partitioned by width. The merge sorts, matching the single-set branches
     * ({@code sink.val(intSet)} / {@code sink.val(longSet)}), which sort too
     * ({@link DirectLongHashSet#toSink(io.questdb.std.str.CharSink)}), so every
     * branch renders in ascending order and an EXPLAIN assertion over an IN list
     * must expect that order. A value present at BOTH widths (e.g.
     * {@code i IN (5, 5::long, 7)}) lands in both sets, so the merge drops the
     * adjacent duplicate to render it once.
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
        } else if (intSet != null) {
            // No long set: render the (possibly empty) int set.
            sink.val(intSet);
        } else {
            // Both sets absent. Unreachable today - the >=3-arg default arm always
            // fills at least one set - but render an empty list explicitly rather
            // than lean on the sink tolerating a null Sinkable, so a future change
            // to the set-partition invariant keeps rendering [] instead of nothing.
            sink.val("[]");
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
            boolean isFound = false;
            if (longSet != null) {
                isFound = longSet.contains(tsFunc.getLong(rec));
            }
            if (!isFound && intSet != null) {
                isFound = intSet.contains(Numbers.intToLong(tsFunc.getInt(rec)));
            }
            return negated != isFound;
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
        private final boolean isNarrowIntKey;
        private final boolean isSplitKey;
        private final Function keyFunc;
        private final DirectLongHashSet longSet;
        private final IntList valueFunctionPositions;
        private final ObjList<Function> valueFunctions;
        // Refreshed by init(): whether the matching set holds any element for this cursor.
        private boolean hasIntSet;
        private boolean hasLongSet;

        public InLongRuntimeConstFunction(
                Function keyFunc,
                ObjList<Function> valueFunctions,
                IntList valueFunctionPositions,
                boolean isNarrowIntKey,
                boolean isSplitKey
        ) {
            this.keyFunc = keyFunc;
            // value functions also contain key function at 0 index.
            this.valueFunctions = valueFunctions;
            this.valueFunctionPositions = valueFunctionPositions;
            this.isNarrowIntKey = isNarrowIntKey;
            this.isSplitKey = isSplitKey;
            // The int/long split is by element TYPE, but a runtime-constant element's type is only
            // final once init() has run - an indexed bind variable is legally UNDEFINED at compile
            // time, and the same factory is re-executed after a re-bind to a different type. So the
            // ctor cannot know which sets init() will fill: sizing them off the compile-time
            // snapshot left parseToSets adding to a null set. A split key routes elements to either
            // set by runtime type, so allocate both; every other key reads the same number at both
            // widths and only ever needs the long one. This mirrors the all-constant path above,
            // whose types ARE final. init() recomputes hasIntSet/hasLongSet from the post-parse
            // sizes, so an unused set costs nothing per row. Both allocations are guarded so a
            // native OOM on the second cannot leak the first.
            DirectLongHashSet intSet = null;
            DirectLongHashSet longSet = null;
            try {
                if (isSplitKey) {
                    intSet = new DirectLongHashSet(valueFunctions.size() - 1, MemoryTag.NATIVE_FUNC_RSS);
                }
                longSet = new DirectLongHashSet(valueFunctions.size() - 1, MemoryTag.NATIVE_FUNC_RSS);
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
            boolean isFound = false;
            if (hasLongSet) {
                isFound = longSet.contains(keyFunc.getLong(rec));
            }
            if (!isFound && hasIntSet) {
                isFound = intSet.contains(Numbers.intToLong(keyFunc.getInt(rec)));
            }
            return negated != isFound;
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
            parseToSets(valueFunctions, valueFunctionPositions, intSet, longSet, isNarrowIntKey, isSplitKey);
            // The ctor sizes the sets from the element TYPES, but parseToSets partitions by VALUE:
            // a numeric-string bind that lands in the INT range feeds the int set and a wider one
            // the long set, so a set allocated here can hold nothing for this cursor. Skip the
            // empty ones instead of probing them on every row.
            hasIntSet = intSet != null && intSet.size() > 0;
            hasLongSet = longSet != null && longSet.size() > 0;
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
        private final boolean isKeyReadInt;
        private final Function longFunc;

        public InLongSingleConstFunction(Function longFunc, long inVal, boolean isKeyReadInt) {
            this.longFunc = longFunc;
            this.inVal = inVal;
            this.isKeyReadInt = isKeyReadInt;
        }

        @Override
        public Function getArg() {
            return longFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            long val = isKeyReadInt ? Numbers.intToLong(longFunc.getInt(rec)) : longFunc.getLong(rec);
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
        // Per-element key read width; see InLongSingleConstFunction#isKeyReadInt.
        private final boolean isKeyRead0Int;
        private final boolean isKeyRead1Int;
        private final Function longFunc;

        public InLongTwoConstFunction(Function longFunc, long inVal0, long inVal1, boolean isKeyRead0Int, boolean isKeyRead1Int) {
            this.longFunc = longFunc;
            this.inVal0 = inVal0;
            this.inVal1 = inVal1;
            this.isKeyRead0Int = isKeyRead0Int;
            this.isKeyRead1Int = isKeyRead1Int;
        }

        @Override
        public Function getArg() {
            return longFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            final long val0 = isKeyRead0Int ? Numbers.intToLong(longFunc.getInt(rec)) : longFunc.getLong(rec);
            if (val0 == inVal0) {
                return !negated;
            }
            // Both elements read the key at the same width in the common case, so reuse val0 rather
            // than reading the key a second time. A split key only reaches the second read on a miss.
            final long val1 = isKeyRead1Int == isKeyRead0Int
                    ? val0
                    : (isKeyRead1Int ? Numbers.intToLong(longFunc.getInt(rec)) : longFunc.getLong(rec));
            return negated != (val1 == inVal1);
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
        private static final int KIND_CONST_INT = 5;
        private static final int KIND_CONST_LONG = 6;
        private static final int KIND_LONG = 1;
        private static final int KIND_NARROW_INT = 0;
        private static final int KIND_NONE = 4;
        private static final int KIND_STR = 2;
        private static final int KIND_VARCHAR = 3;
        private final ObjList<Function> args;
        private final ObjList<DirectLongHashSet> constSets;
        private final LongList constValues;
        // Dynamic entries hold their args index. Constant entries hold -(run index + 1).
        private final IntList elementIndexes;
        private final IntList elementKinds;
        // Compile-time snapshots of the KEY's width, unlike elementKinds, which init() refreshes.
        // They cannot be refreshed in isolation: the ctor already baked them into constValues and
        // the constSets width partitioning, so recomputing them would need those re-derived too.
        // Re-binding the KEY of "WHERE $1 IN (col, ...)" to a different width is therefore
        // unsupported - it reads the key through the old width's accessor. Reachable only from the
        // embedded API: every wire protocol reconciles parameter types and recompiles instead.
        private final boolean isNarrowIntKey;
        private final boolean isSplitKey;

        public InLongVarFunction(ObjList<Function> args, boolean isNarrowIntKey, boolean isSplitKey) {
            this.args = args;
            this.isNarrowIntKey = isNarrowIntKey;
            this.isSplitKey = isSplitKey;
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
                        final long value = constElementValue(func, tag, isNarrowIntKey);
                        final int constKind = isIntWidthElement(func, value, isSplitKey)
                                ? KIND_CONST_INT
                                : KIND_CONST_LONG;
                        if (currentConstKind != constKind) {
                            currentConstSet = null;
                            constSets.add(null);
                            constValues.add(value);
                            elementIndexes.add(-constSets.size());
                            elementKinds.add(constKind);
                            currentConstKind = constKind;
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
            long keyLong = 0;
            long keyInt = 0;
            boolean hasKeyLong = false;
            boolean hasKeyInt = false;

            for (int i = 0, n = elementIndexes.size(); i < n; i++) {
                final int elementIndex = elementIndexes.getQuick(i);
                final int kind = elementKinds.getQuick(i);
                if (kind == KIND_CONST_LONG) {
                    if (!hasKeyLong) {
                        keyLong = keyFunc.getLong(rec);
                        hasKeyLong = true;
                    }
                    final int runIndex = -elementIndex - 1;
                    final DirectLongHashSet set = constSets.getQuick(runIndex);
                    if (set != null ? set.contains(keyLong) : constValues.getQuick(runIndex) == keyLong) {
                        return !negated;
                    }
                    continue;
                }
                if (kind == KIND_CONST_INT) {
                    if (!hasKeyInt) {
                        keyInt = Numbers.intToLong(keyFunc.getInt(rec));
                        hasKeyInt = true;
                    }
                    final int runIndex = -elementIndex - 1;
                    final DirectLongHashSet set = constSets.getQuick(runIndex);
                    if (set != null ? set.contains(keyInt) : constValues.getQuick(runIndex) == keyInt) {
                        return !negated;
                    }
                    continue;
                }

                final Function func = args.getQuick(elementIndex);
                final long inVal;
                boolean isIntWidth = false;
                switch (kind) {
                    case KIND_NARROW_INT:
                        if (isNarrowIntKey) {
                            inVal = Numbers.intToLong(func.getInt(rec));
                            isIntWidth = true;
                        } else {
                            inVal = func.getLong(rec);
                        }
                        break;
                    case KIND_LONG:
                        inVal = func.getLong(rec);
                        break;
                    case KIND_VARCHAR:
                        Utf8Sequence seq = func.getVarcharA(rec);
                        inVal = Numbers.parseLongQuiet(seq == null ? null : seq.asAsciiCharSequence());
                        // A non-parsing or null string IS null, so it takes INT width like an
                        // untyped null (the KIND_NONE arm below) - see isIntWidthElement.
                        isIntWidth = inVal == Numbers.LONG_NULL || isIntRangeValue(inVal);
                        break;
                    case KIND_STR:
                        inVal = Numbers.parseLongQuiet(func.getStrA(rec));
                        isIntWidth = inVal == Numbers.LONG_NULL || isIntRangeValue(inVal);
                        break;
                    default:
                        inVal = Numbers.LONG_NULL;
                        isIntWidth = true;
                        break;
                }

                final long keyVal;
                if (isIntWidth && isSplitKey) {
                    if (!hasKeyInt) {
                        keyInt = Numbers.intToLong(keyFunc.getInt(rec));
                        hasKeyInt = true;
                    }
                    keyVal = keyInt;
                } else {
                    if (!hasKeyLong) {
                        keyLong = keyFunc.getLong(rec);
                        hasKeyLong = true;
                    }
                    keyVal = keyLong;
                }
                if (inVal == keyVal) {
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

        private static long constElementValue(Function func, int tag, boolean isNarrowIntKey) {
            switch (tag) {
                case ColumnType.BYTE:
                case ColumnType.SHORT:
                case ColumnType.INT:
                    return isNarrowIntKey ? Numbers.intToLong(func.getInt(null)) : func.getLong(null);
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
                case ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT -> KIND_NARROW_INT;
                case ColumnType.LONG, ColumnType.TIMESTAMP -> KIND_LONG;
                case ColumnType.VARCHAR -> KIND_VARCHAR;
                case ColumnType.STRING, ColumnType.SYMBOL -> KIND_STR;
                default -> KIND_NONE;
            };
        }
    }
}
