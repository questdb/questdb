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

package io.questdb.griffin.engine.functions;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

/**
 * Wraps a runtime-constant subtree of a fixed-width scalar type, evaluates it once per cursor in
 * {@link #init(SymbolTableSource, SqlExecutionContext)} and serves the cached primitive per row.
 * Without it, a subtree like {@code dateadd('d', -30, to_timezone(now(), 'Asia/Kolkata'))} -
 * runtime constant, yet not compile-time constant - re-runs the timezone lookup and date
 * arithmetic for every row.
 * <p>
 * One concrete subclass per foldable type (see {@link #newInstance(Function)}); each extends the
 * matching typed function base class (e.g. {@link LongFunction}) and overrides only that type's
 * native getter to return the cached value. The cross-type getters (e.g. reading a long-typed
 * subtree as a double, as numeric promotion does) are inherited from the base class and route
 * through the native getter, so every conversion - including NULL handling - matches what a real
 * function of that type would return. A flat hand-rolled getter table would have to reproduce all
 * of that by hand, which is exactly where a wrong-field read can sneak in. The one exception is
 * {@link IntRuntimeConstFunction#getLong}: an overflowing INT arithmetic arg wraps in getInt() but
 * widens in getLong(), so that subclass keeps the wrapped int from init() and fills the widened
 * long lazily on the first LONG-promoting read rather than re-deriving it from the wrapped int (see
 * its comments).
 * <p>
 * Transparent in plans ({@link #toPlan(PlanSink)} delegates to the argument).
 */
public interface RuntimeConstFunction extends UnaryFunction {

    /**
     * True if the function is worth folding: runtime constant (but not compile-time constant), of a
     * fixed-width type, and composite (performs per-row work). Trivial runtime-constant leaves
     * (bind variables, {@code now()}) already cache their value, so wrapping them only adds overhead.
     */
    static boolean isFoldable(Function function) {
        // O(1) gates first; the two recursive predicates (isConstant/isRuntimeConstant walk the whole
        // arg subtree) run last, so a non-composite or non-foldable-type arg never pays for them.
        return function != null
                && function.extendedOps() == null
                && !(function instanceof RuntimeConstFunction)
                && isComposite(function)
                && isFoldableType(function.getType())
                && !function.isConstant()
                && function.isRuntimeConstant();
    }

    static boolean isFoldableType(int type) {
        switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
            case ColumnType.SHORT:
            case ColumnType.CHAR:
            case ColumnType.INT:
            case ColumnType.LONG:
            case ColumnType.FLOAT:
            case ColumnType.DOUBLE:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
            case ColumnType.IPv4:
            case ColumnType.GEOBYTE:
            case ColumnType.GEOSHORT:
            case ColumnType.GEOINT:
            case ColumnType.GEOLONG:
            case ColumnType.UUID:
                return true;
            default:
                return false;
        }
    }

    /**
     * Builds the wrapper for {@code arg}'s type. The caller must have checked {@link #isFoldable}
     * first, so the type is always one of the cases below.
     */
    static RuntimeConstFunction newInstance(Function arg) {
        switch (ColumnType.tagOf(arg.getType())) {
            case ColumnType.BOOLEAN:
                return new BooleanRuntimeConstFunction(arg);
            case ColumnType.BYTE:
                return new ByteRuntimeConstFunction(arg);
            case ColumnType.SHORT:
                return new ShortRuntimeConstFunction(arg);
            case ColumnType.CHAR:
                return new CharRuntimeConstFunction(arg);
            case ColumnType.INT:
                return new IntRuntimeConstFunction(arg);
            case ColumnType.LONG:
                return new LongRuntimeConstFunction(arg);
            case ColumnType.FLOAT:
                return new FloatRuntimeConstFunction(arg);
            case ColumnType.DOUBLE:
                return new DoubleRuntimeConstFunction(arg);
            case ColumnType.DATE:
                return new DateRuntimeConstFunction(arg);
            case ColumnType.TIMESTAMP:
                return new TimestampRuntimeConstFunction(arg);
            case ColumnType.IPv4:
                return new IPv4RuntimeConstFunction(arg);
            case ColumnType.GEOBYTE:
                return new GeoByteRuntimeConstFunction(arg);
            case ColumnType.GEOSHORT:
                return new GeoShortRuntimeConstFunction(arg);
            case ColumnType.GEOINT:
                return new GeoIntRuntimeConstFunction(arg);
            case ColumnType.GEOLONG:
                return new GeoLongRuntimeConstFunction(arg);
            case ColumnType.UUID:
                return new UuidRuntimeConstFunction(arg);
            default:
                throw new UnsupportedOperationException("not a foldable type: " + ColumnType.nameOf(arg.getType()));
        }
    }

    // Correctness hinges on this gate: only a composite foldable-type function is ever wrapped, and the
    // wrapper trusts that function's isRuntimeConstant() report - it freezes the value at init() and
    // serves it per row. A composite function that read per-row data while reporting runtime constant
    // would be silently mis-folded, so this relies on the contract that QuestDB's per-row composite
    // functions (e.g. arithmetic over a column) correctly report isRuntimeConstant()==false. Trivial
    // runtime-constant leaves (bind variables, now()) are deliberately excluded by the composite check:
    // they already cache their value, so wrapping them would only add an indirection.
    private static boolean isComposite(Function function) {
        return function instanceof UnaryFunction
                || function instanceof BinaryFunction
                || function instanceof TernaryFunction
                || function instanceof QuaternaryFunction
                || function instanceof MultiArgFunction;
    }

    @Override
    default boolean isConstant() {
        return false;
    }

    @Override
    default boolean isRuntimeConstant() {
        return true;
    }

    @Override
    default boolean shouldMemoize() {
        // already cached per cursor; per-row memoization would be pointless
        return false;
    }

    @Override
    default void toPlan(PlanSink sink) {
        getArg().toPlan(sink); // stay transparent in plans
    }

    final class BooleanRuntimeConstFunction extends BooleanFunction implements RuntimeConstFunction {
        private final Function arg;
        private boolean value;

        BooleanRuntimeConstFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return value;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            value = arg.getBool(null);
        }
    }

    final class ByteRuntimeConstFunction extends ByteFunction implements RuntimeConstFunction {
        private final Function arg;
        private byte value;

        ByteRuntimeConstFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public byte getByte(Record rec) {
            return value;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            value = arg.getByte(null);
        }
    }

    final class CharRuntimeConstFunction extends CharFunction implements RuntimeConstFunction {
        private final Function arg;
        private char value;

        CharRuntimeConstFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public char getChar(Record rec) {
            return value;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            value = arg.getChar(null);
        }
    }

    final class DateRuntimeConstFunction extends DateFunction implements RuntimeConstFunction {
        private final Function arg;
        private long value;

        DateRuntimeConstFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getDate(Record rec) {
            return value;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            value = arg.getDate(null);
        }
    }

    final class DoubleRuntimeConstFunction extends DoubleFunction implements RuntimeConstFunction {
        private final Function arg;
        private double value;

        DoubleRuntimeConstFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public double getDouble(Record rec) {
            return value;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            value = arg.getDouble(null);
        }
    }

    final class FloatRuntimeConstFunction extends FloatFunction implements RuntimeConstFunction {
        private final Function arg;
        private float value;

        FloatRuntimeConstFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public float getFloat(Record rec) {
            return value;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            value = arg.getFloat(null);
        }
    }

    final class GeoByteRuntimeConstFunction extends GeoByteFunction implements RuntimeConstFunction {
        private final Function arg;
        private byte value;

        GeoByteRuntimeConstFunction(Function arg) {
            super(arg.getType());
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public byte getGeoByte(Record rec) {
            return value;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            value = arg.getGeoByte(null);
        }
    }

    final class GeoIntRuntimeConstFunction extends GeoIntFunction implements RuntimeConstFunction {
        private final Function arg;
        private int value;

        GeoIntRuntimeConstFunction(Function arg) {
            super(arg.getType());
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public int getGeoInt(Record rec) {
            return value;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            value = arg.getGeoInt(null);
        }
    }

    final class GeoLongRuntimeConstFunction extends GeoLongFunction implements RuntimeConstFunction {
        private final Function arg;
        private long value;

        GeoLongRuntimeConstFunction(Function arg) {
            super(arg.getType());
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getGeoLong(Record rec) {
            return value;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            value = arg.getGeoLong(null);
        }
    }

    final class GeoShortRuntimeConstFunction extends GeoShortFunction implements RuntimeConstFunction {
        private final Function arg;
        private short value;

        GeoShortRuntimeConstFunction(Function arg) {
            super(arg.getType());
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public short getGeoShort(Record rec) {
            return value;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            value = arg.getGeoShort(null);
        }
    }

    final class IPv4RuntimeConstFunction extends IPv4Function implements RuntimeConstFunction {
        private final Function arg;
        private int value;

        IPv4RuntimeConstFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public int getIPv4(Record rec) {
            return value;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            value = arg.getIPv4(null);
        }
    }

    final class IntRuntimeConstFunction extends IntFunction implements RuntimeConstFunction {
        private final Function arg;
        private long longValue;
        private boolean longValueComputed;
        private int value;

        IntRuntimeConstFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public int getInt(Record rec) {
            return value;
        }

        @Override
        public long getLong(Record rec) {
            // INT is the one foldable type whose getLong() is not a pure function of getInt():
            // an overflowing INT arithmetic arg (e.g. an INT*INT product) wraps mod 2^32 in
            // getInt() but widens to the full-width result in getLong(). For + - * the two agree
            // on their low 32 bits (a modular ring homomorphism), but division breaks even that:
            // (1000000 * 1000000) / 7 wraps to -103911424 under getInt() yet widens to
            // 142857142857 under getLong(), whose low 32 bits (1123222089) are a different
            // number. So the widened value cannot be derived from the cached int. Reading both
            // getters in init() would evaluate the composite subtree twice; instead init() reads
            // only getInt() and this getter fills the widened long lazily, the first time a
            // LONG-promoting context asks for it, then serves it for the remaining rows. The lazy
            // fill mutates state outside init(), so this subclass is not read thread-safe (see
            // isThreadSafe()) and runs on a per-worker copy.
            if (!longValueComputed) {
                longValue = arg.getLong(null);
                longValueComputed = true;
            }
            return longValue;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            // Evaluate the composite subtree exactly once, at its native INT width. getLong()
            // widens lazily on first use (see its comment), so only the rarer LONG-promoting read
            // pays a second evaluation while the common INT read stays single-evaluation. NULL
            // flows through unchanged: getInt() yields INT_NULL and getLong() yields LONG_NULL.
            value = arg.getInt(null);
            longValueComputed = false;
        }

        @Override
        public boolean isThreadSafe() {
            // getLong() fills longValue lazily, mutating state outside init(), so a single
            // instance cannot be shared across worker threads; each worker gets its own copy.
            return false;
        }
    }

    final class LongRuntimeConstFunction extends LongFunction implements RuntimeConstFunction {
        private final Function arg;
        private long value;

        LongRuntimeConstFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getLong(Record rec) {
            return value;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            value = arg.getLong(null);
        }
    }

    final class ShortRuntimeConstFunction extends ShortFunction implements RuntimeConstFunction {
        private final Function arg;
        private short value;

        ShortRuntimeConstFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public short getShort(Record rec) {
            return value;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            value = arg.getShort(null);
        }
    }

    final class TimestampRuntimeConstFunction extends TimestampFunction implements RuntimeConstFunction {
        private final Function arg;
        private long value;

        TimestampRuntimeConstFunction(Function arg) {
            super(arg.getType());
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getTimestamp(Record rec) {
            return value;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            value = arg.getTimestamp(null);
        }
    }

    final class UuidRuntimeConstFunction extends UuidFunction implements RuntimeConstFunction {
        private final Function arg;
        private long hi;
        private long lo;

        UuidRuntimeConstFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getLong128Hi(Record rec) {
            return hi;
        }

        @Override
        public long getLong128Lo(Record rec) {
            return lo;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            lo = arg.getLong128Lo(null);
            hi = arg.getLong128Hi(null);
        }
    }
}
