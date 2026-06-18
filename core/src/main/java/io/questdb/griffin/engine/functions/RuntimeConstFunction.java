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
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

/**
 * Wraps a runtime-constant subtree of a fixed-width scalar type, evaluates it once per cursor in
 * {@link #init(SymbolTableSource, SqlExecutionContext)} and serves the cached primitive per row.
 * Without it, a subtree like {@code dateadd('d', -30, to_timezone(now(), 'Asia/Kolkata'))} -
 * runtime constant, yet not compile-time constant - re-runs the timezone lookup and date
 * arithmetic for every row.
 * <p>
 * Transparent in plans ({@link #toPlan(PlanSink)} delegates to the argument). Only fixed-width
 * types are folded (see {@link #isFoldableType(int)}); getters for other types delegate to the arg.
 */
public final class RuntimeConstFunction implements UnaryFunction {
    private final Function arg;
    private final int type;
    private double doubleValue;
    private long longValue;
    private long longValueHi;

    public RuntimeConstFunction(Function arg) {
        this.arg = arg;
        this.type = arg.getType();
    }

    /**
     * True if the function is worth folding: runtime constant (but not compile-time constant), of a
     * fixed-width type, and composite (performs per-row work). Trivial runtime-constant leaves
     * (bind variables, {@code now()}) already cache their value, so wrapping them only adds overhead.
     */
    public static boolean isFoldable(Function function) {
        return function != null
                && !function.isConstant()
                && function.isRuntimeConstant()
                && function.extendedOps() == null
                && !(function instanceof RuntimeConstFunction)
                && isComposite(function)
                && isFoldableType(function.getType());
    }

    public static boolean isFoldableType(int type) {
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

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public ArrayView getArray(Record rec) {
        return arg.getArray(rec);
    }

    @Override
    public BinarySequence getBin(Record rec) {
        return arg.getBin(rec);
    }

    @Override
    public long getBinLen(Record rec) {
        return arg.getBinLen(rec);
    }

    @Override
    public boolean getBool(Record rec) {
        return longValue != 0;
    }

    @Override
    public byte getByte(Record rec) {
        return (byte) longValue;
    }

    @Override
    public char getChar(Record rec) {
        return (char) longValue;
    }

    @Override
    public long getDate(Record rec) {
        return longValue;
    }

    @Override
    public void getDecimal128(Record rec, Decimal128 sink) {
        arg.getDecimal128(rec, sink);
    }

    @Override
    public short getDecimal16(Record rec) {
        return arg.getDecimal16(rec);
    }

    @Override
    public void getDecimal256(Record rec, Decimal256 sink) {
        arg.getDecimal256(rec, sink);
    }

    @Override
    public int getDecimal32(Record rec) {
        return arg.getDecimal32(rec);
    }

    @Override
    public long getDecimal64(Record rec) {
        return arg.getDecimal64(rec);
    }

    @Override
    public byte getDecimal8(Record rec) {
        return arg.getDecimal8(rec);
    }

    @Override
    public double getDouble(Record rec) {
        return doubleValue;
    }

    @Override
    public float getFloat(Record rec) {
        return (float) doubleValue;
    }

    @Override
    public byte getGeoByte(Record rec) {
        return (byte) longValue;
    }

    @Override
    public int getGeoInt(Record rec) {
        return (int) longValue;
    }

    @Override
    public long getGeoLong(Record rec) {
        return longValue;
    }

    @Override
    public short getGeoShort(Record rec) {
        return (short) longValue;
    }

    @Override
    public int getIPv4(Record rec) {
        return (int) longValue;
    }

    @Override
    public int getInt(Record rec) {
        return (int) longValue;
    }

    @Override
    public @NotNull Interval getInterval(Record rec) {
        return arg.getInterval(rec);
    }

    @Override
    public long getLong(Record rec) {
        return longValue;
    }

    @Override
    public long getLong128Hi(Record rec) {
        return longValueHi;
    }

    @Override
    public long getLong128Lo(Record rec) {
        return longValue;
    }

    @Override
    public void getLong256(Record rec, CharSink<?> sink) {
        arg.getLong256(rec, sink);
    }

    @Override
    public Long256 getLong256A(Record rec) {
        return arg.getLong256A(rec);
    }

    @Override
    public Long256 getLong256B(Record rec) {
        return arg.getLong256B(rec);
    }

    @Override
    public String getName() {
        return arg.getName();
    }

    @Override
    public RecordCursorFactory getRecordCursorFactory() {
        return arg.getRecordCursorFactory();
    }

    @Override
    public short getShort(Record rec) {
        return (short) longValue;
    }

    @Override
    public CharSequence getStrA(Record rec) {
        return arg.getStrA(rec);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return arg.getStrB(rec);
    }

    @Override
    public int getStrLen(Record rec) {
        return arg.getStrLen(rec);
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return arg.getSymbol(rec);
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        return arg.getSymbolB(rec);
    }

    @Override
    public long getTimestamp(Record rec) {
        return longValue;
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public Utf8Sequence getVarcharA(Record rec) {
        return arg.getVarcharA(rec);
    }

    @Override
    public Utf8Sequence getVarcharB(Record rec) {
        return arg.getVarcharB(rec);
    }

    @Override
    public int getVarcharSize(Record rec) {
        return arg.getVarcharSize(rec);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        arg.init(symbolTableSource, executionContext);
        // Runtime constant: after init() the value no longer depends on the record, so read it once.
        switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN:
                longValue = arg.getBool(null) ? 1 : 0;
                break;
            case ColumnType.BYTE:
                longValue = arg.getByte(null);
                break;
            case ColumnType.SHORT:
                longValue = arg.getShort(null);
                break;
            case ColumnType.CHAR:
                longValue = arg.getChar(null);
                break;
            case ColumnType.INT:
                longValue = arg.getInt(null);
                break;
            case ColumnType.LONG:
                longValue = arg.getLong(null);
                break;
            case ColumnType.FLOAT:
                doubleValue = arg.getFloat(null);
                break;
            case ColumnType.DOUBLE:
                doubleValue = arg.getDouble(null);
                break;
            case ColumnType.DATE:
                longValue = arg.getDate(null);
                break;
            case ColumnType.TIMESTAMP:
                longValue = arg.getTimestamp(null);
                break;
            case ColumnType.IPv4:
                longValue = arg.getIPv4(null);
                break;
            case ColumnType.GEOBYTE:
                longValue = arg.getGeoByte(null);
                break;
            case ColumnType.GEOSHORT:
                longValue = arg.getGeoShort(null);
                break;
            case ColumnType.GEOINT:
                longValue = arg.getGeoInt(null);
                break;
            case ColumnType.GEOLONG:
                longValue = arg.getGeoLong(null);
                break;
            case ColumnType.UUID:
                longValue = arg.getLong128Lo(null);
                longValueHi = arg.getLong128Hi(null);
                break;
            default:
                throw new UnsupportedOperationException("not a foldable type: " + ColumnType.nameOf(type));
        }
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public boolean shouldMemoize() {
        // already cached per cursor; per-row memoization would be pointless
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        arg.toPlan(sink); // stay transparent in plans
    }

    private static boolean isComposite(Function function) {
        return function instanceof UnaryFunction
                || function instanceof BinaryFunction
                || function instanceof TernaryFunction
                || function instanceof QuaternaryFunction
                || function instanceof MultiArgFunction;
    }
}
