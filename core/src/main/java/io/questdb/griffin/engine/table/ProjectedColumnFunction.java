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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.FunctionExtension;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

/**
 * A column of a projection over an Async Filter whose values the filtering threads computed into
 * the frames (see {@link FrameProjection}): a read through the projection cursor's own record,
 * of the column's own type, on a projected frame, returns the frame's value; every other read is
 * the projection's own function's, which this wraps and to which it forwards everything else, so
 * that plans, initialization and lifetimes are as without it.
 */
public class ProjectedColumnFunction implements Function {
    private final Function function;
    private final int index;
    private final FrameProjection projection;
    // the column type tag the frame serves, the function's own
    private final int servedTag;

    public ProjectedColumnFunction(@NotNull Function function, @NotNull FrameProjection projection, int index) {
        this.function = function;
        this.projection = projection;
        this.index = index;
        this.servedTag = ColumnType.tagOf(function.getType());
    }

    @Override
    public void assignType(int type, BindVariableService bindVariableService) throws SqlException {
        function.assignType(type, bindVariableService);
    }

    @Override
    public void clear() {
        function.clear();
    }

    @Override
    public void close() {
        function.close();
    }

    @Override
    public void cursorClosed() {
        function.cursorClosed();
    }

    @Override
    public FunctionExtension extendedOps() {
        return function.extendedOps();
    }

    @Override
    public ArrayView getArray(Record rec) {
        return function.getArray(rec);
    }

    @Override
    public BinarySequence getBin(Record rec) {
        return function.getBin(rec);
    }

    @Override
    public long getBinLen(Record rec) {
        return function.getBinLen(rec);
    }

    @Override
    public boolean getBool(Record rec) {
        if (servedTag == ColumnType.BOOLEAN && projection.isServed(rec)) {
            return projection.getBool(index);
        }
        return function.getBool(rec);
    }

    @Override
    public byte getByte(Record rec) {
        if (servedTag == ColumnType.BYTE && projection.isServed(rec)) {
            return projection.getByte(index);
        }
        return function.getByte(rec);
    }

    @Override
    public char getChar(Record rec) {
        if (servedTag == ColumnType.CHAR && projection.isServed(rec)) {
            return projection.getChar(index);
        }
        return function.getChar(rec);
    }

    @Override
    public int getComplexity() {
        return function.getComplexity();
    }

    @Override
    public long getDate(Record rec) {
        if (servedTag == ColumnType.DATE && projection.isServed(rec)) {
            return projection.getLong(index);
        }
        return function.getDate(rec);
    }

    @Override
    public void getDecimal128(Record rec, Decimal128 sink) {
        function.getDecimal128(rec, sink);
    }

    @Override
    public short getDecimal16(Record rec) {
        return function.getDecimal16(rec);
    }

    @Override
    public void getDecimal256(Record rec, Decimal256 sink) {
        function.getDecimal256(rec, sink);
    }

    @Override
    public int getDecimal32(Record rec) {
        return function.getDecimal32(rec);
    }

    @Override
    public long getDecimal64(Record rec) {
        return function.getDecimal64(rec);
    }

    @Override
    public byte getDecimal8(Record rec) {
        return function.getDecimal8(rec);
    }

    @Override
    public double getDouble(Record rec) {
        if (servedTag == ColumnType.DOUBLE && projection.isServed(rec)) {
            return projection.getDouble(index);
        }
        return function.getDouble(rec);
    }

    @Override
    public float getFloat(Record rec) {
        if (servedTag == ColumnType.FLOAT && projection.isServed(rec)) {
            return projection.getFloat(index);
        }
        return function.getFloat(rec);
    }

    /**
     * The address of the column's values from the current block's first row on, or 0 when the
     * block is not of a projected frame; see {@link FrameProjection#getBlockAddress(int)}.
     */
    public long getBlockAddress() {
        return projection.isBlockServed() ? projection.getBlockAddress(index) : 0;
    }

    /**
     * The projection's own function, which computes what the frame does not serve.
     */
    public Function getFunction() {
        return function;
    }

    @Override
    public byte getGeoByte(Record rec) {
        return function.getGeoByte(rec);
    }

    @Override
    public int getGeoInt(Record rec) {
        return function.getGeoInt(rec);
    }

    @Override
    public long getGeoLong(Record rec) {
        return function.getGeoLong(rec);
    }

    @Override
    public short getGeoShort(Record rec) {
        return function.getGeoShort(rec);
    }

    @Override
    public int getIPv4(Record rec) {
        return function.getIPv4(rec);
    }

    @Override
    public int getInt(Record rec) {
        if (servedTag == ColumnType.INT && projection.isServed(rec)) {
            return projection.getInt(index);
        }
        return function.getInt(rec);
    }

    @Override
    public Interval getInterval(Record rec) {
        return function.getInterval(rec);
    }

    @Override
    public long getLong(Record rec) {
        if (servedTag == ColumnType.LONG && projection.isServed(rec)) {
            return projection.getLong(index);
        }
        return function.getLong(rec);
    }

    @Override
    public long getLong128Hi(Record rec) {
        return function.getLong128Hi(rec);
    }

    @Override
    public long getLong128Lo(Record rec) {
        return function.getLong128Lo(rec);
    }

    @Override
    public void getLong256(Record rec, CharSink<?> sink) {
        function.getLong256(rec, sink);
    }

    @Override
    public Long256 getLong256A(Record rec) {
        return function.getLong256A(rec);
    }

    @Override
    public Long256 getLong256B(Record rec) {
        return function.getLong256B(rec);
    }

    @Override
    public RecordMetadata getMetadata() {
        return function.getMetadata();
    }

    @Override
    public String getName() {
        return function.getName();
    }

    @Override
    public RecordCursorFactory getRecordCursorFactory() {
        return function.getRecordCursorFactory();
    }

    @Override
    public short getShort(Record rec) {
        if (servedTag == ColumnType.SHORT && projection.isServed(rec)) {
            return projection.getShort(index);
        }
        return function.getShort(rec);
    }

    @Override
    public CharSequence getStrA(Record rec) {
        return function.getStrA(rec);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return function.getStrB(rec);
    }

    @Override
    public int getStrLen(Record rec) {
        return function.getStrLen(rec);
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return function.getSymbol(rec);
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        return function.getSymbolB(rec);
    }

    @Override
    public long getTimestamp(Record rec) {
        if (servedTag == ColumnType.TIMESTAMP && projection.isServed(rec)) {
            return projection.getLong(index);
        }
        return function.getTimestamp(rec);
    }

    @Override
    public int getType() {
        return function.getType();
    }

    @Override
    public Utf8Sequence getVarcharA(Record rec) {
        return function.getVarcharA(rec);
    }

    @Override
    public Utf8Sequence getVarcharB(Record rec) {
        return function.getVarcharB(rec);
    }

    @Override
    public int getVarcharSize(Record rec) {
        return function.getVarcharSize(rec);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        function.init(symbolTableSource, executionContext);
    }

    @Override
    public boolean isConstant() {
        return function.isConstant();
    }

    @Override
    public boolean isConstantOrRuntimeConstant() {
        return function.isConstantOrRuntimeConstant();
    }

    @Override
    public boolean isEquivalentTo(Function obj) {
        return function.isEquivalentTo(obj instanceof ProjectedColumnFunction p ? p.function : obj);
    }

    @Override
    public boolean isNonDeterministic() {
        return function.isNonDeterministic();
    }

    @Override
    public boolean isNullConstant() {
        return function.isNullConstant();
    }

    @Override
    public boolean isOperator() {
        return function.isOperator();
    }

    @Override
    public boolean isRandom() {
        return function.isRandom();
    }

    @Override
    public boolean isRuntimeConstant() {
        return function.isRuntimeConstant();
    }

    @Override
    public boolean isStableWithinExecution() {
        return function.isStableWithinExecution();
    }

    @Override
    public boolean isThreadSafe() {
        return function.isThreadSafe();
    }

    @Override
    public boolean isUndefined() {
        return function.isUndefined();
    }

    @Override
    public void offerStateTo(Function that) {
        function.offerStateTo(that instanceof ProjectedColumnFunction p ? p.function : that);
    }

    @Override
    public boolean shouldMemoize() {
        return function.shouldMemoize();
    }

    @Override
    public boolean supportsParallelism() {
        return function.supportsParallelism();
    }

    @Override
    public boolean supportsRandomAccess() {
        return function.supportsRandomAccess();
    }

    @Override
    public void toPlan(PlanSink sink) {
        function.toPlan(sink);
    }

    @Override
    public void toTop() {
        function.toTop();
    }
}
